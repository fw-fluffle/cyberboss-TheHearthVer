/**
 * Active Line v0 —— Hearth 微信主动出站「本地控制面」
 *
 * 设计哲学（狐狸工头版）：
 * - 必须挂在 channel-only 同一进程里，蹭已经登录好的 adapter / token cache，
 *   绝不开第二套微信登录去抢单实例锁（那是赛博修罗场）。
 * - 只监听 127.0.0.1，默认 3200；端口被占就光明正大地炸，绝不偷偷改公网或随机端口。
 * - 鉴权用独立的 HEARTH_WEIXIN_CONTROL_TOKEN，严禁复用 Hearth / Dashboard / Supabase 钥匙。
 * - 日志只报 request_id / source / 长度 / 状态，正文和 token 统统闭嘴。
 */

const http = require("http");
const { resolvePreferredSenderId } = require("../core/default-targets");

const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_PORT = 3200;
const DEFAULT_DEDUP_TTL_MS = 15 * 60 * 1000;
const MAX_TEXT_LENGTH = 3800;
const MAX_BODY_BYTES = 64 * 1024;
const MAX_REQUEST_ID_LENGTH = 128;

/**
 * 进程内 request_id TTL 去重器。
 * - inflight：正在飞往微信，并发同 id 直接 duplicate_request
 * - sent / failed：TTL 内继续拒绝，避免人工手抖双发
 * - 进程重启后 Map 清空，这是 MVP 可接受行为（文档里写清楚）
 */
function createProactiveSendDeduper({
  ttlMs = DEFAULT_DEDUP_TTL_MS,
  now = () => Date.now(),
} = {}) {
  const entries = new Map();

  function purgeExpired(at = now()) {
    for (const [requestId, entry] of entries) {
      if (!entry || entry.expiresAt <= at) {
        entries.delete(requestId);
      }
    }
  }

  function begin(requestId) {
    const at = now();
    purgeExpired(at);
    const existing = entries.get(requestId);
    if (existing && existing.expiresAt > at) {
      return { accepted: false, status: "duplicate_request", state: existing.state };
    }
    entries.set(requestId, { state: "inflight", expiresAt: at + ttlMs });
    return { accepted: true, status: "inflight", state: "inflight" };
  }

  function complete(requestId, state) {
    const at = now();
    const previous = entries.get(requestId);
    entries.set(requestId, {
      state,
      expiresAt: previous?.expiresAt && previous.expiresAt > at ? previous.expiresAt : at + ttlMs,
    });
  }

  function size() {
    purgeExpired();
    return entries.size;
  }

  return { begin, complete, purgeExpired, size, entries };
}

function parseControlPlaneConfig(env = process.env) {
  const host = DEFAULT_HOST;
  const rawPort = env.HEARTH_WEIXIN_CONTROL_PORT;
  const port = rawPort == null || String(rawPort).trim() === ""
    ? DEFAULT_PORT
    : Number(String(rawPort).trim());
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("HEARTH_WEIXIN_CONTROL_PORT must be an integer between 1 and 65535");
  }

  const controlToken = String(env.HEARTH_WEIXIN_CONTROL_TOKEN || "").trim();
  if (!controlToken) {
    throw new Error("HEARTH_WEIXIN_CONTROL_TOKEN is required for the proactive send control plane");
  }

  const rawTtl = env.HEARTH_WEIXIN_CONTROL_DEDUP_TTL_MS;
  const ttlMs = rawTtl == null || String(rawTtl).trim() === ""
    ? DEFAULT_DEDUP_TTL_MS
    : Number(String(rawTtl).trim());
  if (!Number.isFinite(ttlMs) || ttlMs < 1000) {
    throw new Error("HEARTH_WEIXIN_CONTROL_DEDUP_TTL_MS must be >= 1000 when set");
  }

  return { host, port, controlToken, ttlMs };
}

function extractBearerToken(authorizationHeader) {
  const raw = typeof authorizationHeader === "string" ? authorizationHeader.trim() : "";
  if (!raw) return "";
  const match = /^Bearer\s+(.+)$/i.exec(raw);
  return match ? String(match[1] || "").trim() : "";
}

function readJsonBody(req, { maxBytes = MAX_BODY_BYTES } = {}) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    let settled = false;

    const fail = (error) => {
      if (settled) return;
      settled = true;
      reject(error);
    };

    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > maxBytes) {
        fail(Object.assign(new Error("request body too large"), { code: "body_too_large" }));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      if (settled) return;
      settled = true;
      const raw = Buffer.concat(chunks).toString("utf8").trim();
      if (!raw) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(raw));
      } catch {
        fail(Object.assign(new Error("invalid json"), { code: "invalid_json" }));
      }
    });
    req.on("error", fail);
  });
}

function httpStatusForBusinessStatus(status) {
  switch (status) {
    case "sent":
      return 200;
    case "unauthorized":
      return 401;
    case "invalid_request":
      return 400;
    case "sidecar_not_ready":
    case "channel_state_unavailable":
      return 503;
    case "sender_not_found":
    case "context_token_missing":
      return 409;
    case "duplicate_request":
      return 409;
    case "weixin_send_failed":
      return 502;
    default:
      return 500;
  }
}

function resolveHasContextToken({ adapter, config, allowedUserIds }) {
  if (!adapter || typeof adapter.getKnownContextTokens !== "function") {
    return false;
  }
  let accountId = "";
  try {
    accountId = adapter.resolveAccount?.()?.accountId || "";
  } catch {
    accountId = "";
  }
  const preferredAllowed = resolveAllowedUserIds({ config, allowedUserIds });
  const senderId = resolvePreferredSenderId({
    config: {
      ...config,
      allowedUserIds: preferredAllowed.length ? preferredAllowed : config?.allowedUserIds,
    },
    accountId,
  });
  if (!senderId) return false;
  const tokens = adapter.getKnownContextTokens() || {};
  const token = typeof tokens[senderId] === "string" ? tokens[senderId].trim() : "";
  return Boolean(token);
}

function handleChannelStateGet({
  req,
  res,
  controlToken,
  adapter,
  config,
  allowedUserIds,
  channelStateStore,
  isReady = () => true,
  logger = console,
}) {
  const presented = extractBearerToken(req.headers?.authorization);
  if (!presented || presented !== controlToken) {
    writeJson(res, httpStatusForBusinessStatus("unauthorized"), {
      ok: false,
      status: "unauthorized",
    });
    logControlSafe(logger, {
      type: "hearth_weixin_channel_state",
      status: "unauthorized",
    });
    return;
  }

  if (!channelStateStore || typeof channelStateStore.getSnapshot !== "function") {
    writeJson(res, httpStatusForBusinessStatus("channel_state_unavailable"), {
      ok: false,
      status: "channel_state_unavailable",
    });
    logControlSafe(logger, {
      type: "hearth_weixin_channel_state",
      status: "channel_state_unavailable",
    });
    return;
  }

  const snapshot = channelStateStore.getSnapshot() || {};
  const ready = Boolean(isReady());
  const hasContextToken = resolveHasContextToken({ adapter, config, allowedUserIds });
  const body = {
    ok: true,
    ready,
    has_context_token: hasContextToken,
    last_user_event_time: snapshot.last_user_event_time || null,
    last_bridge_received_at: snapshot.last_bridge_received_at || null,
    last_message_id: snapshot.last_message_id || null,
    updated_at: snapshot.updated_at || null,
  };
  writeJson(res, 200, body);
  logControlSafe(logger, {
    type: "hearth_weixin_channel_state",
    status: "ok",
    ready,
    has_context_token: hasContextToken,
    has_last_user_event_time: Boolean(snapshot.last_user_event_time),
  });
}

function writeJson(res, httpStatus, body) {
  const payload = JSON.stringify(body);
  res.writeHead(httpStatus, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(payload),
  });
  res.end(payload);
}

function logControlSafe(logger, payload) {
  const line = JSON.stringify(payload);
  if (typeof logger?.info === "function") logger.info(line);
  else if (typeof logger?.log === "function") logger.log(line);
}

function resolveAllowedUserIds({ config, allowedUserIds }) {
  if (Array.isArray(allowedUserIds) && allowedUserIds.length) {
    return allowedUserIds.map((value) => String(value || "").trim()).filter(Boolean);
  }
  if (allowedUserIds instanceof Set && allowedUserIds.size) {
    return Array.from(allowedUserIds).map((value) => String(value || "").trim()).filter(Boolean);
  }
  if (Array.isArray(config?.allowedUserIds)) {
    return config.allowedUserIds.map((value) => String(value || "").trim()).filter(Boolean);
  }
  return [];
}

function createProactiveSendRequestHandler({
  adapter,
  config,
  controlToken,
  allowedUserIds,
  channelStateStore = null,
  deduper,
  isReady = () => true,
  now = () => Date.now(),
  logger = console,
} = {}) {
  if (!adapter || typeof adapter.sendText !== "function") {
    throw new Error("proactive send handler requires adapter.sendText");
  }
  if (!controlToken) {
    throw new Error("proactive send handler requires controlToken");
  }
  const dedupe = deduper || createProactiveSendDeduper({ now });

  return async function handleProactiveSendRequest(req, res) {
    const method = String(req.method || "").toUpperCase();
    const url = new URL(req.url || "/", "http://127.0.0.1");

    // Active Line v0-C1：只读 channel state，复用同一把 localhost 钥匙。
    if (method === "GET" && url.pathname === "/internal/channel-state") {
      handleChannelStateGet({
        req,
        res,
        controlToken,
        adapter,
        config,
        allowedUserIds,
        channelStateStore,
        isReady,
        logger,
      });
      return;
    }

    if (method !== "POST" || url.pathname !== "/internal/send") {
      writeJson(res, 404, { ok: false, status: "invalid_request", error: "not_found" });
      return;
    }

    const presented = extractBearerToken(req.headers?.authorization);
    if (!presented || presented !== controlToken) {
      writeJson(res, httpStatusForBusinessStatus("unauthorized"), {
        ok: false,
        status: "unauthorized",
      });
      logControlSafe(logger, {
        type: "hearth_weixin_proactive_send",
        status: "unauthorized",
        target: "unknown",
        text_length: 0,
      });
      return;
    }

    let body;
    try {
      body = await readJsonBody(req);
    } catch (error) {
      writeJson(res, httpStatusForBusinessStatus("invalid_request"), {
        ok: false,
        status: "invalid_request",
        error: error?.code || "invalid_body",
      });
      return;
    }

    const requestId = typeof body?.request_id === "string" ? body.request_id.trim() : "";
    const target = typeof body?.target === "string" ? body.target.trim() : "";
    const text = typeof body?.text === "string" ? body.text : "";
    const source = typeof body?.source === "string" ? body.source.trim() : "";
    const textLength = Buffer.byteLength(text, "utf8");

    const baseLog = {
      type: "hearth_weixin_proactive_send",
      request_id: requestId || undefined,
      source: source || undefined,
      text_length: textLength,
      target: target || "unknown",
    };

    if (!requestId || requestId.length > MAX_REQUEST_ID_LENGTH) {
      writeJson(res, httpStatusForBusinessStatus("invalid_request"), {
        ok: false,
        status: "invalid_request",
        error: "request_id_required",
      });
      logControlSafe(logger, { ...baseLog, status: "invalid_request" });
      return;
    }

    if (!text.trim() || textLength > MAX_TEXT_LENGTH) {
      writeJson(res, httpStatusForBusinessStatus("invalid_request"), {
        ok: false,
        status: "invalid_request",
        request_id: requestId,
        error: "text_invalid",
      });
      logControlSafe(logger, { ...baseLog, status: "invalid_request" });
      return;
    }

    if (target !== "default_owner") {
      writeJson(res, httpStatusForBusinessStatus("invalid_request"), {
        ok: false,
        status: "invalid_request",
        request_id: requestId,
        error: "target_unsupported",
      });
      logControlSafe(logger, { ...baseLog, status: "invalid_request" });
      return;
    }

    // 基本校验通过后才占坑，避免无效请求污染去重表。
    const reservation = dedupe.begin(requestId);
    if (!reservation.accepted) {
      writeJson(res, httpStatusForBusinessStatus("duplicate_request"), {
        ok: false,
        status: "duplicate_request",
        request_id: requestId,
      });
      logControlSafe(logger, { ...baseLog, status: "duplicate_request" });
      return;
    }

    try {
      if (!isReady()) {
        dedupe.complete(requestId, "failed");
        writeJson(res, httpStatusForBusinessStatus("sidecar_not_ready"), {
          ok: false,
          status: "sidecar_not_ready",
          request_id: requestId,
        });
        logControlSafe(logger, { ...baseLog, status: "sidecar_not_ready" });
        return;
      }

      let account;
      try {
        account = typeof adapter.resolveAccount === "function" ? adapter.resolveAccount() : null;
      } catch {
        account = null;
      }
      if (!account?.accountId) {
        dedupe.complete(requestId, "failed");
        writeJson(res, httpStatusForBusinessStatus("sidecar_not_ready"), {
          ok: false,
          status: "sidecar_not_ready",
          request_id: requestId,
        });
        logControlSafe(logger, { ...baseLog, status: "sidecar_not_ready" });
        return;
      }

      const preferredAllowed = resolveAllowedUserIds({ config, allowedUserIds });
      const senderId = resolvePreferredSenderId({
        config: {
          ...config,
          allowedUserIds: preferredAllowed.length ? preferredAllowed : config?.allowedUserIds,
        },
        accountId: account.accountId,
      });
      if (!senderId) {
        dedupe.complete(requestId, "failed");
        writeJson(res, httpStatusForBusinessStatus("sender_not_found"), {
          ok: false,
          status: "sender_not_found",
          request_id: requestId,
        });
        logControlSafe(logger, { ...baseLog, status: "sender_not_found" });
        return;
      }

      let contextToken = "";
      if (typeof adapter.getKnownContextTokens === "function") {
        const cache = adapter.getKnownContextTokens() || {};
        contextToken = typeof cache[senderId] === "string" ? cache[senderId].trim() : "";
      }
      if (!contextToken) {
        dedupe.complete(requestId, "failed");
        writeJson(res, httpStatusForBusinessStatus("context_token_missing"), {
          ok: false,
          status: "context_token_missing",
          request_id: requestId,
        });
        logControlSafe(logger, { ...baseLog, status: "context_token_missing" });
        return;
      }

      try {
        await adapter.sendText({
          userId: senderId,
          text,
          contextToken,
        });
      } catch {
        dedupe.complete(requestId, "failed");
        writeJson(res, httpStatusForBusinessStatus("weixin_send_failed"), {
          ok: false,
          status: "weixin_send_failed",
          request_id: requestId,
        });
        logControlSafe(logger, { ...baseLog, status: "weixin_send_failed" });
        return;
      }

      // 只有底层 sendText 明确成功，才签发“小狼敲到窗边”的时间戳。
      const assistantSentAt = new Date(now()).toISOString();
      dedupe.complete(requestId, "sent");
      writeJson(res, httpStatusForBusinessStatus("sent"), {
        ok: true,
        status: "sent",
        request_id: requestId,
        assistant_sent_at: assistantSentAt,
      });
      logControlSafe(logger, {
        ...baseLog,
        status: "sent",
        assistant_sent_at: assistantSentAt,
      });
    } catch {
      dedupe.complete(requestId, "failed");
      writeJson(res, httpStatusForBusinessStatus("weixin_send_failed"), {
        ok: false,
        status: "weixin_send_failed",
        request_id: requestId,
      });
      logControlSafe(logger, { ...baseLog, status: "weixin_send_failed" });
    }
  };
}

/**
 * 启动本地控制面。listen 固定 127.0.0.1；端口冲突直接 reject，绝不改绑。
 */
function startHearthProactiveSendServer({
  adapter,
  config,
  env = process.env,
  allowedUserIds,
  channelStateStore = null,
  isReady,
  deduper,
  now = () => Date.now(),
  logger = console,
  createServer = http.createServer,
} = {}) {
  const plane = parseControlPlaneConfig(env);
  const handler = createProactiveSendRequestHandler({
    adapter,
    config,
    controlToken: plane.controlToken,
    allowedUserIds,
    channelStateStore,
    deduper: deduper || createProactiveSendDeduper({ ttlMs: plane.ttlMs, now }),
    isReady,
    now,
    logger,
  });

  const server = createServer((req, res) => {
    Promise.resolve(handler(req, res)).catch(() => {
      if (!res.headersSent) {
        writeJson(res, 500, { ok: false, status: "weixin_send_failed" });
      }
    });
  });

  const listening = new Promise((resolve, reject) => {
    const onError = (error) => {
      server.off("listening", onListening);
      reject(error);
    };
    const onListening = () => {
      server.off("error", onError);
      resolve();
    };
    server.once("error", onError);
    server.once("listening", onListening);
    // 明确失败：EADDRINUSE 原样抛出，调用方不得自动改端口或改成 0.0.0.0。
    server.listen(plane.port, plane.host);
  });

  async function close() {
    await new Promise((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  }

  return {
    host: plane.host,
    port: plane.port,
    server,
    ready: listening,
    close,
  };
}

module.exports = {
  DEFAULT_HOST,
  DEFAULT_PORT,
  DEFAULT_DEDUP_TTL_MS,
  MAX_TEXT_LENGTH,
  createProactiveSendDeduper,
  parseControlPlaneConfig,
  extractBearerToken,
  createProactiveSendRequestHandler,
  startHearthProactiveSendServer,
  httpStatusForBusinessStatus,
  resolveHasContextToken,
  handleChannelStateGet,
};
