const crypto = require("crypto");

const DEFAULT_TIMEOUT_MS = 10_000;
const DEFAULT_IMAGE_TIMEOUT_MS = 90_000;
const COMPANION_PATH = "/internal/channel/companion";
const RUNTIME_CONFIG_PATH = "/internal/channel/runtime-config";

/**
 * Phase 2C-C1：可配置的真实 Hearth companion 客户端。
 * 复用 Phase 2B seam：同一 request(event) 形状，测试可注入 fetchImpl。
 */
function createHearthBridgeClient({
  env = process.env,
  fetchImpl = fetch,
  timeoutMs,
} = {}) {
  const resolvedUrl = resolveCompanionUrl(env);
  const token = String(env.HEARTH_INTERNAL_TOKEN || "").trim();
  const pseudonymSecret = String(env.HEARTH_BRIDGE_PSEUDONYM_SECRET || "").trim();
  const resolvedTimeoutMs = resolveTimeoutMs(env, timeoutMs);
  const resolvedImageTimeoutMs = resolveImageTimeoutMs(env);

  if (!resolvedUrl) {
    throw Object.assign(new Error("Hearth companion URL is not configured"), { code: "config_url_missing" });
  }
  if (!token) {
    throw Object.assign(new Error("Hearth internal token is not configured"), { code: "config_token_missing" });
  }
  if (!pseudonymSecret) {
    throw Object.assign(new Error("Hearth pseudonym secret is not configured"), { code: "config_secret_missing" });
  }

  const url = new URL(resolvedUrl);
  assertLocalHttpCompanionUrl(url);

  return {
    url: url.toString(),
    timeoutMs: resolvedTimeoutMs,
    imageTimeoutMs: resolvedImageTimeoutMs,
    async getRuntimeConfig() {
      const configUrl = new URL(url.toString());
      configUrl.pathname = RUNTIME_CONFIG_PATH;
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), resolvedTimeoutMs);
      try {
        const response = await fetchImpl(configUrl, {
          method: "GET",
          headers: { Authorization: `Bearer ${token}` },
          signal: controller.signal,
        });
        if (!response.ok) {
          throw Object.assign(new Error("Bridge runtime config failure"), {
            code: classifyHttpStatus(response.status),
          });
        }
        const body = await response.json();
        return validateRuntimeConfig(body);
      } finally {
        clearTimeout(timer);
      }
    },
    async request(event) {
      const controller = new AbortController();
      const envelope = buildChannelEnvelope(event, pseudonymSecret);
      // 普通文字仍走原来的 10 秒短窗；只有图片轮允许视觉助理戴眼镜慢一点。
      // 这样不会为了几张照片，把每句“好呀”也养成九十秒长跑运动员。
      const requestTimeoutMs = Array.isArray(envelope.images) && envelope.images.length
        ? resolvedImageTimeoutMs
        : resolvedTimeoutMs;
      const timer = setTimeout(() => controller.abort(), requestTimeoutMs);
      try {
        const response = await fetchImpl(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify(envelope),
          signal: controller.signal,
        });
        if (!response.ok) {
          // 只按状态码归类；绝不把上游错误正文带出厨房。
          throw Object.assign(new Error("Bridge HTTP failure"), {
            code: classifyHttpStatus(response.status),
            httpStatus: response.status,
          });
        }
        let body;
        try {
          body = await response.json();
        } catch {
          throw Object.assign(new Error("Bridge invalid JSON"), { code: "invalid_json" });
        }
        return validateBridgeResponse(body);
      } catch (error) {
        if (error?.name === "AbortError") {
          throw Object.assign(new Error("Bridge timeout"), { code: "timeout" });
        }
        throw error;
      } finally {
        clearTimeout(timer);
      }
    },
  };
}

function resolveCompanionUrl(env = process.env) {
  // 优先完整 companion URL；兼容 Phase 2B 的 base URL。
  const explicit = String(env.HEARTH_COMPANION_URL || "").trim();
  if (explicit) {
    return normalizeCompanionUrl(explicit);
  }
  const base = String(env.HEARTH_BRIDGE_URL || "").trim();
  if (!base) return "";
  return normalizeCompanionUrl(base);
}

function normalizeCompanionUrl(raw) {
  const text = String(raw || "").trim();
  if (!text) return "";
  const url = new URL(text);
  const pathname = url.pathname.replace(/\/+$/, "") || "";
  if (!pathname || pathname === "/") {
    url.pathname = COMPANION_PATH;
  } else if (!pathname.endsWith(COMPANION_PATH)) {
    // base-only 形态：允许 http://127.0.0.1:8000 → 自动补路径
    url.pathname = `${pathname}${COMPANION_PATH}`.replace(/\/{2,}/g, "/");
  }
  return url.toString();
}

function assertLocalHttpCompanionUrl(url) {
  if (url.protocol !== "http:") {
    throw Object.assign(new Error("Bridge URL must use local HTTP"), { code: "config_url_invalid" });
  }
  if (!["127.0.0.1", "localhost"].includes(url.hostname)) {
    throw Object.assign(new Error("Bridge URL must be an explicit local HTTP endpoint"), { code: "config_url_invalid" });
  }
  const path = url.pathname.replace(/\/+$/, "");
  if (path !== COMPANION_PATH) {
    throw Object.assign(new Error("Bridge URL path must end with /internal/channel/companion"), { code: "config_url_invalid" });
  }
}

function resolveTimeoutMs(env, explicitTimeoutMs) {
  if (Number.isFinite(Number(explicitTimeoutMs)) && Number(explicitTimeoutMs) > 0) {
    return Math.floor(Number(explicitTimeoutMs));
  }
  const fromEnv = Number(env.HEARTH_REQUEST_TIMEOUT_MS);
  if (Number.isFinite(fromEnv) && fromEnv > 0) {
    return Math.floor(fromEnv);
  }
  return DEFAULT_TIMEOUT_MS;
}

function resolveImageTimeoutMs(env = process.env) {
  const fromEnv = Number(env.HEARTH_IMAGE_REQUEST_TIMEOUT_MS);
  if (Number.isFinite(fromEnv) && fromEnv > 0) {
    return Math.floor(fromEnv);
  }
  return DEFAULT_IMAGE_TIMEOUT_MS;
}

function classifyHttpStatus(status) {
  const code = Number(status);
  if (code === 401 || code === 403) return "internal_auth";
  if (code === 409) return "duplicate_or_processing";
  if (code === 413) return "payload_too_large";
  if (code === 429) return "rate_limit";
  if (code === 502 || code === 503) return "hearth_unavailable";
  return `http_${code}`;
}

function pseudonym(value, secret, label) {
  return `${label}_${crypto.createHmac("sha256", secret).update(String(value || "")).digest("hex").slice(0, 24)}`;
}

function buildChannelEnvelope(event, secret) {
  const accountId = pseudonym(event.account_id, secret, "wa");
  const senderId = pseudonym(event.sender_id, secret, "wu");
  // context_token / 原始微信 payload / 时间戳正文拼接：全部禁止进 Envelope。
  // client_id：只对发往 Hearth 的 raw_metadata 做 HMAC 伪名（wcid_ + 24hex ≈ 29 字），
  // 绝不截断原值、绝不明文透传；空值则省略字段（不生成空伪名）。
  // Sidecar 本地幂等键仍用原始 client_id，本函数碰不到那一层。
  const rawMetadata = {
    seq: String(event.seq || ""),
  };
  const clientIdRaw = String(event.client_id || "").trim();
  if (clientIdRaw) {
    rawMetadata.client_id = pseudonym(clientIdRaw, secret, "wcid");
  }
  const images = [];
  for (const item of Array.isArray(event.images) ? event.images : []) {
    const mediaType = String(item?.media_type || item?.mediaType || "").trim().toLowerCase();
    const data = String(item?.data || "").trim();
    if (!mediaType || !data) continue;
    images.push({ media_type: mediaType, data });
  }
  const envelope = {
    channel: "weixin",
    account_id: accountId,
    sender_id: senderId,
    chat_id: pseudonym(`${event.account_id}:${event.sender_id}`, secret, "wc"),
    thread_key: `weixin:${accountId}:${senderId}`,
    message_id: String(event.message_id || ""),
    text: String(event.text || ""),
    event_time: new Date(event.event_time).toISOString(),
    bridge_received_at: event.bridge_received_at,
    raw_metadata: rawMetadata,
    bubble_count: Math.max(1, Math.min(8, Number(event.bubble_count) || 1)),
  };
  const bubbleTexts = Array.isArray(event.bubble_texts)
    ? event.bubble_texts.slice(0, 8).map((value) => String(value || "").trim())
    : [];
  const bubbleMessageIds = Array.isArray(event.bubble_message_ids)
    ? event.bubble_message_ids.slice(0, 8).map((value) => String(value || "").trim()).filter(Boolean)
    : [];
  if (bubbleTexts.length) envelope.bubble_texts = bubbleTexts;
  if (bubbleMessageIds.length) envelope.bubble_message_ids = bubbleMessageIds;
  if (event.logical_turn_id) envelope.logical_turn_id = String(event.logical_turn_id);
  if (event.event_time_start) envelope.event_time_start = new Date(event.event_time_start).toISOString();
  if (event.event_time_end) envelope.event_time_end = new Date(event.event_time_end).toISOString();
  if (images.length) {
    envelope.images = images;
  }
  return envelope;
}

function validateRuntimeConfig(body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw Object.assign(new Error("Bridge runtime config invalid"), { code: "invalid_schema" });
  }
  const seconds = Number(body.inbound_buffer_seconds);
  const bubbleMode = String(body.bubble_mode || "").trim().toLowerCase();
  if (!Number.isInteger(seconds) || seconds < 0 || seconds > 15) {
    throw Object.assign(new Error("Bridge runtime config invalid"), { code: "invalid_schema" });
  }
  if (!["single", "natural", "murmur"].includes(bubbleMode)) {
    throw Object.assign(new Error("Bridge runtime config invalid"), { code: "invalid_schema" });
  }
  return {
    inboundBufferSeconds: seconds,
    bubbleMode,
  };
}

function validateBridgeResponse(body) {
  if (!body || typeof body !== "object" || Array.isArray(body) || body.action !== "send") {
    throw Object.assign(new Error("Bridge response schema invalid"), { code: "invalid_schema" });
  }

  const replyTextRaw = typeof body.reply_text === "string" ? body.reply_text.trim() : "";
  let segments;
  if (Array.isArray(body.segments)) {
    // 优先非空 segments；空数组时再回退 reply_text。
    segments = body.segments.map((value) => String(value || "").trim()).filter(Boolean);
    if (!segments.length && replyTextRaw) {
      segments = [replyTextRaw];
    }
  } else if (replyTextRaw) {
    segments = [replyTextRaw];
  } else {
    segments = [];
  }

  if (!segments.length) {
    throw Object.assign(new Error("Bridge empty reply"), { code: "empty_reply" });
  }

  return {
    requestId: typeof body.request_id === "string" ? body.request_id : "",
    replyText: replyTextRaw || segments.join("\n"),
    segments,
    gatewayProcessedAt: typeof body.gateway_processed_at === "string" ? body.gateway_processed_at : "",
  };
}

/**
 * 测试级 dry-run delivery sink：只记录顺序，不碰真实 sendmessage，
 * 也不生成 assistant_sent_at / delivered。
 */
function createDryRunDeliverySink() {
  const calls = [];
  return {
    calls,
    async deliverSegments(segments) {
      const list = Array.isArray(segments) ? segments.map((value) => String(value || "")) : [];
      calls.push({ atMs: Date.now(), segments: list });
      return { delivered: false, dryRun: true, segmentCount: list.length };
    },
  };
}

module.exports = {
  COMPANION_PATH,
  RUNTIME_CONFIG_PATH,
  assertLocalHttpCompanionUrl,
  buildChannelEnvelope,
  classifyHttpStatus,
  createDryRunDeliverySink,
  createHearthBridgeClient,
  normalizeCompanionUrl,
  pseudonym,
  resolveCompanionUrl,
  resolveImageTimeoutMs,
  resolveTimeoutMs,
  validateBridgeResponse,
  validateRuntimeConfig,
};
