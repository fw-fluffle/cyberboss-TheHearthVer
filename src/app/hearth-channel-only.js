const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { readConfig } = require("../core/config");
const { ChannelIdempotencyStore, hashIdempotencyKey } = require("../core/channel-idempotency-store");
const { createHearthBridgeClient, createDryRunDeliverySink } = require("../core/hearth-bridge-client");
const { startHearthProactiveSendServer } = require("../server/hearth-proactive-send-server");

const DEFAULT_LONG_POLL_TIMEOUT_MS = 35_000;
const RETRY_DELAY_MS = 2_000;
const BACKOFF_DELAY_MS = 30_000;
const MAX_CONSECUTIVE_FAILURES = 3;

function acquireSingleInstanceLock({
  filePath,
  pid = process.pid,
  now = () => Date.now(),
  isProcessAlive = defaultIsProcessAlive,
} = {}) {
  if (!filePath) {
    throw new Error("Channel-only instance lock requires filePath");
  }
  fs.mkdirSync(path.dirname(filePath), { recursive: true });

  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const descriptor = fs.openSync(filePath, "wx", 0o600);
      try {
        fs.writeFileSync(descriptor, JSON.stringify({ pid, startedAtMs: now() }), "utf8");
      } finally {
        fs.closeSync(descriptor);
      }
      try {
        fs.chmodSync(filePath, 0o600);
      } catch {
        // Best effort on Windows and restrictive filesystems.
      }

      let released = false;
      return {
        release() {
          if (released) return;
          released = true;
          try {
            const current = JSON.parse(fs.readFileSync(filePath, "utf8"));
            if (Number(current?.pid) === Number(pid)) {
              fs.unlinkSync(filePath);
            }
          } catch {
            // Never remove a lock that cannot be verified as ours.
          }
        },
      };
    } catch (error) {
      if (error?.code !== "EEXIST") throw error;
      const holderPid = readLockPid(filePath);
      if (holderPid > 0 && isProcessAlive(holderPid)) {
        throw new Error("Another hearth channel-only sidecar is already running");
      }
      try {
        fs.unlinkSync(filePath);
      } catch (unlinkError) {
        if (unlinkError?.code !== "ENOENT") throw unlinkError;
      }
    }
  }
  throw new Error("Could not acquire hearth channel-only instance lock");
}

function readLockPid(filePath) {
  try {
    const parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));
    const pid = Number(parsed?.pid);
    return Number.isSafeInteger(pid) && pid > 0 ? pid : 0;
  } catch {
    return 0;
  }
}

function defaultIsProcessAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error?.code === "EPERM";
  }
}

function parseObserveConfig(env = process.env) {
  const mode = String(env.HEARTH_WEIXIN_MODE || "observe").trim().toLowerCase() || "observe";
  // bridge：真实投递（仍可注入 dry-run delivery）；bridge_dry_run：契约接线专用，不写 delivered。
  if (!["observe", "echo", "bridge", "bridge_dry_run"].includes(mode)) {
    throw new Error("HEARTH_WEIXIN_MODE only supports observe, echo, bridge or bridge_dry_run");
  }
  const allowedUserIds = new Set(
    String(env.HEARTH_WEIXIN_ALLOWED_USER_IDS || "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean),
  );
  return { mode, allowedUserIds, isBridge: mode === "bridge" || mode === "bridge_dry_run", dryRun: mode === "bridge_dry_run" };
}

function createHearthChannelOnlyObserver({
  adapter,
  config,
  env = process.env,
  idempotencyStore,
  bridgeClient,
  delivery,
  logger = console,
  now = () => Date.now(),
} = {}) {
  if (!adapter) {
    throw new Error("Hearth channel-only observer requires a channel adapter");
  }
  const observeConfig = parseObserveConfig(env);
  const bridge = observeConfig.isBridge ? (bridgeClient || createHearthBridgeClient({ env })) : null;
  const dryRunSink = observeConfig.dryRun ? (delivery || createDryRunDeliverySink()) : null;
  const deliver = delivery || null;
  const ledger = idempotencyStore || new ChannelIdempotencyStore({
    filePath: path.join(config?.stateDir || process.cwd(), "hearth-channel-idempotency.json"),
    now,
    onWarning: (message) => logSafe(logger, { type: "hearth_weixin_observe", status: "warning", message }),
  });
  // 进程内 inflight：同 key 并发第二请求不得再次打 Hearth。
  const inflightKeys = new Set();

  async function processRawMessage(rawMessage, account) {
    const bridgeReceivedAtMs = now();
    const bridgeReceivedAt = new Date(bridgeReceivedAtMs).toISOString();
    const metadata = buildRawMetadata(rawMessage, account?.accountId, bridgeReceivedAtMs, bridgeReceivedAt);

    if (!metadata.sender_id) {
      logObserve(logger, metadata, "malformed");
      return { status: "malformed", event: null };
    }
    if (!observeConfig.allowedUserIds.has(metadata.sender_id)) {
      logObserve(logger, metadata, "denied");
      return { status: "denied", event: null };
    }

    const idempotencyKey = hashIdempotencyKey([
      metadata.account_id,
      metadata.sender_id,
      metadata.message_id,
      metadata.seq,
      metadata.event_time,
      metadata.client_id,
    ]);
    if (ledger.has(idempotencyKey)) {
      logObserve(logger, metadata, "duplicate");
      return { status: "duplicate", event: null };
    }
    if (inflightKeys.has(idempotencyKey)) {
      logObserve(logger, metadata, "processing");
      return { status: "processing", event: null };
    }

    let normalized;
    try {
      normalized = adapter.normalizeIncomingMessage(rawMessage);
    } catch {
      logObserve(logger, metadata, "malformed");
      return { status: "malformed", event: null };
    }
    if (!normalized) {
      logObserve(logger, metadata, "malformed");
      return { status: "malformed", event: null };
    }

    const event = {
      ...metadata,
      has_text: Boolean(String(normalized.text || "").trim()),
      has_attachments: Array.isArray(normalized.attachments) && normalized.attachments.length > 0,
      text: String(normalized.text || ""),
    };
    if (observeConfig.mode === "echo") {
      await adapter.sendTyping({
        userId: metadata.sender_id,
        status: 1,
        contextToken: normalized.contextToken,
      }).catch((error) => {
        logSafe(logger, {
          type: "hearth_weixin_echo",
          status: "typing_failed",
          error_type: safeErrorType(error),
        });
      });
      try {
        await adapter.sendText({
          userId: metadata.sender_id,
          text: "bridge ok",
          contextToken: normalized.contextToken,
        });
      } catch (error) {
        logSafe(logger, {
          type: "hearth_weixin_echo",
          status: "echo_failed",
          error_type: safeErrorType(error),
        });
        return { status: "echo_failed", event: null };
      }
      ledger.mark(idempotencyKey, { status: "echoed", recordedAtMs: bridgeReceivedAtMs });
      logObserve(logger, event, "echoed");
      return { status: "echoed", event };
    }

    if (observeConfig.isBridge) {
      inflightKeys.add(idempotencyKey);
      try {
        await adapter.sendTyping({ userId: metadata.sender_id, status: 1, contextToken: normalized.contextToken }).catch((error) => {
          logSafe(logger, { type: "hearth_weixin_bridge", status: "typing_failed", error_type: safeErrorType(error) });
        });
        let response;
        try {
          response = await bridge.request(event);
        } catch (error) {
          const code = safeErrorCode(error);
          logSafe(logger, {
            type: "hearth_weixin_bridge",
            status: "bridge_failed",
            error_type: safeErrorType(error),
            error_code: code,
          });
          // Gateway 409：视为已处理，落 ledger 防重复调用；不投递、不写 assistant_sent_at。
          if (code === "duplicate_or_processing") {
            ledger.mark(idempotencyKey, { status: "bridge_duplicate", recordedAtMs: bridgeReceivedAtMs });
            logObserve(logger, event, "bridge_duplicate");
            return { status: "bridge_duplicate", event: null };
          }
          return { status: "bridge_failed", event: null };
        }

        try {
          if (observeConfig.dryRun) {
            await dryRunSink.deliverSegments(response.segments);
          } else if (deliver && typeof deliver.deliverSegments === "function") {
            await deliver.deliverSegments(response.segments, {
              userId: metadata.sender_id,
              contextToken: normalized.contextToken,
            });
          } else {
            for (const segment of response.segments) {
              await adapter.sendText({
                userId: metadata.sender_id,
                text: segment,
                contextToken: normalized.contextToken,
              });
            }
          }
        } catch (error) {
          logSafe(logger, { type: "hearth_weixin_bridge", status: "delivery_failed", error_type: safeErrorType(error) });
          return { status: "delivery_failed", event: null };
        }

        if (observeConfig.dryRun) {
          // Dry-run：只记契约成功，绝不冒充真实微信 delivered / assistant_sent_at。
          ledger.mark(idempotencyKey, { status: "bridged_dry_run", recordedAtMs: bridgeReceivedAtMs });
          logObserve(logger, event, "bridged_dry_run");
          return {
            status: "bridged_dry_run",
            event,
            segments: response.segments,
            request_id: response.requestId || "",
            gateway_processed_at: response.gatewayProcessedAt || "",
          };
        }

        const assistantSentAt = new Date(now()).toISOString();
        ledger.mark(idempotencyKey, { status: "delivered", recordedAtMs: Date.parse(assistantSentAt) });
        logObserve(logger, event, "delivered");
        return { status: "delivered", event: { ...event, assistant_sent_at: assistantSentAt } };
      } finally {
        inflightKeys.delete(idempotencyKey);
      }
    }

    ledger.mark(idempotencyKey, { status: "observed", recordedAtMs: bridgeReceivedAtMs });
    logObserve(logger, event, "observed");
    return { status: "observed", event };
  }

  async function processUpdateResponse(response, account) {
    const messages = sortRawMessages(Array.isArray(response?.msgs) ? response.msgs : [], now());
    const results = [];
    for (const message of messages) {
      try {
        results.push(await processRawMessage(message, account));
      } catch {
        const fallback = buildRawMetadata(message, account?.accountId, now(), new Date(now()).toISOString());
        logObserve(logger, fallback, "malformed");
        results.push({ status: "malformed", event: null });
      }
    }
    return results;
  }

  return {
    mode: observeConfig.mode,
    allowedUserIds: observeConfig.allowedUserIds,
    dryRun: observeConfig.dryRun,
    dryRunSink,
    processRawMessage,
    processUpdateResponse,
  };
}

function buildRawMetadata(message, accountId, bridgeReceivedAtMs, bridgeReceivedAt) {
  const eventTime = resolveEventTime(message, bridgeReceivedAtMs);
  return {
    account_id: cleanText(accountId),
    sender_id: cleanText(message?.from_user_id),
    message_id: cleanText(message?.message_id),
    seq: normalizeOrderValue(message?.seq),
    client_id: cleanText(message?.client_id),
    create_time_ms: normalizePositiveNumber(message?.create_time_ms),
    create_time: normalizePositiveNumber(message?.create_time),
    event_time: eventTime.value,
    event_time_source: eventTime.source,
    bridge_received_at: bridgeReceivedAt,
  };
}

function resolveEventTime(message, bridgeReceivedAtMs) {
  const milliseconds = normalizePositiveNumber(message?.create_time_ms);
  if (milliseconds) {
    return { value: milliseconds, source: "create_time_ms" };
  }
  const seconds = normalizePositiveNumber(message?.create_time);
  if (seconds) {
    return { value: seconds * 1000, source: "create_time" };
  }
  return { value: normalizePositiveNumber(bridgeReceivedAtMs) || Date.now(), source: "fallback" };
}

function sortRawMessages(messages, fallbackTimeMs = Date.now()) {
  return Array.isArray(messages)
    ? messages.slice().sort((left, right) => compareRawMessages(left, right, fallbackTimeMs))
    : [];
}

function compareRawMessages(left, right, fallbackTimeMs) {
  const leftTime = resolveEventTime(left, fallbackTimeMs).value;
  const rightTime = resolveEventTime(right, fallbackTimeMs).value;
  if (leftTime !== rightTime) return leftTime - rightTime;
  const leftSeq = normalizeOrderValue(left?.seq);
  const rightSeq = normalizeOrderValue(right?.seq);
  if (leftSeq !== rightSeq) return leftSeq - rightSeq;
  const leftMessageId = normalizeOrderValue(left?.message_id);
  const rightMessageId = normalizeOrderValue(right?.message_id);
  if (leftMessageId !== rightMessageId) return leftMessageId - rightMessageId;
  return cleanText(left?.client_id).localeCompare(cleanText(right?.client_id));
}

function logObserve(logger, event, status) {
  logSafe(logger, {
    type: "hearth_weixin_observe",
    status,
    bridge_received_at: event.bridge_received_at,
    event_time: event.event_time,
    event_time_source: event.event_time_source,
    message_id: event.message_id,
    seq: event.seq,
    client_id_present: Boolean(event.client_id),
    account_id_hash: shortHash(event.account_id),
    sender_id_hash: shortHash(event.sender_id),
    has_text: Boolean(event.has_text),
    has_attachments: Boolean(event.has_attachments),
  });
}

function logSafe(logger, payload) {
  const line = JSON.stringify(payload);
  if (typeof logger?.info === "function") logger.info(line);
  else if (typeof logger?.log === "function") logger.log(line);
}

function shortHash(value) {
  const text = cleanText(value);
  return text ? crypto.createHash("sha256").update(text).digest("hex").slice(0, 12) : "";
}

function safeErrorType(error) {
  const name = error instanceof Error ? error.name : typeof error;
  return /^[A-Za-z][A-Za-z0-9_]{0,63}$/.test(name) ? name : "Error";
}

function safeErrorCode(error) { const code = String(error?.code || ""); return /^[a-z0-9_]{1,32}$/.test(code) ? code : ""; }

function cleanText(value) {
  return value == null ? "" : String(value).trim();
}

function normalizePositiveNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 0;
}

function normalizeOrderValue(value) {
  return normalizePositiveNumber(value);
}

async function main() {
  const config = readConfig();
  const instanceLock = acquireSingleInstanceLock({
    filePath: path.join(config.stateDir, "hearth-channel-only.lock"),
  });
  let stopped = false;
  const stop = () => { stopped = true; };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  let controlPlane = null;
  try {
    const { createWeixinChannelAdapter } = require("../adapters/channel/weixin");
    const adapter = createWeixinChannelAdapter(config);
    const observer = createHearthChannelOnlyObserver({ adapter, config });
    // 账号必须先就绪：主动发送控制面要共用这套登录态 / token cache，禁止另起炉灶。
    const account = adapter.resolveAccount();
    const observeConfig = parseObserveConfig(process.env);

    // Active Line v0：同一进程挂 localhost 控制面，Dylan smoke 只敲这扇门。
    controlPlane = startHearthProactiveSendServer({
      adapter,
      config,
      env: process.env,
      allowedUserIds: observeConfig.allowedUserIds,
      isReady: () => !stopped && Boolean(account?.accountId),
      logger: console,
    });
    await controlPlane.ready;
    logSafe(console, {
      type: "hearth_weixin_proactive_send",
      status: "control_plane_listening",
      host: controlPlane.host,
      port: controlPlane.port,
    });

    let consecutiveFailures = 0;

    while (!stopped) {
      try {
        const response = await adapter.getUpdates({
          syncBuffer: adapter.loadSyncBuffer(),
          timeoutMs: DEFAULT_LONG_POLL_TIMEOUT_MS,
        });
        consecutiveFailures = 0;
        await observer.processUpdateResponse(response, account);
      } catch (error) {
        consecutiveFailures += 1;
        logSafe(console, {
          type: "hearth_weixin_observe",
          status: "poll_error",
          retry_delay_ms: consecutiveFailures >= MAX_CONSECUTIVE_FAILURES ? BACKOFF_DELAY_MS : RETRY_DELAY_MS,
        });
        await sleep(consecutiveFailures >= MAX_CONSECUTIVE_FAILURES ? BACKOFF_DELAY_MS : RETRY_DELAY_MS);
      }
    }
  } finally {
    process.off("SIGINT", stop);
    process.off("SIGTERM", stop);
    if (controlPlane) {
      try {
        await controlPlane.close();
      } catch {
        // 关控制面失败也不要挡住锁释放；进程反正要退场了。
      }
    }
    instanceLock.release();
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = {
  acquireSingleInstanceLock,
  buildRawMetadata,
  compareRawMessages,
  createHearthChannelOnlyObserver,
  parseObserveConfig,
  resolveEventTime,
  safeErrorType,
  shortHash,
  sortRawMessages,
};

if (require.main === module) {
  main().catch(() => {
    logSafe(console, { type: "hearth_weixin_observe", status: "fatal" });
    process.exitCode = 1;
  });
}
