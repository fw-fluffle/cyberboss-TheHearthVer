const crypto = require("crypto");
const path = require("path");
const { readConfig } = require("../core/config");
const { ChannelIdempotencyStore, hashIdempotencyKey } = require("../core/channel-idempotency-store");

const DEFAULT_LONG_POLL_TIMEOUT_MS = 35_000;
const RETRY_DELAY_MS = 2_000;
const BACKOFF_DELAY_MS = 30_000;
const MAX_CONSECUTIVE_FAILURES = 3;

function parseObserveConfig(env = process.env) {
  const mode = String(env.HEARTH_WEIXIN_MODE || "observe").trim().toLowerCase() || "observe";
  if (mode !== "observe") {
    throw new Error("HEARTH_WEIXIN_MODE only supports observe in Phase 1A");
  }
  const allowedUserIds = new Set(
    String(env.HEARTH_WEIXIN_ALLOWED_USER_IDS || "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean),
  );
  return { mode, allowedUserIds };
}

function createHearthChannelOnlyObserver({
  adapter,
  config,
  env = process.env,
  idempotencyStore,
  logger = console,
  now = () => Date.now(),
} = {}) {
  if (!adapter) {
    throw new Error("Hearth channel-only observer requires a channel adapter");
  }
  const observeConfig = parseObserveConfig(env);
  const ledger = idempotencyStore || new ChannelIdempotencyStore({
    filePath: path.join(config?.stateDir || process.cwd(), "hearth-channel-idempotency.json"),
    now,
    onWarning: (message) => logSafe(logger, { type: "hearth_weixin_observe", status: "warning", message }),
  });

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
    };
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
  const { createWeixinChannelAdapter } = require("../adapters/channel/weixin");
  const adapter = createWeixinChannelAdapter(config);
  const observer = createHearthChannelOnlyObserver({ adapter, config });
  const account = adapter.resolveAccount();
  let stopped = false;
  const stop = () => { stopped = true; };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
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
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = {
  buildRawMetadata,
  compareRawMessages,
  createHearthChannelOnlyObserver,
  parseObserveConfig,
  resolveEventTime,
  shortHash,
  sortRawMessages,
};

if (require.main === module) {
  main().catch(() => {
    logSafe(console, { type: "hearth_weixin_observe", status: "fatal" });
    process.exitCode = 1;
  });
}
