/**
 * Active Line v0-C1 —— 无敏感 channel state（per-account）
 *
 * 只记时间戳和 message_id，绝不碰 context_token / 正文 / 凭据。
 * 写入：临时文件 + rename，抄 idempotency ledger 的原子小抄。
 *
 * 字段语义（请勿望文生义）：
 * - last_user_event_time：微信 create_time* 对应的真实事件时间（只前进不倒退）
 * - last_bridge_received_at：本条入站在 Sidecar 被接受处理的抵达/处理时间
 * - last_message_id：最近一次「抵达并成功写入本状态」的消息 ID
 *   （即最后抵达/处理的那条，不是「事件时间最晚」的那条；
 *    乱序旧消息晚到时，event_time 不倒退，但 last_message_id 仍会换成这条迟到消息）
 */

const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { normalizeAccountId } = require("./account-store");

function resolveHearthChannelStatePath(config, accountId) {
  const normalized = normalizeAccountId(accountId);
  if (!normalized) {
    throw new Error("hearth channel state requires accountId");
  }
  const accountsDir = config?.accountsDir || path.join(config?.stateDir || process.cwd(), "accounts");
  return path.join(accountsDir, `${normalized}.hearth-channel-state.json`);
}

function isValidIsoTimestamp(value) {
  if (typeof value !== "string" || !value.trim()) return false;
  const ms = Date.parse(value);
  return Number.isFinite(ms);
}

function emptyState() {
  return {
    last_user_event_time: "",
    last_bridge_received_at: "",
    last_message_id: "",
    updated_at: "",
  };
}

function sanitizeLoaded(raw) {
  const base = emptyState();
  if (!raw || typeof raw !== "object") return base;
  return {
    last_user_event_time: isValidIsoTimestamp(raw.last_user_event_time) ? raw.last_user_event_time.trim() : "",
    last_bridge_received_at: isValidIsoTimestamp(raw.last_bridge_received_at) ? raw.last_bridge_received_at.trim() : "",
    last_message_id: typeof raw.last_message_id === "string" ? raw.last_message_id.trim().slice(0, 128) : "",
    updated_at: isValidIsoTimestamp(raw.updated_at) ? raw.updated_at.trim() : "",
  };
}

function atomicWriteJson(filePath, payload) {
  const directory = path.dirname(filePath);
  fs.mkdirSync(directory, { recursive: true });
  const contents = `${JSON.stringify(payload, null, 2)}\n`;
  const temporaryPath = path.join(
    directory,
    `.${path.basename(filePath)}.${process.pid}.${crypto.randomUUID()}.tmp`,
  );
  try {
    fs.writeFileSync(temporaryPath, contents, { encoding: "utf8", mode: 0o600 });
    try {
      fs.chmodSync(temporaryPath, 0o600);
    } catch {
      // Windows may shrug at POSIX modes.
    }
    fs.renameSync(temporaryPath, filePath);
    try {
      fs.chmodSync(filePath, 0o600);
    } catch {
      // Best effort.
    }
  } catch (error) {
    try {
      if (fs.existsSync(temporaryPath)) fs.unlinkSync(temporaryPath);
    } catch {
      // Best effort.
    }
    throw error;
  }
}

function createHearthChannelStateStore({
  config,
  accountId,
  now = () => Date.now(),
} = {}) {
  const filePath = resolveHearthChannelStatePath(config, accountId);
  let state = emptyState();

  function load() {
    try {
      if (!fs.existsSync(filePath)) {
        state = emptyState();
        return getSnapshot();
      }
      const parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));
      state = sanitizeLoaded(parsed);
      return getSnapshot();
    } catch {
      state = emptyState();
      return getSnapshot();
    }
  }

  function getSnapshot() {
    return { ...state };
  }

  function persist() {
    atomicWriteJson(filePath, {
      version: 1,
      ...state,
    });
  }

  /**
   * 入站合法用户消息后调用。
   * - last_user_event_time：仅当 eventTimeSource 为微信 create_time* 时，取较晚者（防乱序倒退）
   * - last_bridge_received_at：本次 Sidecar 处理时间，可前进
   * - last_message_id：始终更新为「本次抵达」的 messageId（最后抵达语义，非最晚 event_time）
   */
  function recordInboundUserEvent({
    eventTimeMs,
    eventTimeSource,
    bridgeReceivedAt,
    messageId = "",
  } = {}) {
    const updatedAt = new Date(now()).toISOString();
    const bridgeIso = isValidIsoTimestamp(bridgeReceivedAt)
      ? String(bridgeReceivedAt).trim()
      : updatedAt;

    state.last_bridge_received_at = bridgeIso;
    state.updated_at = updatedAt;
    // 最后抵达：只要本条被接受写入，就刷新 ID（即使其 event_time 更旧）。
    if (typeof messageId === "string" && messageId.trim()) {
      state.last_message_id = messageId.trim().slice(0, 128);
    }

    const weixinSource = eventTimeSource === "create_time_ms" || eventTimeSource === "create_time";
    const eventMs = Number(eventTimeMs);
    if (weixinSource && Number.isFinite(eventMs) && eventMs > 0) {
      const candidateIso = new Date(eventMs).toISOString();
      const previousMs = state.last_user_event_time ? Date.parse(state.last_user_event_time) : 0;
      if (!previousMs || eventMs >= previousMs) {
        state.last_user_event_time = candidateIso;
      }
    }

    persist();
    return getSnapshot();
  }

  // 启动时加载一次。
  load();

  return {
    filePath,
    load,
    getSnapshot,
    recordInboundUserEvent,
  };
}

module.exports = {
  createHearthChannelStateStore,
  resolveHearthChannelStatePath,
  isValidIsoTimestamp,
  emptyState,
};
