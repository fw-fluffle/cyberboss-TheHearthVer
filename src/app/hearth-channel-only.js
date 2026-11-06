const crypto = require("crypto");
const fs = require("fs/promises");
const path = require("path");
const { readConfig } = require("../core/config");
const { ChannelIdempotencyStore, hashIdempotencyKey } = require("../core/channel-idempotency-store");
const { createHearthBridgeClient, createDryRunDeliverySink } = require("../core/hearth-bridge-client");
const { invalidSticker, withStickerFile } = require("../core/hearth-sticker-delivery");
const { startHearthProactiveSendServer } = require("../server/hearth-proactive-send-server");
const { createHearthChannelStateStore } = require("../adapters/channel/weixin/hearth-channel-state-store");
const { persistIncomingWeixinAttachments } = require("../adapters/channel/weixin/media-receive");

const DEFAULT_LONG_POLL_TIMEOUT_MS = 35_000;
const RETRY_DELAY_MS = 2_000;
const BACKOFF_DELAY_MS = 30_000;
const MAX_CONSECUTIVE_FAILURES = 3;
// 微信发送接口如果在同一眨眼里连收几泡，客户端可能把它们压成一只带空行的大气泡。
// 650ms 只放在哥哥“泡与泡之间”：单泡不罚站，最后一泡也不拖着尾巴睡觉。
const WEIXIN_OUTBOUND_BUBBLE_INTERVAL_MS = 650;
const MAX_BRIDGE_IMAGES = 4;
const MAX_BRIDGE_IMAGE_BYTES = 10 * 1024 * 1024;
const MAX_BRIDGE_IMAGE_TOTAL_BYTES = 20 * 1024 * 1024;
const ALLOWED_BRIDGE_IMAGE_MEDIA_TYPES = new Set([
  "image/jpeg",
  "image/png",
  "image/webp",
  "image/gif",
]);

async function prepareBridgeImages({ attachments, config } = {}) {
  const incoming = Array.isArray(attachments) ? attachments : [];
  if (!incoming.length) {
    return { images: [], failures: [] };
  }

  // channel-only 目前只批准“给哥哥看图片”，不是通用文件上传口。
  // 文件、视频、语音与未知附件宁可明确停车，也不伪装成普通文字悄悄漏过去。
  const imageAttachments = incoming.filter((item) => String(item?.kind || "").trim().toLowerCase() === "image");
  const failures = incoming
    .filter((item) => String(item?.kind || "").trim().toLowerCase() !== "image")
    .map(() => ({ reason: "unsupported_attachment_type" }));
  if (imageAttachments.length > MAX_BRIDGE_IMAGES) {
    return {
      images: [],
      failures: [...failures, { reason: "too_many_images" }],
    };
  }

  const persisted = await persistIncomingWeixinAttachments({
    attachments: imageAttachments,
    stateDir: config?.stateDir,
    cdnBaseUrl: config?.weixinCdnBaseUrl,
    messageId: config?.messageId || "",
    receivedAt: config?.receivedAt || "",
  });
  failures.push(...persisted.failed.map(() => ({ reason: "attachment_intake_failed" })));

  const images = [];
  let totalBytes = 0;
  for (const item of persisted.saved) {
    const mediaType = resolveBridgeImageMediaType(item);
    const declaredBytes = Number(item?.sizeBytes || 0);
    if (!item?.isImage || !mediaType) {
      failures.push({ reason: "unsupported_image_format" });
      continue;
    }
    if (declaredBytes <= 0 || declaredBytes > MAX_BRIDGE_IMAGE_BYTES) {
      failures.push({ reason: "image_too_large" });
      continue;
    }
    if (totalBytes + declaredBytes > MAX_BRIDGE_IMAGE_TOTAL_BYTES) {
      failures.push({ reason: "total_image_data_too_large" });
      continue;
    }
    let bytes;
    try {
      bytes = await fs.readFile(item.absolutePath);
    } catch {
      failures.push({ reason: "saved_image_unavailable" });
      continue;
    }
    if (!bytes.length || bytes.length !== declaredBytes || bytes.length > MAX_BRIDGE_IMAGE_BYTES) {
      failures.push({ reason: "saved_image_size_mismatch" });
      continue;
    }
    totalBytes += bytes.length;
    images.push({
      media_type: mediaType,
      data: bytes.toString("base64"),
    });
  }
  return { images, failures };
}

function resolveBridgeImageMediaType(item) {
  const contentType = String(item?.contentType || "").split(";")[0].trim().toLowerCase();
  if (ALLOWED_BRIDGE_IMAGE_MEDIA_TYPES.has(contentType)) {
    return contentType;
  }
  const extension = path.extname(String(item?.fileName || "")).toLowerCase();
  if (extension === ".jpg" || extension === ".jpeg") return "image/jpeg";
  if (extension === ".png") return "image/png";
  if (extension === ".webp") return "image/webp";
  if (extension === ".gif") return "image/gif";
  return "";
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

function resolveFallbackInboundBufferSeconds(env = process.env) {
  const parsed = Number(env.HEARTH_INBOUND_BUFFER_SECONDS);
  // 正式环境会从 Gateway 内部配置票读取；0 只是旧测试 / 配置票暂不可用时的兼容兜底，
  // 不会在 Sidecar 里另养一份永久真相。
  if (!Number.isInteger(parsed)) return 0;
  return Math.max(0, Math.min(15, parsed));
}

function createHearthChannelOnlyObserver({
  adapter,
  config,
  env = process.env,
  idempotencyStore,
  channelStateStore = null,
  bridgeClient,
  prepareImages = prepareBridgeImages,
  delivery,
  logger = console,
  now = () => Date.now(),
  sleepFn = sleep,
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
  // 同一微信会话永远只允许一轮哥哥在跑。缓冲期内的新气泡会重置静默计时；
  // 一轮已经离开缓冲区后，后来消息就排到下一轮，不硬杀模型、不制造半条 assistant。
  const pendingTurns = new Map();
  const sessionTurnChains = new Map();
  const backgroundTurns = new Set();
  let chatRuntimeConfig = {
    inboundBufferSeconds: resolveFallbackInboundBufferSeconds(env),
    bubbleMode: "natural",
  };
  let chatRuntimeConfigReadAt = 0;

  async function refreshChatRuntimeConfig() {
    const ageMs = now() - chatRuntimeConfigReadAt;
    if (chatRuntimeConfigReadAt > 0 && ageMs >= 0 && ageMs < 30_000) return chatRuntimeConfig;
    chatRuntimeConfigReadAt = now();
    if (!bridge || typeof bridge.getRuntimeConfig !== "function") return chatRuntimeConfig;
    try {
      chatRuntimeConfig = await bridge.getRuntimeConfig();
    } catch (error) {
      logSafe(logger, {
        type: "hearth_weixin_chat_runtime",
        status: "config_fallback",
        error_type: safeErrorType(error),
      });
    }
    return chatRuntimeConfig;
  }

  function sessionQueueKey(metadata) {
    return `${metadata.account_id}\u0000${metadata.sender_id}`;
  }

  function aggregateBufferedRecords(records) {
    const first = records[0];
    const last = records[records.length - 1];
    const bubbleTexts = records.map((record) => String(record.event.text || "").trim());
    const bubbleMessageIds = records.map((record) => String(record.metadata.message_id || ""));
    const logicalTurnId = `wturn_${crypto.createHash("sha256").update(
      bubbleMessageIds.join("\u0000")
    ).digest("hex").slice(0, 24)}`;
    return {
      ...last.event,
      text: bubbleTexts.filter(Boolean).join("\n"),
      images: records.flatMap((record) => Array.isArray(record.event.images) ? record.event.images : []),
      event_time: first.metadata.event_time,
      event_time_start: first.metadata.event_time,
      event_time_end: last.metadata.event_time,
      bridge_received_at: first.event.bridge_received_at,
      message_id: last.metadata.message_id,
      logical_turn_id: logicalTurnId,
      bubble_count: records.length,
      bubble_texts: bubbleTexts,
      bubble_message_ids: bubbleMessageIds,
    };
  }

  async function executeBufferedTurn(records) {
    const aggregate = aggregateBufferedRecords(records);
    const latest = records[records.length - 1];
    try {
      await adapter.sendTyping({
        userId: latest.metadata.sender_id,
        status: 1,
        contextToken: latest.normalized.contextToken,
      }).catch((error) => {
        logSafe(logger, {
          type: "hearth_weixin_bridge",
          status: "typing_failed",
          error_type: safeErrorType(error),
        });
      });
      let response;
      try {
        response = await bridge.request(aggregate);
      } catch (error) {
        const code = safeErrorCode(error);
        logSafe(logger, {
          type: "hearth_weixin_bridge",
          status: "bridge_failed",
          error_type: safeErrorType(error),
          error_code: code,
          bubble_count: records.length,
        });
        if (code === "duplicate_or_processing") {
          records.forEach((record) => {
            ledger.mark(record.idempotencyKey, {
              status: "bridge_duplicate",
              recordedAtMs: record.bridgeReceivedAtMs,
            });
            logObserve(logger, record.event, "bridge_duplicate");
          });
          return { status: "bridge_duplicate", event: null };
        }
        return { status: "bridge_failed", event: null };
      }

      try {
        if (observeConfig.dryRun) {
          await dryRunSink.deliverSegments(response.segments);
        } else if (deliver && typeof deliver.deliverSegments === "function") {
          await deliver.deliverSegments(response.segments, {
            userId: latest.metadata.sender_id,
            contextToken: latest.normalized.contextToken,
          });
        } else {
          // Gateway 已经切好展示气泡；Sidecar 只做平台硬上限保护，不再二次按句号切菜。
          // 注意：adapter 内部 350ms 只管“同一泡超长后的硬切块”，管不到这里的独立泡。
          // 所以间隔必须住在外层循环；否则三个单泡会像赶末班车一样同时撞进微信。
          for (let index = 0; index < response.segments.length; index += 1) {
            const segment = response.segments[index];
            await adapter.sendText({
              userId: latest.metadata.sender_id,
              text: segment,
              contextToken: latest.normalized.contextToken,
              preserveBlock: true,
            });
            if (index < response.segments.length - 1) {
              await sleepFn(WEIXIN_OUTBOUND_BUBBLE_INTERVAL_MS);
            }
          }
        }
        // 只记“发了几泡”和固定间隔，不记哥哥正文；以后再遇到一只大气泡，
        // 值班小机一眼就能分清“Gateway 只切一泡”还是“微信把多泡粘起来”。
        logSafe(logger, {
          type: "hearth_weixin_bridge",
          status: "segments_delivered",
          outbound_segment_count: response.segments.length,
          bubble_interval_ms: WEIXIN_OUTBOUND_BUBBLE_INTERVAL_MS,
        });
      } catch (error) {
        logSafe(logger, {
          type: "hearth_weixin_bridge",
          status: "delivery_failed",
          error_type: safeErrorType(error),
          bubble_count: records.length,
        });
        return { status: "delivery_failed", event: null };
      }

      const sticker = response.sticker;
      let deliveryStatus = "delivered";
      let deliveredAtMs;
      if (sticker) {
        const textSentAtMs = now();
        try {
          if (!observeConfig.dryRun && (deliver
            ? typeof deliver.deliverSticker !== "function" || typeof deliver.deliverSegments !== "function"
            : typeof adapter.sendFile !== "function")) throw invalidSticker();
          const bytes = await bridge.readSticker(sticker);
          if (observeConfig.dryRun) {
            await dryRunSink.deliverSticker?.({ id: sticker.id });
          } else {
            const recipient = { userId: latest.metadata.sender_id, contextToken: latest.normalized.contextToken };
            await withStickerFile(sticker, bytes, filePath => deliver
              ? deliver.deliverSticker({ ...recipient, filePath })
              : adapter.sendFile({ ...recipient, filePath }));
          }
        } catch (error) {
          logSafe(logger, {
            type: "hearth_weixin_bridge",
            status: "sticker_delivery_failed",
            error_type: safeErrorType(error),
          });
          if (observeConfig.dryRun || !response.segments.length) {
            return { status: "delivery_failed", event: null };
          }
          deliveryStatus = "partial_delivery";
          deliveredAtMs = textSentAtMs;
        }
      }

      if (observeConfig.dryRun) {
        records.forEach((record) => {
          ledger.mark(record.idempotencyKey, {
            status: "bridged_dry_run",
            recordedAtMs: record.bridgeReceivedAtMs,
          });
          logObserve(logger, record.event, "bridged_dry_run");
        });
        return {
          status: "bridged_dry_run",
          event: aggregate,
          segments: response.segments,
          request_id: response.requestId || "",
          gateway_processed_at: response.gatewayProcessedAt || "",
        };
      }

      const assistantSentAt = new Date(deliveredAtMs ?? now()).toISOString();
      records.forEach((record) => {
        ledger.mark(record.idempotencyKey, {
          status: deliveryStatus,
          recordedAtMs: Date.parse(assistantSentAt),
        });
        logObserve(logger, record.event, deliveryStatus);
      });
      if (deliveryStatus === "delivered" && sticker?.history_receipt) {
        try {
          await bridge.reportStickerDelivery(sticker);
        } catch {
          logSafe(logger, { type: "hearth_weixin_bridge", status: "sticker_history_receipt_failed" });
        }
      }
      return {
        status: deliveryStatus,
        event: { ...aggregate, assistant_sent_at: assistantSentAt },
      };
    } finally {
      records.forEach((record) => inflightKeys.delete(record.idempotencyKey));
    }
  }

  function queueBufferedTurn(sessionKey, records) {
    const previous = sessionTurnChains.get(sessionKey) || Promise.resolve();
    const next = previous
      .catch(() => null)
      .then(() => executeBufferedTurn(records));
    sessionTurnChains.set(sessionKey, next);
    backgroundTurns.add(next);
    next.finally(() => {
      backgroundTurns.delete(next);
      if (sessionTurnChains.get(sessionKey) === next) sessionTurnChains.delete(sessionKey);
    }).catch(() => null);
    return next;
  }

  function flushPendingTurn(sessionKey) {
    const pending = pendingTurns.get(sessionKey);
    if (!pending) return null;
    pendingTurns.delete(sessionKey);
    if (pending.timer) clearTimeout(pending.timer);
    return queueBufferedTurn(sessionKey, pending.records);
  }

  function enqueueBufferedTurn(sessionKey, record, quietSeconds) {
    let pending = pendingTurns.get(sessionKey);
    if (pending && pending.records.length) {
      const pendingTextChars = pending.records.reduce(
        (sum, item) => sum + String(item.event.text || "").trim().length,
        0,
      );
      const pendingImageCount = pending.records.reduce(
        (sum, item) => sum + (Array.isArray(item.event.images) ? item.event.images.length : 0),
        0,
      );
      const newTextChars = String(record.event.text || "").trim().length;
      const newImageCount = Array.isArray(record.event.images) ? record.event.images.length : 0;
      // Gateway 信封上限是 8000 字 / 4 图。达到饭盒边缘就先封上一轮，绝不把两轮
      // 硬塞成一张注定 400 的巨型车票。
      if (
        pending.records.length >= 8
        || pendingTextChars + newTextChars > 8000
        || pendingImageCount + newImageCount > 4
      ) {
        flushPendingTurn(sessionKey);
        pending = null;
      }
    }
    if (!pending) {
      pending = { records: [], timer: null };
      pendingTurns.set(sessionKey, pending);
    }
    pending.records.push(record);
    if (pending.timer) clearTimeout(pending.timer);
    if (pending.records.length >= 8 || quietSeconds <= 0) {
      return flushPendingTurn(sessionKey);
    }
    pending.timer = setTimeout(() => {
      flushPendingTurn(sessionKey);
    }, quietSeconds * 1000);
    return null;
  }

  function rememberChannelState(metadata, bridgeReceivedAt) {
    if (!channelStateStore || typeof channelStateStore.recordInboundUserEvent !== "function") {
      return;
    }
    try {
      channelStateStore.recordInboundUserEvent({
        eventTimeMs: metadata.event_time,
        eventTimeSource: metadata.event_time_source,
        bridgeReceivedAt,
        messageId: metadata.message_id,
      });
    } catch {
      // 状态落盘失败不得阻断入站主链。
      logSafe(logger, {
        type: "hearth_weixin_channel_state",
        status: "persist_failed",
      });
    }
  }

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

    // 白名单 + 非重复 + 标准化成功后，才推进真实微信事件时间（防乱序倒退由 store 负责）。
    rememberChannelState(metadata, bridgeReceivedAt);

    const event = {
      ...metadata,
      has_text: Boolean(String(normalized.text || "").trim()),
      has_attachments: Array.isArray(normalized.attachments) && normalized.attachments.length > 0,
      text: String(normalized.text || ""),
      images: [],
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
      const normalizedAttachments = Array.isArray(normalized.attachments) ? normalized.attachments : [];
      if (normalizedAttachments.length) {
        let preparedImages;
        try {
          preparedImages = await prepareImages({
            attachments: normalizedAttachments,
            config: {
              stateDir: config?.stateDir,
              weixinCdnBaseUrl: config?.weixinCdnBaseUrl,
              messageId: normalized.messageId || metadata.message_id,
              receivedAt: normalized.receivedAt || bridgeReceivedAt,
            },
          });
        } catch {
          preparedImages = { images: [], failures: [{ reason: "attachment_intake_failed" }] };
        }
        event.images = Array.isArray(preparedImages?.images) ? preparedImages.images : [];
        const failures = Array.isArray(preparedImages?.failures) ? preparedImages.failures : [];
        // 图片轮必须全收齐；半张成功半张失败仍会误导哥哥，所以宁可请狐狸重发。
        if (failures.length || event.images.length !== normalizedAttachments.length) {
          await adapter.sendText({
            userId: metadata.sender_id,
            text: "⚠️ 图片暂时没能安全送到哥哥那里，请稍后重发一次。",
            contextToken: normalized.contextToken,
            preserveBlock: true,
          }).catch(() => {});
          ledger.mark(idempotencyKey, { status: "attachment_failed", recordedAtMs: bridgeReceivedAtMs });
          logSafe(logger, {
            type: "hearth_weixin_bridge",
            status: "attachment_failed",
            error_code: "attachment_intake_failed",
            image_count: event.images.length,
          });
          logObserve(logger, event, "attachment_failed");
          return { status: "attachment_failed", event: null };
        }
      }

      inflightKeys.add(idempotencyKey);
      const runtime = await refreshChatRuntimeConfig();
      const record = {
        metadata,
        normalized,
        event,
        idempotencyKey,
        bridgeReceivedAtMs,
      };
      const queued = enqueueBufferedTurn(
        sessionQueueKey(metadata),
        record,
        runtime.inboundBufferSeconds,
      );
      if (queued && runtime.inboundBufferSeconds <= 0) {
        // 兼容测试/显式 0 秒：这一轮仍等到真实投递后返回。
        return await queued;
      }
      logObserve(logger, event, "buffered");
      return { status: "buffered", event };
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
    async flushPendingTurns() {
      Array.from(pendingTurns.keys()).forEach((key) => flushPendingTurn(key));
      await Promise.allSettled(Array.from(backgroundTurns));
    },
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
    image_count: Array.isArray(event.images) ? event.images.length : 0,
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

const SAFE_STARTUP_STAGES = new Set([
  "config_read",
  "adapter_create",
  "account_resolve",
  "state_store_create",
  "observer_create",
  "control_plane_bind",
  "poll_loop",
]);

function safeStartupFailure(error) {
  const rawStage = String(error?.hearthStartupStage || "");
  const stage = SAFE_STARTUP_STAGES.has(rawStage) ? rawStage : "unknown";
  let errorCode = "startup_failed";
  if (error?.code === "EADDRINUSE") {
    errorCode = "port_in_use";
  } else if (stage === "config_read") {
    errorCode = "configuration_invalid";
  } else if (["adapter_create", "account_resolve", "state_store_create"].includes(stage)) {
    errorCode = "channel_bootstrap_failed";
  } else if (stage === "observer_create") {
    errorCode = "observer_setup_failed";
  } else if (stage === "control_plane_bind") {
    errorCode = "control_plane_failed";
  } else if (stage === "poll_loop") {
    errorCode = "runtime_failed";
  }
  return {
    type: "hearth_weixin_observe",
    status: "fatal",
    stage,
    error_type: safeErrorType(error),
    error_code: errorCode,
  };
}

async function main() {
  let stopped = false;
  const stop = () => { stopped = true; };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  let controlPlane = null;
  let observer = null;
  let startupStage = "config_read";
  try {
    const config = readConfig();
    startupStage = "adapter_create";
    const { createWeixinChannelAdapter } = require("../adapters/channel/weixin");
    const adapter = createWeixinChannelAdapter(config);
    // 账号必须先就绪：主动发送控制面要共用这套登录态 / token cache，禁止另起炉灶。
    startupStage = "account_resolve";
    const account = adapter.resolveAccount();
    startupStage = "state_store_create";
    const channelStateStore = createHearthChannelStateStore({
      config,
      accountId: account.accountId,
    });
    startupStage = "observer_create";
    observer = createHearthChannelOnlyObserver({ adapter, config, channelStateStore });
    const observeConfig = parseObserveConfig(process.env);

    // 3200 的 localhost 监听由操作系统独占，既是控制面，也是唯一单实例门牌。它在任何
    // 微信长轮询之前完成绑定：第二只进程只会收到 EADDRINUSE，不会产生双拉取，更不会
    // 再被一枚可能残留、可能无权限删除的文件锁困住。
    startupStage = "control_plane_bind";
    controlPlane = startHearthProactiveSendServer({
      adapter,
      config,
      env: process.env,
      allowedUserIds: observeConfig.allowedUserIds,
      channelStateStore,
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

    startupStage = "poll_loop";
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
  } catch (error) {
    if (error && typeof error === "object") {
      error.hearthStartupStage = startupStage;
    }
    throw error;
  } finally {
    process.off("SIGINT", stop);
    process.off("SIGTERM", stop);
    if (observer && typeof observer.flushPendingTurns === "function") {
      try {
        await observer.flushPendingTurns();
      } catch {
        // systemd 收班时尽力清空已验票缓冲；失败只允许结束进程，不能在 finally 里死循环。
      }
    }
    if (controlPlane) {
      try {
        await controlPlane.close();
      } catch {
        // 进程已经退场时，控制面收尾失败不再制造第二个致命错误。
      }
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
  resolveFallbackInboundBufferSeconds,
  prepareBridgeImages,
  resolveEventTime,
  resolveBridgeImageMediaType,
  safeErrorType,
  safeStartupFailure,
  shortHash,
  WEIXIN_OUTBOUND_BUBBLE_INTERVAL_MS,
  sortRawMessages,
};

if (require.main === module) {
  main().catch((error) => {
    logSafe(console, safeStartupFailure(error));
    process.exitCode = 1;
  });
}
