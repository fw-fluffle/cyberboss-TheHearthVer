const crypto = require("crypto");

const DEFAULT_TIMEOUT_MS = 10_000;

function createHearthBridgeClient({ env = process.env, fetchImpl = fetch, timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  const baseUrl = String(env.HEARTH_BRIDGE_URL || "").trim();
  const token = String(env.HEARTH_INTERNAL_TOKEN || "").trim();
  const pseudonymSecret = String(env.HEARTH_BRIDGE_PSEUDONYM_SECRET || "").trim();
  if (!baseUrl || !token || !pseudonymSecret) throw new Error("Bridge configuration is incomplete");
  const url = new URL("/internal/channel/companion", baseUrl);
  if (url.protocol !== "http:" || !["127.0.0.1", "localhost"].includes(url.hostname)) {
    throw new Error("Bridge URL must be an explicit local HTTP endpoint");
  }

  return {
    async request(event) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const response = await fetchImpl(url, {
          method: "POST",
          headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
          body: JSON.stringify(buildChannelEnvelope(event, pseudonymSecret)),
          signal: controller.signal,
        });
        if (!response.ok) throw Object.assign(new Error("Bridge HTTP failure"), { code: `http_${response.status}` });
        let body;
        try { body = await response.json(); } catch { throw Object.assign(new Error("Bridge invalid JSON"), { code: "invalid_json" }); }
        return validateBridgeResponse(body);
      } catch (error) {
        if (error?.name === "AbortError") throw Object.assign(new Error("Bridge timeout"), { code: "timeout" });
        throw error;
      } finally { clearTimeout(timer); }
    },
  };
}

function pseudonym(value, secret, label) {
  return `${label}_${crypto.createHmac("sha256", secret).update(String(value || "")).digest("hex").slice(0, 24)}`;
}

function buildChannelEnvelope(event, secret) {
  const accountId = pseudonym(event.account_id, secret, "wa");
  const senderId = pseudonym(event.sender_id, secret, "wu");
  return {
    channel: "weixin", account_id: accountId, sender_id: senderId,
    chat_id: pseudonym(`${event.account_id}:${event.sender_id}`, secret, "wc"),
    thread_key: `weixin:${accountId}:${senderId}`,
    message_id: String(event.message_id || ""), text: String(event.text || ""),
    event_time: new Date(event.event_time).toISOString(), bridge_received_at: event.bridge_received_at,
    raw_metadata: { seq: String(event.seq || ""), client_id: String(event.client_id || "") }, bubble_count: 1,
  };
}

function validateBridgeResponse(body) {
  if (!body || typeof body !== "object" || Array.isArray(body) || body.action !== "send" || typeof body.reply_text !== "string" || !body.reply_text.trim()) {
    throw Object.assign(new Error("Bridge response schema invalid"), { code: "invalid_schema" });
  }
  const segments = Array.isArray(body.segments) ? body.segments.map((value) => String(value || "").trim()).filter(Boolean) : [body.reply_text.trim()];
  if (!segments.length || (Array.isArray(body.segments) && segments.join("\n") !== body.reply_text.trim())) {
    throw Object.assign(new Error("Bridge segments invalid"), { code: "invalid_segments" });
  }
  return { requestId: typeof body.request_id === "string" ? body.request_id : "", replyText: body.reply_text.trim(), segments, gatewayProcessedAt: body.gateway_processed_at || "" };
}

module.exports = { buildChannelEnvelope, createHearthBridgeClient, pseudonym, validateBridgeResponse };
