const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("http");
const fs = require("fs");
const os = require("os");
const path = require("path");

const { ChannelIdempotencyStore } = require("../src/core/channel-idempotency-store");
const {
  buildChannelEnvelope,
  classifyHttpStatus,
  createDryRunDeliverySink,
  createHearthBridgeClient,
  normalizeCompanionUrl,
  pseudonym,
  resolveCompanionUrl,
  resolveImageTimeoutMs,
  validateBridgeResponse,
  validateRuntimeConfig,
} = require("../src/core/hearth-bridge-client");
const { createHearthChannelOnlyObserver } = require("../src/app/hearth-channel-only");

function tempPath() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "cyberboss-hearth-c1-"));
  return path.join(directory, "ledger.json");
}

function createAdapter() {
  const calls = { normalize: 0, sendText: 0, sendTyping: 0, sendOrder: [], textPayloads: [] };
  return {
    calls,
    normalizeIncomingMessage(message) {
      calls.normalize += 1;
      if (!message?.from_user_id || message?.malformed) return null;
      return {
        text: message.text || "",
        attachments: message.attachments || [],
        contextToken: message.context_token || "",
      };
    },
    async sendText(payload) {
      calls.sendText += 1;
      calls.sendOrder.push("text");
      calls.textPayloads.push(payload);
    },
    async sendTyping() {
      calls.sendTyping += 1;
      calls.sendOrder.push("typing");
    },
  };
}

function createLogger() {
  const lines = [];
  return { lines, info(line) { lines.push(line); } };
}

function message(overrides = {}) {
  return {
    from_user_id: "allowed-user",
    message_id: "c1-11",
    seq: 2,
    client_id: "client-a",
    create_time_ms: 1000,
    create_time: 1,
    text: "private message body",
    context_token: "private-context-token",
    raw_weixin_payload: { secret: "do-not-forward" },
    ...overrides,
  };
}

function bridgeEnv(overrides = {}) {
  return {
    HEARTH_COMPANION_URL: "http://127.0.0.1:3210/internal/channel/companion",
    HEARTH_INTERNAL_TOKEN: "test-hearth-internal-token",
    HEARTH_BRIDGE_PSEUDONYM_SECRET: "test-pseudonym-secret",
    HEARTH_REQUEST_TIMEOUT_MS: "250",
    ...overrides,
  };
}

test("companion URL and token fail closed when missing", () => {
  assert.throws(
    () => createHearthBridgeClient({ env: { HEARTH_INTERNAL_TOKEN: "t", HEARTH_BRIDGE_PSEUDONYM_SECRET: "s" } }),
    /URL/,
  );
  assert.throws(
    () => createHearthBridgeClient({
      env: { HEARTH_COMPANION_URL: "http://127.0.0.1:3210/internal/channel/companion", HEARTH_BRIDGE_PSEUDONYM_SECRET: "s" },
    }),
    /token/,
  );
});

test("HEARTH_COMPANION_URL and HEARTH_BRIDGE_URL both resolve to companion path", () => {
  assert.equal(
    resolveCompanionUrl({ HEARTH_COMPANION_URL: "http://127.0.0.1:8000/internal/channel/companion" }),
    "http://127.0.0.1:8000/internal/channel/companion",
  );
  assert.equal(
    normalizeCompanionUrl("http://127.0.0.1:8000"),
    "http://127.0.0.1:8000/internal/channel/companion",
  );
  assert.equal(
    resolveCompanionUrl({ HEARTH_BRIDGE_URL: "http://localhost:3210" }),
    "http://localhost:3210/internal/channel/companion",
  );
});

test("classifyHttpStatus maps auth/duplicate/size/rate/unavailable", () => {
  assert.equal(classifyHttpStatus(401), "internal_auth");
  assert.equal(classifyHttpStatus(403), "internal_auth");
  assert.equal(classifyHttpStatus(409), "duplicate_or_processing");
  assert.equal(classifyHttpStatus(413), "payload_too_large");
  assert.equal(classifyHttpStatus(429), "rate_limit");
  assert.equal(classifyHttpStatus(502), "hearth_unavailable");
  assert.equal(classifyHttpStatus(503), "hearth_unavailable");
});

test("response mapping prefers segments and rejects empty replies", () => {
  const preferred = validateBridgeResponse({
    action: "send",
    reply_text: "fallback",
    segments: ["one", "two"],
    request_id: "r1",
    gateway_processed_at: "2026-07-12T00:00:00.000Z",
  });
  assert.deepEqual(preferred.segments, ["one", "two"]);
  assert.equal(preferred.replyText, "fallback");

  const fallback = validateBridgeResponse({ action: "send", reply_text: "only-reply" });
  assert.deepEqual(fallback.segments, ["only-reply"]);

  const emptySegments = validateBridgeResponse({ action: "send", reply_text: "only-reply", segments: [] });
  assert.deepEqual(emptySegments.segments, ["only-reply"]);

  assert.throws(() => validateBridgeResponse({ action: "send", reply_text: "", segments: [] }), /empty/);
  assert.throws(() => validateBridgeResponse({ action: "silent", reply_text: "x", segments: ["x"] }), /schema/);
});

test("envelope mapping is pseudonymous and excludes context_token and raw payload", () => {
  const event = {
    account_id: "account-raw",
    sender_id: "sender-raw",
    message_id: "m1",
    text: "hello",
    event_time: 1_720_000_000_000,
    bridge_received_at: "2026-07-12T21:30:01+08:00",
    seq: 9,
    client_id: "cid",
    context_token: "must-not-appear",
  };
  const envelope = buildChannelEnvelope(event, "secret");
  const serialized = JSON.stringify(envelope);
  assert.equal(envelope.channel, "weixin");
  assert.equal(envelope.bubble_count, 1);
  assert.equal(envelope.message_id, "m1");
  assert.equal(envelope.text, "hello");
  assert.ok(envelope.event_time.includes("T"));
  assert.equal(envelope.bridge_received_at, "2026-07-12T21:30:01+08:00");
  assert.equal(envelope.raw_metadata.seq, "9");
  assert.equal(envelope.raw_metadata.client_id, pseudonym("cid", "secret", "wcid"));
  assert.equal(envelope.sender_id, pseudonym("sender-raw", "secret", "wu"));
  assert.equal(envelope.account_id, pseudonym("account-raw", "secret", "wa"));
  assert.ok(!serialized.includes("context_token"));
  assert.ok(!serialized.includes("must-not-appear"));
  assert.ok(!serialized.includes("account-raw"));
  assert.ok(!serialized.includes("sender-raw"));
  assert.ok(!serialized.includes("\"cid\""));
  assert.ok(!serialized.includes("raw_weixin"));
});

test("runtime config accepts only the bounded chat presentation ticket", () => {
  assert.deepEqual(
    validateRuntimeConfig({ inbound_buffer_seconds: 6, bubble_mode: "natural" }),
    { inboundBufferSeconds: 6, bubbleMode: "natural" },
  );
  assert.throws(() => validateRuntimeConfig({ inbound_buffer_seconds: 16, bubble_mode: "natural" }));
  assert.throws(() => validateRuntimeConfig({ inbound_buffer_seconds: 6, bubble_mode: "wild" }));
});

test("image envelope strips transport secrets and image timeout is independent", () => {
  const event = {
    account_id: "account-raw",
    sender_id: "sender-raw",
    message_id: "image-envelope",
    text: "",
    event_time: 1_720_000_000_000,
    bridge_received_at: "2026-07-12T21:30:01+08:00",
    seq: 9,
    client_id: "cid",
    images: [{
      media_type: "image/webp",
      data: "c21hbGwtaW1hZ2U=",
      absolutePath: "/private/fox.webp",
      rawPayload: "PRIVATE_RAW_PAYLOAD",
    }],
  };
  const envelope = buildChannelEnvelope(event, "secret");
  assert.deepEqual(envelope.images, [{
    media_type: "image/webp",
    data: "c21hbGwtaW1hZ2U=",
  }]);
  assert.ok(!JSON.stringify(envelope).includes("PRIVATE"));
  assert.ok(!JSON.stringify(envelope).includes("absolutePath"));
  assert.equal(resolveImageTimeoutMs({ HEARTH_IMAGE_REQUEST_TIMEOUT_MS: "4321" }), 4321);
  assert.equal(resolveImageTimeoutMs({ HEARTH_IMAGE_REQUEST_TIMEOUT_MS: "0" }), 90_000);
});

test("envelope raw_metadata.client_id is HMAC-pseudonymized under Gateway length cap", () => {
  const secret = "test-pseudonym-secret";
  const longClientId = `ilink-real-client-id-${"x".repeat(80)}`;
  assert.ok(longClientId.length > 64);

  const envelope = buildChannelEnvelope(
    {
      account_id: "account-raw",
      sender_id: "sender-raw",
      message_id: "m-long-cid",
      text: "hello",
      event_time: 1_720_000_000_000,
      bridge_received_at: "2026-07-12T21:30:01+08:00",
      seq: 1,
      client_id: longClientId,
    },
    secret,
  );
  const outgoing = envelope.raw_metadata.client_id;
  assert.equal(outgoing, pseudonym(longClientId, secret, "wcid"));
  assert.ok(outgoing.startsWith("wcid_"));
  assert.ok(outgoing.length < 64);
  assert.ok(!JSON.stringify(envelope).includes(longClientId));

  const again = buildChannelEnvelope(
    {
      account_id: "account-raw",
      sender_id: "sender-raw",
      message_id: "m-long-cid",
      text: "hello",
      event_time: 1_720_000_000_000,
      bridge_received_at: "2026-07-12T21:30:01+08:00",
      seq: 1,
      client_id: longClientId,
    },
    secret,
  );
  assert.equal(again.raw_metadata.client_id, outgoing);

  const otherInput = buildChannelEnvelope(
    {
      account_id: "account-raw",
      sender_id: "sender-raw",
      message_id: "m-long-cid",
      text: "hello",
      event_time: 1_720_000_000_000,
      bridge_received_at: "2026-07-12T21:30:01+08:00",
      seq: 1,
      client_id: `${longClientId}-b`,
    },
    secret,
  );
  assert.notEqual(otherInput.raw_metadata.client_id, outgoing);

  const otherSecret = buildChannelEnvelope(
    {
      account_id: "account-raw",
      sender_id: "sender-raw",
      message_id: "m-long-cid",
      text: "hello",
      event_time: 1_720_000_000_000,
      bridge_received_at: "2026-07-12T21:30:01+08:00",
      seq: 1,
      client_id: longClientId,
    },
    "other-secret",
  );
  assert.notEqual(otherSecret.raw_metadata.client_id, outgoing);

  const emptyEnvelope = buildChannelEnvelope(
    {
      account_id: "account-raw",
      sender_id: "sender-raw",
      message_id: "m-empty-cid",
      text: "hello",
      event_time: 1_720_000_000_000,
      bridge_received_at: "2026-07-12T21:30:01+08:00",
      seq: 1,
      client_id: "   ",
    },
    secret,
  );
  assert.equal("client_id" in emptyEnvelope.raw_metadata, false);
  assert.ok(!JSON.stringify(emptyEnvelope.raw_metadata).includes("wcid_"));
});

test("real client posts Authorization bearer and never logs secrets", async () => {
  const logger = createLogger();
  let seenAuth = "";
  let seenBody = null;
  const client = createHearthBridgeClient({
    env: bridgeEnv(),
    fetchImpl: async (url, init) => {
      seenAuth = init.headers.Authorization;
      seenBody = JSON.parse(init.body);
      logger.info(JSON.stringify({ type: "fake_fetch", url: String(url), has_auth: Boolean(init.headers.Authorization) }));
      return {
        ok: true,
        status: 200,
        async json() {
          return {
            request_id: "req-1",
            action: "send",
            reply_text: "channel companion stub ok",
            segments: ["channel companion stub ok"],
            gateway_processed_at: "2026-07-12T21:30:02+08:00",
          };
        },
      };
    },
  });
  const result = await client.request({
    account_id: "account-1",
    sender_id: "allowed-user",
    message_id: "m-auth",
    text: "channel contract smoke",
    event_time: 1000,
    bridge_received_at: new Date(1000).toISOString(),
    seq: 1,
    client_id: "c",
  });
  assert.equal(seenAuth, "Bearer test-hearth-internal-token");
  assert.equal(seenBody.channel, "weixin");
  assert.ok(!("context_token" in seenBody));
  assert.deepEqual(result.segments, ["channel companion stub ok"]);
  const output = logger.lines.join("\n");
  assert.ok(!output.includes("test-hearth-internal-token"));
  assert.ok(!output.includes("Bearer "));
  assert.ok(!output.includes("channel contract smoke"));
});

test("real client classifies http failures without echoing bodies", async () => {
  for (const [status, code] of [
    [401, "internal_auth"],
    [403, "internal_auth"],
    [409, "duplicate_or_processing"],
    [413, "payload_too_large"],
    [429, "rate_limit"],
    [503, "hearth_unavailable"],
  ]) {
    const client = createHearthBridgeClient({
      env: bridgeEnv(),
      fetchImpl: async () => ({
        ok: false,
        status,
        async text() { return "SECRET_UPSTREAM_BODY"; },
        async json() { return { error: "SECRET_UPSTREAM_BODY" }; },
      }),
    });
    await assert.rejects(
      () => client.request({
        account_id: "a", sender_id: "s", message_id: "m", text: "t",
        event_time: 1, bridge_received_at: new Date(1).toISOString(), seq: 1, client_id: "c",
      }),
      (error) => error.code === code && !String(error.message).includes("SECRET_UPSTREAM_BODY"),
    );
  }
});

test("real client times out and rejects invalid JSON/contracts", async () => {
  const timeoutClient = createHearthBridgeClient({
    env: bridgeEnv({ HEARTH_REQUEST_TIMEOUT_MS: "20" }),
    fetchImpl: async (_url, init) => new Promise((resolve, reject) => {
      init.signal.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })));
    }),
  });
  await assert.rejects(
    () => timeoutClient.request({
      account_id: "a", sender_id: "s", message_id: "m", text: "t",
      event_time: 1, bridge_received_at: new Date(1).toISOString(), seq: 1, client_id: "c",
    }),
    (error) => error.code === "timeout",
  );

  // 图片轮单独使用更长（这里故意调短）的窗口，不会把普通文字的 250ms 配置偷偷改掉。
  const imageTimeoutClient = createHearthBridgeClient({
    env: bridgeEnv({
      HEARTH_REQUEST_TIMEOUT_MS: "250",
      HEARTH_IMAGE_REQUEST_TIMEOUT_MS: "20",
    }),
    fetchImpl: async (_url, init) => new Promise((_resolve, reject) => {
      init.signal.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })));
    }),
  });
  assert.equal(imageTimeoutClient.timeoutMs, 250);
  assert.equal(imageTimeoutClient.imageTimeoutMs, 20);
  await assert.rejects(
    () => imageTimeoutClient.request({
      account_id: "a",
      sender_id: "s",
      message_id: "image-timeout",
      text: "",
      images: [{ media_type: "image/jpeg", data: "c21hbGwtaW1hZ2U=" }],
      event_time: 1,
      bridge_received_at: new Date(1).toISOString(),
      seq: 1,
      client_id: "c",
    }),
    (error) => error.code === "timeout",
  );

  const badJson = createHearthBridgeClient({
    env: bridgeEnv(),
    fetchImpl: async () => ({ ok: true, status: 200, async json() { throw new Error("bad"); } }),
  });
  await assert.rejects(
    () => badJson.request({
      account_id: "a", sender_id: "s", message_id: "m", text: "t",
      event_time: 1, bridge_received_at: new Date(1).toISOString(), seq: 1, client_id: "c",
    }),
    (error) => error.code === "invalid_json",
  );
});

test("bridge_dry_run uses Hearth segments without assistant_sent_at or real send", async () => {
  const adapter = createAdapter();
  const ledgerPath = tempPath();
  const logger = createLogger();
  let requests = 0;
  const sink = createDryRunDeliverySink();
  const observer = createHearthChannelOnlyObserver({
    adapter,
    config: { stateDir: path.dirname(ledgerPath) },
    env: { HEARTH_WEIXIN_MODE: "bridge_dry_run", HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" },
    logger,
    idempotencyStore: new ChannelIdempotencyStore({ filePath: ledgerPath }),
    delivery: sink,
    bridgeClient: {
      async request(event) {
        requests += 1;
        assert.equal(event.text, "private message body");
        assert.ok(!("context_token" in event));
        return {
          requestId: "r-dry",
          replyText: "channel companion stub ok",
          segments: ["seg-a", "seg-b"],
          gatewayProcessedAt: "2026-07-12T21:30:02+08:00",
        };
      },
    },
  });

  const first = await observer.processRawMessage(message(), { accountId: "account-1" });
  assert.equal(first.status, "bridged_dry_run");
  assert.equal(first.event.assistant_sent_at, undefined);
  assert.deepEqual(first.segments, ["seg-a", "seg-b"]);
  assert.equal(adapter.calls.sendText, 0);
  assert.equal(sink.calls.length, 1);
  assert.deepEqual(sink.calls[0].segments, ["seg-a", "seg-b"]);
  assert.equal(requests, 1);

  const second = await observer.processRawMessage(message(), { accountId: "account-1" });
  assert.equal(second.status, "duplicate");
  assert.equal(requests, 1);

  const restart = createHearthChannelOnlyObserver({
    adapter: createAdapter(),
    config: { stateDir: path.dirname(ledgerPath) },
    env: { HEARTH_WEIXIN_MODE: "bridge_dry_run", HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" },
    logger: createLogger(),
    idempotencyStore: new ChannelIdempotencyStore({ filePath: ledgerPath }),
    bridgeClient: { async request() { throw new Error("should not call"); } },
  });
  assert.equal((await restart.processRawMessage(message(), { accountId: "account-1" })).status, "duplicate");

  const output = logger.lines.join("\n");
  assert.ok(!output.includes("private message body"));
  assert.ok(!output.includes("private-context-token"));
  assert.ok(!output.includes("allowed-user"));
  assert.ok(!output.includes("account-1"));
  assert.ok(!output.includes("seg-a"));
});

test("concurrent same message does not call Hearth twice", async () => {
  const adapter = createAdapter();
  const ledgerPath = tempPath();
  let requests = 0;
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const observer = createHearthChannelOnlyObserver({
    adapter,
    config: { stateDir: path.dirname(ledgerPath) },
    env: { HEARTH_WEIXIN_MODE: "bridge_dry_run", HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" },
    logger: createLogger(),
    idempotencyStore: new ChannelIdempotencyStore({ filePath: ledgerPath }),
    delivery: createDryRunDeliverySink(),
    bridgeClient: {
      async request() {
        requests += 1;
        await gate;
        return { requestId: "r", replyText: "ok", segments: ["ok"], gatewayProcessedAt: "" };
      },
    },
  });
  const firstPromise = observer.processRawMessage(message({ message_id: "concurrent-1" }), { accountId: "account-1" });
  await new Promise((resolve) => setTimeout(resolve, 10));
  const second = await observer.processRawMessage(message({ message_id: "concurrent-1" }), { accountId: "account-1" });
  assert.equal(second.status, "processing");
  release();
  const first = await firstPromise;
  assert.equal(first.status, "bridged_dry_run");
  assert.equal(requests, 1);
});

test("gateway 409 marks ledger without delivery or assistant_sent_at", async () => {
  const adapter = createAdapter();
  const ledgerPath = tempPath();
  let requests = 0;
  const observer = createHearthChannelOnlyObserver({
    adapter,
    config: { stateDir: path.dirname(ledgerPath) },
    env: { HEARTH_WEIXIN_MODE: "bridge_dry_run", HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" },
    logger: createLogger(),
    idempotencyStore: new ChannelIdempotencyStore({ filePath: ledgerPath }),
    delivery: createDryRunDeliverySink(),
    bridgeClient: {
      async request() {
        requests += 1;
        throw Object.assign(new Error("dup"), { code: "duplicate_or_processing" });
      },
    },
  });
  const first = await observer.processRawMessage(message({ message_id: "dup-409" }), { accountId: "account-1" });
  assert.equal(first.status, "bridge_duplicate");
  assert.equal(adapter.calls.sendText, 0);
  const second = await observer.processRawMessage(message({ message_id: "dup-409" }), { accountId: "account-1" });
  assert.equal(second.status, "duplicate");
  assert.equal(requests, 1);
});

test("local fake HTTP server contract smoke for companion client", async () => {
  const token = "test-hearth-internal-token";
  let seenAuth = "";
  let seenBody = null;
  const server = http.createServer((req, res) => {
    if (req.method !== "POST" || req.url !== "/internal/channel/companion") {
      res.writeHead(404).end();
      return;
    }
    seenAuth = req.headers.authorization || "";
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      seenBody = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        request_id: "fake-local",
        action: "send",
        reply_text: "channel companion stub ok",
        segments: ["channel companion stub ok"],
        gateway_processed_at: new Date().toISOString(),
      }));
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();
  try {
    const client = createHearthBridgeClient({
      env: bridgeEnv({
        HEARTH_COMPANION_URL: `http://127.0.0.1:${port}/internal/channel/companion`,
        HEARTH_INTERNAL_TOKEN: token,
      }),
    });
    const result = await client.request({
      account_id: "account-1",
      sender_id: "allowed-user",
      message_id: "smoke-1",
      text: "channel contract smoke",
      event_time: 1_720_000_000_000,
      bridge_received_at: "2026-07-12T21:30:01+08:00",
      seq: 1,
      client_id: "cid",
    });
    assert.equal(seenAuth, `Bearer ${token}`);
    assert.equal(seenBody.channel, "weixin");
    assert.ok(!("context_token" in seenBody));
    assert.equal(result.replyText, "channel companion stub ok");
    assert.deepEqual(result.segments, ["channel companion stub ok"]);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("allowlist deny-all still blocks bridge_dry_run before Hearth", async () => {
  let requests = 0;
  const observer = createHearthChannelOnlyObserver({
    adapter: createAdapter(),
    config: { stateDir: path.dirname(tempPath()) },
    env: { HEARTH_WEIXIN_MODE: "bridge_dry_run" },
    logger: createLogger(),
    bridgeClient: { async request() { requests += 1; throw new Error("should not call"); } },
  });
  assert.equal((await observer.processRawMessage(message(), { accountId: "account-1" })).status, "denied");
  assert.equal(requests, 0);
});
