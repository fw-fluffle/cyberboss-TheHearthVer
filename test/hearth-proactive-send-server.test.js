const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("http");

const {
  createProactiveSendDeduper,
  createProactiveSendRequestHandler,
  startHearthProactiveSendServer,
  parseControlPlaneConfig,
} = require("../src/server/hearth-proactive-send-server");
const { createHearthChannelOnlyObserver } = require("../src/app/hearth-channel-only");
const path = require("path");
const os = require("os");
const fs = require("fs");

function createLogger() {
  const lines = [];
  return {
    lines,
    info(line) { lines.push(String(line)); },
    log(line) { lines.push(String(line)); },
  };
}

function createFakeAdapter({
  accountId = "acct-1",
  tokens = { "owner-user": "ctx-token-abc" },
  sendImpl,
  resolveThrows = false,
} = {}) {
  const calls = { sendText: 0, payloads: [] };
  return {
    calls,
    resolveAccount() {
      if (resolveThrows) throw new Error("account boom");
      return { accountId, userId: "bot-user", baseUrl: "https://example.invalid", token: "acct-token" };
    },
    getKnownContextTokens() {
      return { ...tokens };
    },
    async sendText(payload) {
      calls.sendText += 1;
      calls.payloads.push(payload);
      if (typeof sendImpl === "function") {
        return sendImpl(payload);
      }
    },
  };
}

async function withServer(handler, run) {
  const server = http.createServer((req, res) => {
    Promise.resolve(handler(req, res)).catch(() => {
      if (!res.headersSent) {
        res.writeHead(500);
        res.end("{}");
      }
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();
  try {
    await run({ port, baseUrl: `http://127.0.0.1:${port}` });
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

async function postSend({
  baseUrl,
  token,
  body,
  headers = {},
}) {
  const response = await fetch(`${baseUrl}/internal/send`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...headers,
    },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }
  return { status: response.status, json, text };
}

const CONTROL_TOKEN = "test-control-token-not-real";

function validBody(overrides = {}) {
  return {
    request_id: `active-line-smoke-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    target: "default_owner",
    text: "Active Line v0：小狼已从 The Hearth 敲到微信窗边。",
    source: "dylan_manual_smoke",
    ...overrides,
  };
}

test("parseControlPlaneConfig requires independent control token and fixed localhost host", () => {
  assert.throws(() => parseControlPlaneConfig({}), /HEARTH_WEIXIN_CONTROL_TOKEN/);
  const parsed = parseControlPlaneConfig({
    HEARTH_WEIXIN_CONTROL_TOKEN: "abc",
    HEARTH_WEIXIN_CONTROL_PORT: "3200",
  });
  assert.equal(parsed.host, "127.0.0.1");
  assert.equal(parsed.port, 3200);
});

test("no Authorization → unauthorized", async () => {
  const adapter = createFakeAdapter();
  const logger = createLogger();
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: ["owner-user"] },
    controlToken: CONTROL_TOKEN,
    logger,
  });
  await withServer(handler, async ({ baseUrl }) => {
    const result = await postSend({ baseUrl, body: validBody() });
    assert.equal(result.json.status, "unauthorized");
    assert.equal(result.status, 401);
    assert.equal(adapter.calls.sendText, 0);
  });
});

test("wrong token → unauthorized", async () => {
  const adapter = createFakeAdapter();
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: ["owner-user"] },
    controlToken: CONTROL_TOKEN,
    logger: createLogger(),
  });
  await withServer(handler, async ({ baseUrl }) => {
    const result = await postSend({
      baseUrl,
      token: "wrong-token",
      body: validBody(),
    });
    assert.equal(result.json.status, "unauthorized");
    assert.equal(adapter.calls.sendText, 0);
  });
});

test("missing request_id → invalid_request", async () => {
  const adapter = createFakeAdapter();
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: ["owner-user"] },
    controlToken: CONTROL_TOKEN,
    logger: createLogger(),
  });
  await withServer(handler, async ({ baseUrl }) => {
    const body = validBody();
    delete body.request_id;
    const result = await postSend({ baseUrl, token: CONTROL_TOKEN, body });
    assert.equal(result.json.status, "invalid_request");
    assert.equal(adapter.calls.sendText, 0);
  });
});

test("empty text → invalid_request", async () => {
  const adapter = createFakeAdapter();
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: ["owner-user"] },
    controlToken: CONTROL_TOKEN,
    logger: createLogger(),
  });
  await withServer(handler, async ({ baseUrl }) => {
    const result = await postSend({
      baseUrl,
      token: CONTROL_TOKEN,
      body: validBody({ text: "   " }),
    });
    assert.equal(result.json.status, "invalid_request");
    assert.equal(adapter.calls.sendText, 0);
  });
});

test("adapter not ready → sidecar_not_ready", async () => {
  const adapter = createFakeAdapter();
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: ["owner-user"] },
    controlToken: CONTROL_TOKEN,
    isReady: () => false,
    logger: createLogger(),
  });
  await withServer(handler, async ({ baseUrl }) => {
    const result = await postSend({
      baseUrl,
      token: CONTROL_TOKEN,
      body: validBody({ request_id: "ready-check-1" }),
    });
    assert.equal(result.json.status, "sidecar_not_ready");
    assert.equal(adapter.calls.sendText, 0);
  });
});

test("default owner missing → sender_not_found", async () => {
  const adapter = createFakeAdapter({ tokens: {} });
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: [] },
    controlToken: CONTROL_TOKEN,
    logger: createLogger(),
  });
  await withServer(handler, async ({ baseUrl }) => {
    const result = await postSend({
      baseUrl,
      token: CONTROL_TOKEN,
      body: validBody({ request_id: "sender-missing-1" }),
    });
    assert.equal(result.json.status, "sender_not_found");
    assert.equal(adapter.calls.sendText, 0);
  });
});

test("context_token missing → context_token_missing", async () => {
  const adapter = createFakeAdapter({ tokens: {} });
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: ["owner-user"] },
    controlToken: CONTROL_TOKEN,
    logger: createLogger(),
  });
  await withServer(handler, async ({ baseUrl }) => {
    const result = await postSend({
      baseUrl,
      token: CONTROL_TOKEN,
      body: validBody({ request_id: "token-missing-1" }),
    });
    assert.equal(result.json.status, "context_token_missing");
    assert.equal(adapter.calls.sendText, 0);
  });
});

test("valid request calls adapter.sendText and returns assistant_sent_at only after success", async () => {
  let resolveSend;
  const sendGate = new Promise((resolve) => { resolveSend = resolve; });
  let enteredSend;
  const enteredSendPromise = new Promise((resolve) => { enteredSend = resolve; });
  const adapter = createFakeAdapter({
    sendImpl: async () => {
      enteredSend();
      await sendGate;
    },
  });
  const logger = createLogger();
  const fixedNow = () => Date.parse("2026-07-14T07:00:00.000Z");
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: ["owner-user"] },
    controlToken: CONTROL_TOKEN,
    now: fixedNow,
    logger,
  });
  await withServer(handler, async ({ baseUrl }) => {
    const pending = postSend({
      baseUrl,
      token: CONTROL_TOKEN,
      body: validBody({ request_id: "happy-path-1" }),
    });
    // 等真正进入 sendText，再证明成功时间戳只能在底层完成后签发。
    await enteredSendPromise;
    assert.equal(adapter.calls.sendText, 1);
    resolveSend();
    const result = await pending;
    assert.equal(result.json.ok, true);
    assert.equal(result.json.status, "sent");
    assert.equal(result.json.request_id, "happy-path-1");
    assert.equal(result.json.assistant_sent_at, "2026-07-14T07:00:00.000Z");
    assert.equal(adapter.calls.payloads[0].userId, "owner-user");
    assert.equal(adapter.calls.payloads[0].contextToken, "ctx-token-abc");
    assert.match(adapter.calls.payloads[0].text, /Active Line v0/);
    // 未传 preserveBlock：保持默认 false，普通分段路径不被关掉。
    assert.equal(adapter.calls.payloads[0].preserveBlock, false);
  });
});

test("preserveBlock true is forwarded; omitted field stays default false", async () => {
  const adapter = createFakeAdapter();
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: ["owner-user"] },
    controlToken: CONTROL_TOKEN,
    logger: createLogger(),
  });
  await withServer(handler, async ({ baseUrl }) => {
    const omitted = await postSend({
      baseUrl,
      token: CONTROL_TOKEN,
      body: validBody({ request_id: "preserve-default-1" }),
    });
    assert.equal(omitted.json.status, "sent");
    assert.equal(adapter.calls.payloads[0].preserveBlock, false);

    const explicit = await postSend({
      baseUrl,
      token: CONTROL_TOKEN,
      body: validBody({ request_id: "preserve-true-1", preserveBlock: true }),
    });
    assert.equal(explicit.json.status, "sent");
    assert.equal(adapter.calls.payloads[1].preserveBlock, true);
  });
});

test("sendText failure → weixin_send_failed without assistant_sent_at", async () => {
  const adapter = createFakeAdapter({
    sendImpl: async () => {
      throw new Error("weixin boom");
    },
  });
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: ["owner-user"] },
    controlToken: CONTROL_TOKEN,
    logger: createLogger(),
  });
  await withServer(handler, async ({ baseUrl }) => {
    const result = await postSend({
      baseUrl,
      token: CONTROL_TOKEN,
      body: validBody({ request_id: "fail-path-1" }),
    });
    assert.equal(result.json.ok, false);
    assert.equal(result.json.status, "weixin_send_failed");
    assert.equal(result.json.assistant_sent_at, undefined);
    assert.equal(adapter.calls.sendText, 1);
  });
});

test("same request_id second call does not call sendText again", async () => {
  const adapter = createFakeAdapter();
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: ["owner-user"] },
    controlToken: CONTROL_TOKEN,
    logger: createLogger(),
  });
  await withServer(handler, async ({ baseUrl }) => {
    const body = validBody({ request_id: "dedupe-1" });
    const first = await postSend({ baseUrl, token: CONTROL_TOKEN, body });
    const second = await postSend({ baseUrl, token: CONTROL_TOKEN, body });
    assert.equal(first.json.status, "sent");
    assert.equal(second.json.status, "duplicate_request");
    assert.equal(adapter.calls.sendText, 1);
  });
});

test("concurrent same request_id only allows one sendText", async () => {
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const adapter = createFakeAdapter({
    sendImpl: async () => gate,
  });
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: ["owner-user"] },
    controlToken: CONTROL_TOKEN,
    logger: createLogger(),
  });
  await withServer(handler, async ({ baseUrl }) => {
    const body = validBody({ request_id: "concurrent-1" });
    const p1 = postSend({ baseUrl, token: CONTROL_TOKEN, body });
    // 给第一个请求一点时间进入 inflight
    await new Promise((r) => setTimeout(r, 20));
    const p2 = postSend({ baseUrl, token: CONTROL_TOKEN, body });
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(adapter.calls.sendText, 1);
    release();
    const [r1, r2] = await Promise.all([p1, p2]);
    const statuses = [r1.json.status, r2.json.status].sort();
    assert.deepEqual(statuses, ["duplicate_request", "sent"]);
    assert.equal(adapter.calls.sendText, 1);
  });
});

test("response and logs never contain control token or context token", async () => {
  const adapter = createFakeAdapter();
  const logger = createLogger();
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: ["owner-user"] },
    controlToken: CONTROL_TOKEN,
    logger,
  });
  await withServer(handler, async ({ baseUrl }) => {
    const result = await postSend({
      baseUrl,
      token: CONTROL_TOKEN,
      body: validBody({ request_id: "safe-log-1" }),
    });
    const blob = `${result.text}\n${logger.lines.join("\n")}`;
    assert.equal(blob.includes(CONTROL_TOKEN), false);
    assert.equal(blob.includes("ctx-token-abc"), false);
    assert.equal(blob.includes("Bearer"), false);
    assert.equal(result.json.status, "sent");
  });
});

test("failed request_id stays rejected within TTL (no second send)", async () => {
  const adapter = createFakeAdapter({
    sendImpl: async () => {
      throw new Error("fail once");
    },
  });
  const deduper = createProactiveSendDeduper({ ttlMs: 60_000, now: () => 1_000_000 });
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: ["owner-user"] },
    controlToken: CONTROL_TOKEN,
    deduper,
    logger: createLogger(),
  });
  await withServer(handler, async ({ baseUrl }) => {
    const body = validBody({ request_id: "fail-dedupe-1" });
    const first = await postSend({ baseUrl, token: CONTROL_TOKEN, body });
    const second = await postSend({ baseUrl, token: CONTROL_TOKEN, body });
    assert.equal(first.json.status, "weixin_send_failed");
    assert.equal(second.json.status, "duplicate_request");
    assert.equal(adapter.calls.sendText, 1);
  });
});

test("startHearthProactiveSendServer binds 127.0.0.1 and fails clearly when port busy", async () => {
  const blocker = http.createServer((_req, res) => res.end("busy"));
  await new Promise((resolve) => blocker.listen(0, "127.0.0.1", resolve));
  const busyPort = blocker.address().port;
  const adapter = createFakeAdapter();
  let started;
  try {
    started = startHearthProactiveSendServer({
      adapter,
      config: { allowedUserIds: ["owner-user"] },
      env: {
        HEARTH_WEIXIN_CONTROL_TOKEN: CONTROL_TOKEN,
        HEARTH_WEIXIN_CONTROL_PORT: String(busyPort),
      },
      logger: createLogger(),
    });
    await assert.rejects(() => started.ready, (error) => error?.code === "EADDRINUSE");
  } finally {
    if (started?.server) {
      try { started.server.close(); } catch { /* ignore */ }
    }
    await new Promise((resolve) => blocker.close(resolve));
  }
});

test("original channel-only inbound observe still works alongside proactive handler module", async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "cyberboss-proactive-inbound-"));
  const calls = { normalize: 0, sendText: 0 };
  const adapter = {
    normalizeIncomingMessage(message) {
      calls.normalize += 1;
      return {
        text: message.text || "",
        attachments: [],
        contextToken: message.context_token || "",
      };
    },
    async sendText() { calls.sendText += 1; },
    async sendTyping() {},
  };
  const observer = createHearthChannelOnlyObserver({
    adapter,
    config: { stateDir: directory },
    env: { HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user", HEARTH_WEIXIN_MODE: "observe" },
    logger: createLogger(),
  });
  const result = await observer.processRawMessage({
    from_user_id: "allowed-user",
    message_id: "42",
    seq: 1,
    client_id: "c1",
    create_time_ms: 1000,
    text: "hello",
    context_token: "tok",
  }, { accountId: "account-1" });
  assert.equal(result.status, "observed");
  assert.equal(calls.normalize, 1);
  assert.equal(calls.sendText, 0);
});
