const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const http = require("http");
const os = require("os");
const path = require("path");

const {
  createHearthChannelStateStore,
} = require("../src/adapters/channel/weixin/hearth-channel-state-store");
const {
  createProactiveSendRequestHandler,
} = require("../src/server/hearth-proactive-send-server");
const {
  createHearthChannelOnlyObserver,
  resolveEventTime,
} = require("../src/app/hearth-channel-only");

const CONTROL_TOKEN = "test-channel-state-token";

function tempStateDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "hearth-channel-state-"));
}

function createLogger() {
  const lines = [];
  return {
    lines,
    info(line) { lines.push(String(line)); },
    log(line) { lines.push(String(line)); },
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

test("create_time_ms converts to millisecond event_time", () => {
  const resolved = resolveEventTime({ create_time_ms: 1_720_960_000_123 }, 9);
  assert.equal(resolved.value, 1_720_960_000_123);
  assert.equal(resolved.source, "create_time_ms");
});

test("missing create_time_ms uses second-level create_time", () => {
  const resolved = resolveEventTime({ create_time: 1_720_960_000 }, 9);
  assert.equal(resolved.value, 1_720_960_000_000);
  assert.equal(resolved.source, "create_time");
});

test("store advances last_user_event_time from event_time and keeps bridge separate", () => {
  const dir = tempStateDir();
  const store = createHearthChannelStateStore({
    config: { accountsDir: dir },
    accountId: "acct-1",
    now: () => Date.parse("2026-07-14T08:00:00.000Z"),
  });
  store.recordInboundUserEvent({
    eventTimeMs: Date.parse("2026-07-14T07:20:30.123Z"),
    eventTimeSource: "create_time_ms",
    bridgeReceivedAt: "2026-07-14T07:20:31.010Z",
    messageId: "m1",
  });
  const snap = store.getSnapshot();
  assert.equal(snap.last_user_event_time, "2026-07-14T07:20:30.123Z");
  assert.equal(snap.last_bridge_received_at, "2026-07-14T07:20:31.010Z");
  assert.notEqual(snap.last_user_event_time, snap.last_bridge_received_at);
});

test("delayed older message does not rewind last_user_event_time", () => {
  const dir = tempStateDir();
  const store = createHearthChannelStateStore({
    config: { accountsDir: dir },
    accountId: "acct-1",
    now: () => 1_000_000,
  });
  store.recordInboundUserEvent({
    eventTimeMs: Date.parse("2026-07-14T12:05:00.000Z"),
    eventTimeSource: "create_time_ms",
    bridgeReceivedAt: "2026-07-14T12:05:01.000Z",
    messageId: "newer",
  });
  store.recordInboundUserEvent({
    eventTimeMs: Date.parse("2026-07-14T12:00:00.000Z"),
    eventTimeSource: "create_time_ms",
    bridgeReceivedAt: "2026-07-14T12:06:00.000Z",
    messageId: "delayed-old",
  });
  const snap = store.getSnapshot();
  assert.equal(snap.last_user_event_time, "2026-07-14T12:05:00.000Z");
  assert.equal(snap.last_bridge_received_at, "2026-07-14T12:06:00.000Z");
  assert.equal(snap.last_message_id, "delayed-old");
});

test("fallback event_time source does not set last_user_event_time", () => {
  const dir = tempStateDir();
  const store = createHearthChannelStateStore({
    config: { accountsDir: dir },
    accountId: "acct-1",
    now: () => 1_000_000,
  });
  store.recordInboundUserEvent({
    eventTimeMs: Date.parse("2026-07-14T12:00:00.000Z"),
    eventTimeSource: "fallback",
    bridgeReceivedAt: "2026-07-14T12:00:00.000Z",
    messageId: "fb",
  });
  const snap = store.getSnapshot();
  assert.equal(snap.last_user_event_time, "");
  assert.equal(snap.last_bridge_received_at, "2026-07-14T12:00:00.000Z");
});

test("duplicate inbound does not update channel state", async () => {
  const dir = tempStateDir();
  const store = createHearthChannelStateStore({
    config: { accountsDir: path.join(dir, "accounts") },
    accountId: "acct-1",
    now: () => 1_000_000,
  });
  const adapter = {
    normalizeIncomingMessage(message) {
      return { text: message.text || "hi", attachments: [], contextToken: "tok" };
    },
    async sendText() {},
    async sendTyping() {},
  };
  const observer = createHearthChannelOnlyObserver({
    adapter,
    config: { stateDir: dir },
    channelStateStore: store,
    env: { HEARTH_WEIXIN_ALLOWED_USER_IDS: "owner-user", HEARTH_WEIXIN_MODE: "observe" },
    logger: createLogger(),
    now: () => Date.parse("2026-07-14T12:00:00.000Z"),
  });
  const msg = {
    from_user_id: "owner-user",
    message_id: "dup-1",
    seq: 1,
    client_id: "c1",
    create_time_ms: Date.parse("2026-07-14T11:00:00.000Z"),
    text: "hello private body",
    context_token: "tok",
  };
  const first = await observer.processRawMessage(msg, { accountId: "acct-1" });
  const mid = store.getSnapshot().last_user_event_time;
  const second = await observer.processRawMessage(msg, { accountId: "acct-1" });
  assert.equal(first.status, "observed");
  assert.equal(second.status, "duplicate");
  assert.equal(store.getSnapshot().last_user_event_time, mid);
});

test("non-allowlisted user does not update channel state", async () => {
  const dir = tempStateDir();
  const store = createHearthChannelStateStore({
    config: { accountsDir: path.join(dir, "accounts") },
    accountId: "acct-1",
  });
  const adapter = {
    normalizeIncomingMessage() { return { text: "x", attachments: [], contextToken: "t" }; },
    async sendText() {},
    async sendTyping() {},
  };
  const observer = createHearthChannelOnlyObserver({
    adapter,
    config: { stateDir: dir },
    channelStateStore: store,
    env: { HEARTH_WEIXIN_ALLOWED_USER_IDS: "owner-user" },
    logger: createLogger(),
  });
  const result = await observer.processRawMessage({
    from_user_id: "stranger",
    message_id: "m9",
    seq: 1,
    create_time_ms: Date.parse("2026-07-14T11:00:00.000Z"),
    text: "nope",
  }, { accountId: "acct-1" });
  assert.equal(result.status, "denied");
  assert.equal(store.getSnapshot().last_user_event_time, "");
});

test("channel state reloads after process restart", () => {
  const dir = tempStateDir();
  const accountsDir = path.join(dir, "accounts");
  const first = createHearthChannelStateStore({
    config: { accountsDir },
    accountId: "acct-1",
    now: () => 1,
  });
  first.recordInboundUserEvent({
    eventTimeMs: Date.parse("2026-07-14T10:00:00.000Z"),
    eventTimeSource: "create_time",
    bridgeReceivedAt: "2026-07-14T10:00:01.000Z",
    messageId: "persist-1",
  });
  const second = createHearthChannelStateStore({
    config: { accountsDir },
    accountId: "acct-1",
  });
  const snap = second.getSnapshot();
  assert.equal(snap.last_user_event_time, "2026-07-14T10:00:00.000Z");
  assert.equal(snap.last_message_id, "persist-1");
});

test("GET /internal/channel-state auth and redaction", async () => {
  const dir = tempStateDir();
  const store = createHearthChannelStateStore({
    config: { accountsDir: dir },
    accountId: "acct-1",
  });
  store.recordInboundUserEvent({
    eventTimeMs: Date.parse("2026-07-14T09:00:00.000Z"),
    eventTimeSource: "create_time_ms",
    bridgeReceivedAt: "2026-07-14T09:00:02.000Z",
    messageId: "safe-1",
  });
  const adapter = {
    resolveAccount() { return { accountId: "acct-1" }; },
    getKnownContextTokens() { return { "owner-user": "super-secret-context-token" }; },
    async sendText() {},
  };
  const logger = createLogger();
  const handler = createProactiveSendRequestHandler({
    adapter,
    config: { allowedUserIds: ["owner-user"] },
    controlToken: CONTROL_TOKEN,
    channelStateStore: store,
    isReady: () => true,
    logger,
  });
  await withServer(handler, async ({ baseUrl }) => {
    const denied = await fetch(`${baseUrl}/internal/channel-state`);
    const deniedJson = await denied.json();
    assert.equal(deniedJson.status, "unauthorized");

    const okRes = await fetch(`${baseUrl}/internal/channel-state`, {
      headers: { Authorization: `Bearer ${CONTROL_TOKEN}` },
    });
    const json = await okRes.json();
    assert.equal(json.ok, true);
    assert.equal(json.ready, true);
    assert.equal(json.has_context_token, true);
    assert.equal(json.last_user_event_time, "2026-07-14T09:00:00.000Z");
    assert.equal(json.last_bridge_received_at, "2026-07-14T09:00:02.000Z");
    const blob = `${JSON.stringify(json)}\n${logger.lines.join("\n")}`;
    assert.equal(blob.includes("super-secret-context-token"), false);
    assert.equal(blob.includes(CONTROL_TOKEN), false);
    assert.equal(blob.includes("hello"), false);
  });
});
