const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");

const { ChannelIdempotencyStore, MAX_ENTRIES, MAX_ENTRY_AGE_MS } = require("../src/core/channel-idempotency-store");
const { buildChannelEnvelope, createHearthBridgeClient, pseudonym, validateBridgeResponse } = require("../src/core/hearth-bridge-client");
const {
  acquireSingleInstanceLock,
  createHearthChannelOnlyObserver,
  parseObserveConfig,
  resolveEventTime,
  sortRawMessages,
} = require("../src/app/hearth-channel-only");

function tempPath() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "cyberboss-hearth-observe-"));
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
    async sendText(payload) { calls.sendText += 1; calls.sendOrder.push("text"); calls.textPayloads.push(payload); },
    async sendTyping() { calls.sendTyping += 1; calls.sendOrder.push("typing"); },
  };
}

function createLogger() {
  const lines = [];
  return { lines, info(line) { lines.push(line); } };
}

function message(overrides = {}) {
  return {
    from_user_id: "allowed-user",
    message_id: "11",
    seq: 2,
    client_id: "client-a",
    create_time_ms: 1000,
    create_time: 1,
    text: "private message body",
    context_token: "private-context-token",
    ...overrides,
  };
}

test("observe configuration defaults to observe and deny all", () => {
  const parsed = parseObserveConfig({});
  assert.equal(parsed.mode, "observe");
  assert.equal(parsed.allowedUserIds.size, 0);
  assert.throws(() => parseObserveConfig({ HEARTH_WEIXIN_MODE: "send" }));
});

test("single-instance lock rejects a live holder and releases its own lock", () => {
  const lockPath = path.join(path.dirname(tempPath()), "sidecar.lock");
  const first = acquireSingleInstanceLock({
    filePath: lockPath,
    pid: 101,
    isProcessAlive: (pid) => pid === 101,
  });
  assert.throws(() => acquireSingleInstanceLock({
    filePath: lockPath,
    pid: 202,
    isProcessAlive: (pid) => pid === 101,
  }), /already running/);
  first.release();
  assert.equal(fs.existsSync(lockPath), false);
});

test("single-instance lock replaces a stale or corrupt lock", () => {
  const lockPath = path.join(path.dirname(tempPath()), "sidecar.lock");
  fs.writeFileSync(lockPath, "not-json", "utf8");
  const lock = acquireSingleInstanceLock({
    filePath: lockPath,
    pid: 303,
    isProcessAlive: () => false,
  });
  const stored = JSON.parse(fs.readFileSync(lockPath, "utf8"));
  assert.equal(stored.pid, 303);
  lock.release();
});

test("non-allowlisted messages are denied before normalization", async () => {
  const adapter = createAdapter();
  const observer = createHearthChannelOnlyObserver({ adapter, config: { stateDir: path.dirname(tempPath()) }, env: {}, logger: createLogger() });
  const result = await observer.processRawMessage(message(), { accountId: "account-1" });
  assert.equal(result.status, "denied");
  assert.equal(adapter.calls.normalize, 0);
});

test("allowlisted messages are observed without typing or text sends", async () => {
  const adapter = createAdapter();
  const logger = createLogger();
  const observer = createHearthChannelOnlyObserver({
    adapter,
    config: { stateDir: path.dirname(tempPath()) },
    env: { HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" },
    logger,
  });
  const result = await observer.processRawMessage(message(), { accountId: "account-1" });
  assert.equal(result.status, "observed");
  assert.equal(result.event.seq, 2);
  assert.equal(result.event.client_id, "client-a");
  assert.equal(adapter.calls.sendText, 0);
  assert.equal(adapter.calls.sendTyping, 0);
  assert.equal(logger.lines.length, 1);
});

test("echo mode sends typing then exactly the fixed bridge reply", async () => {
  const adapter = createAdapter();
  const observer = createHearthChannelOnlyObserver({
    adapter,
    config: { stateDir: path.dirname(tempPath()) },
    env: { HEARTH_WEIXIN_MODE: "echo", HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" },
    logger: createLogger(),
  });
  const result = await observer.processRawMessage(message(), { accountId: "account-1" });
  assert.equal(result.status, "echoed");
  assert.deepEqual(adapter.calls.sendOrder, ["typing", "text"]);
  assert.equal(adapter.calls.textPayloads[0].text, "bridge ok");
});

test("bridge envelope is stable, pseudonymous and excludes context token", () => {
  const event = { account_id: "account", sender_id: "sender", message_id: "m1", text: "private", event_time: 1000, bridge_received_at: new Date(1000).toISOString(), seq: 2, client_id: "c" };
  const envelope = buildChannelEnvelope(event, "secret");
  assert.equal(envelope.channel, "weixin"); assert.equal(envelope.bubble_count, 1); assert.equal(envelope.message_id, "m1");
  assert.ok(!JSON.stringify(envelope).includes("context_token")); assert.ok(!JSON.stringify(envelope).includes("account\""));
  assert.equal(pseudonym("sender", "secret", "wu"), envelope.sender_id); assert.notEqual(pseudonym("other", "secret", "wu"), envelope.sender_id);
});

test("bridge delivers validated segments once and persists delivered", async () => {
  const adapter = createAdapter(); const ledgerPath = tempPath(); let requests = 0;
  const observer = createHearthChannelOnlyObserver({ adapter, config: { stateDir: path.dirname(ledgerPath) }, env: { HEARTH_WEIXIN_MODE: "bridge", HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" }, logger: createLogger(), idempotencyStore: new ChannelIdempotencyStore({ filePath: ledgerPath }), bridgeClient: { async request() { requests += 1; return { segments: ["one", "two"] }; } } });
  assert.equal((await observer.processRawMessage(message(), { accountId: "account-1" })).status, "delivered");
  assert.deepEqual(adapter.calls.sendOrder, ["typing", "text", "text"]); assert.deepEqual(adapter.calls.textPayloads.map((p) => p.text), ["one", "two"]);
  assert.equal((await observer.processRawMessage(message(), { accountId: "account-1" })).status, "duplicate"); assert.equal(requests, 1);
  const restart = createHearthChannelOnlyObserver({ adapter: createAdapter(), config: { stateDir: path.dirname(ledgerPath) }, env: { HEARTH_WEIXIN_MODE: "bridge", HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" }, logger: createLogger(), idempotencyStore: new ChannelIdempotencyStore({ filePath: ledgerPath }), bridgeClient: { async request() { throw new Error("should not call"); } } });
  assert.equal((await restart.processRawMessage(message(), { accountId: "account-1" })).status, "duplicate");
});

test("bridge validation and failures never deliver partial invalid responses", async () => {
  assert.throws(() => validateBridgeResponse({ action: "send", reply_text: "", segments: [] }));
  assert.throws(() => validateBridgeResponse({ action: "silent", reply_text: "x", segments: ["x"] }));
  const adapter = createAdapter(); const observer = createHearthChannelOnlyObserver({ adapter, config: { stateDir: path.dirname(tempPath()) }, env: { HEARTH_WEIXIN_MODE: "bridge", HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" }, logger: createLogger(), bridgeClient: { async request() { throw Object.assign(new Error("timeout"), { code: "timeout" }); } } });
  assert.equal((await observer.processRawMessage(message(), { accountId: "account-1" })).status, "bridge_failed"); assert.equal(adapter.calls.sendText, 0);
});

test("bridge fails closed without secrets and stops after a segment send failure", async () => {
  assert.throws(() => createHearthBridgeClient({ env: {} }), /URL|token|secret|configured/i);
  const adapter = createAdapter(); let sent = 0;
  adapter.sendText = async (payload) => { sent += 1; adapter.calls.textPayloads.push(payload); if (sent === 2) throw new Error("private send failure"); };
  const observer = createHearthChannelOnlyObserver({ adapter, config: { stateDir: path.dirname(tempPath()) }, env: { HEARTH_WEIXIN_MODE: "bridge", HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" }, logger: createLogger(), bridgeClient: { async request() { return { segments: ["one", "two", "three"] }; } } });
  assert.equal((await observer.processRawMessage(message(), { accountId: "account-1" })).status, "delivery_failed");
  assert.equal(sent, 2);
});

test("echo mode never sends for non-allowlisted messages", async () => {
  const adapter = createAdapter();
  const observer = createHearthChannelOnlyObserver({
    adapter,
    config: { stateDir: path.dirname(tempPath()) },
    env: { HEARTH_WEIXIN_MODE: "echo" },
    logger: createLogger(),
  });
  assert.equal((await observer.processRawMessage(message(), { accountId: "account-1" })).status, "denied");
  assert.equal(adapter.calls.sendTyping, 0);
  assert.equal(adapter.calls.sendText, 0);
});

test("echo mode deduplicates in process and after ledger reload", async () => {
  const ledgerPath = tempPath();
  const env = { HEARTH_WEIXIN_MODE: "echo", HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" };
  const firstAdapter = createAdapter();
  const first = createHearthChannelOnlyObserver({
    adapter: firstAdapter,
    config: { stateDir: path.dirname(ledgerPath) }, env, logger: createLogger(),
    idempotencyStore: new ChannelIdempotencyStore({ filePath: ledgerPath }),
  });
  assert.equal((await first.processRawMessage(message(), { accountId: "account-1" })).status, "echoed");
  assert.equal((await first.processRawMessage(message(), { accountId: "account-1" })).status, "duplicate");
  const restartedAdapter = createAdapter();
  const restarted = createHearthChannelOnlyObserver({
    adapter: restartedAdapter,
    config: { stateDir: path.dirname(ledgerPath) }, env, logger: createLogger(),
    idempotencyStore: new ChannelIdempotencyStore({ filePath: ledgerPath }),
  });
  assert.equal((await restarted.processRawMessage(message(), { accountId: "account-1" })).status, "duplicate");
  assert.equal(firstAdapter.calls.sendText, 1);
  assert.equal(restartedAdapter.calls.sendText, 0);
});

test("echo sends fixed text after typing failure and does not mark failed text sends", async () => {
  const adapter = createAdapter();
  adapter.sendTyping = async () => { adapter.calls.sendTyping += 1; adapter.calls.sendOrder.push("typing"); throw new TypeError("private typing failure"); };
  const logger = createLogger();
  const observer = createHearthChannelOnlyObserver({
    adapter,
    config: { stateDir: path.dirname(tempPath()) },
    env: { HEARTH_WEIXIN_MODE: "echo", HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" },
    logger,
  });
  assert.equal((await observer.processRawMessage(message(), { accountId: "account-1" })).status, "echoed");
  assert.equal(adapter.calls.sendText, 1);
  assert.ok(logger.lines.some((line) => line.includes("typing_failed")));

  const failingAdapter = createAdapter();
  failingAdapter.sendText = async () => { failingAdapter.calls.sendText += 1; throw new Error("private send failure"); };
  const failing = createHearthChannelOnlyObserver({
    adapter: failingAdapter,
    config: { stateDir: path.dirname(tempPath()) },
    env: { HEARTH_WEIXIN_MODE: "echo", HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" },
    logger: createLogger(),
  });
  const failedMessage = message({ message_id: "failed" });
  assert.equal((await failing.processRawMessage(failedMessage, { accountId: "account-1" })).status, "echo_failed");
  assert.equal((await failing.processRawMessage(failedMessage, { accountId: "account-1" })).status, "echo_failed");
  assert.equal(failingAdapter.calls.sendText, 2);
});

test("a malformed message is isolated and does not stop later messages in the batch", async () => {
  const adapter = createAdapter();
  const observer = createHearthChannelOnlyObserver({
    adapter,
    config: { stateDir: path.dirname(tempPath()) },
    env: { HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" },
    logger: createLogger(),
  });
  const results = await observer.processUpdateResponse({
    msgs: [message({ from_user_id: "", message_id: "bad" }), message({ message_id: "good" })],
  }, { accountId: "account-1" });
  assert.deepEqual(results.map((result) => result.status), ["malformed", "observed"]);
});

test("raw messages sort by event time, seq, message id, then client id", () => {
  const sorted = sortRawMessages([
    message({ create_time_ms: 2000, seq: 1, message_id: "1", client_id: "a" }),
    message({ create_time_ms: 1000, seq: 2, message_id: "1", client_id: "a" }),
    message({ create_time_ms: 1000, seq: 1, message_id: "2", client_id: "a" }),
    message({ create_time_ms: 1000, seq: 1, message_id: "1", client_id: "z" }),
    message({ create_time_ms: 1000, seq: 1, message_id: "1", client_id: "a" }),
  ]);
  assert.deepEqual(sorted.map((item) => item.client_id), ["a", "z", "a", "a", "a"]);
  assert.deepEqual(sorted.map((item) => item.message_id), ["1", "1", "2", "1", "1"]);
  assert.deepEqual(sorted.map((item) => item.create_time_ms), [1000, 1000, 1000, 1000, 2000]);
});

test("create_time_ms takes precedence and fallback is marked", () => {
  assert.deepEqual(resolveEventTime({ create_time_ms: 5000, create_time: 2 }, 9000), { value: 5000, source: "create_time_ms" });
  assert.deepEqual(resolveEventTime({ create_time: 2 }, 9000), { value: 2000, source: "create_time" });
  assert.deepEqual(resolveEventTime({}, 9000), { value: 9000, source: "fallback" });
});

test("same message is observed once in process and after ledger reload", async () => {
  const ledgerPath = tempPath();
  const adapter = createAdapter();
  const env = { HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" };
  const logger = createLogger();
  const store = new ChannelIdempotencyStore({ filePath: ledgerPath });
  const observer = createHearthChannelOnlyObserver({ adapter, config: { stateDir: path.dirname(ledgerPath) }, env, logger, idempotencyStore: store });
  assert.equal((await observer.processRawMessage(message(), { accountId: "account-1" })).status, "observed");
  assert.equal((await observer.processRawMessage(message(), { accountId: "account-1" })).status, "duplicate");
  const restarted = createHearthChannelOnlyObserver({
    adapter: createAdapter(),
    config: { stateDir: path.dirname(ledgerPath) },
    env,
    logger: createLogger(),
    idempotencyStore: new ChannelIdempotencyStore({ filePath: ledgerPath }),
  });
  assert.equal((await restarted.processRawMessage(message(), { accountId: "account-1" })).status, "duplicate");
});

test("idempotency ledger prunes old entries and retains at most 1000", () => {
  let current = 2 * MAX_ENTRY_AGE_MS;
  const store = new ChannelIdempotencyStore({ filePath: tempPath(), now: () => current });
  store.mark("old", { recordedAtMs: current - MAX_ENTRY_AGE_MS - 1 });
  for (let index = 0; index < MAX_ENTRIES + 10; index += 1) {
    current += 1;
    store.mark(`key-${index}`, { recordedAtMs: current });
  }
  const entries = store.snapshot();
  assert.equal(entries.length, MAX_ENTRIES);
  assert.equal(store.has("old"), false);
});

test("corrupt idempotency ledger safely falls back to empty with a safe warning", () => {
  const ledgerPath = tempPath();
  fs.writeFileSync(ledgerPath, "not-json", "utf8");
  const warnings = [];
  const store = new ChannelIdempotencyStore({ filePath: ledgerPath, onWarning: (messageText) => warnings.push(messageText) });
  assert.equal(store.snapshot().length, 0);
  assert.equal(warnings.length, 1);
  assert.ok(!warnings[0].includes("not-json"));
});

test("observe logs redact body, context token and full identities", async () => {
  const adapter = createAdapter();
  const logger = createLogger();
  const observer = createHearthChannelOnlyObserver({
    adapter,
    config: { stateDir: path.dirname(tempPath()) },
    env: { HEARTH_WEIXIN_ALLOWED_USER_IDS: "allowed-user" },
    logger,
  });
  await observer.processRawMessage(message(), { accountId: "account-1" });
  const output = logger.lines.join("\n");
  assert.ok(!output.includes("private message body"));
  assert.ok(!output.includes("private-context-token"));
  assert.ok(!output.includes("allowed-user"));
  assert.ok(!output.includes("account-1"));
});

test("channel-only module does not load CyberbossApp or runtime/tooling modules", () => {
  const loaded = Object.keys(require.cache).join("\n");
  assert.ok(!loaded.includes("src/core/app.js"));
  assert.ok(!loaded.includes("src/adapters/runtime/codex"));
  assert.ok(!loaded.includes("src/adapters/runtime/claudecode"));
  assert.ok(!loaded.includes("src/tools/create-project-tooling"));
});

test("the existing default start entry remains unchanged", () => {
  const packageJson = JSON.parse(fs.readFileSync(path.resolve(__dirname, "..", "package.json"), "utf8"));
  assert.equal(packageJson.scripts.start, "node ./bin/cyberboss.js start");
  assert.equal(packageJson.scripts["hearth:channel-only"], "node ./src/app/hearth-channel-only.js");
});
