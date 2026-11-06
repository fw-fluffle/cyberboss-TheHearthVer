const test = require("node:test");
const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs/promises");
const path = require("node:path");
const { createHearthBridgeClient } = require("../src/core/hearth-bridge-client");
const { createHearthChannelOnlyObserver } = require("../src/app/hearth-channel-only");

const images = {
  gif: Buffer.from("R0lGODlhAgACAIEAAP9/UAAAAAAAAAAAACH/C05FVFNDQVBFMi4wAwEAAAAh+QQADAAAACwAAAAAAgACAAAIBgABCAQQEAAh+QQBDAABACwAAAAAAgACAIH/1wAAAAAAAAAAAAAIBgABCAQQEAA7", "base64"),
  png: Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAIAAAD91JpzAAAAEklEQVR4nGP8Xx/AwMDAxAAGABaBAdIpmcarAAAAAElFTkSuQmCC", "base64"),
};
const env = {
  HEARTH_COMPANION_URL: "http://127.0.0.1:3210/internal/channel/companion",
  HEARTH_INTERNAL_TOKEN: "test-internal-token",
  HEARTH_BRIDGE_PSEUDONYM_SECRET: "test-pseudonym-secret",
  HEARTH_WEIXIN_STICKER_DELIVERY: "1",
  HEARTH_WEIXIN_MODE: "bridge",
  HEARTH_WEIXIN_ALLOWED_USER_IDS: "fixture-user",
  HEARTH_INBOUND_BUFFER_SECONDS: "0",
};

async function exercise({
  segments = [], format = "gif", fault = "", deliveryEnabled = true,
  dryRun = false, includeSticker = true, replay = false,
} = {}) {
  const image = images[format];
  const digest = crypto.createHash("sha256").update(image).digest("hex");
  const sticker = { id: "small_rabbit", mime_type: `image/${format}`, sha256: digest,
    ticket: `1800000000.${digest}.${format}.${"c".repeat(64)}` };
  const requests = [], sent = [], ledger = [];
  const seen = new Set();
  let file;
  const runEnv = { ...env, HEARTH_WEIXIN_STICKER_DELIVERY: deliveryEnabled ? "1" : "0",
    HEARTH_WEIXIN_MODE: dryRun ? "bridge_dry_run" : "bridge" };
  const bridge = createHearthBridgeClient({ env: runEnv, fetchImpl: async (url, options) => {
    requests.push(url.pathname);
    assert.equal(url.hostname, "127.0.0.1");
    if (url.pathname === "/internal/channel/runtime-config") {
      return Response.json({ inbound_buffer_seconds: 0, bubble_mode: "natural" });
    }
    if (url.pathname === "/internal/channel/companion") {
      assert.equal(options.headers["X-Hearth-Sticker-Delivery"], deliveryEnabled ? "v1" : undefined);
      const body = { action: "send", reply_text: segments.join("\n\n"), segments };
      if (includeSticker) body.sticker = fault === "bad_id" ? { ...sticker, id: "../anywhere" } : sticker;
      return Response.json(body);
    }
    assert.equal(url.pathname, "/internal/channel/stickers/small_rabbit/image");
    assert.equal(url.search, "");
    assert.equal(options.redirect, "error");
    assert.equal(options.headers.Authorization, "Bearer test-internal-token");
    assert.equal(options.headers["X-Hearth-Sticker-Ticket"], sticker.ticket);
    return new Response(fault === "wrong_digest" ? Buffer.from("other image") : image,
      { headers: { "Content-Type": sticker.mime_type } });
  } });
  const observer = createHearthChannelOnlyObserver({
    bridgeClient: bridge, env: runEnv, logger: { info() {} },
    sleepFn: async ms => { sent.push(ms); },
    idempotencyStore: {
      has: key => seen.has(key),
      mark(key, value) { seen.add(key); ledger.push(value.status); },
    },
    adapter: {
      normalizeIncomingMessage: () => ({ text: "fixture input", contextToken: "fixture-context" }),
      sendTyping: async () => {},
      sendText: async ({ text, preserveBlock }) => {
        assert.equal(preserveBlock, true);
        sent.push(text);
      },
      sendFile: async ({ filePath, userId, contextToken }) => {
        file = filePath;
        assert.equal(userId, "fixture-user");
        assert.equal(contextToken, "fixture-context");
        assert.equal(path.extname(filePath), "." + format);
        assert.deepEqual(await fs.readFile(filePath), image);
        if (fault === "adapter") throw new Error("fixture failure");
        sent.push(format.toUpperCase());
      },
    },
  });
  const incoming = { from_user_id: "fixture-user", message_id: "fixture-01",
    create_time_ms: 1800000000000, text: "fixture input" };
  const account = { accountId: "fixture-account" };
  const result = await observer.processRawMessage(incoming, account);
  const repeated = replay ? await observer.processRawMessage(incoming, account) : null;
  if (file) await assert.rejects(fs.stat(path.dirname(file)), { code: "ENOENT" });
  return { result, repeated, requests, sent, ledger };
}

test("a pure GIF and a PNG after two paced text bubbles reach the adapter intact", async () => {
  for (const [options, expected] of [
    [{}, ["GIF"]],
    [{ segments: ["接到了。", "过来抱抱。"], format: "png" }, ["接到了。", 650, "过来抱抱。", "PNG"]],
  ]) {
    const { result, requests, sent, ledger } = await exercise(options);
    assert.equal(result.status, "delivered");
    assert.deepEqual(sent, expected);
    assert.equal(requests.filter(url => url.endsWith("/image")).length, 1);
    assert.deepEqual(ledger, ["delivered"]);
  }
});

test("the existing text cadence stays active with the sticker switch off", async () => {
  const { result, requests, sent, ledger } = await exercise({
    segments: ["第一泡", "第二泡", "第三泡"], deliveryEnabled: false, includeSticker: false,
  });
  assert.equal(result.status, "delivered");
  assert.deepEqual(sent, ["第一泡", 650, "第二泡", 650, "第三泡"]);
  assert.equal(requests.filter(url => url.endsWith("/image")).length, 0);
  assert.deepEqual(ledger, ["delivered"]);
});

test("a failed sticker preserves text and does not resend the same reply", async () => {
  for (const fault of ["wrong_digest", "adapter"]) {
    const { result, repeated, requests, sent, ledger } = await exercise({
      segments: ["第一泡", "第二泡"], fault, replay: true,
    });
    assert.equal(result.status, "partial_delivery");
    assert.ok(result.event.assistant_sent_at);
    assert.deepEqual(sent, ["第一泡", 650, "第二泡"]);
    assert.deepEqual(ledger, ["partial_delivery"]);
    assert.equal(repeated.status, "duplicate");
    assert.equal(requests.filter(url => url.endsWith("/companion")).length, 1);
  }
});

test("unexpected, unverified or unsuccessfully sent stickers do not record delivery", async () => {
  for (const options of [{ deliveryEnabled: false }, { fault: "bad_id" }, { fault: "wrong_digest" }, { fault: "adapter" }]) {
    const { result, sent, ledger } = await exercise(options);
    assert.ok(["bridge_failed", "delivery_failed"].includes(result.status));
    assert.deepEqual(sent, []);
    assert.deepEqual(ledger, []);
  }
  const { result, sent, ledger } = await exercise({ dryRun: true });
  assert.equal(result.status, "bridged_dry_run");
  assert.deepEqual(sent, []);
  assert.deepEqual(ledger, ["bridged_dry_run"]);
});
