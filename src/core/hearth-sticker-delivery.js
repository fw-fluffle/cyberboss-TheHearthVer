const crypto = require("node:crypto");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");

const EXTENSIONS = { "image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif" };
const MAX_BYTES = 8 * 1024 * 1024;
const ID = /^[a-z][a-z0-9_]{0,39}$/;
const SHA = /^[a-f0-9]{64}$/;
const TICKET = /^\d{1,12}\.[a-f0-9]{64}\.(jpg|png|gif)\.[a-f0-9]{64}$/;
const HISTORY_RECEIPT = /^\d{1,12}\.[1-9]\d{0,18}\.[a-f0-9-]{36}\.[a-f0-9]{64}$/;

function invalidSticker() {
  return Object.assign(new Error("Bridge sticker delivery invalid"), { code: "invalid_sticker_delivery" });
}

function validateStickerDelivery(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)
      || !["id,mime_type,sha256,ticket", "history_receipt,id,mime_type,sha256,ticket"]
        .includes(Object.keys(value).sort().join(","))
      || typeof value.id !== "string" || !ID.test(value.id)
      || typeof value.sha256 !== "string" || !SHA.test(value.sha256)
      || typeof value.mime_type !== "string" || !Object.hasOwn(EXTENSIONS, value.mime_type)
      || typeof value.ticket !== "string" || !TICKET.test(value.ticket)
      || (Object.hasOwn(value, "history_receipt") && (typeof value.history_receipt !== "string"
          || !HISTORY_RECEIPT.test(value.history_receipt)))) throw invalidSticker();
  return { ...value };
}

async function readStickerBytes(response, sticker) {
  const length = Number(response.headers.get("content-length"));
  if (!response.ok || response.headers.get("content-type")?.split(";", 1)[0] !== sticker.mime_type
      || length > MAX_BYTES || !response.body?.getReader) throw invalidSticker();
  const reader = response.body.getReader(), chunks = [];
  const sha = crypto.createHash("sha256");
  let total = 0, ended = false;
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) { ended = true; break; }
      total += chunk.value.byteLength;
      if (total > MAX_BYTES) throw invalidSticker();
      const bytes = Buffer.from(chunk.value);
      sha.update(bytes);chunks.push(bytes);
    }
    if (!total || (length && total !== length) || sha.digest("hex") !== sticker.sha256) throw invalidSticker();
    return Buffer.concat(chunks, total);
  } finally {
    if (!ended) await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

async function withStickerFile(sticker, bytes, send) {
  const { mime_type: mime } = validateStickerDelivery(sticker);
  if (!Buffer.isBuffer(bytes) || !bytes.length || bytes.length > MAX_BYTES) throw invalidSticker();
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "hearth-sticker-"));
  const filePath = path.join(directory, "sticker" + EXTENSIONS[mime]);
  try {
    await fs.writeFile(filePath, bytes, { flag: "wx", mode: 0o600 });
    return await send(filePath);
  } finally {
    await fs.unlink(filePath).catch(() => {});
    await fs.rmdir(directory).catch(() => {});
  }
}

module.exports = { invalidSticker, validateStickerDelivery, readStickerBytes, withStickerFile };
