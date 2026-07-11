const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const MAX_ENTRY_AGE_MS = 24 * 60 * 60 * 1000;
const MAX_ENTRIES = 1000;

class ChannelIdempotencyStore {
  constructor({ filePath, now = () => Date.now(), onWarning = () => {} } = {}) {
    if (!filePath) {
      throw new Error("ChannelIdempotencyStore requires filePath");
    }
    this.filePath = filePath;
    this.now = now;
    this.onWarning = onWarning;
    this.entries = new Map();
    this.load();
  }

  has(key) {
    this.prune();
    return this.entries.has(String(key || ""));
  }

  mark(key, { status = "observed", recordedAtMs = this.now() } = {}) {
    const normalizedKey = String(key || "").trim();
    if (!normalizedKey) {
      return false;
    }
    this.entries.set(normalizedKey, {
      recordedAtMs: normalizeTime(recordedAtMs, this.now()),
      status: normalizeStatus(status),
    });
    this.prune();
    this.persist();
    return true;
  }

  snapshot() {
    this.prune();
    return Array.from(this.entries.entries()).map(([key, value]) => ({ key, ...value }));
  }

  load() {
    let raw = "";
    try {
      raw = fs.readFileSync(this.filePath, "utf8");
    } catch (error) {
      if (error?.code !== "ENOENT") {
        this.warn("idempotency ledger could not be read; using an empty ledger");
      }
      return;
    }

    try {
      const parsed = JSON.parse(raw);
      const entries = Array.isArray(parsed?.entries) ? parsed.entries : [];
      for (const entry of entries) {
        const key = typeof entry?.key === "string" ? entry.key.trim() : "";
        const recordedAtMs = normalizeTime(entry?.recordedAtMs, 0);
        if (!key || !recordedAtMs) {
          continue;
        }
        this.entries.set(key, { recordedAtMs, status: normalizeStatus(entry?.status) });
      }
      if (this.prune()) {
        this.persist();
      }
    } catch {
      this.entries.clear();
      this.warn("idempotency ledger is invalid; using an empty ledger");
    }
  }

  prune() {
    let changed = false;
    const cutoff = this.now() - MAX_ENTRY_AGE_MS;
    for (const [key, entry] of this.entries.entries()) {
      if (!entry || entry.recordedAtMs < cutoff) {
        this.entries.delete(key);
        changed = true;
      }
    }
    if (this.entries.size <= MAX_ENTRIES) {
      return changed;
    }
    const retained = Array.from(this.entries.entries())
      .sort((left, right) => right[1].recordedAtMs - left[1].recordedAtMs)
      .slice(0, MAX_ENTRIES);
    this.entries = new Map(retained);
    return true;
  }

  persist() {
    const directory = path.dirname(this.filePath);
    fs.mkdirSync(directory, { recursive: true });
    const contents = JSON.stringify({
      version: 1,
      entries: this.snapshot(),
    }, null, 2);
    const temporaryPath = path.join(
      directory,
      `.${path.basename(this.filePath)}.${process.pid}.${crypto.randomUUID()}.tmp`,
    );
    try {
      fs.writeFileSync(temporaryPath, contents, { encoding: "utf8", mode: 0o600 });
      try {
        fs.chmodSync(temporaryPath, 0o600);
      } catch {
        // Windows and restrictive filesystems may not support POSIX modes.
      }
      fs.renameSync(temporaryPath, this.filePath);
      try {
        fs.chmodSync(this.filePath, 0o600);
      } catch {
        // Best effort only.
      }
    } catch (error) {
      try {
        if (fs.existsSync(temporaryPath)) {
          fs.unlinkSync(temporaryPath);
        }
      } catch {
        // Best effort only.
      }
      throw error;
    }
  }

  warn(message) {
    try {
      this.onWarning(String(message || "idempotency ledger warning"));
    } catch {
      // Logging must not affect polling.
    }
  }
}

function hashIdempotencyKey(parts) {
  const source = Array.isArray(parts) ? parts.map((part) => String(part ?? "")).join("\u001f") : String(parts || "");
  return crypto.createHash("sha256").update(source).digest("hex");
}

function normalizeTime(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function normalizeStatus(value) {
  const status = typeof value === "string" ? value.trim() : "";
  return status || "observed";
}

module.exports = {
  ChannelIdempotencyStore,
  MAX_ENTRIES,
  MAX_ENTRY_AGE_MS,
  hashIdempotencyKey,
};
