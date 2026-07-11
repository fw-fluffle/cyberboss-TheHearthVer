# Phase 2C-C1 Result

## 1. The Hearth Full Offline Regression

Command:

```text
cd D:\G 修建施工中
python -m unittest discover -s gateway -p "test_*.py"
git status -sb
```

Result of the requested discover command: **39 passed / 2 failed**

Failure cause (not a production logic regression):

- `discover -s gateway` puts `gateway/` on `sys.path`, so Gateway imports `channel_companion` as a top-level module while tests import `gateway.channel_companion`.
- Confirmed: `channel_companion is gateway.channel_companion` → `False`; idempotency caches are different objects.
- Endpoint tests reset the package cache, but the live endpoint uses the other module’s cache → flaky **409** collisions.

Package-path re-check (same suite as B1):

```text
python -m unittest gateway.test_channel_companion_endpoint \
  gateway.test_redacted_companion_diagnostics \
  gateway.test_persona_companion_seam \
  gateway.test_context_visibility_contract
```

Result: **41 passed / 0 failed**

Working tree: clean (`feat/active-line-v0` aligned with origin).  
The Hearth was **not modified** this round (read-only).  
Follow-up (future Hearth chore): unify imports or reset `gateway.hearth_gateway_alpha.CHANNEL_IDEMPOTENCY_CACHE` in tests.

## 2. Cyberboss Git Preflight

- Repo: `D:\cyberboss-TheHearthVer`
- Branch: `feat/hearth-weixin-sidecar-v0` (clean before changes; was ahead 4)
- Latest prior commit: `2a12612 feat: bridge channel-only sidecar to fake Hearth`
- No AGENTS.md / CLAUDE.md in Cyberboss; followed task rules + existing sidecar conventions

## 3. Existing Fake Hearth Seam

| Item | Actual |
|---|---|
| Client | `src/core/hearth-bridge-client.js` → `createHearthBridgeClient` |
| Sidecar | `src/app/hearth-channel-only.js` → `createHearthChannelOnlyObserver` |
| Envelope | `buildChannelEnvelope` |
| Fake stub server | `test/fake-hearth-server.js` (manual) |
| Allowlist | `HEARTH_WEIXIN_ALLOWED_USER_IDS` (empty = deny all) |
| Ledger | `src/core/channel-idempotency-store.js` |
| context_token | stays in Weixin adapter / sendTyping/sendText only |
| Delivery | `adapter.sendText` per segment |
| Seams | injectable `bridgeClient`, injectable `fetchImpl`, env URL |

## 4. Real Hearth Client

Upgraded the same client (no second bridge):

- URL: `HEARTH_COMPANION_URL` (preferred) or `HEARTH_BRIDGE_URL` (compat); auto-append `/internal/channel/companion`
- Token: `HEARTH_INTERNAL_TOKEN` (fail closed)
- Timeout: `HEARTH_REQUEST_TIMEOUT_MS`
- Auth: `Authorization: Bearer …`
- Local HTTP only (`127.0.0.1` / `localhost`) in v0
- HTTP status classification without echoing upstream bodies
- Example env template: `env.hearth-channel.example`

## 5. Envelope Mapping

Unchanged B1 Channel Envelope v0 mapping:

- `channel=weixin`, pseudonymous `account_id` / `sender_id` / `chat_id`, stable `thread_key`
- `message_id`, `text`, aware `event_time` / `bridge_received_at`
- `raw_metadata.seq` / `client_id`, `bubble_count=1`
- **Never** includes `context_token` or raw WeChat payload
- Timestamps never concatenated into `text`

## 6. Response Mapping

- Accept only `action=send`
- Prefer non-empty `segments`; fallback to `[reply_text]`
- Empty segments + empty reply → controlled `empty_reply`
- `gateway_processed_at` is metadata only — never treated as `assistant_sent_at`

## 7. Dedupe and Ledger Compatibility

- Sidecar durable ledger remains first-layer dedupe
- Process inflight set prevents concurrent double Hearth calls
- Gateway 409 → ledger `bridge_duplicate`, no delivery, no `assistant_sent_at`
- Restart reload still blocks reprocessing
- Not exactly-once

## 8. Dry-run Delivery

- New mode: `HEARTH_WEIXIN_MODE=bridge_dry_run`
- `createDryRunDeliverySink()` records segment order
- No real `sendText` / sendmessage
- No `assistant_sent_at`
- Ledger status: `bridged_dry_run` (not `delivered`)

## 9. Privacy Review

Allowed in logs: status, hashes, error codes, structural flags.  
Forbidden: token, Authorization, context_token, user/assistant text, raw identities, upstream error bodies.  
Covered by redaction + dry-run tests with sentinels.

## 10. Files Changed

- `src/core/hearth-bridge-client.js`
- `src/app/hearth-channel-only.js`
- `test/hearth-channel-only.test.js`
- `test/hearth-companion-client.test.js` **(new)**
- `env.hearth-channel.example` **(new)**
- `package.json` (syntax-check coverage)
- `docs/phase-2c-c1-result.md` **(this report)**

The Hearth: **unchanged**

## 11. Automated Tests

```text
node --check ./src/core/hearth-bridge-client.js
node --check ./src/app/hearth-channel-only.js
node --test test/hearth-channel-only.test.js test/hearth-companion-client.test.js
```

Result: **35 passed / 0 failed / 0 skipped**

Mock scope:

- injectable `fetchImpl` / fake local HTTP server
- injectable bridge client + dry-run delivery sink
- temp ledger files

External services contacted: **none** (no real WeChat / model / Supabase / remote Hearth)

## 12. Commit / Push

- Commit: `08ff10f feat: connect sidecar to Hearth companion endpoint`
- Push: ordinary `git push origin HEAD` succeeded (`373ab17..08ff10f` → `feat/hearth-weixin-sidecar-v0`)
- Branch aligned with origin
- No tokens / `.env` / persona text committed

## 13. Known Limitations

- Companion URL restricted to local HTTP in v0
- `bridge` mode can still real-send when allowlisted and secrets present; use `bridge_dry_run` for contract-only runs
- No Bubble Turn Aggregator / Dylan / Bark / outbox
- The Hearth `unittest discover -s gateway` dual-import cache isolation remains a harness follow-up

## 14. Phase 2C-C2 Real WeChat Smoke Plan

1. Configure local Gateway with `HEARTH_INTERNAL_TOKEN` + canonical persona
2. Configure Sidecar `bridge` (or controlled dry-run then bridge) with matching token + `HEARTH_COMPANION_URL`
3. Allowlist one real WeChat user id
4. Send one short text message
5. Verify: one Hearth call, one logical reply delivery, ledger marks delivered, `assistant_sent_at` only after real send
6. Verify logs remain redacted; no second processing on restart

## 15. Final Verdict

**GO**

真实 Hearth client 已接入 Sidecar，契约、鉴权、脱敏、去重和 dry-run delivery 全部通过，可以进入真实微信单消息 smoke。
