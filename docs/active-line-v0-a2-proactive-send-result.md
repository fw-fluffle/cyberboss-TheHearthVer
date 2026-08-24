# Active Line v0-A2｜主动微信出站（安全 Worktree）完成报告

> 日期：2026-07-14  
> 范围：仅两个新 Git worktree；不 commit / 不 push；不做真实微信发送  
> The Hearth：只读，未修改

---

## 1. Verdict

**GO（fake / mock 通过）**

实现与定向测试已完成。未连接真实微信，未运行真实 Sidecar smoke。等待狐狸工头批准唯一一次真实微信发送。

---

## 2. 原始仓状态是否保持不变

**是。** 未清理、未改写两个原始脏工作区中的现有文件。

| 仓 | 路径 | 说明 |
|---|---|---|
| Dylan 原仓 | `D:\G 修建施工中\dylan-heartbeat` | 仍脏（`server.js` / `wake_up.js` / `.env.example` 等） |
| Cyberboss 原仓 | `D:\cyberboss-TheHearthVer` | 仍脏（`README.zh-CN.md`、删除的 `docs/phase-2c-c1-result.md`） |
| The Hearth | `D:\G 修建施工中` | 只读，未修改 |

---

## 3. Worktree 路径与分支

| 仓 | Worktree | 分支 | 基线 HEAD |
|---|---|---|---|
| Dylan | `D:\G-worktrees\dylan-weixin-outbound-smoke` | `feat/weixin-outbound-smoke-v0` | `4e5384b` |
| Cyberboss | `D:\G-worktrees\cyberboss-hearth-proactive-send` | `feat/hearth-proactive-send-v0` | `6587381` |

创建方式：各自从原仓当时 HEAD `worktree add -b ...`；创建后两 worktree 均为干净再施工。

---

## 4. Cyberboss files changed

施工目录：`D:\G-worktrees\cyberboss-hearth-proactive-send`

| 文件 | 动作 |
|---|---|
| `src/server/hearth-proactive-send-server.js` | 新增：localhost 控制面 |
| `src/app/hearth-channel-only.js` | 修改：adapter/账号就绪后同进程启动控制面 |
| `test/hearth-proactive-send-server.test.js` | 新增：fake 测试 |
| `env.hearth-channel.example` | 修改：补充控制面环境变量说明 |
| `docs/active-line-v0-proactive-send.md` | 新增：契约与去重说明 |
| `docs/active-line-v0-a2-proactive-send-result.md` | 新增：本完成报告 |
| `package.json` | 修改：`check` 纳入新模块 |

---

## 5. Dylan files changed

施工目录：`D:\G-worktrees\dylan-weixin-outbound-smoke`

| 文件 | 动作 |
|---|---|
| `scripts/send-weixin-smoke.js` | 新增：手动 smoke（不进 scheduler） |
| `test/send-weixin-smoke.test.js` | 新增：fake 测试 |
| `.env.example` | 修改：控制面 URL/TOKEN 占位 |
| `README.md` | 修改：手动 smoke 用法 |
| `package.json` | 修改：`check` / `test:weixin-smoke` / `smoke:weixin` |

**明确未改：** `server.js`、`wake_up.js`、现有 scheduler、quiet hours、cooldown、Bark 链路。

---

## 6. `/internal/send` 契约

`POST /internal/send`

Headers:

- `Authorization: Bearer <HEARTH_WEIXIN_CONTROL_TOKEN>`
- `Content-Type: application/json`

Body:

```json
{
  "request_id": "active-line-smoke-唯一值",
  "target": "default_owner",
  "text": "Active Line v0：小狼已从 The Hearth 敲到微信窗边。",
  "source": "dylan_manual_smoke"
}
```

成功响应：

```json
{
  "ok": true,
  "status": "sent",
  "request_id": "...",
  "assistant_sent_at": "ISO-8601"
}
```

稳定业务 `status`：

- `sent`
- `unauthorized`
- `invalid_request`
- `sidecar_not_ready`
- `sender_not_found`
- `context_token_missing`
- `duplicate_request`
- `weixin_send_failed`

处理顺序：localhost 边界 → 独立 token → `request_id` → 非空受控正文 → adapter ready → `resolvePreferredSenderId` → token cache/store → `adapter.sendText` → 成功后才签发 `assistant_sent_at`。

---

## 7. 监听地址与配置名

| 项 | 值 |
|---|---|
| Host | 固定 `127.0.0.1` |
| Port | 默认 `3200`（`HEARTH_WEIXIN_CONTROL_PORT`） |
| Token | `HEARTH_WEIXIN_CONTROL_TOKEN`（独立密钥，禁止复用 Hearth/Dashboard/Supabase） |
| 端口占用 | 明确失败（`EADDRINUSE`），不改公网、不随机端口 |

**注意：** 本 feat 下 `hearth:channel-only` 启动时若缺少 `HEARTH_WEIXIN_CONTROL_TOKEN`，进程会失败退出（控制面为硬依赖）。

Dylan 侧：

- `HEARTH_WEIXIN_CONTROL_URL`（默认 `http://127.0.0.1:3200`）
- `HEARTH_WEIXIN_CONTROL_TOKEN`（须与 Cyberboss 相同）

---

## 8. default owner 与 context_token 使用方式

1. 使用现有 `resolvePreferredSenderId` 解析 `target: "default_owner"`
2. 优先把 `HEARTH_WEIXIN_ALLOWED_USER_IDS` 注入为 allowed 列表，再回退 `config.allowedUserIds` / binding / 唯一持久化 token 用户
3. 通过 `adapter.getKnownContextTokens()` 取内存 cache（其背后已接持久化 store）
4. 缺 sender → `sender_not_found`；缺 token → `context_token_missing`
5. 调用现有 `adapter.sendText({ userId, text, contextToken })`
6. **禁止**新开第二套微信 adapter 或第二轮询进程

---

## 9. 成功边界

只有底层微信 API（`adapter.sendText`）明确成功后，才生成 `assistant_sent_at` 并返回 `status: "sent"`。  
`sendText` 抛错 → `weixin_send_failed`，响应不含 `assistant_sent_at`。

---

## 10. 去重行为（MVP）

- 结构：进程内 `Map<request_id, { state, expiresAt }>`
- TTL：默认 15 分钟
- 状态：`inflight` / `sent` / `failed`
- 同 `request_id` 第二次：不调用 `sendText`，返回 `duplicate_request`
- 并发同 id：第一个进入 inflight 后，第二个直接拒绝
- 第一次失败：TTL 内继续拒绝，避免人工误操作双发
- 进程重启后清空；不写数据库；不建队列；不按正文 hash 去重
- `request_id` 仅在通过基本校验后才占坑

---

## 11. Tests and results

### Cyberboss

```text
npm run check
→ pass

node --test test/hearth-proactive-send-server.test.js test/hearth-channel-only.test.js
→ 38/38 pass
```

覆盖要点：无/错 Authorization、缺 `request_id`、空正文、未 ready、缺 sender、缺 context_token、成功才签发时间戳、发送失败、二次去重、并发去重、日志/响应不含 token、原 channel-only 入站仍通过、端口占用明确失败。

### Dylan

```text
npm run check
→ pass

npm run test:weixin-smoke
→ 8/8 pass
```

覆盖要点：成功退出 0、非 sent 非 0、超时非 0、非 JSON 安全失败、唯一 `request_id`、不打印 token、不自动重试、本地 fake HTTP CLI 路径。

**本轮未做：** 真实微信发送、真实 Sidecar 连接、读取真实 token 正文。

---

## 12. 真实冒烟启动命令（待批准后）

```powershell
# === Cyberboss worktree ===
cd "D:\G-worktrees\cyberboss-hearth-proactive-send"
# 需已 npm login / 账号就绪，并配置：
# HEARTH_WEIXIN_CONTROL_PORT=3200
# HEARTH_WEIXIN_CONTROL_TOKEN=<独立随机密钥>
# HEARTH_WEIXIN_ALLOWED_USER_IDS=<default owner>
# （以及既有 channel-only 所需变量）
npm run hearth:channel-only

# === Dylan worktree（另开终端）===
cd "D:\G-worktrees\dylan-weixin-outbound-smoke"
$env:HEARTH_WEIXIN_CONTROL_URL="http://127.0.0.1:3200"
$env:HEARTH_WEIXIN_CONTROL_TOKEN="<与 Sidecar 相同>"
npm run smoke:weixin

# 如需测去重：
# node ./scripts/send-weixin-smoke.js --request-id fixed-smoke-id
```

固定正文：

`Active Line v0：小狼已从 The Hearth 敲到微信窗边。`

---

## 13. 真实冒烟前缺少的配置

1. 生成独立 `HEARTH_WEIXIN_CONTROL_TOKEN`，两端配置相同；勿复用 `HEARTH_INTERNAL_TOKEN` / Dashboard admin key / Supabase key
2. Cyberboss：微信已登录；`HEARTH_WEIXIN_ALLOWED_USER_IDS`（或可解析出唯一 default owner）；该用户已有可用 `context_token`
3. 本机 `127.0.0.1:3200` 空闲
4. Dylan：`HEARTH_WEIXIN_CONTROL_URL` + 同 token
5. 狐狸工头批准**唯一一次**真实发送

---

## 14. 两个 worktree 最终 git status（施工后快照）

### Dylan worktree

```text
## feat/weixin-outbound-smoke-v0
 M .env.example
 M README.md
 M package.json
?? scripts/send-weixin-smoke.js
?? test/
```

### Cyberboss worktree

```text
## feat/hearth-proactive-send-v0
 M env.hearth-channel.example
 M package.json
 M src/app/hearth-channel-only.js
?? docs/active-line-v0-proactive-send.md
?? docs/active-line-v0-a2-proactive-send-result.md
?? src/server/
?? test/hearth-proactive-send-server.test.js
```

本轮：**不 commit、不 push、不 fetch、不 merge、不 rebase、不 reset、不删除 worktree。**

---

## 15. 原始两个脏仓最终 git status（施工后快照）

### Dylan 原仓 `D:\G 修建施工中\dylan-heartbeat`

```text
## main...origin/main [ahead 3, behind 3]
 M .env.example
 M .gitignore
 M server.js
 M wake_up.js
?? docs/dylan-core-wake-bugfix-plan-2026-06-04.md
?? docs/dylan-wake-core-fixes-2026-06-04.md
?? docs/vps-cloudflare-tunnel-predeploy-audit-2026-06-08.md
?? scripts/compact-enhanced-messages.js
```

### Cyberboss 原仓 `D:\cyberboss-TheHearthVer`

```text
## feat/hearth-weixin-sidecar-v0...origin/feat/hearth-weixin-sidecar-v0
 M README.zh-CN.md
 D docs/phase-2c-c1-result.md
```

与施工前一致，未被本轮改动。

---

## 停止条件

实现与 fake tests 已通过。  
**停止等待狐狸工头批准唯一一次真实微信发送。**
