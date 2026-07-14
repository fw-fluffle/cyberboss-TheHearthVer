# Active Line v0 — Hearth 微信主动出站控制面

## 位置

- 模块：`src/server/hearth-proactive-send-server.js`
- 挂载：`src/app/hearth-channel-only.js`（adapter / 账号就绪后，同一进程启动）
- 监听：固定 `127.0.0.1`，默认端口 `3200`
- 环境变量见：`env.hearth-channel.example`

## 契约

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

成功：

```json
{
  "ok": true,
  "status": "sent",
  "request_id": "...",
  "assistant_sent_at": "ISO-8601"
}
```

业务 `status` 稳定值：

`unauthorized` / `invalid_request` / `sidecar_not_ready` / `sender_not_found` / `context_token_missing` / `duplicate_request` / `weixin_send_failed` / `sent`

## 去重（MVP）

- 进程内 `Map<request_id, expiresAt/state>`，TTL 默认 15 分钟
- 同 `request_id` 第二次不调用 `sendText`，返回 `duplicate_request`
- 并发同 id：第一个 inflight 时第二个直接拒绝
- 第一次发送失败后，TTL 内继续拒绝（防人工误操作双发）
- 进程重启后清空；不写数据库、不建队列、不按正文 hash 去重

## 安全

- 独立 `HEARTH_WEIXIN_CONTROL_TOKEN`，禁止复用 `HEARTH_INTERNAL_TOKEN` / Dashboard / Supabase key
- 日志只含：`request_id`、`source`、`text_length`、`target`、最终 `status`、`assistant_sent_at`
- 端口占用时明确失败，不改绑公网、不随机端口

## Channel state（只读）

`GET /internal/channel-state`（同一 localhost 控制面与同一 Bearer）

安全字段：

- `last_user_event_time`：微信 `create_time_ms` / `create_time` 的真实事件时间（只前进）
- `last_bridge_received_at`：Sidecar 接受处理该入站的抵达时间
- `last_message_id`：**最后抵达并写入状态的消息 ID**（不是 event_time 最晚的那条；乱序迟到消息会更新此字段，但不会倒退 `last_user_event_time`）
- `has_context_token`：布尔；不返回 token 原文
- `ready`：当前通道是否可工作；与历史时间是否存在无关

持久化：`{accountsDir}/{accountId}.hearth-channel-state.json`（无 token / 无正文）
