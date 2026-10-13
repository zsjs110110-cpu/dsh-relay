# dsh-relay

端对端平等消息中继 MCP 服务器。让 DSH / Codex / ZCode / Qoder 等所有 AI 端平等互联。

## 架构

单体双模式（参考 webhook-relay-mcp）：一个 Node 进程 = HTTP API + MCP stdio server + SQLite WAL。

## 安装

```bash
cd G:\work\_tools\dsh-relay
npm install  # 可选: better-sqlite3（失败则回退 node:sqlite）
```

## 配置

`cordis.patch.yml` 的 `mcpServers` 加：

```yaml
dsh-relay:
  module: stdio
  command: "G:\\node.exe"
  args: ["G:\\work\\_tools\\dsh-relay\\server.js"]
  env:
    DSH_RELAY_HTTP_PORT: "8081"
    DSH_RELAY_DB_PATH: "C:\\Users\\www\\.dsh\\storages\\dsh-relay.db"
    DSH_RELAY_HMAC_SECRET: "<secret>"
    DSH_RELAY_ADMIN_TOKEN: "<admin-token>"
```

⚠️ 改 cordis.patch.yml 需重启 DSH 宿主。

## MCP 工具（6 个）

| 工具 | 用途 |
|---|---|
| relay_register | 注册当前会话为端 |
| relay_poll | 取未读消息（at-least-once，不删） |
| relay_ack | 累积回执（seq <= upToSeq 标记已消费） |
| relay_send | 向其他端发消息 |
| relay_stats | 队列/端统计 |
| relay_history | 消息历史（游标分页） |

## HTTP API

- `POST /endpoints {name,type,mode,ttl?}` → `{id, token}`
- `POST /inbox {target,payload,...}` (Bearer + HMAC) → 202
- `DELETE /endpoints/{id}` | `POST /endpoints/{id}/heartbeat`
- `GET /endpoints/{id}/inbox?lastAckedSeq=N` (非 DSH 端 HTTP poll)
- `GET /health` | `GET /metrics` | `GET /audit` | `GET /help` | `GET /` (Web UI)

## 关键设计

- **id 一律 base62 短码**（禁 UUID：DSH 遮蔽机制）
- **at-least-once + 累积 ACK**（poll 不删，ack 后才标记完成；重连重放）
- **SQLite WAL** source of truth（重启不丢；30 天保留期）
- **long-poll 等待者**（进程内 PollWaiter，新消息到达即唤醒）
- **安全三层**（HMAC + per-IP 限速 + 队列上限）🔒 GPT 待审

## 参考文档

- 设计：`01-DSH机制/桥-inbox通道-独立版-独立MCP服务器-设计-20261012.md`
- 调研：`01-调研报告/dsh-relay调研-消息中继项目对比-20261012.md`
