# dsh-relay

端对端平等消息中继 MCP 服务器。让 DSH / Codex / ZCode / Qoder 等所有 AI 端平等互联。

## 架构

单体双模式（参考 webhook-relay-mcp）：一个 Node 进程 = HTTP API + MCP stdio server + SQLite WAL。

## 安装

```bash
cd G:\work\_tools\dsh-relay
npm install  # 可选: better-sqlite3（失败则回退 node:sqlite，Node ≥22 内置）
```

## 配置

`cordis.patch.yml` 的 `mcpServers` 加：

```yaml
dsh-relay:
  module: stdio
  command: "G:\\node.exe"
  args: ["G:\\work\\_tools\\dsh-relay\\server.js"]
  env:
    DSH_RELAY_HTTP_PORT: "8081"       # 0 = 不启用 HTTP，纯 MCP stdio
    DSH_RELAY_DB_PATH: "C:\\Users\\www\\.dsh\\storages\\dsh-relay.db"
    DSH_RELAY_HMAC_SECRET: "<secret>"  # 生产必填（DEV=1 可跳过）
    DSH_RELAY_ADMIN_TOKEN: "<admin>"   # 生产必填
    DSH_RELAY_RETENTION_DAYS: "30"
    DSH_RELAY_MAX_QUEUE_PER_TARGET: "1000"
    DSH_RELAY_RATE_LIMIT_PER_IP: "100"
    DSH_RELAY_DEV: "0"                 # 1 = 跳过 HMAC 启动检查
```

⚠️ 改 cordis.patch.yml 需重启 DSH 宿主。

## MCP 工具（6 个）

| 工具 | 用途 | 关键参数 |
|---|---|---|
| relay_register | 注册当前会话为端 | name, type, mode, ttl?, metadata? |
| relay_poll | 取未读消息（long-poll） | limit?, timeout?=30 (0=非阻塞) |
| relay_ack | 累积回执 | upToSeq |
| relay_send | 向其他端发消息 | target, payload, priority?, deadline?, idempotencyKey?, correlationId?, rawPayload? |
| relay_stats | 队列/端统计 | — |
| relay_history | 消息历史（游标分页） | cursor?, limit? |

## HTTP API

### 端管理
- `POST /endpoints {name,type,mode,ttl?,metadata?}` → 201 `{id, token}`
- `DELETE /endpoints/{id}` → 200
- `POST /endpoints/{id}/heartbeat` → 200
- `GET /endpoints` → 自己的端（admin token → 全部端列表）

### 消息
- `POST /inbox {target,payload,...}` (Bearer + HMAC) → 202
- `POST /endpoints/{id}/ack {upToSeq}` → 200
- `GET /endpoints/{id}/inbox?limit=N&wait=30` → 200（long-poll，wait 秒内阻塞等新消息）

### 监控
- `GET /health` → `{status:"ok"}`
- `GET /health/ready` → `{status:"ready",db:true}`
- `GET /metrics` → Prometheus 格式
- `GET /audit?limit=100` → 审计日志（admin token）
- `GET /help` → API 手册
- `GET /` → Web UI 入口

### Agent 友好
- 教学式 4xx：每个错误带 `hint` 字段
- `Accept: text/plain` → 可 grep 纯文本响应

## 关键设计

- **id 一律 base62 短码**（禁 UUID：DSH 遮蔽机制）
- **at-least-once + 累积 ACK**（poll 不删，ack 后才标记完成；重连重放）
- **long-poll + push 唤醒**（pollMessages 无消息时阻塞等待，enqueueTx 成功后 notifyWaiters 唤醒）
- **SQLite WAL** source of truth（重启不丢；30 天保留期）
- **安全三层**（HMAC + nonce 防重放 + per-IP 限速 + 队列上限 + fail-closed 启动）
- **metadata / raw_payload** 透传（端自定义字段 + 消息原始载荷）

## 测试

```bash
node test-e2e.js       # HTTP API 端到端
node test-mcp.js       # MCP stdio 通道
node test-prod.js      # HMAC + fail-closed + nonce 防重放
node test-longpoll.js  # long-poll + push 唤醒 + 新字段
```

## 参考文档

- 设计：`01-DSH机制/桥-inbox通道-独立版-独立MCP服务器-设计-20261012.md`
- 调研：`01-调研报告/dsh-relay调研-消息中继项目对比-20261012.md`
