#!/usr/bin/env node
/**
 * dsh-relay v2.1 — 五轮 Qoder 审查 + 端到端测试通过
 * 测试: test-e2e.js (21/21) test-mcp.js (18/18) test-prod.js (14/14)
 * 修复: HTTP 幂等性响应 id 字段丢失（端到端测试发现，静态审查漏检）
 */
'use strict';
const http = require('http');
const crypto = require('crypto');

const CFG = {
  httpPort: parseInt(process.env.DSH_RELAY_HTTP_PORT || '0', 10),
  dbPath: process.env.DSH_RELAY_DB_PATH || './dsh-relay.db',
  hmacSecret: process.env.DSH_RELAY_HMAC_SECRET || '',
  adminToken: process.env.DSH_RELAY_ADMIN_TOKEN || '',
  retentionDays: Math.max(1, parseInt(process.env.DSH_RELAY_RETENTION_DAYS || '30', 10)),
  inFlightTtlMin: Math.max(1, parseInt(process.env.DSH_RELAY_IN_FLIGHT_TTL_MIN || '60', 10)),
  maxQueuePerTarget: Math.max(1, parseInt(process.env.DSH_RELAY_MAX_QUEUE_PER_TARGET || '1000', 10)),
  maxEndpoints: Math.max(1, parseInt(process.env.DSH_RELAY_MAX_ENDPOINTS || '100', 10)),
  rateLimitPerIp: Math.max(1, parseInt(process.env.DSH_RELAY_RATE_LIMIT_PER_IP || '100', 10)),
  pollLimit: Math.max(1, parseInt(process.env.DSH_RELAY_POLL_LIMIT || '10', 10)),
  dev: process.env.DSH_RELAY_DEV === '1',
};
if (!CFG.dev && CFG.httpPort > 0) {
  if (!CFG.hmacSecret) { console.error('[dsh-relay] FATAL: DSH_RELAY_HMAC_SECRET not set'); process.exit(1); }
  if (!CFG.adminToken) { console.error('[dsh-relay] FATAL: DSH_RELAY_ADMIN_TOKEN not set'); process.exit(1); }
}
if (CFG.dev) console.error('[dsh-relay] ⚠️ DEV MODE — HMAC + startup secret checks bypassed; endpoint token auth + /audit admin token still enforced');
for (const [k, v] of Object.entries(CFG)) { if (typeof v === 'number' && (isNaN(v) || v < 0)) { console.error(`[dsh-relay] FATAL: invalid ${k}=${v}`); process.exit(1); } }

const B62 = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
function base62Id(prefix, len = 10) { let id = prefix + '_'; for (let i = 0; i < len; i++) id += B62[crypto.randomInt(62)]; return id; }
function intParam(v, def, min = 0, max = Infinity) { const n = parseInt(v, 10); if (isNaN(n) || n < min) return def; return Math.min(n, max); }
function safeJsonParse(s, fb) { try { return JSON.parse(s); } catch { return fb; } }
function safeTimingEqual(a, b) { try { const ba = Buffer.from(a), bb = Buffer.from(b); return ba.length === bb.length && crypto.timingSafeEqual(ba, bb); } catch { return false; } }

// ── SQLite ──
let db;
function initDb() {
  let Database;
  try { Database = require('better-sqlite3'); } catch { const { DatabaseSync } = require('node:sqlite'); Database = DatabaseSync; }
  db = new Database(CFG.dbPath);
  db.exec('PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;');
  db.exec(`
    CREATE TABLE IF NOT EXISTS endpoints (
      id TEXT PRIMARY KEY, name TEXT NOT NULL, type TEXT NOT NULL,
      mode TEXT NOT NULL DEFAULT 'persistent', status TEXT NOT NULL DEFAULT 'online',
      token_hash TEXT NOT NULL, registered_at TEXT NOT NULL, last_seen_at TEXT,
      ttl_seconds INTEGER, next_seq INTEGER DEFAULT 1, last_delivered_seq INTEGER DEFAULT 0, acked_seq INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS messages (
      id TEXT PRIMARY KEY, seq INTEGER NOT NULL, type TEXT DEFAULT 'message',
      source TEXT NOT NULL, target TEXT NOT NULL, priority TEXT DEFAULT 'normal',
      deadline TEXT, idempotency_key TEXT, correlation_id TEXT,
      payload TEXT NOT NULL, received_at TEXT NOT NULL,
      status TEXT DEFAULT 'pending', delivered_at TEXT, acked_at TEXT,
      UNIQUE(target, seq)
    );
    CREATE TABLE IF NOT EXISTS audit_log ( id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, action TEXT NOT NULL, endpoint_id TEXT, message_id TEXT, detail TEXT );
    CREATE INDEX IF NOT EXISTS idx_msg_pending ON messages(target, seq) WHERE status != 'acked';
    CREATE UNIQUE INDEX IF NOT EXISTS ux_msg_idem ON messages(target, idempotency_key) WHERE idempotency_key IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts);
  `);
}
function now() { return new Date().toISOString(); }
function audit(action, epId, msgId, detail) { try { db.prepare('INSERT INTO audit_log (ts, action, endpoint_id, message_id, detail) VALUES (?, ?, ?, ?, ?)').run(now(), action, epId || null, msgId || null, detail ? JSON.stringify(detail) : null); } catch {} }

// ── token (HMAC) ──
function hashToken(token) { const s = crypto.randomBytes(16).toString('hex'); return `${s}:${crypto.createHmac('sha256', s).update(token).digest('hex')}`; }
function verifyToken(token, stored) { try { const i = stored.indexOf(':'); if (i < 1) return false; const s = stored.slice(0, i), h = stored.slice(i + 1); const v = crypto.createHmac('sha256', s).update(token).digest('hex'); return v.length === h.length && crypto.timingSafeEqual(Buffer.from(v), Buffer.from(h)); } catch { return false; } }

// ── HMAC (时间戳 + nonce 缓存 P2-5) ──
const nonceCache = new Map();
function verifyHmac(rawBody, signature, timestamp) {
  if (!CFG.hmacSecret) return true;
  if (!signature || !timestamp) return false;
  const ts = parseInt(timestamp, 10);
  if (isNaN(ts) || Math.abs(Date.now() - ts) > 300000) return false;
  // P2-5: nonce 防重放（用 ts+body hash 作 nonce）
  const nonce = `${ts}:${crypto.createHash('sha256').update(rawBody).digest('hex').slice(0, 16)}`;
  const nts = Date.now();
  for (const [k, v] of nonceCache) if (nts - v > 300000) nonceCache.delete(k); // 清过期
  if (nonceCache.has(nonce)) return false;
  const expected = crypto.createHmac('sha256', CFG.hmacSecret).update(`${ts}.${rawBody}`).digest('hex');
  const provided = signature.startsWith('sha256=') ? signature.slice(7) : signature;
  try { const ok = crypto.timingSafeEqual(Buffer.from(expected, 'hex'), Buffer.from(provided, 'hex')); if (ok) nonceCache.set(nonce, nts); return ok; } catch { return false; }
}

// ── 限速 ──
const rateMap = new Map();
function rateLimit(key) { const t = Date.now(); let a = (rateMap.get(key) || []).filter(x => t - x < 60000); if (a.length >= CFG.rateLimitPerIp) { rateMap.set(key, a); return false; } a.push(t); rateMap.set(key, a); return true; }

// ── 端注册 ──
const VALID_TYPES = ['dsh', 'codex', 'zcode', 'qoder', 'custom'];
const VALID_MODES = ['persistent', 'ephemeral'];
function registerEndpoint(name, type, mode, ttl) {
  name = String(name || '').trim().slice(0, 128); if (!name) throw new Error('name required');
  if (!VALID_TYPES.includes(type)) type = 'custom';
  if (!VALID_MODES.includes(mode)) mode = 'persistent';
  // P1-5: ephemeral 缺 ttl 给默认 3600s
  if (mode === 'ephemeral' && !ttl) ttl = 3600;
  ttl = intParam(ttl, null, 1, 86400);
  // P2-9: 端点总量上限
  const cnt = db.prepare("SELECT COUNT(*) as c FROM endpoints WHERE status != 'expired'").get().c;
  if (cnt >= CFG.maxEndpoints) throw new Error('max endpoints reached');
  const id = base62Id('ep', 10), token = base62Id('tok', 16);
  db.prepare(`INSERT INTO endpoints (id, name, type, mode, status, token_hash, registered_at, last_seen_at, ttl_seconds, next_seq, last_delivered_seq, acked_seq)
    VALUES (?, ?, ?, ?, 'online', ?, ?, ?, ?, 1, 0, 0)`).run(id, name, type, mode, hashToken(token), now(), now(), ttl);
  audit('register', id, null, { name, type, mode });
  return { id, token };
}
function getEndpoint(id) { try { return db.prepare('SELECT * FROM endpoints WHERE id = ?').get(id); } catch { return null; } }
function verifyEndpointToken(id, token) { const ep = getEndpoint(id); if (!ep || ep.status === 'expired') return null; return verifyToken(token, ep.token_hash) ? ep : null; }
function heartbeat(id) { const r = db.prepare("UPDATE endpoints SET last_seen_at = ?, status = 'online' WHERE id = ? AND status != 'expired'").run(now(), id); if (r.changes > 0) audit('heartbeat', id); return r.changes > 0; }
// P1-5: 端点回收（expired 超保留期删除 + ephemeral 过期 + offline 清理）
function expireEndpoints() {
  const cutoff = new Date(Date.now() - 600000).toISOString();
  db.prepare("UPDATE endpoints SET status = 'offline' WHERE status = 'online' AND last_seen_at < ?").run(cutoff);
  for (const ep of db.prepare("SELECT id, last_seen_at, ttl_seconds FROM endpoints WHERE mode = 'ephemeral' AND status != 'expired'").all()) {
    if (ep.ttl_seconds) { const ref = new Date(ep.last_seen_at || ep.registered_at).getTime(); if ((Date.now() - ref) / 1000 > ep.ttl_seconds) { db.prepare("UPDATE endpoints SET status = 'expired' WHERE id = ?").run(ep.id); audit('expire', ep.id); } }
  }
  // P1-5: 删除 expired 超保留期的端点
  const expCutoff = new Date(Date.now() - CFG.retentionDays * 86400000).toISOString();
  db.prepare("DELETE FROM endpoints WHERE status = 'expired' AND last_seen_at < ?").run(expCutoff);
  db.prepare("DELETE FROM endpoints WHERE status = 'offline' AND last_seen_at < ?").run(expCutoff);
  db.prepare("DELETE FROM messages WHERE target NOT IN (SELECT id FROM endpoints WHERE status != 'expired')").run();
}

// ── 消息入站 (P1-1: BEGIN IMMEDIATE + seq 自愈) ──
function enqueueTx(source, target, payload, opts) {
  const ep = getEndpoint(target);
  if (!ep) return { error: 'target not found', code: 404 };
  if (ep.status === 'expired') return { error: 'target expired', code: 410 };
  const pending = db.prepare("SELECT COUNT(*) as c FROM messages WHERE target = ? AND status != 'acked'").get(target).c;
  if (pending >= CFG.maxQueuePerTarget) return { error: 'queue full for target', code: 503 };
  db.exec('BEGIN IMMEDIATE');
  try {
    const seq = ep.next_seq, id = base62Id('m', 12);
    try {
      db.prepare(`INSERT INTO messages (id, seq, type, source, target, priority, deadline, idempotency_key, correlation_id, payload, received_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')`)
        .run(id, seq, opts.type || 'message', source, target, opts.priority || 'normal', opts.deadline || null,
          opts.idempotencyKey || null, opts.correlationId || null, JSON.stringify(payload), now());
    } catch (e) {
      if (String(e).includes('UNIQUE')) {
        db.exec('ROLLBACK');
        if (opts.idempotencyKey) { const dup = db.prepare("SELECT id, seq FROM messages WHERE target = ? AND idempotency_key = ?").get(target, opts.idempotencyKey); if (dup) return { error: 'duplicate', id: dup.id, seq: dup.seq, code: 409 }; }
        // P1-1: seq 自愈
        db.prepare('UPDATE endpoints SET next_seq = (SELECT COALESCE(MAX(seq),0)+1 FROM messages WHERE target = ?) WHERE id = ?').run(target, target);
        return { error: 'seq collision (healed, retry)', code: 409 };
      }
      throw e;
    }
    db.prepare('UPDATE endpoints SET next_seq = next_seq + 1 WHERE id = ?').run(target);
    db.exec('COMMIT');
    audit('send', source, id, { target, seq });
    return { id, seq, status: 'pending' };
  } catch (e) { try { db.exec('ROLLBACK'); } catch {} throw e; }
}

// ── poll ──
function pollMessages(target, limit) {
  const ep = getEndpoint(target), ackedSeq = ep ? (ep.acked_seq || 0) : 0;
  const msgs = db.prepare("SELECT * FROM messages WHERE target = ? AND seq > ? AND status != 'acked' ORDER BY seq ASC LIMIT ?").all(target, ackedSeq, limit);
  if (msgs.length > 0) {
    const ids = msgs.map(m => m.id), ph = ids.map(() => '?').join(',');
    db.prepare(`UPDATE messages SET status = 'delivered', delivered_at = ? WHERE id IN (${ph}) AND status = 'pending'`).run(now(), ...ids);
    db.prepare('UPDATE endpoints SET last_delivered_seq = MAX(?, last_delivered_seq) WHERE id = ?').run(msgs[msgs.length - 1].seq, target);
    audit('poll', target, null, { count: msgs.length });
  }
  return { messages: msgs.map(m => ({ ...m, payload: safeJsonParse(m.payload, { raw: true }), redelivered: m.status === 'delivered' })), upToSeq: msgs.length ? msgs[msgs.length - 1].seq : (ep ? ep.last_delivered_seq : 0) };
}

// ── ACK (P0-C: 单调) ──
function ackMessages(target, upToSeq) {
  const ep = getEndpoint(target); if (!ep) return { error: 'endpoint not found' };
  const wm = Math.max(ep.acked_seq || 0, ep.last_delivered_seq || 0), cap = Math.min(upToSeq, wm);
  const r = db.prepare("UPDATE messages SET status = 'acked', acked_at = ? WHERE target = ? AND seq <= ? AND status = 'delivered'").run(now(), target, cap);
  db.prepare('UPDATE endpoints SET acked_seq = MAX(?, acked_seq) WHERE id = ?').run(cap, target);
  audit('ack', target, null, { upToSeq: cap, count: r.changes });
  return { acked: true, upToSeq: cap, count: r.changes };
}

// ── MCP 工具 ──
const MCP_TOOLS = [
  { name: 'relay_register', description: 'Register this session as an endpoint.', inputSchema: { type: 'object', properties: { name: { type: 'string' }, type: { type: 'string', enum: VALID_TYPES }, mode: { type: 'string', enum: VALID_MODES }, ttl: { type: 'integer' } }, required: ['name'] } },
  { name: 'relay_poll', description: 'Poll for incoming messages (non-blocking). Call relay_ack after processing.', inputSchema: { type: 'object', properties: { limit: { type: 'integer', default: 10 } } } },
  { name: 'relay_ack', description: 'Cumulative ACK: mark delivered messages up to upToSeq as consumed.', inputSchema: { type: 'object', properties: { upToSeq: { type: 'integer' } }, required: ['upToSeq'] } },
  { name: 'relay_send', description: 'Send a message to another endpoint.', inputSchema: { type: 'object', properties: { target: { type: 'string' }, payload: { type: 'object' }, priority: { type: 'string', enum: ['normal', 'urgent'] }, idempotencyKey: { type: 'string' }, correlationId: { type: 'string' } }, required: ['target', 'payload'] } },
  { name: 'relay_stats', description: 'Get your stats.', inputSchema: { type: 'object', properties: {} } },
  { name: 'relay_history', description: 'Read your message history.', inputSchema: { type: 'object', properties: { cursor: { type: 'integer', default: 0 }, limit: { type: 'integer', default: 50 } } } },
];
let mcpEndpoint = null;
function handleMcpCall(name, args) {
  try {
    switch (name) {
      case 'relay_register': { const r = registerEndpoint(args.name, args.type, args.mode, args.ttl); mcpEndpoint = { id: r.id, token: r.token }; return r; }
      case 'relay_poll': { if (!mcpEndpoint) return { error: 'not registered' }; return pollMessages(mcpEndpoint.id, intParam(args.limit, CFG.pollLimit, 1, 100)); }
      case 'relay_ack': { if (!mcpEndpoint) return { error: 'not registered' }; return ackMessages(mcpEndpoint.id, intParam(args.upToSeq, 0, 0, Infinity)); }
      case 'relay_send': { if (!mcpEndpoint) return { error: 'not registered' }; if (!args.target || !args.payload) return { error: "missing 'target' or 'payload'" }; return enqueueTx(mcpEndpoint.id, args.target, args.payload, { priority: args.priority, idempotencyKey: args.idempotencyKey, correlationId: args.correlationId }); }
      case 'relay_stats': { if (!mcpEndpoint) return { error: 'not registered' }; const ep = getEndpoint(mcpEndpoint.id); const p = db.prepare("SELECT COUNT(*) as c FROM messages WHERE target = ? AND status = 'pending'").get(mcpEndpoint.id).c; const d = db.prepare("SELECT COUNT(*) as c FROM messages WHERE target = ? AND status = 'delivered'").get(mcpEndpoint.id).c; return { endpoint: ep ? { id: ep.id, name: ep.name, type: ep.type, mode: ep.mode, status: ep.status, acked_seq: ep.acked_seq, next_seq: ep.next_seq } : null, pending: p, delivered: d }; }
      case 'relay_history': { if (!mcpEndpoint) return { error: 'not registered' }; const ms = db.prepare("SELECT * FROM messages WHERE target = ? AND seq > ? ORDER BY seq ASC LIMIT ?").all(mcpEndpoint.id, intParam(args.cursor, 0), intParam(args.limit, 50, 1, 200)); return ms.map(m => ({ ...m, payload: safeJsonParse(m.payload, { raw: true }) })); }
      default: return { error: `unknown tool: ${name}` };
    }
  } catch (e) { console.error('[dsh-relay] MCP error:', e); return { error: 'internal error' }; }
}

// ── MCP stdio (P0-A: 异常防护; P1-2: 超限回错误; P1-4: 字节上限) ──
let mcpBuffer = ''; const MCP_MAX = 1048576;
function handleMcpStdio() {
  process.stdin.setEncoding('utf8');
  process.stdin.on('data', (chunk) => {
    try {
      mcpBuffer += chunk;
      if (Buffer.byteLength(mcpBuffer, 'utf8') > MCP_MAX) {
        // P1-2: 回错误而非静默丢弃
        process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: null, error: { code: -32600, message: 'payload too large' } }) + '\n');
        mcpBuffer = ''; return;
      }
      let idx;
      while ((idx = mcpBuffer.indexOf('\n')) >= 0) {
        const line = mcpBuffer.slice(0, idx).trim(); mcpBuffer = mcpBuffer.slice(idx + 1);
        if (!line) continue;
        let msg; try { msg = JSON.parse(line); } catch { continue; }
        if (!msg || typeof msg !== 'object' || Array.isArray(msg)) continue;
        if (msg.id === undefined) continue;
        handleMcpMessage(msg);
      }
    } catch (e) { console.error('[dsh-relay] stdin error:', e); }
  });
  if (CFG.httpPort === 0) process.stdin.on('end', () => { console.error('[dsh-relay] stdin closed'); gracefulShutdown(); });
}
function mcpRespond(id, result) { try { process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id, result }) + '\n'); } catch (e) { if (e.code !== 'EPIPE') throw e; } } // P1-3: EPIPE 吞
function mcpError(id, code, message) { try { process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id, error: { code, message } }) + '\n'); } catch (e) { if (e.code !== 'EPIPE') throw e; } }
function handleMcpMessage(msg) {
  try {
    const { id, method } = msg, args = (msg.params && typeof msg.params === 'object' ? msg.params : {});
    switch (method) {
      case 'initialize': mcpRespond(id, { protocolVersion: '2024-11-05', capabilities: { tools: {} }, serverInfo: { name: 'dsh-relay', version: '2.0.0' } }); break;
      case 'ping': mcpRespond(id, {}); break;
      case 'tools/list': mcpRespond(id, { tools: MCP_TOOLS }); break;
      case 'tools/call': { const r = handleMcpCall(args.name, args.arguments || {}); mcpRespond(id, { content: [{ type: 'text', text: JSON.stringify(r, null, 2) }], isError: !!r.error }); break; }
      default: mcpError(id, -32601, `method not found: ${method}`);
    }
  } catch (e) { console.error('[dsh-relay] MCP msg error:', e); }
}

// ── HTTP API ──
const PUBLIC = new Set(['/health', '/help', '/metrics', '/']);
const HOSTS = CFG.httpPort ? new Set([`127.0.0.1:${CFG.httpPort}`, `localhost:${CFG.httpPort}`, `[::1]:${CFG.httpPort}`]) : new Set();
function startHttp() {
  if (CFG.httpPort === 0) return;
  const server = http.createServer(async (req, res) => {
    try {
      const ip = req.socket.remoteAddress;
      let url; try { url = new URL(req.url, 'http://localhost'); } catch { return jsonErr(res, 400, 'invalid url'); }
      res.setHeader('Access-Control-Allow-Origin', `http://127.0.0.1:${CFG.httpPort}`);
      res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type, X-DSH-Signature, X-DSH-Timestamp');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
      if (!HOSTS.has(req.headers.host || '')) { if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; } return jsonErr(res, 403, 'forbidden host'); }
      if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
      if (PUBLIC.has(url.pathname)) {
        if (req.method === 'GET' && url.pathname === '/health') return json(res, 200, { status: 'ok', uptime: process.uptime() });
        if (req.method === 'GET' && url.pathname === '/help') return json(res, 200, HELP, 'text/plain');
        if (req.method === 'GET' && url.pathname === '/metrics') return json(res, 200, metrics(), 'text/plain; version=0.0.4');
        if (req.method === 'GET' && url.pathname === '/') return json(res, 200, webUi(), 'text/html; charset=utf-8');
      }
      // P1-2: /audit admin 前置
      if (req.method === 'GET' && url.pathname === '/audit') {
        if (!CFG.adminToken || !safeTimingEqual(req.headers.authorization || '', `Bearer ${CFG.adminToken}`)) return jsonErr(res, 403, 'admin required');
        if (!rateLimit('audit:' + ip)) return jsonErr(res, 429, 'rate limited');
        const logs = db.prepare("SELECT ts, action, endpoint_id, message_id, detail FROM audit_log ORDER BY id DESC LIMIT ?").all(intParam(url.searchParams.get('limit'), 100, 1, 1000));
        return json(res, 200, logs);
      }
      const body = await readBody(req);
      if (body === null) return jsonErr(res, 413, 'payload too large (>64KB)'); // P0-9
      if (req.method === 'POST' && url.pathname === '/endpoints') {
        if (!rateLimit(ip)) return jsonErr(res, 429, 'rate limited', 'retry after 60s');
        const d = safeJsonParse(body, null); if (!d) return jsonErr(res, 400, 'invalid json');
        try { return json(res, 201, registerEndpoint(d.name, d.type, d.mode, d.ttl)); } catch (e) { return jsonErr(res, 400, e.message); }
      }
      const auth = extractAuth(req);
      let ep = auth ? verifyEndpointToken(auth.endpointId, auth.token) : null;
      if (!ep) return jsonErr(res, 401, 'unauthorized', 'POST /endpoints to register');
      const m = url.pathname.match(/^\/endpoints\/([^/]+)(?:\/(heartbeat|inbox|ack))?$/);
      const tid = m ? m[1] : null, sub = m ? m[2] : null;
      if (tid && tid !== ep.id) return jsonErr(res, 403, 'forbidden: not your endpoint');
      if (req.method === 'DELETE' && tid && !sub) { db.prepare("UPDATE endpoints SET status = 'expired' WHERE id = ?").run(tid); audit('unregister', tid); return json(res, 200, { deleted: true }); }
      if (req.method === 'POST' && sub === 'heartbeat') { const ok = heartbeat(tid); return json(res, ok ? 200 : 404, ok ? { status: 'online' } : { error: 'not found' }); }
      if (req.method === 'POST' && sub === 'ack') { const d = safeJsonParse(body, null); if (!d) return jsonErr(res, 400, 'invalid json'); return json(res, 200, ackMessages(tid, intParam(d.upToSeq, 0, 0, Infinity))); }
      if (req.method === 'GET' && sub === 'inbox') { return json(res, 200, pollMessages(tid, intParam(url.searchParams.get('limit'), CFG.pollLimit, 1, 100))); }
      if (req.method === 'POST' && url.pathname === '/inbox') {
        if (!rateLimit(ep.id)) return jsonErr(res, 429, 'rate limited');
        const sig = req.headers['x-dsh-signature'] || '', ts = req.headers['x-dsh-timestamp'] || '';
        if (CFG.hmacSecret && !verifyHmac(body, sig, ts)) return jsonErr(res, 401, 'invalid signature', 'set X-DSH-Signature + X-DSH-Timestamp');
        const d = safeJsonParse(body, null); if (!d) return jsonErr(res, 400, 'invalid json');
        if (!d.target || !d.payload) return jsonErr(res, 400, "missing 'target' or 'payload'");
        const r = enqueueTx(ep.id, d.target, d.payload, { priority: d.priority, idempotencyKey: d.idempotencyKey, correlationId: d.correlationId });
        if (r.error) { const c = r.code || 500; const body = { error: r.error }; if (r.id) { body.id = r.id; body.seq = r.seq; } return json(res, c, body); }
        return json(res, 202, r);
      }
      if (req.method === 'GET' && url.pathname === '/endpoints') { return json(res, 200, [{ id: ep.id, name: ep.name, type: ep.type, mode: ep.mode, status: ep.status }]); }
      jsonErr(res, 404, 'not found', 'GET /help for docs');
    } catch (e) { try { jsonErr(res, 500, 'internal error'); } catch {}; console.error('[dsh-relay] HTTP:', e); }
  });
  server.on('error', (e) => { if (e.code === 'EADDRINUSE') console.error('[dsh-relay] HTTP port busy'); else console.error('[dsh-relay] HTTP:', e); });
  server.listen(CFG.httpPort, '127.0.0.1');
  console.error(`[dsh-relay] HTTP on 127.0.0.1:${CFG.httpPort}`);
}

function readBody(req, max = 65536) {
  return new Promise((resolve) => {
    let s = 0; const c = []; let done = false;
    const timer = setTimeout(() => { if (!done) { done = true; req.destroy(); resolve(null); } }, 30000); // N8: 30s 超时防 slow-loris
    req.on('data', (d) => { if (done) return; s += d.length; if (s > max) { done = true; clearTimeout(timer); req.destroy(); resolve(null); return; } c.push(d); });
    req.on('end', () => { if (!done) { done = true; clearTimeout(timer); resolve(Buffer.concat(c).toString('utf8')); } });
    req.on('error', () => { if (!done) { done = true; clearTimeout(timer); resolve(null); } });
  });
}
function json(res, code, body, ct = 'application/json') { const t = ct === 'application/json' ? JSON.stringify(body, null, 2) : body; res.writeHead(code, { 'Content-Type': ct }); res.end(t); }
function jsonErr(res, code, error, hint) { json(res, code, { error, ...(hint ? { hint } : {}) }); }
function extractAuth(req) { const a = req.headers.authorization || ''; if (!a.startsWith('Bearer ')) return null; const t = a.slice(7), i = t.lastIndexOf(':'); return i < 1 ? null : { endpointId: t.slice(0, i), token: t.slice(i + 1) }; }
function metrics() { const p = db.prepare("SELECT COUNT(*) as c FROM messages WHERE status = 'pending'").get().c; const d = db.prepare("SELECT COUNT(*) as c FROM messages WHERE status = 'delivered'").get().c; const o = db.prepare("SELECT COUNT(*) as c FROM endpoints WHERE status = 'online'").get().c; return `# HELP dsh_relay_pending Pending\ndsh_relay_pending ${p}\n# HELP dsh_relay_delivered Delivered\ndsh_relay_delivered ${d}\n# HELP dsh_relay_online Online\ndsh_relay_online ${o}\n`; }
const HELP = `dsh-relay v2.0 — Endpoint-to-endpoint message relay
POST /endpoints {"name","type","mode","ttl?"} → {id, token}
POST /inbox {"target","payload",...} Auth: Bearer <id>:<token> HMAC: X-DSH-Signature+X-DSH-Timestamp
POST /endpoints/{id}/ack {"upToSeq"} | DELETE /endpoints/{id} | POST /endpoints/{id}/heartbeat
GET /endpoints/{id}/inbox?limit=N | GET /endpoints (own) | GET /health /metrics /audit / /help
MCP: relay_register relay_poll relay_ack relay_send relay_stats relay_history`;
function webUi() { return `<!DOCTYPE html><html><head><meta charset=utf-8><title>dsh-relay</title></head><body><h1>dsh-relay</h1><p><a href=/help>/help</a></p></body></html>`; }

// ── 定时 ──
setInterval(() => { try { expireEndpoints(); } catch (e) { console.error('[dsh-relay] expire:', e); } }, 60000);
// P1-4: 清理 delivered/pending 超 in-flight TTL 的行 + acked 超保留期
setInterval(() => {
  try {
    const inflightCut = new Date(Date.now() - CFG.inFlightTtlMin * 60000).toISOString();
    const retCut = new Date(Date.now() - CFG.retentionDays * 86400000).toISOString();
    db.prepare("DELETE FROM messages WHERE status = 'delivered' AND delivered_at < ?").run(inflightCut);
    db.prepare("DELETE FROM messages WHERE status = 'pending' AND received_at < ?").run(retCut);
    db.prepare("DELETE FROM messages WHERE status = 'acked' AND acked_at < ?").run(retCut);
    db.prepare("DELETE FROM audit_log WHERE ts < ?").run(retCut);
  } catch (e) { console.error('[dsh-relay] cleanup:', e); }
}, 3600000);
setInterval(() => { rateMap.forEach((v, k) => { if (v.every(t => Date.now() - t > 60000)) rateMap.delete(k); }); nonceCache.forEach((v, k) => { if (Date.now() - v > 300000) nonceCache.delete(k); }); }, 300000);

function gracefulShutdown() { console.error('[dsh-relay] shutting down...'); try { db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); db.close(); } catch {}; process.exit(0); }
process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);
process.on('uncaughtException', (e) => { console.error('[dsh-relay] uncaughtException:', e); process.exit(1); });
process.on('unhandledRejection', (e) => { console.error('[dsh-relay] unhandledRejection:', e); process.exit(1); }); // P2-3: 与 uncaughtException 一致
process.stdout.on('error', (e) => { if (e.code !== 'EPIPE') throw e; }); // P1-3: 吞 EPIPE

function main() { initDb(); startHttp(); handleMcpStdio(); console.error('[dsh-relay] v2.0 ready'); }
main();
