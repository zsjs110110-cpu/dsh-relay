#!/usr/bin/env node
/**
 * dsh-relay MCP stdio 通道测试
 * HTTP 进程注册端点 → stdio 进程 initialize/register/poll/ack
 */
'use strict';
const { spawn } = require('child_process');
const http = require('http');

const PORT = 18998;
const DB = './test-mcp.db';
let pass = 0, fail = 0;

function log(ok, name, detail) {
  if (ok) { pass++; } else { fail++; }
  console.log(ok ? `  ✅ ${name}` : `  ❌ ${name}: ${detail || ''}`);
}

function httpReq(method, path, body, headers) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : null;
    const h = { 'Content-Type': 'application/json' };
    if (headers) Object.assign(h, headers);
    if (data) h['Content-Length'] = Buffer.byteLength(data);
    const r = http.request({ host: '127.0.0.1', port: PORT, method, path, headers: h, timeout: 5000 }, (res) => {
      let buf = ''; res.on('data', d => buf += d);
      res.on('end', () => { let j=null; try{j=JSON.parse(buf)}catch{} resolve({status:res.statusCode,body:buf,json:j}) });
    });
    r.on('error', reject); r.on('timeout', () => { r.destroy(); reject(new Error('timeout')); });
    if (data) r.write(data); r.end();
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// MCP stdio 客户端
class McpClient {
  constructor(proc) { this.proc = proc; this.buf = ''; this.pending = new Map(); this.nextId = 1;
    proc.stdout.setEncoding('utf8');
    proc.stdout.on('data', (d) => {
      this.buf += d;
      let idx;
      while ((idx = this.buf.indexOf('\n')) >= 0) {
        const line = this.buf.slice(0, idx).trim(); this.buf = this.buf.slice(idx + 1);
        if (!line) continue;
        try { const msg = JSON.parse(line); if (msg.id !== undefined && this.pending.has(msg.id)) { this.pending.get(msg.id)(msg); this.pending.delete(msg.id); } } catch {}
      }
    });
  }
  call(method, params) {
    return new Promise((resolve) => {
      const id = this.nextId++;
      this.pending.set(id, resolve);
      this.proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params: params || {} }) + '\n');
    });
  }
}

async function run() {
  console.log('=== dsh-relay MCP stdio Test ===\n');

  // 清理
  for (const f of [DB, DB+'-wal', DB+'-shm']) try { require('fs').unlinkSync(f); } catch {}

  // 1. 启动 HTTP 进程（共享 DB）
  console.log('[1] 启动 HTTP 进程...');
  const httpProc = spawn(process.execPath, ['server.js'], {
    cwd: __dirname,
    env: { ...process.env, DSH_RELAY_DEV: '1', DSH_RELAY_HTTP_PORT: String(PORT), DSH_RELAY_DB_PATH: DB },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  httpProc.stderr.on('data', d => process.stderr.write(d));
  await sleep(1500);

  // 2. HTTP 注册端点 A
  console.log('[2] HTTP 注册端点 A...');
  let r = await httpReq('POST', '/endpoints', { name: 'http-A', type: 'dsh' });
  log(r.status === 201 && r.json.id, 'HTTP register A', `got ${r.status}`);
  const epA = r.json;

  // 3. 启动 stdio 进程（共享 DB，无 HTTP）
  console.log('[3] 启动 stdio 进程...');
  const stdioProc = spawn(process.execPath, ['server.js'], {
    cwd: __dirname,
    env: { ...process.env, DSH_RELAY_DEV: '1', DSH_RELAY_HTTP_PORT: '0', DSH_RELAY_DB_PATH: DB },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  stdioProc.stderr.on('data', d => process.stderr.write(d));
  await sleep(1000);

  const mcp = new McpClient(stdioProc);

  try {
    // 4. MCP initialize
    console.log('\n[4] MCP initialize...');
    let resp = await mcp.call('initialize');
    log(resp.result && resp.result.serverInfo && resp.result.serverInfo.name === 'dsh-relay', 'initialize → serverInfo', JSON.stringify(resp));

    // 5. MCP ping
    console.log('\n[5] MCP ping...');
    resp = await mcp.call('ping');
    log(resp.result && Object.keys(resp.result).length === 0, 'ping → {}', JSON.stringify(resp));

    // 6. MCP tools/list
    console.log('\n[6] MCP tools/list...');
    resp = await mcp.call('tools/list');
    const tools = resp.result && resp.result.tools;
    log(tools && tools.length === 6, `tools/list → 6 tools (got ${tools ? tools.length : 0})`, '');
    if (tools) {
      const names = tools.map(t => t.name).sort().join(',');
      log(names === 'relay_ack,relay_history,relay_poll,relay_register,relay_send,relay_stats', `tools: ${names}`, '');
    }

    // 7. MCP relay_register
    console.log('\n[7] MCP relay_register...');
    resp = await mcp.call('tools/call', { name: 'relay_register', arguments: { name: 'stdio-B', type: 'codex' } });
    let regResult;
    try { regResult = JSON.parse(resp.result.content[0].text); } catch { regResult = null; }
    log(regResult && regResult.id && regResult.token, `register → ${regResult ? regResult.id : 'null'}`, '');
    const epB = regResult;

    // 8. HTTP: A 向 stdio-B 发消息
    console.log('\n[8] HTTP A → stdio-B 发消息...');
    const authA = `Bearer ${epA.id}:${epA.token}`;
    r = await httpReq('POST', '/inbox', { target: epB.id, payload: { text: 'hello from HTTP A to stdio B' } }, { Authorization: authA });
    log(r.status === 202, `HTTP A→B → 202 seq=${r.json ? r.json.seq : '?'}`, `got ${r.status}`);

    // 9. MCP relay_poll（stdio-B 收消息）
    console.log('\n[9] MCP relay_poll...');
    resp = await mcp.call('tools/call', { name: 'relay_poll', arguments: { limit: 10 } });
    let pollResult;
    try { pollResult = JSON.parse(resp.result.content[0].text); } catch { pollResult = null; }
    log(pollResult && pollResult.messages && pollResult.messages.length === 1, `poll → ${pollResult && pollResult.messages ? pollResult.messages.length : 0} msgs`, '');
    if (pollResult && pollResult.messages && pollResult.messages.length > 0) {
      log(pollResult.messages[0].payload.text === 'hello from HTTP A to stdio B', 'msg content ok', pollResult.messages[0].payload.text);
      log(pollResult.messages[0].seq === 1, 'seq=1', '');
    }

    // 10. MCP relay_stats
    console.log('\n[10] MCP relay_stats...');
    resp = await mcp.call('tools/call', { name: 'relay_stats', arguments: {} });
    let statsResult;
    try { statsResult = JSON.parse(resp.result.content[0].text); } catch { statsResult = null; }
    log(statsResult && statsResult.endpoint && statsResult.endpoint.name === 'stdio-B', `stats → name=${statsResult && statsResult.endpoint ? statsResult.endpoint.name : '?'}`, '');
    log(statsResult && statsResult.delivered === 1, `stats → delivered=${statsResult ? statsResult.delivered : '?'}`, '');

    // 11. MCP relay_ack
    console.log('\n[11] MCP relay_ack...');
    resp = await mcp.call('tools/call', { name: 'relay_ack', arguments: { upToSeq: 1 } });
    let ackResult;
    try { ackResult = JSON.parse(resp.result.content[0].text); } catch { ackResult = null; }
    log(ackResult && ackResult.acked === true, `ack → ${ackResult ? JSON.stringify(ackResult) : 'null'}`, '');

    // 12. MCP relay_poll again (empty)
    console.log('\n[12] MCP relay_poll again (应空)...');
    resp = await mcp.call('tools/call', { name: 'relay_poll', arguments: { limit: 10 } });
    let poll2;
    try { poll2 = JSON.parse(resp.result.content[0].text); } catch { poll2 = null; }
    log(poll2 && poll2.messages && poll2.messages.length === 0, `poll → ${poll2 && poll2.messages ? poll2.messages.length : '?'} msgs`, '');

    // 13. MCP relay_history
    console.log('\n[13] MCP relay_history...');
    resp = await mcp.call('tools/call', { name: 'relay_history', arguments: { limit: 50 } });
    let hist;
    try { hist = JSON.parse(resp.result.content[0].text); } catch { hist = null; }
    log(Array.isArray(hist) && hist.length === 1, `history → ${Array.isArray(hist) ? hist.length : '?'} msgs`, '');

    // 14. MCP unknown method → error
    console.log('\n[14] MCP unknown method...');
    resp = await mcp.call('nonexistent_method');
    log(resp.error && resp.error.code === -32601, `unknown method → -32601`, JSON.stringify(resp));

    // 15. MCP relay_send（stdio-B 向 HTTP-A 发消息，双向）
    console.log('\n[15] MCP relay_send B→A...');
    resp = await mcp.call('tools/call', { name: 'relay_send', arguments: { target: epA.id, payload: { text: 'reply from stdio B' } } });
    let sendResult;
    try { sendResult = JSON.parse(resp.result.content[0].text); } catch { sendResult = null; }
    log(sendResult && sendResult.id && sendResult.seq === 1, `send → id=${sendResult ? sendResult.id : '?'} seq=${sendResult ? sendResult.seq : '?'}`, '');

    // 16. HTTP A 轮询收到消息
    console.log('\n[16] HTTP A 轮询...');
    r = await httpReq('GET', `/endpoints/${epA.id}/inbox?limit=10`, null, { Authorization: authA });
    log(r.status === 200 && r.json && r.json.messages && r.json.messages.length === 1, `HTTP A poll → ${r.json && r.json.messages ? r.json.messages.length : '?'} msgs`, '');

  } catch (e) {
    console.log('\n❌ 异常:', e.message, e.stack);
  } finally {
    console.log('\n=== 清理 ===');
    stdioProc.kill('SIGTERM');
    httpProc.kill('SIGTERM');
    await sleep(500);
    for (const f of [DB, DB+'-wal', DB+'-shm']) try { require('fs').unlinkSync(f); } catch {}
    console.log(`\n=== 结果: ${pass} passed, ${fail} failed ===`);
    process.exit(fail > 0 ? 1 : 0);
  }
}

run();
