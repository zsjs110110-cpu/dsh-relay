#!/usr/bin/env node
/**
 * dsh-relay 端到端冒烟测试
 * 启动服务 → 注册端点 → 发消息 → 轮询 → ACK → 验证 → 关闭
 */
'use strict';
const { spawn } = require('child_process');
const http = require('http');

const PORT = 18999;
const DB = './test-e2e.db';

let serverProc;
let pass = 0, fail = 0;
const results = [];

function log(ok, name, detail) {
  if (ok) { pass++; results.push(`  ✅ ${name}`); }
  else { fail++; results.push(`  ❌ ${name}: ${detail || ''}`); }
  console.log(ok ? `  ✅ ${name}` : `  ❌ ${name}: ${detail || ''}`);
}

function req(method, path, body, headers) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : null;
    const h = { 'Content-Type': 'application/json' };
    if (headers) Object.assign(h, headers);
    if (data) h['Content-Length'] = Buffer.byteLength(data);
    const r = http.request({ host: '127.0.0.1', port: PORT, method, path, headers: h, timeout: 5000 }, (res) => {
      let buf = '';
      res.on('data', d => buf += d);
      res.on('end', () => {
        let json = null;
        try { json = JSON.parse(buf); } catch {}
        resolve({ status: res.statusCode, body: buf, json });
      });
    });
    r.on('error', reject);
    r.on('timeout', () => { r.destroy(); reject(new Error('timeout')); });
    if (data) r.write(data);
    r.end();
  });
}

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function run() {
  console.log('=== dsh-relay E2E Test ===\n');

  // 清理旧 db
  try { require('fs').unlinkSync(DB); } catch {}
  try { require('fs').unlinkSync(DB + '-wal'); } catch {}
  try { require('fs').unlinkSync(DB + '-shm'); } catch {}

  // 1. 启动服务
  console.log('[1] 启动服务...');
  serverProc = spawn(process.execPath, ['server.js'], {
    cwd: __dirname,
    env: { ...process.env, DSH_RELAY_DEV: '1', DSH_RELAY_HTTP_PORT: String(PORT), DSH_RELAY_DB_PATH: DB },
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  let stderr = '';
  serverProc.stderr.on('data', d => { stderr += d; process.stderr.write(d); });

  // 等待启动
  await sleep(1500);

  // 检查服务是否活着
  if (serverProc.exitCode !== null) {
    console.log('\n❌ 服务启动失败，exit code:', serverProc.exitCode);
    console.log('stderr:', stderr);
    process.exit(1);
  }

  try {
    // 2. 健康检查
    console.log('\n[2] 健康检查...');
    let r = await req('GET', '/health');
    log(r.status === 200 && r.json && r.json.status === 'ok', 'GET /health → 200 ok', `got ${r.status} ${r.body}`);

    // 3. 注册端点 A (dsh)
    console.log('\n[3] 注册端点 A (dsh)...');
    r = await req('POST', '/endpoints', { name: 'endpoint-A', type: 'dsh', mode: 'persistent' });
    log(r.status === 201 && r.json && r.json.id && r.json.token, 'POST /endpoints → 201 {id,token}', `got ${r.status} ${r.body}`);
    const epA = r.json;

    // 4. 注册端点 B (codex)
    console.log('\n[4] 注册端点 B (codex)...');
    r = await req('POST', '/endpoints', { name: 'endpoint-B', type: 'codex', mode: 'persistent' });
    log(r.status === 201 && r.json && r.json.id && r.json.token, 'POST /endpoints → 201 {id,token}', `got ${r.status} ${r.body}`);
    const epB = r.json;

    // 5. 未授权访问应被拒
    console.log('\n[5] 未授权访问...');
    r = await req('GET', '/endpoints');
    log(r.status === 401, '无 token → 401', `got ${r.status}`);

    // 6. A 向 B 发消息
    console.log('\n[6] A → B 发消息...');
    const authA = `Bearer ${epA.id}:${epA.token}`;
    r = await req('POST', '/inbox', { target: epB.id, payload: { text: 'hello from A' } }, { Authorization: authA });
    log(r.status === 202 && r.json && r.json.id && r.json.seq === 1, 'POST /inbox → 202 {id,seq:1}', `got ${r.status} ${r.body}`);
    const msg1 = r.json;

    // 7. A 再发一条给 B
    console.log('\n[7] A → B 发第二条...');
    r = await req('POST', '/inbox', { target: epB.id, payload: { text: 'second message' } }, { Authorization: authA });
    log(r.status === 202 && r.json && r.json.seq === 2, 'POST /inbox → 202 {seq:2}', `got ${r.status} ${r.body}`);

    // 8. B 轮询消息
    console.log('\n[8] B 轮询...');
    const authB = `Bearer ${epB.id}:${epB.token}`;
    r = await req('GET', `/endpoints/${epB.id}/inbox?limit=10`, null, { Authorization: authB });
    log(r.status === 200 && r.json && r.json.messages && r.json.messages.length === 2, 'GET /inbox → 200 [2 msgs]', `got ${r.status} msgs=${r.json && r.json.messages ? r.json.messages.length : '?'}`);
    
    // 9. 验证消息内容
    if (r.json && r.json.messages) {
      log(r.json.messages[0].seq === 1 && r.json.messages[0].payload.text === 'hello from A', 'msg[0] seq=1 content ok', '');
      log(r.json.messages[1].seq === 2 && r.json.messages[1].payload.text === 'second message', 'msg[1] seq=2 content ok', '');
    }

    // 10. B ACK 到 seq=2
    console.log('\n[10] B ACK seq=2...');
    r = await req('POST', `/endpoints/${epB.id}/ack`, { upToSeq: 2 }, { Authorization: authB });
    log(r.status === 200 && r.json && r.json.acked === true, 'POST /ack → 200 {acked:true}', `got ${r.status} ${r.body}`);

    // 11. B 再次轮询（应为空）
    console.log('\n[11] B 再次轮询（应空）...');
    r = await req('GET', `/endpoints/${epB.id}/inbox?limit=10`, null, { Authorization: authB });
    log(r.status === 200 && r.json && r.json.messages && r.json.messages.length === 0, 'GET /inbox → 200 [] (empty)', `got ${r.status} msgs=${r.json && r.json.messages ? r.json.messages.length : '?'}`);

    // 12. 幂等性测试：相同 idempotencyKey
    console.log('\n[12] 幂等性测试...');
    r = await req('POST', '/inbox', { target: epB.id, payload: { text: 'idempotent' }, idempotencyKey: 'key-001' }, { Authorization: authA });
    log(r.status === 202, '第一次 → 202', `got ${r.status}`);
    const idem1 = r.json;
    r = await req('POST', '/inbox', { target: epB.id, payload: { text: 'idempotent' }, idempotencyKey: 'key-001' }, { Authorization: authA });
    log(r.status === 409 && r.json && r.json.id === idem1.id, '重复 idempotencyKey → 409 same id', `got ${r.status} ${r.body}`);

    // 13. 向不存在的端点发消息
    console.log('\n[13] 向不存在端点发消息...');
    r = await req('POST', '/inbox', { target: 'ep_NONEXISTENT', payload: { text: 'x' } }, { Authorization: authA });
    log(r.status === 404, 'target not found → 404', `got ${r.status} ${r.body}`);

    // 14. 端点 B 向 A 发消息（双向）
    console.log('\n[14] B → A 发消息（双向）...');
    r = await req('POST', '/inbox', { target: epA.id, payload: { text: 'reply from B' } }, { Authorization: authB });
    log(r.status === 202 && r.json && r.json.seq === 1, 'B→A → 202 {seq:1}', `got ${r.status} ${r.body}`);

    // 15. A 轮询
    console.log('\n[15] A 轮询...');
    r = await req('GET', `/endpoints/${epA.id}/inbox?limit=10`, null, { Authorization: authA });
    log(r.status === 200 && r.json && r.json.messages && r.json.messages.length === 1, 'A GET /inbox → 200 [1 msg]', `got ${r.status}`);

    // 16. 心跳
    console.log('\n[16] 心跳...');
    r = await req('POST', `/endpoints/${epA.id}/heartbeat`, null, { Authorization: authA });
    log(r.status === 200 && r.json && r.json.status === 'online', 'POST /heartbeat → 200 online', `got ${r.status} ${r.body}`);

    // 17. DELETE 端点
    console.log('\n[17] 注销端点 B...');
    r = await req('DELETE', `/endpoints/${epB.id}`, null, { Authorization: authB });
    log(r.status === 200 && r.json && r.json.deleted === true, 'DELETE /endpoints/{id} → 200', `got ${r.status} ${r.body}`);

    // 18. /metrics
    console.log('\n[18] /metrics...');
    r = await req('GET', '/metrics');
    log(r.status === 200 && r.body.includes('dsh_relay_'), 'GET /metrics → 200 prometheus', `got ${r.status}`);

    // 19. /help
    console.log('\n[19] /help...');
    r = await req('GET', '/help');
    log(r.status === 200 && r.body.includes('dsh-relay'), 'GET /help → 200', `got ${r.status}`);

    // 20. forbidden host
    console.log('\n[20] forbidden host...');
    r = await new Promise((resolve) => {
      http.request({ host: '127.0.0.1', port: PORT, method: 'GET', path: '/health', headers: { Host: 'evil.com' } }, (res) => {
        let b = ''; res.on('data', d => b += d); res.on('end', () => resolve({ status: res.statusCode, body: b }));
      }).end();
    });
    log(r.status === 403, 'evil Host → 403', `got ${r.status}`);

  } catch (e) {
    console.log('\n❌ 测试异常:', e.message);
    console.log(e.stack);
  } finally {
    console.log('\n=== 清理 ===');
    serverProc.kill('SIGTERM');
    await sleep(500);
    if (serverProc.exitCode === null) serverProc.kill('SIGKILL');
    try { require('fs').unlinkSync(DB); } catch {}
    try { require('fs').unlinkSync(DB + '-wal'); } catch {}
    try { require('fs').unlinkSync(DB + '-shm'); } catch {}

    console.log(`\n=== 结果: ${pass} passed, ${fail} failed ===`);
    process.exit(fail > 0 ? 1 : 0);
  }
}

run();
