#!/usr/bin/env node
/**
 * dsh-relay 生产模式测试：HMAC 签名 + fail-closed 启动
 */
'use strict';
const { spawn } = require('child_process');
const http = require('http');
const crypto = require('crypto');

const PORT = 18997;
const DB = './test-prod.db';
let pass = 0, fail = 0;
const SECRET = 'test-secret-key-2026';
const ADMIN = 'admin-token-xyz';

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

function signHmac(body) {
  const ts = Date.now();
  const sig = crypto.createHmac('sha256', SECRET).update(`${ts}.${body}`).digest('hex');
  return { 'X-DSH-Signature': `sha256=${sig}`, 'X-DSH-Timestamp': String(ts) };
}

async function run() {
  console.log('=== dsh-relay 生产模式测试 ===\n');

  // 清理
  for (const f of [DB, DB+'-wal', DB+'-shm']) try { require('fs').unlinkSync(f); } catch {}

  // 1. fail-closed: 无 HMAC_SECRET 应 exit 1
  console.log('[1] fail-closed 启动测试（无 SECRET）...');
  const noSecretProc = spawn(process.execPath, ['server.js'], {
    cwd: __dirname,
    env: { ...process.env, DSH_RELAY_HTTP_PORT: String(PORT), DSH_RELAY_DB_PATH: DB + '.nos', DSH_RELAY_DEV: '0', DSH_RELAY_HMAC_SECRET: '', DSH_RELAY_ADMIN_TOKEN: '' },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  let noSecretErr = '';
  noSecretProc.stderr.on('data', d => noSecretErr += d);
  await sleep(2000);
  log(noSecretProc.exitCode === 1 && noSecretErr.includes('FATAL'), `无 SECRET → exit 1 (got exitCode=${noSecretProc.exitCode})`, noSecretErr.trim());

  // 2. 有 SECRET + ADMIN → 正常启动
  console.log('\n[2] 生产模式启动（有 SECRET + ADMIN）...');
  const server = spawn(process.execPath, ['server.js'], {
    cwd: __dirname,
    env: { ...process.env, DSH_RELAY_HTTP_PORT: String(PORT), DSH_RELAY_DB_PATH: DB, DSH_RELAY_DEV: '0', DSH_RELAY_HMAC_SECRET: SECRET, DSH_RELAY_ADMIN_TOKEN: ADMIN },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  let srvErr = '';
  server.stderr.on('data', d => { srvErr += d; process.stderr.write(d); });
  await sleep(1500);
  log(server.exitCode === null && srvErr.includes('ready'), '生产模式启动成功', `exitCode=${server.exitCode} err=${srvErr.trim()}`);

  try {
    // 3. 健康检查
    let r = await httpReq('GET', '/health');
    log(r.status === 200, 'GET /health → 200', `got ${r.status}`);

    // 4. 注册端点 A
    r = await httpReq('POST', '/endpoints', { name: 'prod-A', type: 'dsh' });
    log(r.status === 201, `register A → 201`, `got ${r.status}`);
    const epA = r.json;

    // 5. 注册端点 B
    r = await httpReq('POST', '/endpoints', { name: 'prod-B', type: 'codex' });
    log(r.status === 201, `register B → 201`, `got ${r.status}`);
    const epB = r.json;

    // 6. 无 HMAC 签名发消息 → 应被拒
    console.log('\n[6] 无 HMAC 签名 → 拒绝...');
    const authA = `Bearer ${epA.id}:${epA.token}`;
    r = await httpReq('POST', '/inbox', { target: epB.id, payload: { text: 'no sig' } }, { Authorization: authA });
    log(r.status === 401, `无签名 → 401`, `got ${r.status} ${r.body}`);

    // 7. 有 HMAC 签名发消息 → 成功
    console.log('\n[7] 有 HMAC 签名 → 接受...');
    const body = JSON.stringify({ target: epB.id, payload: { text: 'signed message' } });
    const sig = signHmac(body);
    r = await httpReq('POST', '/inbox', { target: epB.id, payload: { text: 'signed message' } }, { Authorization: authA, ...sig });
    log(r.status === 202, `有签名 → 202`, `got ${r.status} ${r.body}`);

    // 8. 错误的 HMAC 签名 → 拒绝
    console.log('\n[8] 错误 HMAC 签名 → 拒绝...');
    r = await httpReq('POST', '/inbox', { target: epB.id, payload: { text: 'bad sig' } }, { Authorization: authA, 'X-DSH-Signature': 'sha256=deadbeef', 'X-DSH-Timestamp': String(Date.now()) });
    log(r.status === 401, `错误签名 → 401`, `got ${r.status} ${r.body}`);

    // 9. 过期时间戳 → 拒绝
    console.log('\n[9] 过期时间戳 → 拒绝...');
    const oldTs = Date.now() - 400000; // >5min
    const oldSig = crypto.createHmac('sha256', SECRET).update(`${oldTs}.${body}`).digest('hex');
    r = await httpReq('POST', '/inbox', { target: epB.id, payload: { text: 'old ts' } }, { Authorization: authA, 'X-DSH-Signature': `sha256=${oldSig}`, 'X-DSH-Timestamp': String(oldTs) });
    log(r.status === 401, `过期时间戳 → 401`, `got ${r.status} ${r.body}`);

    // 10. B 轮询收到消息
    console.log('\n[10] B 轮询...');
    const authB = `Bearer ${epB.id}:${epB.token}`;
    r = await httpReq('GET', `/endpoints/${epB.id}/inbox?limit=10`, null, { Authorization: authB });
    log(r.status === 200 && r.json && r.json.messages && r.json.messages.length === 1, `B poll → 1 msg`, `got ${r.status}`);

    // 11. /audit 需要 admin token
    console.log('\n[11] /audit 权限...');
    r = await httpReq('GET', '/audit');
    log(r.status === 403, `无 admin → 403`, `got ${r.status}`);
    r = await httpReq('GET', '/audit?limit=10', null, { Authorization: `Bearer ${ADMIN}` });
    log(r.status === 200 && Array.isArray(r.json), `有 admin → 200 [log]`, `got ${r.status}`);

    // 12. 重复 nonce 防重放
    console.log('\n[12] nonce 防重放...');
    const replayBody = JSON.stringify({ target: epB.id, payload: { text: 'replay attempt' } });
    const replaySig = signHmac(replayBody);
    r = await httpReq('POST', '/inbox', { target: epB.id, payload: { text: 'replay attempt' } }, { Authorization: authA, ...replaySig });
    log(r.status === 202, `第一次 → 202`, `got ${r.status}`);
    // 用完全相同的 ts+sig 再发一次（模拟重放）
    r = await httpReq('POST', '/inbox', { target: epB.id, payload: { text: 'replay attempt' } }, { Authorization: authA, ...replaySig });
    log(r.status === 401, `重放 → 401`, `got ${r.status} ${r.body}`);

  } catch (e) {
    console.log('\n❌ 异常:', e.message, e.stack);
  } finally {
    console.log('\n=== 清理 ===');
    server.kill('SIGTERM');
    await sleep(500);
    for (const f of [DB, DB+'-wal', DB+'-shm', DB+'.nos']) try { require('fs').unlinkSync(f); } catch {}
    console.log(`\n=== 结果: ${pass} passed, ${fail} failed ===`);
    process.exit(fail > 0 ? 1 : 0);
  }
}

run();
