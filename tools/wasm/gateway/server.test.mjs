import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import dgram from 'node:dgram';
import { once } from 'node:events';
import WebSocket from 'ws';
import { createGateway, gatewayOptionsFromEnv, LIMITS } from './server.mjs';

const origin = 'https://game.example';
const listen = server => new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
// Resolves once `done()` holds, checked on each relay datagram, or after `ms`.
const until = (udp, done, ms) => new Promise(resolve => {
  const check = () => { if (done()) finish(); };
  const finish = () => { clearTimeout(timer); udp.off('message', check); resolve(); };
  const timer = setTimeout(finish, ms);
  udp.on('message', check);
  check();
});

async function fixture(t, extra = {}) {
  const udp = dgram.createSocket('udp4');
  udp.bind(0, '127.0.0.1');
  await once(udp, 'listening');
  const requests = [];
  const upstream = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', bytes => chunks.push(bytes));
    req.on('end', () => {
      requests.push({ path: req.url, method: req.method, auth: req.headers.authorization,
        cookie: req.headers.cookie, address: req.headers['vera-client-address'],
        key: req.headers['vera-gateway-key'], body: Buffer.concat(chunks) });
      if (req.url === '/v1/oversize') { res.end(Buffer.alloc(LIMITS.responseBytes + 1)); return; }
      res.writeHead(409, { 'content-type': 'application/json' });
      res.end(Buffer.from('{"error":{"code":"map_pending"}}'));
    });
  });
  await listen(upstream);
  const options = { lobby: `http://127.0.0.1:${upstream.address().port}`, allowHttp: true,
    relayHost: '127.0.0.1', relayPort: udp.address().port, origin, ...extra };
  const gateway = createGateway(options);
  await listen(gateway.server);
  t.after(async () => {
    await gateway.close();
    await new Promise(resolve => { upstream.close(resolve); upstream.closeAllConnections(); });
    udp.close();
  });
  const base = `http://127.0.0.1:${gateway.server.address().port}`;
  const socket = async (route = 'primary') => {
    const ws = new WebSocket(`${base.replace('http:', 'ws:')}/game/${route}`, { origin });
    t.after(() => ws.terminate());
    await once(ws, 'open');
    return ws;
  };
  return { udp, options, gateway, requests, base, socket };
}

test('lobby HTTP remains byte-for-byte, bounded and restricted to its configured upstream', { timeout: 10000 }, async t => {
  const { base, options, requests } = await fixture(t);
  assert.equal(gatewayOptionsFromEnv({}).lobby, 'https://lobby.vera20k.org');
  assert.throws(() => createGateway({ ...options, lobby: 'http://remote.example', allowHttp: true }), /HTTPS/);
  assert.throws(() => createGateway({ ...options, lobby: 'https://user:password@lobby.example' }), /HTTPS/);
  const body = '{ "request_id": "7", "payload": {"text":"中文"} }';
  const response = await fetch(`${base}/v1/commands?after=2`, {
    method: 'POST', headers: { origin, authorization: 'Bearer test-token', cookie: 'must-not-forward=1' }, body,
  });
  assert.equal(response.status, 409);
  assert.equal(response.headers.get('access-control-allow-origin'), origin);
  assert.equal(await response.text(), '{"error":{"code":"map_pending"}}');
  // Without the lobby key the gateway names no client address.
  assert.deepEqual(requests[0], { path: '/v1/commands?after=2', method: 'POST', auth: 'Bearer test-token',
    cookie: undefined, address: undefined, key: undefined, body: Buffer.from(body) });
  const preflight = await fetch(`${base}/v1/commands`, { method: 'OPTIONS', headers: {
    origin, 'access-control-request-method': 'POST', 'access-control-request-headers': 'Authorization, Content-Type',
  } });
  assert.equal(preflight.status, 204);
  assert.equal(preflight.headers.get('access-control-allow-headers'), 'Authorization, Content-Type');
  assert.equal((await fetch(`${base}/v1/events`, { headers: { origin: 'https://foreign.example' } })).status, 403);
  assert.equal((await fetch(`${base}/v1/events`, { method: 'OPTIONS', headers: {
    origin, 'access-control-request-method': 'POST', 'access-control-request-headers': 'x-target-host',
  } })).status, 403);
  assert.equal((await fetch(`${base}/v1/commands`, { method: 'POST', body: Buffer.alloc(LIMITS.requestBytes + 1) })).status, 413);
  assert.equal(requests.length, 1);
  await assert.rejects(async () => { const oversized = await fetch(`${base}/v1/oversize`); await oversized.arrayBuffer(); });
});

test('with the lobby key, the lobby learns each player\'s own address, which requests cannot name', { timeout: 10000 }, async t => {
  const lobbyKey = 'k'.repeat(32);
  const { base, options, requests } = await fixture(t, { lobbyKey, clientIpHeader: 'x-forwarded-for' });
  assert.throws(() => createGateway({ ...options, lobbyKey: 'short' }), /32 characters/);
  assert.equal(gatewayOptionsFromEnv({ VERA_GATEWAY_KEY: lobbyKey }).lobbyKey, lobbyKey);
  const response = await fetch(`${base}/v1/events`, { headers: { origin, 'x-forwarded-for': '10.0.0.1, 203.0.113.7',
    'vera-client-address': '192.0.2.1', 'vera-gateway-key': 'x'.repeat(32) } });
  assert.equal(response.status, 409);
  assert.deepEqual([requests[0].address, requests[0].key], ['203.0.113.7', lobbyKey]);
});

test('each WS channel has a stable isolated UDP port, preserves binary boundaries and releases the port', { timeout: 10000 }, async t => {
  const { udp, socket } = await fixture(t);
  const ports = new Set();
  udp.on('message', (data, peer) => {
    ports.add(peer.port);
    udp.send(data, peer.port, peer.address);
  });
  const first = await socket('primary');
  const alternate = await socket('alternate');
  const second = await socket('primary');
  // Includes all byte values and exceeds today's 827-byte encrypted relay maximum.
  const payload = Buffer.from(Array.from({ length: 2048 }, (_, index) => index & 255));
  for (const ws of [first, alternate, second]) {
    const received = once(ws, 'message');
    ws.send(payload);
    const [data, binary] = await received;
    assert.equal(binary, true);
    assert.deepEqual(data, payload);
  }
  assert.equal(ports.size, 3);
  const messages = [];
  const twoReceived = new Promise(resolve => first.on('message', data => {
    messages.push(data); if (messages.length === 2) resolve();
  }));
  first.send(Buffer.from([0, 255]));
  first.send(Buffer.from([1]));
  await twoReceived;
  assert.deepEqual(messages, [Buffer.from([0, 255]), Buffer.from([1])]);
  assert.equal(ports.size, 3);
  for (const ws of [first, alternate, second]) {
    const closed = once(ws, 'close');
    ws.close();
    await closed;
  }
  // A fresh socket can bind each exact old source port after channel cleanup.
  for (const port of ports) {
    const rebound = dgram.createSocket('udp4');
    t.after(() => { try { rebound.close(); } catch {} });
    rebound.bind(port, '127.0.0.1');
    await once(rebound, 'listening');
    rebound.close();
  }
});

test('WS rejects arbitrary routes, foreign origins, text and oversized messages', { timeout: 10000 }, async t => {
  const { base, socket } = await fixture(t);
  const url = `${base.replace('http:', 'ws:')}/game/primary`;
  const foreign = new WebSocket(url, { origin: 'https://foreign.example' });
  await once(foreign, 'error');
  const arbitrary = new WebSocket(`${url}?target=127.0.0.1:9`, { origin });
  await once(arbitrary, 'error');
  const text = await socket();
  let closed = once(text, 'close');
  text.send('not a datagram');
  await closed;
  const oversized = await socket();
  closed = once(oversized, 'close');
  oversized.send(Buffer.alloc(2049));
  const [code] = await closed;
  assert.equal(code, 1009);
});

for (const lanSignal of [undefined, { iceServers: [] }]) {
  test(`malformed upgrade URLs are rejected with LAN signaling ${lanSignal ? 'on' : 'off'}`, { timeout: 10000 }, async t => {
    const { base, udp, socket, gateway } = await fixture(t, { lanSignal });
    // Fail synchronously if the callback throws, before opening an upgraded socket
    // that the failed callback would leave outside the gateway's cleanup registry.
    assert.doesNotThrow(() => gateway.server.emit('upgrade',
      { url: 'http://[', headers: {} }, { end() {} }, Buffer.alloc(0)));
    const response = await new Promise((resolve, reject) => {
      // Node accepts this HTTP request target, but the WHATWG URL parser rejects it.
      const request = http.get(base, {
        path: 'http://[', headers: { connection: 'Upgrade', upgrade: 'websocket' },
      }, resolve);
      request.on('error', reject);
      request.setTimeout(2000, () => request.destroy(new Error('Upgrade rejection timed out')));
    });
    const chunks = [];
    for await (const chunk of response) chunks.push(chunk);
    assert.equal(response.statusCode, 400);
    assert.equal(response.headers.connection, 'close');
    assert.equal(Buffer.concat(chunks).length, 0);

    // The same gateway must still carry real game datagrams after the rejection.
    udp.on('message', (data, peer) => udp.send(data, peer.port, peer.address));
    const game = await socket();
    const echoed = once(game, 'message');
    const payload = Buffer.from([1, 2, 3]);
    game.send(payload);
    assert.deepEqual((await echoed)[0], payload);
    if (lanSignal) {
      const signaling = new WebSocket(`${base.replace('http:', 'ws:')}/lan/signal`, { origin });
      t.after(() => signaling.terminate());
      const [welcome] = await once(signaling, 'message');
      assert.equal(JSON.parse(welcome.toString()).type, 'welcome');
    }
  });
}

test('datagrams past a connection budget or full queue are lost while the connection stays open', { timeout: 10000 }, async t => {
  const { udp, socket } = await fixture(t);
  const flood = await socket();
  // Small messages overrun both the bounded queue and the one-second budget.
  for (let i = 0; i <= LIMITS.packetsPerSecond; i++) flood.send(Buffer.from([1]));
  await sleep(1100);
  assert.equal(flood.readyState, WebSocket.OPEN);
  let marked = false;
  udp.on('message', data => { if (data[0] === 2) marked = true; });
  flood.send(Buffer.from([2]));
  await until(udp, () => marked, 2000);
  assert.equal(marked, true);
});

test('connections spending the aggregate budget cannot take a steady connection\'s share', { timeout: 10000 }, async t => {
  const { udp, socket } = await fixture(t);
  const ports = new Map();
  const steadyPackets = [];
  udp.on('message', (data, peer) => {
    if (!ports.has(data[0])) ports.set(data[0], peer.port);
    if (data[0] === 0) steadyPackets.push(data);
  });
  const steady = await socket();
  // One more flooder than the aggregate needs, so its budget is spent in full.
  const count = LIMITS.totalPacketsPerSecond / LIMITS.packetsPerSecond + 1;
  const flooders = await Promise.all(Array.from({ length: count }, () => socket()));
  let flooded = 0;
  for (const ws of flooders) ws.on('message', () => { flooded += 1; });
  // The first window records each connection's traffic: four steady datagrams.
  for (let i = 0; i < 4; i++) steady.send(Buffer.from([0]));
  flooders.forEach((ws, index) => ws.send(Buffer.from([index + 1])));
  await until(udp, () => ports.size === count + 1 && steadyPackets.length === 4, 2000);
  await sleep(1100);
  // Next window the relay floods every flooder past its own budget.
  const targets = flooders.map((_, index) => ports.get(index + 1));
  for (let sent = 0; sent < LIMITS.packetsPerSecond; sent += 32) {
    for (const port of targets) for (let i = 0; i < 32; i++) udp.send(Buffer.from([9]), port, '127.0.0.1');
    await new Promise(resolve => setImmediate(resolve));
  }
  const spare = LIMITS.totalPacketsPerSecond - 4;
  const deadline = Date.now() + 500;
  while (flooded < spare && Date.now() < deadline) await sleep(5);
  for (let i = 0; i < 4; i++) steady.send(Buffer.from([0]));
  await until(udp, () => steadyPackets.length === 8, 500);
  assert.equal(steadyPackets.length, 8);
  // The flooders spent all the rest, and every connection stays open.
  assert.equal(flooded, spare);
  for (const ws of [steady, ...flooders]) assert.equal(ws.readyState, WebSocket.OPEN);
});
