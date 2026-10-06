'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const aedes = require('aedes');
const { MqttOutput } = require('../mqtt-output');
const { loadConfig } = require('../config');
const { devicePublications } = require('../publications');

const logger = { info() {}, warn() {} };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(predicate, timeout = 5000) {
  const end = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() > end) throw new Error('Condition timed out');
    await sleep(10);
  }
}

async function broker() {
  const mqttBroker = aedes();
  const sockets = new Set();
  const received = [];
  const statuses = [];
  let stall = false;
  mqttBroker.authorizePublish = (_client, packet, callback) => {
    if (stall && packet.topic.includes('/state/')) return;
    callback(null);
  };
  mqttBroker.on('publish', (packet, client) => {
    if (!client) return;
    if (packet.topic.includes('/state/')) received.push(packet.payload.toString());
    if (packet.topic.endsWith('/bridge/status')) statuses.push(packet.payload.toString());
  });
  const server = net.createServer(socket => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    mqttBroker.handle(socket);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  const disconnect = async () => {
    sockets.forEach(socket => socket.destroy());
    if (server.listening) await new Promise(resolve => server.close(resolve));
  };
  return {
    url: `mqtt://127.0.0.1:${port}`, received, statuses,
    stall(value) { stall = value; },
    disconnect,
    async restart() { await new Promise(resolve => server.listen(port, '127.0.0.1', resolve)); },
    async close() { await disconnect(); await new Promise(resolve => mqttBroker.close(resolve)); }
  };
}

function output(name, url, bufferLimit = 100) {
  return new MqttOutput({ name, url, bufferLimit, topicPrefix: 'ewelink', qos: 1, retain: true }, {
    logger, publishTimeout: 250, reconnectDelay: 50
  });
}
const publications = (...values) => values.map(value => ({ topic: 'device/state/value', payload: String(value) }));

test('two real brokers: FIFO fan-out, disconnect isolation, reconnect drain, and overflow', async t => {
  const home = await broker();
  const remote = await broker();
  const outputs = [output('home', home.url), output('remote', remote.url, 3)];
  t.after(async () => {
    await Promise.all(outputs.map(item => item.stop(200)));
    await Promise.all([home.close(), remote.close()]);
  });
  outputs.forEach(item => item.start());
  await until(() => outputs.every(item => item.ready));
  const update = devicePublications({ deviceid: 'device', params: { a: 1, b: 2, c: 3 } });
  outputs.forEach(item => item.enqueue(update));
  await until(() => outputs.every(item => item.buffer.depth === 0));
  assert.deepEqual(home.received, ['1', '2', '3']);
  assert.deepEqual(remote.received, home.received);
  await remote.disconnect();
  await until(() => !outputs[1].ready);
  outputs.forEach(item => item.enqueue(publications(4, 5, 6, 7, 8)));
  await until(() => outputs[0].buffer.depth === 0);
  assert.deepEqual(home.received, ['1', '2', '3', '4', '5', '6', '7', '8']);
  assert.equal(outputs[1].buffer.depth, 3);
  assert.equal(outputs[1].buffer.drops, 2);
  await remote.restart();
  await until(() => outputs[1].buffer.depth === 0);
  assert.deepEqual(remote.received, ['1', '2', '3', '6', '7', '8']);
  assert.ok(outputs[1].reconnects > 0);
  await Promise.all(outputs.map(item => item.stop(500)));
  assert.equal(home.statuses.at(-1), 'offline');
  assert.equal(remote.statuses.at(-1), 'offline');
  assert.ok(remote.statuses.filter(status => status === 'online').length >= 2);
});

test('stalled PUBACK does not block healthy output; in-flight is pinned and retried', async t => {
  const home = await broker();
  const remote = await broker();
  const healthy = output('home', home.url);
  const stalled = output('remote', remote.url, 3);
  t.after(async () => {
    await Promise.all([healthy.stop(200), stalled.stop(200)]);
    await Promise.all([home.close(), remote.close()]);
  });
  healthy.start(); stalled.start();
  await until(() => healthy.ready && stalled.ready);
  remote.stall(true);
  stalled.enqueue(publications(1));
  await until(() => stalled.buffer.inFlight);
  for (const item of [healthy, stalled]) item.enqueue(publications(2, 3, 4, 5));
  assert.equal(stalled.buffer.depth, 3);
  assert.equal(stalled.buffer.drops, 2);
  await until(() => healthy.buffer.depth === 0);
  assert.deepEqual(home.received, ['2', '3', '4', '5']);
  await until(() => stalled.failures > 0);
  remote.stall(false);
  await until(() => stalled.buffer.depth === 0);
  assert.deepEqual(remote.received, ['1', '4', '5']);
});

test('unavailable broker does not delay startup or fixed-deadline shutdown; legacy config connects', async t => {
  const home = await broker();
  const unavailable = await broker();
  await unavailable.disconnect();
  const config = loadConfig({ EWELINK_ACCOUNT: 'user', EWELINK_PASSWORD: 'pass', EWELINK_APP_ID: 'id', EWELINK_APP_SECRET: 'secret', MQTT_URL: home.url });
  const legacy = new MqttOutput(config.outputs[0], { logger, publishTimeout: 250, reconnectDelay: 50 });
  const missing = output('unavailable', unavailable.url);
  t.after(async () => {
    await Promise.all([legacy.stop(100), missing.stop(100)]);
    await Promise.all([home.close(), unavailable.close()]);
  });
  missing.start(); legacy.start();
  missing.enqueue(publications(1, 2));
  legacy.enqueue(publications(1, 2));
  await until(() => legacy.buffer.depth === 0);
  assert.deepEqual(home.received, ['1', '2']);
  assert.equal(missing.buffer.depth, 2);
  // Withhold offline confirmation as well: stop must still finish.
  const original = legacy.client.publish.bind(legacy.client);
  legacy.client.publish = (topic, payload, options, callback) => {
    if (payload === 'offline') return;
    return original(topic, payload, options, callback);
  };
  const start = Date.now();
  await Promise.all([legacy.stop(100), missing.stop(100)]);
  assert.ok(Date.now() - start < 1000);
  assert.equal(missing.stopped, true);
});
