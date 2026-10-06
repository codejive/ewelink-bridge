'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const net = require('node:net');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const aedes = require('aedes');
const { connect } = require('@nats-io/transport-node');
const { MqttOutput } = require('../mqtt-output');
const { NatsOutput } = require('../nats-output');
const { loadConfig } = require('../config');
const { devicePublications } = require('../publications');
const binary = process.env.NATS_SERVER_BINARY;
const logger = { info() {}, warn() {} };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(predicate, timeout = 5000) {
  const end = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() > end) throw new Error('condition timed out');
    await sleep(10);
  }
}
async function freePort() {
  const server = net.createServer();
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  await new Promise(resolve => server.close(resolve));
  return port;
}
async function natsServer(t, extra = []) {
  const port = await freePort();
  let child;
  async function stop() {
    if (!child || child.exitCode !== null || child.signalCode !== null) return;
    const exited = new Promise(resolve => child.once('exit', resolve));
    child.kill();
    await exited;
  }
  t.after(stop);
  return {
    url: `nats://127.0.0.1:${port}`, stop,
    async start() {
      let ready = false, failure, logs = '';
      child = spawn(binary, ['-a', '127.0.0.1', '-p', String(port), ...extra], { windowsHide: true, stdio: ['ignore', 'ignore', 'pipe'] });
      child.on('error', err => { failure = err; });
      child.stderr.on('data', data => { logs += data.toString(); ready = logs.includes('Server is ready'); });
      await until(() => {
        if (failure) throw failure;
        if (child.exitCode !== null) throw new Error(logs);
        return ready;
      });
    }
  };
}
function config(env) {
  return loadConfig({ EWELINK_ACCOUNT: 'a', EWELINK_PASSWORD: 'b', EWELINK_APP_ID: 'c', EWELINK_APP_SECRET: 'd',
    NATS_RECONNECT_TIME_WAIT: '20', ...env });
}
function collect(client, received) {
  const subscription = client.subscribe('ewelink.>');
  const task = (async () => {
    for await (const message of subscription) received.push([message.subject, message.data.toString()]);
  })();
  return task;
}

test('real MQTT and NATS fan-out stays independent through initial outage, overflow and reconnect replay', { skip: !binary }, async t => {
  const broker = aedes(), mqttReceived = [];
  const sockets = new Set();
  const tcp = net.createServer(socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); broker.handle(socket); });
  broker.on('publish', (packet, client) => { if (client && packet.topic.includes('/state/')) mqttReceived.push(packet.payload.toString()); });
  await new Promise(resolve => tcp.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => tcp.close(resolve));
    await new Promise(resolve => broker.close(resolve));
  });
  const server = await natsServer(t);
  const settings = config({ BRIDGE_OUTPUTS: 'home,events', MQTT_HOME_URL: `mqtt://127.0.0.1:${tcp.address().port}`,
    NATS_EVENTS_SERVERS: server.url, NATS_EVENTS_BUFFER_LIMIT: '3' }).outputs;
  const mqtt = new MqttOutput(settings[0], { logger });
  const nats = new NatsOutput(settings[1], { logger });
  let subscriberReady;
  const subscriptionReady = new Promise(resolve => { subscriberReady = resolve; });
  const initialActivate = nats.activate.bind(nats);
  nats.activate = async (...args) => { await subscriptionReady; return initialActivate(...args); };
  t.after(() => Promise.all([mqtt.stop(200), nats.stop(200)]));
  mqtt.start(); nats.start();
  const publications = (...values) => values.flatMap(value => devicePublications({ deviceid: 'device', params: { value } }));
  for (const output of [mqtt, nats]) output.enqueue(publications(1, 2, 3, 4, 5));
  await until(() => mqtt.buffer.depth === 0 && mqtt.ready);
  assert.deepEqual(mqttReceived, ['1', '2', '3', '4', '5']);
  assert.equal(nats.buffer.depth, 3);
  assert.equal(nats.buffer.drops, 2);
  // Subscribe before allowing the output to connect: Core NATS has no retention.
  await server.start();
  const received = [];
  const subscriber = await connect({ servers: server.url, reconnectTimeWait: 20 });
  t.after(() => subscriber.close());
  collect(subscriber, received);
  await subscriber.flush();
  subscriberReady();
  await until(() => nats.ready && nats.buffer.depth === 0);
  await until(() => received.filter(([subject]) => subject.includes('.state.')).length === 3);
  assert.deepEqual(received.filter(([subject]) => subject.includes('.state.')).map(([, payload]) => payload), ['3', '4', '5']);
  await server.stop();
  await until(() => !nats.ready);
  for (const output of [mqtt, nats]) output.enqueue(publications(6, 7));
  await until(() => mqtt.buffer.depth === 0);
  assert.deepEqual(mqttReceived, ['1', '2', '3', '4', '5', '6', '7']);
  assert.equal(nats.buffer.depth, 2);
  // Wait for subscriber reconnection before output replay for deterministic observation.
  const reconnectStatus = (async () => {
    for await (const status of subscriber.status()) if (status.type === 'reconnect') return;
  })();
  const activate = nats.activate.bind(nats);
  nats.activate = async (...args) => { await reconnectStatus; return activate(...args); };
  await server.start();
  await until(() => nats.ready && nats.buffer.depth === 0);
  await until(() => received.filter(([subject]) => subject.includes('.state.')).length === 5);
  assert.deepEqual(received.filter(([subject]) => subject.includes('.state.')).map(([, payload]) => payload), ['3', '4', '5', '6', '7']);
  await nats.stop(1000);
  await until(() => received.some(([subject, payload]) => subject === 'ewelink.bridge.status' && payload === 'offline'));
});

test('real NATS publish denial preserves queued data while healthy output continues', { skip: !binary }, async t => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'ewelink-test-'));
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const file = path.join(directory, 'server.conf');
  await fs.writeFile(file, 'authorization { users: [{ user: "bridge", password: "secret", permissions: { publish: ["allowed.>"] } }] }');
  const deniedServer = await natsServer(t, ['-c', file]);
  const goodServer = await natsServer(t);
  await deniedServer.start(); await goodServer.start();
  const outputs = config({ BRIDGE_OUTPUTS: 'denied,good', NATS_DENIED_SERVERS: deniedServer.url,
    NATS_DENIED_USER: 'bridge', NATS_DENIED_PASS: 'secret', NATS_DENIED_MAX_RECONNECT_ATTEMPTS: '0',
    NATS_GOOD_SERVERS: goodServer.url }).outputs.map(settings => new NatsOutput(settings, { logger }));
  t.after(() => Promise.all(outputs.map(output => output.stop(200))));
  for (const output of outputs) { output.start(); output.enqueue(devicePublications({ deviceid: '123', params: { switch: 'on' } })); }
  await until(() => outputs[0].failed && outputs[1].ready && outputs[1].buffer.depth === 0);
  assert.equal(outputs[0].buffer.depth, 1);
  assert.equal(outputs[0].ready, false);
});
