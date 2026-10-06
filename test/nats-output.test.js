'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { NatsOutput } = require('../nats-output');
const { loadConfig } = require('../config');
const { devicePublications } = require('../publications');

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(predicate) {
  const deadline = Date.now() + 2000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('condition timed out');
    await sleep(5);
  }
}
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
function fakeClient() {
  const closed = deferred();
  const statuses = [];
  let wake, ended = false;
  return {
    published: [], closeCalls: 0,
    publish(subject, payload) { this.published.push([subject, payload.toString()]); },
    flush: async () => {}, drain: async () => {},
    closed: () => closed.promise,
    isClosed: () => ended,
    async close() { this.closeCalls++; ended = true; wake?.(); closed.resolve(); },
    emit(status) { statuses.push(status); wake?.(); },
    async *status() {
      while (!ended) {
        if (!statuses.length) await new Promise(resolve => { wake = resolve; });
        while (statuses.length) yield statuses.shift();
      }
    }
  };
}
function config(settings = {}) {
  return loadConfig({ EWELINK_ACCOUNT: 'a', EWELINK_PASSWORD: 'b', EWELINK_APP_ID: 'c', EWELINK_APP_SECRET: 'd',
    NATS_SERVERS: 'localhost', NATS_RECONNECT_TIME_WAIT: '1', ...settings }).outputs[0];
}
const logger = { info() {}, warn() {} };
const publications = (...values) => devicePublications({ deviceid: 'a.b', params: Object.fromEntries(values.map((value, index) => [String(index), value])) });

test('initial failures buffer with overflow, retry FIFO, publish only parameters and drain status', async t => {
  const client = fakeClient();
  let attempts = 0;
  const output = new NatsOutput(config({ NATS_BUFFER_LIMIT: '2' }), {
    connect: async () => { if (++attempts === 1) throw new Error('secret'); return client; }, logger
  });
  t.after(() => output.stop(20));
  output.enqueue(publications(1, 2, 3));
  output.start();
  await until(() => output.ready && output.buffer.depth === 0);
  assert.equal(output.buffer.drops, 1);
  assert.equal(output.reconnects, 1);
  assert.deepEqual(client.published, [['ewelink.bridge.status', 'online'], ['ewelink.a_b.state.1', '2'], ['ewelink.a_b.state.2', '3']]);
  let drained = false;
  client.drain = async () => { drained = true; };
  await output.stop(20);
  assert.equal(drained, true);
  assert.deepEqual(client.published.at(-1), ['ewelink.bridge.status', 'offline']);
});

test('disconnect pins data for replay; late flush cannot remove the retried head', async t => {
  const client = fakeClient();
  const flushes = [];
  const output = new NatsOutput(config({ NATS_BUFFER_LIMIT: '2' }), { connect: async () => client, logger });
  t.after(() => output.stop(20));
  output.start();
  await until(() => output.ready);
  client.flush = () => { const operation = deferred(); flushes.push(operation); return operation.promise; };
  output.enqueue(publications('first'));
  await until(() => flushes.length === 1);
  output.enqueue(publications('discard', 'last'));
  assert.equal(output.buffer.drops, 1);
  client.emit({ type: 'disconnect' });
  await until(() => !output.ready);
  assert.equal(output.buffer.inFlight, false);
  client.emit({ type: 'reconnect' });
  await until(() => flushes.length === 2);
  flushes[1].resolve();
  await until(() => flushes.length === 3);
  flushes[0].resolve();
  await sleep(10);
  assert.equal(output.buffer.depth, 2);
  assert.equal(output.buffer.inFlight, true);
  flushes[2].resolve();
  await until(() => flushes.length === 4);
  flushes[3].resolve();
  await until(() => output.buffer.depth === 0);
  assert.deepEqual(client.published.filter(([subject]) => subject.includes('.state.')).map(([, payload]) => payload), ['first', 'first', 'last']);
});

test('asynchronous permission error invalidates status flush and only fails this output', async t => {
  const denied = fakeClient(), healthy = fakeClient();
  denied.flush = async () => { denied.emit({ type: 'error', error: new Error('secret permissions') }); };
  const logs = [];
  const failed = new NatsOutput(config({ NATS_MAX_RECONNECT_ATTEMPTS: '0' }), {
    connect: async () => denied, logger: { info() {}, warn: message => logs.push(message) }
  });
  const good = new NatsOutput(config(), { connect: async () => healthy, logger });
  t.after(() => Promise.all([failed.stop(20), good.stop(20)]));
  failed.start(); good.start();
  failed.enqueue(publications(1)); good.enqueue(publications(1));
  await until(() => failed.failed && good.buffer.depth === 0 && good.ready);
  assert.equal(failed.ready, false);
  assert.equal(failed.buffer.depth, 1);
  assert.ok(logs.every(message => !message.includes('secret')));
});

test('status and publication stalls obey deadlines and preserve bounded buffers', async t => {
  for (const phase of ['status', 'publication']) {
    const client = fakeClient();
    const output = new NatsOutput(config({ NATS_MAX_RECONNECT_ATTEMPTS: '0' }), { connect: async () => client, logger, publishTimeout: 20 });
    t.after(() => output.stop(20));
    if (phase === 'status') client.flush = () => new Promise(() => {});
    output.start();
    if (phase === 'publication') {
      await until(() => output.ready);
      client.flush = () => new Promise(() => {});
    }
    output.enqueue(publications(1));
    await until(() => output.failed);
    assert.equal(output.buffer.depth, 1);
    assert.equal(output.buffer.inFlight, false);
  }
});

test('finite initial retry budget is respected and late connections close after stop', async t => {
  let attempts = 0;
  const output = new NatsOutput(config({ NATS_MAX_RECONNECT_ATTEMPTS: '2' }), {
    connect: async () => { attempts++; throw new Error('unavailable'); }, logger
  });
  t.after(() => output.stop(20));
  output.start();
  await until(() => output.failed);
  assert.equal(attempts, 3);
  const connection = deferred(), client = fakeClient();
  const pending = new NatsOutput(config(), { connect: () => connection.promise, logger });
  pending.start();
  await pending.stop(20);
  connection.resolve(client);
  await until(() => client.closeCalls === 1);
  assert.deepEqual(client.published, []);
});

test('credentials authenticator uses contents; stalled drain cannot delay shutdown', async () => {
  const client = fakeClient();
  client.drain = () => new Promise(() => {});
  const authenticator = () => {};
  const output = new NatsOutput(config({ NATS_CREDS: 'creds\ncontents' }), {
    credsAuthenticator: contents => { assert.equal(contents.toString(), 'creds\ncontents'); return authenticator; },
    connect: async options => { assert.equal(options.authenticator, authenticator); return client; }, logger
  });
  output.start();
  await until(() => output.ready);
  const start = Date.now();
  await output.stop(20);
  await until(() => client.closeCalls > 0);
  assert.ok(Date.now() - start < 500);
});
