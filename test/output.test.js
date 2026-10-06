'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { MqttOutput } = require('../mqtt-output');
const tick = () => new Promise(resolve => setImmediate(resolve));

test('connection and online-status stalls both reconnect under a deadline', async () => {
  for (const phase of ['connection', 'online']) {
    const clients = [];
    const output = new MqttOutput({ name: phase, url: 'mqtt://broker', topicPrefix: 'p', qos: 1, retain: true, bufferLimit: 2 }, {
      connect: () => {
        const client = new EventEmitter();
        client.connected = phase === 'online';
        client.publish = () => {};
        client.end = () => {};
        clients.push(client);
        return client;
      },
      logger: { info() {}, warn() {} }, publishTimeout: 20, reconnectDelay: 1
    });
    try {
      output.start();
      if (phase === 'online') clients[0].emit('connect');
      output.enqueue([{ topic: 'a', payload: '1' }]);
      await new Promise(resolve => setTimeout(resolve, 60));
      assert.ok(output.reconnects > 0);
      assert.ok(clients.length > 1);
      assert.equal(output.buffer.depth, 1);
      assert.equal(output.ready, false);
      if (phase === 'online') assert.equal(output.failures, 1);
    } finally { await output.stop(20); }
  }
});

test('late callback after disconnect cannot acknowledge a retried head', async () => {
  const clients = [];
  const logs = [];
  const connect = () => {
    const client = new EventEmitter();
    client.connected = true;
    client.calls = [];
    client.publish = (topic, payload, options, callback) => {
      if (payload === 'online' || payload === 'offline') callback();
      else client.calls.push({ topic, payload, options, callback });
    };
    client.end = (_force, callback) => { client.connected = false; callback?.(); };
    clients.push(client);
    return client;
  };
  const output = new MqttOutput({ name: 'remote', url: 'mqtt://user:secret@broker', topicPrefix: 'prefix', qos: 0, retain: false, bufferLimit: 2 }, {
    connect, logger: { info: message => logs.push(message), warn: message => logs.push(message) }, reconnectDelay: 1, publishTimeout: 1000
  });
  try {
    output.start(); clients[0].emit('connect');
    output.enqueue([{ topic: 'device/state/a', payload: 'first' }, { topic: 'device/state/a', payload: 'second' }]);
    await tick();
    const oldCallback = clients[0].calls[0].callback;
    clients[0].emit('close');
    await new Promise(resolve => setTimeout(resolve, 20));
    clients[1].emit('connect');
    await tick();
    oldCallback();
    assert.equal(output.buffer.depth, 2);
    assert.equal(output.buffer.inFlight, true);
    assert.equal(clients[1].calls[0].payload, 'first');
    assert.deepEqual(clients[1].calls[0].options, { qos: 0, retain: false });
    clients[1].calls[0].callback();
    await tick();
    assert.equal(clients[1].calls[1].payload, 'second');
    clients[1].calls[1].callback();
    assert.equal(output.buffer.depth, 0);
    assert.equal(output.failures, 1);
    assert.ok(logs.every(message => message.includes('[MQTT remote]')));
    assert.ok(logs.every(message => !message.includes('secret') && !message.includes('user:')));
  } finally { await output.stop(20); }
});

test('synchronous publish exceptions retry without removing data and overflow logs are limited', async () => {
  const client = new EventEmitter();
  client.connected = true;
  client.end = () => {};
  client.publish = (_topic, payload, _options, callback) => {
    if (payload === 'online') callback();
    else throw new Error('secret must not appear in logs');
  };
  const logs = [];
  const output = new MqttOutput({ name: 'home', url: 'mqtt://broker', topicPrefix: 'p', qos: 1, retain: true, bufferLimit: 1 }, {
    connect: () => client, logger: { info() {}, warn: message => logs.push(message) }, reconnectDelay: 1000
  });
  try {
    output.start(); client.emit('connect');
    output.enqueue([{ topic: 'a', payload: '1' }]);
    await tick();
    assert.equal(output.buffer.depth, 1);
    assert.equal(output.buffer.inFlight, false);
    assert.equal(output.failures, 1);
    for (let i = 0; i < 10; i++) output.enqueue([{ topic: 'a', payload: String(i) }]);
    assert.equal(output.buffer.drops, 10);
    assert.equal(logs.filter(message => message.includes('overflow;')).length, 1);
    assert.ok(logs.every(message => !message.includes('secret')));
  } finally { await output.stop(20); }
});
