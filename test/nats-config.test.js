'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { loadConfig } = require('../config');
const credentials = { EWELINK_ACCOUNT: 'a', EWELINK_PASSWORD: 'b', EWELINK_APP_ID: 'c', EWELINK_APP_SECRET: 'd' };

test('mixed outputs inherit protocol settings without inheriting credentials', () => {
  const outputs = loadConfig({ ...credentials, BRIDGE_OUTPUTS: 'home,events',
    MQTT_HOME_URL: 'mqtt://home', NATS_EVENTS_SERVERS: 'nats://one:4222, nats://two:4222',
    NATS_TOKEN: 'ignored', NATS_TLS_CA: 'ignored', NATS_EVENTS_TOKEN: 'token', NATS_EVENTS_TLS_CA: 'pem',
    SUBJECT_PREFIX: 'home.events', NATS_BUFFER_LIMIT: '12', NATS_EVENTS_BUFFER_LIMIT: '2',
    NATS_CONNECT_TIMEOUT: '100', NATS_EVENTS_RECONNECT_TIME_WAIT: '0' }).outputs;
  assert.equal(outputs[0].type, 'mqtt');
  assert.equal(outputs[1].type, 'nats');
  assert.equal(outputs[1].bufferLimit, 2);
  assert.equal(outputs[1].subjectPrefix, 'home.events');
  assert.deepEqual(outputs[1].options.servers, ['nats://one:4222', 'nats://two:4222']);
  assert.equal(outputs[1].options.name, 'ewelink-bridge-events');
  assert.equal(outputs[1].options.timeout, 100);
  assert.equal(outputs[1].options.reconnectTimeWait, 0);
  assert.equal(outputs[1].options.token, 'token');
  assert.deepEqual(outputs[1].options.tls, { ca: 'pem', cert: undefined, key: undefined });
  const output = loadConfig({ ...credentials, BRIDGE_OUTPUTS: 'events', NATS_EVENTS_SERVERS: 'localhost:4222', NATS_TOKEN: 'ignored', NATS_TLS_CA: 'ignored' }).outputs[0];
  assert.equal(output.options.token, undefined);
  assert.equal(output.options.tls, undefined);
});

test('unnamed NATS supports authentication and TLS contents', () => {
  const output = loadConfig({ ...credentials, NATS_SERVERS: 'localhost:4222', NATS_CREDS: 'multiline\ncredentials', NATS_TLS_CERT: 'cert', NATS_TLS_KEY: 'key' }).outputs[0];
  assert.equal(output.creds, 'multiline\ncredentials');
  assert.equal(output.options.name, 'ewelink-bridge');
  assert.deepEqual(output.options.tls, { ca: undefined, cert: 'cert', key: 'key' });
  assert.equal(loadConfig({ ...credentials, NATS_SERVERS: 'localhost', MQTT_QOS: 'bad', TOPIC_PREFIX: '#' }).outputs[0].type, 'nats');
  assert.equal(loadConfig({ ...credentials, MQTT_URL: 'mqtt://host', NATS_TLS: 'bad', SUBJECT_PREFIX: '*' }).outputs[0].type, 'mqtt');
});

test('reject missing, empty, ambiguous selectors and invalid NATS settings', () => {
  for (const settings of [{}, { MQTT_URL: '' }, { NATS_SERVERS: '' },
    { MQTT_URL: 'mqtt://host', NATS_SERVERS: 'localhost' },
    { BRIDGE_OUTPUTS: 'a', MQTT_A_URL: 'mqtt://host', NATS_A_SERVERS: 'localhost' }]) {
    assert.throws(() => loadConfig({ ...credentials, ...settings }));
  }
  for (const settings of [{ SUBJECT_PREFIX: 'a..b' }, { SUBJECT_PREFIX: 'a.*' }, { NATS_SERVERS: 'a,' },
    { NATS_USER: 'user', NATS_TOKEN: 'token' }, { NATS_TOKEN: 'token', NATS_CREDS: 'creds' },
    { NATS_PASS: 'pass' }, { NATS_TLS_CERT: 'cert' }, { NATS_TLS_KEY: 'key' },
    { NATS_TLS: 'bad' }, { NATS_CONNECT_TIMEOUT: '0' }, { NATS_MAX_RECONNECT_ATTEMPTS: '-2' },
    { NATS_RECONNECT_TIME_WAIT: '-1' }, { NATS_BUFFER_LIMIT: '1.5' }]) {
    assert.throws(() => loadConfig({ ...credentials, NATS_SERVERS: 'localhost', ...settings }));
  }
});
