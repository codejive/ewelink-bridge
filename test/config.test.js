'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { loadConfig } = require('../config');
const credentials = { EWELINK_EMAIL: 'account', EWELINK_PASSWORD: 'password', EWELINK_APP_ID: 'app', EWELINK_APP_SECRET: 'secret' };

test('legacy defaults and explicit single broker configuration', () => {
  const config = loadConfig(credentials);
  assert.equal(config.outputs.length, 1);
  assert.equal(config.outputs[0].bufferLimit, 100000);
  assert.equal(config.outputs[0].url, 'mqtt://127.0.0.1:1883');
  const output = loadConfig({ ...credentials, MQTT_URL: 'mqtt://broker', MQTT_USER: 'user', MQTT_PASS: 'pass', TOPIC_PREFIX: 'custom', MQTT_QOS: '0', MQTT_RETAIN: 'off' }).outputs[0];
  assert.equal(output.username, 'user');
  assert.equal(output.password, 'pass');
  assert.equal(output.topicPrefix, 'custom');
  assert.equal(output.qos, 0);
  assert.equal(output.retain, false);
});

test('named outputs inherit shared defaults and override independently', () => {
  const outputs = loadConfig({ ...credentials, MQTT_OUTPUTS: 'home, remote', MQTT_HOME_URL: 'mqtt://home', MQTT_REMOTE_URL: 'mqtts://remote', MQTT_URL: 'invalid legacy URL', MQTT_BUFFER_LIMIT: '10', MQTT_HOME_BUFFER_LIMIT: '2', TOPIC_PREFIX: 'shared', MQTT_REMOTE_TOPIC_PREFIX: 'remote', MQTT_REMOTE_QOS: '2', MQTT_REMOTE_RETAIN: 'false', MQTT_HOME_USER: 'user' }).outputs;
  assert.equal(outputs[0].bufferLimit, 2);
  assert.equal(outputs[1].bufferLimit, 10);
  assert.equal(outputs[0].topicPrefix, 'shared');
  assert.equal(outputs[1].topicPrefix, 'remote');
  assert.equal(outputs[1].qos, 2);
  assert.equal(outputs[1].retain, false);
  assert.equal(outputs[1].username, undefined);
});

test('invalid configuration fails before any connections', () => {
  for (const override of [
    { MQTT_OUTPUTS: '' }, { MQTT_OUTPUTS: 'home,HOME' }, { MQTT_OUTPUTS: 'bad-name' },
    { MQTT_OUTPUTS: 'home' }, { MQTT_URL: 'http://broker' },
    { MQTT_QOS: '3' }, { MQTT_BUFFER_LIMIT: '0' }, { MQTT_BUFFER_LIMIT: '1.5' },
    { MQTT_RETAIN: 'typo' }, { TOPIC_PREFIX: 'bad/#' }, { EWELINK_REGION: 'invalid' },
    { MQTT_OUTPUTS: 'home', MQTT_HOME_URL: 'mqtt://home', MQTT_HOME_BUFFER_LIMIT: '-1' },
    { EWELINK_APP_SECRET: '' }
  ]) assert.throws(() => loadConfig({ ...credentials, ...override }));
});

test('malformed URL credentials fail validation for legacy and named outputs', () => {
  for (const protocol of ['mqtt', 'mqtts', 'ws', 'wss']) {
    for (const auth of ['user:bad%ZZ', 'bad%ZZ:secret', 'user:secret%', 'user:%FF']) {
      const url = `${protocol}://${auth}@broker:1883`;
      assert.throws(() => loadConfig({ ...credentials, MQTT_URL: url }), {
        message: 'MQTT_URL must be a valid broker URL'
      });
      assert.throws(() => loadConfig({ ...credentials, MQTT_OUTPUTS: 'home,remote', MQTT_HOME_URL: 'mqtt://home', MQTT_REMOTE_URL: url }), {
        message: 'MQTT_REMOTE_URL must be a valid broker URL'
      });
    }
  }
});

test('valid percent-encoded URL credentials are preserved', () => {
  for (const protocol of ['mqtt', 'mqtts', 'ws', 'wss']) {
    const url = `${protocol}://user%40example.com:p%25ss%3A%C3%A9@broker:1883`;
    assert.equal(loadConfig({ ...credentials, MQTT_URL: url }).outputs[0].url, url);
    assert.equal(loadConfig({ ...credentials, MQTT_OUTPUTS: 'remote', MQTT_REMOTE_URL: url }).outputs[0].url, url);
  }
});
