'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

test('one cloud source follows region redirect, fans out neutral updates, and stops mixed outputs', () => {
  const script = `
    const Module = require('node:module');
    const assert = require('node:assert/strict');
    const original = Module._load;
    const outputs = [];
    let logins = 0, sockets = 0, websocketClosed = false;
    class Output {
      constructor(config) { this.config = config; this.updates = []; outputs.push(this); }
      start() { this.started = true; }
      enqueue(publications) { this.updates.push(publications); }
      async stop() {
        assert.equal(websocketClosed, true);
        assert.deepEqual(outputs.map(output => output.config.type), ['mqtt', 'nats']);
        assert.equal(sockets, 1);
        assert.equal(logins, 2);
        assert.equal(this.updates.length, 1);
        assert.deepEqual(this.updates[0], [
          {deviceId:'a.b/+',key:'switch',payload:'on'},
          {deviceId:'a.b/+',key:'values',payload:'[1,2]'},
          {deviceId:'a.b/+',key:'empty',payload:''}
        ]);
        assert.equal(outputs[0].updates[0], outputs[1].updates[0]);
        console.log('OUTPUT_STOPPED_' + this.config.type);
      }
    }
    class WebAPI {
      constructor() {
        assert.ok(outputs.every(output => output.started));
        this.at = 'access'; this.userApiKey = 'api';
        this.user = {login: async () => ++logins === 1 ? {error:10004,data:{region:'eu'}} : {error:0}};
      }
      setUrl(region) { assert.equal(region, 'eu'); }
    }
    class Ws {
      constructor(config) {
        assert.equal(config.region, 'eu');
        this.Connect = {create: async (config, opened, closed, error, message) => {
          sockets++;
          assert.equal(config.at, 'access');
          opened();
          message(null, {data:'ping'});
          message(null, {data:'{"config":{}}'});
          message(null, {data:'{"action":"query","deviceid":"123","params":{"switch":"off"}}'});
          message(null, {data:Buffer.from('{"action":"update","deviceid":"a.b/+","params":{"switch":"on","values":[1,2],"empty":null}}')});
          setImmediate(() => process.emit('SIGTERM'));
          return {close() { websocketClosed = true; closed(); }};
        }};
      }
    }
    Module._load = function(name, ...args) {
      if (name === 'ewelink-api-next') return {default:{WebAPI,Ws}};
      if (name === './mqtt-output') return {MqttOutput:Output};
      if (name === './nats-output') return {NatsOutput:Output};
      return original.call(this, name, ...args);
    };
    require('./bridge');
  `;
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^(MQTT_|NATS_|BRIDGE_OUTPUTS$)/.test(key)));
  const result = spawnSync(process.execPath, ['-e', script], {
    cwd: path.join(__dirname, '..'), encoding: 'utf8', timeout: 10000,
    env: { ...env, EWELINK_ACCOUNT: 'test', EWELINK_PASSWORD: 'test', EWELINK_APP_ID: 'test', EWELINK_APP_SECRET: 'test',
      BRIDGE_OUTPUTS: 'home,events', MQTT_HOME_URL: 'mqtt://home', NATS_EVENTS_SERVERS: 'localhost' }
  });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /OUTPUT_STOPPED_mqtt/);
  assert.match(result.stdout, /OUTPUT_STOPPED_nats/);
});
