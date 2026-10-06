'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { PublicationBuffer } = require('../publication-buffer');
const { redactUrl } = require('../mqtt-output');
const { devicePublications, mqttTopic, natsSubject } = require('../publications');

test('FIFO preserves repeated topics and drops oldest pending publication', () => {
  const buffer = new PublicationBuffer(3);
  for (let i = 0; i < 5; i++) buffer.enqueue({ topic: 'same', payload: String(i) });
  assert.equal(buffer.depth, 3);
  assert.equal(buffer.drops, 2);
  for (const value of ['2', '3', '4']) {
    assert.equal(buffer.begin().payload, value);
    assert.equal(buffer.depth > 0, true);
    buffer.complete();
  }
  assert.equal(buffer.depth, 0);
});

test('in-flight counts toward capacity and cannot be discarded', () => {
  const buffer = new PublicationBuffer(2);
  buffer.enqueue('first');
  assert.equal(buffer.begin(), 'first');
  buffer.enqueue('second');
  buffer.enqueue('third');
  assert.equal(buffer.depth, 2);
  assert.equal(buffer.drops, 1);
  buffer.retry();
  assert.equal(buffer.begin(), 'first');
  buffer.complete();
  assert.equal(buffer.begin(), 'third');
  buffer.complete();
});

test('capacity one rejects incoming publication while head is in flight', () => {
  const buffer = new PublicationBuffer(1);
  buffer.enqueue('first');
  buffer.begin();
  assert.equal(buffer.enqueue('second'), false);
  assert.equal(buffer.depth, 1);
  assert.equal(buffer.drops, 1);
  buffer.complete();
  assert.equal(buffer.depth, 0);
});

test('conversion creates individual publications in parameter order', () => {
  assert.deepEqual(devicePublications({ deviceid: 'device/+', params: { temperature: 22, values: { a: 1 }, empty: null } }), [
    { deviceId: 'device/+', key: 'temperature', payload: '22' },
    { deviceId: 'device/+', key: 'values', payload: '{"a":1}' },
    { deviceId: 'device/+', key: 'empty', payload: '' }
  ]);
  assert.equal(mqttTopic({ deviceId: 'device/+', key: 'a/#' }), 'device__/state/a__');
  assert.equal(natsSubject({ deviceId: 'a.b', key: 'a.* >' }), 'a_b.state.a____');
  assert.deepEqual(devicePublications({ deviceid: '123', params: { switch: 'on', raw: 1 } }).map(p => p.key), ['switch', 'raw']);
  assert.equal(redactUrl('mqtts://user:secret@broker:8883?password=secret'), 'mqtts://broker:8883');
});
