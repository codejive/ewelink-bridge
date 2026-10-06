'use strict';

function redactUrl(value) {
  const url = new URL(value);
  url.username = '';
  url.password = '';
  url.search = '';
  url.hash = '';
  return url.toString();
}

// Linked FIFO: constant-time enqueue/dequeue even at the default 100,000 limit.
class PublicationBuffer {
  constructor(limit) {
    this.limit = limit;
    this.head = null;
    this.tail = null;
    this.depth = 0;
    this.inFlight = false;
    this.drops = 0;
  }

  enqueue(publication) {
    if (this.depth === this.limit) {
      // The head cannot be withdrawn while MQTT owns its publication.
      if (this.inFlight) {
        if (this.depth === 1) { this.drops++; return false; }
        this.head.next = this.head.next.next;
        if (!this.head.next) this.tail = this.head;
      } else {
        this.head = this.head.next;
        if (!this.head) this.tail = null;
      }
      this.depth--;
      this.drops++;
    }
    const node = { publication, next: null };
    if (this.tail) this.tail.next = node;
    else this.head = node;
    this.tail = node;
    this.depth++;
    return true;
  }

  begin() {
    if (this.inFlight || !this.head) return null;
    this.inFlight = true;
    return this.head.publication;
  }

  complete() {
    if (!this.inFlight) return;
    this.head = this.head.next;
    if (!this.head) this.tail = null;
    this.depth--;
    this.inFlight = false;
  }

  retry() { this.inFlight = false; }
}

class MqttOutput {
  constructor(config, { connect = require('mqtt').connect, logger = console,
    publishTimeout = 15000, reconnectDelay = 3000, reportInterval = 60000 } = {}) {
    this.config = config;
    this.connect = connect;
    this.logger = logger;
    this.publishTimeout = publishTimeout;
    this.reconnectDelay = reconnectDelay;
    this.buffer = new PublicationBuffer(config.bufferLimit);
    this.failures = 0;
    this.reconnects = 0;
    this.generation = 0;
    this.stopped = false;
    this.ready = false;
    this.logTimes = new Map();
    this.statusTopic = `${config.topicPrefix}/bridge/status`;
    this.reportTimer = setInterval(() => this.log('info', 'stats', this.metrics()), reportInterval);
    this.reportTimer.unref();
  }

  metrics() {
    return `queue=${this.buffer.depth}/${this.buffer.limit} inFlight=${Number(this.buffer.inFlight)} overflowDrops=${this.buffer.drops} publicationFailures=${this.failures} reconnects=${this.reconnects}`;
  }

  log(level, key, message, limited = false) {
    const now = Date.now();
    if (limited && this.logTimes.has(key) && now - this.logTimes.get(key) < 30000) return;
    this.logTimes.set(key, now);
    (this.logger[level] || this.logger.log).call(this.logger, `[MQTT ${this.config.name}] ${message}`);
  }

  start() {
    if (this.stopped || this.client) return;
    const generation = ++this.generation;
    let client;
    try {
      client = this.connect(this.config.url, {
        username: this.config.username, password: this.config.password,
        reconnectPeriod: 0, connectTimeout: this.publishTimeout,
        queueQoSZero: false, clean: true,
        will: { topic: this.statusTopic, payload: 'offline', qos: 1, retain: true }
      });
    } catch {
      this.fail(generation, 'connection creation failed');
      return;
    }
    this.client = client;
    this.operationTimer = setTimeout(() => this.fail(generation, 'connection timed out'), this.publishTimeout);
    client.on('error', () => this.fail(generation, 'broker error'));
    client.on('close', () => this.fail(generation, 'broker disconnected'));
    client.on('offline', () => this.fail(generation, 'broker offline'));
    client.on('disconnect', () => this.fail(generation, 'broker disconnected'));
    client.on('connect', () => {
      if (generation !== this.generation || this.stopped) return;
      clearTimeout(this.operationTimer);
      this.log('info', 'connect', `connected to ${redactUrl(this.config.url)}; ${this.metrics()}`, true);
      this.publish(this.statusTopic, 'online', { qos: 1, retain: true }, generation, () => {
        this.ready = true;
        this.schedule();
      });
    });
  }

  enqueue(publications) {
    if (this.stopped) return;
    const before = this.buffer.drops;
    for (const publication of publications) this.buffer.enqueue(publication);
    if (before !== this.buffer.drops) this.log('warn', 'overflow', `buffer overflow; ${this.metrics()}`, true);
    this.schedule();
  }

  schedule() {
    if (this.scheduled || this.stopped) return;
    this.scheduled = true;
    setImmediate(() => { this.scheduled = false; this.deliver(); });
  }

  deliver() {
    if (this.stopped || !this.ready || !this.client || !this.client.connected) return;
    const publication = this.buffer.begin();
    if (!publication) return;
    this.publish(`${this.config.topicPrefix}/${publication.topic}`, publication.payload,
      { qos: this.config.qos, retain: this.config.retain }, this.generation, () => {
        this.buffer.complete();
        this.schedule();
      });
  }

  publish(topic, payload, options, generation, success) {
    this.operationTimer = setTimeout(() => this.fail(generation, 'publication callback timed out', true), this.publishTimeout);
    let settled = false;
    const callback = err => {
      if (settled || generation !== this.generation || this.stopped) return;
      settled = true;
      clearTimeout(this.operationTimer);
      if (err) this.fail(generation, 'publication failed', true);
      else success();
    };
    try { this.client.publish(topic, payload, options, callback); }
    catch { callback(new Error('publish failed')); }
  }

  fail(generation, reason, publicationFailure = false) {
    if (generation !== this.generation || this.stopped) return;
    if (publicationFailure || this.buffer.inFlight) this.failures++;
    ++this.generation; // Invalidate callbacks before destroying the old transport.
    clearTimeout(this.operationTimer);
    this.ready = false;
    const client = this.client;
    this.client = null;
    this.dispose(client);
    this.buffer.retry();
    this.log('warn', 'outage', `${reason}; ${this.metrics()}`, true);
    this.reconnectTimer = setTimeout(() => {
      this.reconnects++;
      this.log('info', 'reconnect', `reconnecting; ${this.metrics()}`, true);
      this.start();
    }, this.reconnectDelay);
  }

  dispose(client) {
    if (!client) return;
    try { client.end(true); } catch { /* Already disconnected. */ }
    try { client.stream?.destroy(); } catch { /* Already destroyed. */ }
  }

  stop(deadline = 5000) {
    if (this.stopPromise) return this.stopPromise;
    this.stopped = true;
    this.ready = false;
    ++this.generation;
    clearInterval(this.reportTimer);
    clearTimeout(this.operationTimer);
    clearTimeout(this.reconnectTimer);
    const client = this.client;
    this.log('info', 'shutdown', `stopping; ${this.metrics()}`);
    this.stopPromise = new Promise(resolve => {
      let finished = false;
      const finish = () => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        this.dispose(client);
        this.client = null;
        resolve();
      };
      const timer = setTimeout(finish, deadline);
      if (!client?.connected) { finish(); return; }
      try {
        client.publish(this.statusTopic, 'offline', { qos: 1, retain: true }, err => {
          if (finished) return;
          if (err) { finish(); return; }
          try { client.end(false, finish); } catch { finish(); }
        });
      } catch { finish(); }
    });
    return this.stopPromise;
  }
}

module.exports = { MqttOutput, PublicationBuffer, redactUrl };
