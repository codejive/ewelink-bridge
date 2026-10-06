'use strict';

const { PublicationBuffer } = require('./publication-buffer');
const { natsSubject } = require('./publications');

class NatsOutput {
  constructor(config, { connect = require('@nats-io/transport-node').connect,
    credsAuthenticator = require('@nats-io/transport-node').credsAuthenticator,
    logger = console, publishTimeout = 15000, reportInterval = 60000 } = {}) {
    this.config = config;
    this.connect = connect;
    this.credsAuthenticator = credsAuthenticator;
    this.logger = logger;
    this.publishTimeout = publishTimeout;
    this.buffer = new PublicationBuffer(config.bufferLimit);
    this.generation = 0;
    this.deliveryGeneration = 0;
    this.failures = 0;
    this.reconnects = 0;
    this.attempts = 0;
    this.ready = false;
    this.stopped = false;
    this.failed = false;
    this.logTimes = new Map();
    this.statusSubject = `${config.subjectPrefix}.bridge.status`;
    this.reportTimer = setInterval(() => this.log('info', 'stats', this.metrics()), reportInterval);
    this.reportTimer.unref();
  }

  metrics() {
    return `queue=${this.buffer.depth}/${this.buffer.limit} inFlight=${Number(this.buffer.inFlight)} overflowDrops=${this.buffer.drops} publicationFailures=${this.failures} reconnects=${this.reconnects} failed=${Number(this.failed)}`;
  }

  log(level, key, message, limited = false) {
    const now = Date.now();
    if (limited && this.logTimes.has(key) && now - this.logTimes.get(key) < 30000) return;
    this.logTimes.set(key, now);
    (this.logger[level] || this.logger.log).call(this.logger, `[NATS ${this.config.name}] ${message}`);
  }

  start() {
    if (this.stopped || this.failed || this.connecting || this.client) return;
    this.connecting = true;
    const generation = ++this.generation;
    const options = { ...this.config.options };
    this.open(generation, options).catch(() => this.fail(generation, 'connection failed'));
  }

  async open(generation, options) {
    if (this.config.creds) options.authenticator = this.credsAuthenticator(Buffer.from(this.config.creds));
    const client = await this.connect(options);
    if (generation !== this.generation || this.stopped) { await client.close(); return; }
    this.connecting = false;
    this.client = client;
    client.closed().then(() => {
      // The client has already exhausted its configured reconnect policy.
      this.fail(generation, 'connection closed', this.config.options.maxReconnectAttempts >= 0);
    }).catch(() => this.fail(generation, 'connection closed'));
    this.monitor(client, generation).catch(() => this.fail(generation, 'status monitor failed'));
    await this.activate(client, generation);
  }

  async bounded(operation, timeout = this.publishTimeout) {
    let timer;
    try {
      return await Promise.race([operation, new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error('operation timed out')), timeout);
      })]);
    } finally { clearTimeout(timer); }
  }

  current(client, generation, deliveryGeneration) {
    return !this.stopped && generation === this.generation && client === this.client &&
      (deliveryGeneration === undefined || deliveryGeneration === this.deliveryGeneration);
  }

  async activate(client, generation) {
    const deliveryGeneration = this.deliveryGeneration;
    try {
      client.publish(this.statusSubject, Buffer.from('online'));
      await this.bounded(client.flush());
      // Permission errors are delivered through status(), not flush().
      await new Promise(resolve => setImmediate(resolve));
      if (!this.current(client, generation, deliveryGeneration)) return;
      this.ready = true;
      this.attempts = 0;
      this.log('info', 'connect', `connected; ${this.metrics()}`, true);
      this.schedule();
    } catch {
      if (this.current(client, generation, deliveryGeneration)) this.fail(generation, 'status publication failed');
    }
  }

  async monitor(client, generation) {
    for await (const status of client.status()) {
      if (!this.current(client, generation)) return;
      if (status.type === 'error') {
        this.fail(generation, 'server error');
        return;
      }
      if (status.type === 'disconnect') {
        this.ready = false;
        ++this.deliveryGeneration;
        if (this.buffer.inFlight) this.failures++;
        this.buffer.retry();
        this.log('warn', 'outage', `disconnected; ${this.metrics()}`, true);
      } else if (status.type === 'reconnect') {
        this.reconnects++;
        // Do not block consumption of asynchronous permission errors.
        this.activate(client, generation);
      }
    }
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
    setImmediate(() => {
      this.scheduled = false;
      this.deliver().catch(() => this.fail(this.generation, 'publication failed'));
    });
  }

  async deliver() {
    if (this.stopped || !this.ready || !this.client) return;
    const publication = this.buffer.begin();
    if (!publication) return;
    const client = this.client, generation = this.generation, deliveryGeneration = this.deliveryGeneration;
    try {
      client.publish(`${this.config.subjectPrefix}.${natsSubject(publication)}`, Buffer.from(publication.payload));
      await this.bounded(client.flush());
      await new Promise(resolve => setImmediate(resolve));
      if (!this.current(client, generation, deliveryGeneration)) return;
      this.buffer.complete();
      this.schedule();
    } catch {
      if (this.current(client, generation, deliveryGeneration)) this.fail(generation, 'publication failed');
    }
  }

  fail(generation, reason, exhausted = false) {
    if (generation !== this.generation || this.stopped) return;
    ++this.generation;
    ++this.deliveryGeneration;
    this.connecting = false;
    this.ready = false;
    if (this.buffer.inFlight) this.failures++;
    this.buffer.retry();
    const client = this.client;
    this.client = null;
    if (client) Promise.resolve().then(() => client.close()).catch(() => {});
    const limit = this.config.options.maxReconnectAttempts;
    this.failed = exhausted || (limit >= 0 && this.attempts >= limit * this.config.options.servers.length);
    this.log('warn', 'outage', `${reason}; ${this.metrics()}`, !this.failed);
    if (this.failed) return;
    this.attempts++;
    this.reconnectTimer = setTimeout(() => { this.reconnects++; this.start(); }, this.config.options.reconnectTimeWait);
  }

  stop(deadline = 5000) {
    if (this.stopPromise) return this.stopPromise;
    this.stopped = true;
    const wasReady = this.ready;
    this.ready = false;
    ++this.generation;
    ++this.deliveryGeneration;
    clearInterval(this.reportTimer);
    clearTimeout(this.reconnectTimer);
    const client = this.client;
    this.client = null;
    this.log('info', 'shutdown', `stopping; ${this.metrics()}`);
    this.stopPromise = (async () => {
      if (!client) return;
      try {
        if (wasReady && !client.isClosed()) {
          client.publish(this.statusSubject, Buffer.from('offline'));
          await this.bounded(client.drain(), deadline);
        }
      } catch { /* Deadline or disconnected transport. */ }
      finally { Promise.resolve().then(() => client.close()).catch(() => {}); }
    })();
    return this.stopPromise;
  }
}

module.exports = { NatsOutput };
