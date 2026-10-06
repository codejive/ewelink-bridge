'use strict';

function loadConfig(env = process.env) {
  const get = (key, fallback) => env[key] === undefined || env[key] === '' ? fallback : env[key];
  const bool = (key, fallback) => {
    const value = String(get(key, fallback)).toLowerCase();
    if (['true', '1', 'yes', 'on'].includes(value)) return true;
    if (['false', '0', 'no', 'off'].includes(value)) return false;
    throw new Error(`${key} must be a boolean`);
  };
  const integer = (key, fallback, min, max = Number.MAX_SAFE_INTEGER) => {
    const value = Number(get(key, fallback));
    if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error(`${key} must be an integer between ${min} and ${max}`);
    return value;
  };
  const prefix = (key, fallback, nats = false) => {
    const value = get(key, fallback);
    const valid = nats ? value && value.split('.').every(part => part && !/[\s*>\u0000]/.test(part)) : value && !/[+#\u0000]/.test(value);
    if (!valid) throw new Error(`${key} must be a nonempty ${nats ? 'dot-separated subject prefix without whitespace or wildcards' : 'MQTT topic prefix without wildcards'}`);
    return value;
  };
  const named = env.BRIDGE_OUTPUTS !== undefined;
  const names = named ? env.BRIDGE_OUTPUTS.split(',').map(name => name.trim()) : ['default'];
  if (names.some(name => !/^[a-zA-Z][a-zA-Z0-9_]*$/.test(name)) || new Set(names.map(name => name.toUpperCase())).size !== names.length) {
    throw new Error('BRIDGE_OUTPUTS must contain unique names beginning with a letter and containing only letters, numbers, or underscores');
  }
  const outputs = names.map(name => {
    const mqttKey = named ? `MQTT_${name.toUpperCase()}_` : 'MQTT_';
    const natsKey = named ? `NATS_${name.toUpperCase()}_` : 'NATS_';
    const hasMqtt = env[`${mqttKey}URL`] !== undefined;
    const hasNats = env[`${natsKey}SERVERS`] !== undefined;
    if (hasMqtt === hasNats) throw new Error(`Output ${name} requires exactly one of ${mqttKey}URL or ${natsKey}SERVERS`);
    const key = hasMqtt ? mqttKey : natsKey;
    const shared = hasMqtt ? 'MQTT_' : 'NATS_';
    const number = (suffix, fallback, min, max) => integer(`${key}${suffix}`, named ? integer(`${shared}${suffix}`, fallback, min, max) : fallback, min, max);
    const flag = (suffix, fallback) => bool(`${key}${suffix}`, named ? bool(`${shared}${suffix}`, fallback) : fallback);
    const bufferLimit = number('BUFFER_LIMIT', 100000, 1);
    if (hasMqtt) {
      const url = get(`${key}URL`);
      let parsed;
      try {
        parsed = new URL(url);
        decodeURIComponent(parsed.username);
        decodeURIComponent(parsed.password);
      } catch { throw new Error(`${key}URL must be a valid broker URL`); }
      if (!['mqtt:', 'mqtts:', 'ws:', 'wss:'].includes(parsed.protocol) || !parsed.hostname) throw new Error(`${key}URL must use mqtt, mqtts, ws, or wss`);
      return { type: 'mqtt', name, url, bufferLimit,
        username: get(`${key}USER`), password: get(`${key}PASS`),
        topicPrefix: named ? prefix(`${key}TOPIC_PREFIX`, prefix('TOPIC_PREFIX', 'ewelink')) : prefix('TOPIC_PREFIX', 'ewelink'),
        qos: number('QOS', 1, 0, 2), retain: flag('RETAIN', true) };
    }
    const servers = get(`${key}SERVERS`, '').split(',').map(value => value.trim());
    if (servers.some(server => !server)) throw new Error(`${key}SERVERS must contain nonempty server addresses`);
    const user = get(`${key}USER`), pass = get(`${key}PASS`), token = get(`${key}TOKEN`), creds = get(`${key}CREDS`);
    if ([Boolean(user || pass), Boolean(token), Boolean(creds)].filter(Boolean).length > 1) throw new Error(`${key} requires a single authentication method: user/pass, token, or credentials`);
    if (pass && !user) throw new Error(`${key}PASS requires ${key}USER`);
    const ca = get(`${key}TLS_CA`), cert = get(`${key}TLS_CERT`), tlsKey = get(`${key}TLS_KEY`);
    if (Boolean(cert) !== Boolean(tlsKey)) throw new Error(`${key}TLS_CERT and ${key}TLS_KEY must be set together`);
    const options = { servers, user, pass, token,
      name: get(`${key}NAME`, named ? `ewelink-bridge-${name}` : 'ewelink-bridge'),
      reconnect: true,
      reconnectTimeWait: number('RECONNECT_TIME_WAIT', 3000, 0),
      maxReconnectAttempts: number('MAX_RECONNECT_ATTEMPTS', -1, -1),
      timeout: number('CONNECT_TIMEOUT', 10000, 1) };
    if (flag('TLS', false) || ca || cert || tlsKey) options.tls = { ca, cert, key: tlsKey };
    return { type: 'nats', name, bufferLimit, options, creds,
      subjectPrefix: named ? prefix(`${key}SUBJECT_PREFIX`, prefix('SUBJECT_PREFIX', 'ewelink', true), true) : prefix('SUBJECT_PREFIX', 'ewelink', true) };
  });
  const config = {
    outputs,
    ewelinkAccount: get('EWELINK_ACCOUNT', get('EWELINK_EMAIL')),
    ewelinkPassword: get('EWELINK_PASSWORD'), ewelinkAppId: get('EWELINK_APP_ID'), ewelinkAppSecret: get('EWELINK_APP_SECRET'),
    ewelinkRegion: get('EWELINK_REGION', 'us'), ewelinkAreaCode: get('EWELINK_AREA_CODE', '+1'),
    verbose: bool('VERBOSE', false), exitOnWsClose: bool('EXIT_ON_WEBSOCKET_CLOSE', true)
  };
  for (const key of ['EWELINK_ACCOUNT', 'EWELINK_PASSWORD', 'EWELINK_APP_ID', 'EWELINK_APP_SECRET']) {
    if (!(key === 'EWELINK_ACCOUNT' ? config.ewelinkAccount : get(key))) throw new Error(`${key} is required`);
  }
  if (!['us', 'eu', 'cn', 'as'].includes(config.ewelinkRegion)) throw new Error('EWELINK_REGION must be us, eu, cn, or as');
  return config;
}

module.exports = { loadConfig };
