'use strict';

function loadConfig(env = process.env) {
  const get = (key, fallback) => env[key] === undefined || env[key] === '' ? fallback : env[key];
  const bool = (key, fallback) => {
    const value = String(get(key, fallback)).toLowerCase();
    if (['true', '1', 'yes', 'on'].includes(value)) return true;
    if (['false', '0', 'no', 'off'].includes(value)) return false;
    throw new Error(`${key} must be a boolean`);
  };
  const integer = (key, fallback, min, max) => {
    const value = Number(get(key, fallback));
    if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error(`${key} must be an integer between ${min} and ${max}`);
    return value;
  };
  const prefix = (key, fallback) => {
    const value = get(key, fallback);
    if (!value || /[+#\u0000]/.test(value)) throw new Error(`${key} must be a nonempty MQTT topic prefix without wildcards`);
    return value;
  };
  const defaults = {
    topicPrefix: prefix('TOPIC_PREFIX', 'ewelink'),
    qos: integer('MQTT_QOS', 1, 0, 2),
    retain: bool('MQTT_RETAIN', true),
    bufferLimit: integer('MQTT_BUFFER_LIMIT', 100000, 1, Number.MAX_SAFE_INTEGER)
  };
  const named = env.MQTT_OUTPUTS !== undefined;
  const names = named ? env.MQTT_OUTPUTS.split(',').map(name => name.trim()) : ['default'];
  if (names.some(name => !/^[a-zA-Z][a-zA-Z0-9_]*$/.test(name)) || new Set(names.map(name => name.toUpperCase())).size !== names.length) {
    throw new Error('MQTT_OUTPUTS must contain unique names beginning with a letter and containing only letters, numbers, or underscores');
  }
  const outputs = names.map(name => {
    const key = named ? `MQTT_${name.toUpperCase()}_` : 'MQTT_';
    const url = get(`${key}URL`, named ? undefined : 'mqtt://127.0.0.1:1883');
    let parsed;
    try {
      parsed = new URL(url);
      // MQTT.js decodes URL credentials; reject malformed escapes before connecting.
      decodeURIComponent(parsed.username);
      decodeURIComponent(parsed.password);
    } catch { throw new Error(`${key}URL must be a valid broker URL`); }
    if (!['mqtt:', 'mqtts:', 'ws:', 'wss:'].includes(parsed.protocol) || !parsed.hostname) throw new Error(`${key}URL must use mqtt, mqtts, ws, or wss`);
    return {
      name, url, username: get(`${key}USER`), password: get(`${key}PASS`),
      topicPrefix: named ? prefix(`${key}TOPIC_PREFIX`, defaults.topicPrefix) : defaults.topicPrefix,
      qos: named ? integer(`${key}QOS`, defaults.qos, 0, 2) : defaults.qos,
      retain: named ? bool(`${key}RETAIN`, defaults.retain) : defaults.retain,
      bufferLimit: named ? integer(`${key}BUFFER_LIMIT`, defaults.bufferLimit, 1, Number.MAX_SAFE_INTEGER) : defaults.bufferLimit
    };
  });
  const config = {
    outputs,
    ewelinkAccount: get('EWELINK_ACCOUNT', get('EWELINK_EMAIL')),
    ewelinkPassword: get('EWELINK_PASSWORD'),
    ewelinkAppId: get('EWELINK_APP_ID'),
    ewelinkAppSecret: get('EWELINK_APP_SECRET'),
    ewelinkRegion: get('EWELINK_REGION', 'us'),
    ewelinkAreaCode: get('EWELINK_AREA_CODE', '+1'),
    verbose: bool('VERBOSE', false),
    exitOnWsClose: bool('EXIT_ON_WEBSOCKET_CLOSE', true)
  };
  for (const key of ['EWELINK_ACCOUNT', 'EWELINK_PASSWORD', 'EWELINK_APP_ID', 'EWELINK_APP_SECRET']) {
    if (!(key === 'EWELINK_ACCOUNT' ? config.ewelinkAccount : get(key))) throw new Error(`${key} is required`);
  }
  if (!['us', 'eu', 'cn', 'as'].includes(config.ewelinkRegion)) throw new Error('EWELINK_REGION must be us, eu, cn, or as');
  return config;
}

module.exports = { loadConfig };
