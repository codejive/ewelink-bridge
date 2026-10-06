'use strict';

const Ewelink = require('ewelink-api-next').default;
const { loadConfig } = require('./config');
const { MqttOutput } = require('./mqtt-output');
const { NatsOutput } = require('./nats-output');
const { devicePublications } = require('./publications');

let config;
try { config = loadConfig(); }
catch (err) { console.error(err.message); process.exit(1); }

let websocket;
let shuttingDown = false;
const outputs = config.outputs.map(output => output.type === 'mqtt' ? new MqttOutput(output) : new NatsOutput(output));

function getWebsocketMessageText(message) {
  if (!message || typeof message.data === 'undefined' || message.data === null) {
    return null;
  }

  return Buffer.isBuffer(message.data) ? message.data.toString('utf8') : String(message.data);
}

function parseWebsocketPayload(text) {
  if (!text || text[0] !== '{') {
    return null;
  }

  let payload;
  try {
    payload = JSON.parse(text);
  } catch (err) {
    console.error('Failed to parse eWeLink websocket message:', err.message || err);
    return null;
  }

  // Handshake packet includes config but no device state.
  if (payload && payload.config) {
    console.log('eWeLink websocket config received');
    return null;
  }

  if (!payload || !payload.deviceid || !payload.params || typeof payload.params !== 'object') {
    return null;
  }

  if (payload.action && payload.action !== 'update') {
    return null;
  }

  return payload;
}

function publishDeviceUpdate(action) {
  if (shuttingDown) return;
  const publications = devicePublications(action);
  for (const output of outputs) output.enqueue(publications);
  if (config.verbose) console.log('Enqueued ' + publications.length + ' publications for each bridge output');
}

async function loginWithRegionFallback(client) {
  let region = config.ewelinkRegion;

  let response = await client.user.login({
    account: config.ewelinkAccount,
    password: config.ewelinkPassword,
    areaCode: config.ewelinkAreaCode,
    lang: 'en'
  });

  if (response && response.error === 10004 && response.data && response.data.region) {
    region = response.data.region;
    console.log(`eWeLink account redirects to region: ${region}`);
    client.setUrl(region);
    response = await client.user.login({
      account: config.ewelinkAccount,
      password: config.ewelinkPassword,
      areaCode: config.ewelinkAreaCode,
      lang: 'en'
    });
  }

  return { response, region };
}

async function startBridge() {
  console.log(`Connecting to eWeLink cloud region: ${config.ewelinkRegion}`);

  const webApi = new Ewelink.WebAPI({
    appId: config.ewelinkAppId,
    appSecret: config.ewelinkAppSecret,
    region: config.ewelinkRegion
  });

  const { response: loginResponse, region: resolvedRegion } = await loginWithRegionFallback(webApi);
  if (shuttingDown) return;
  if (!loginResponse || loginResponse.error) {
    const message = loginResponse && loginResponse.msg ? loginResponse.msg : JSON.stringify(loginResponse || {});
    throw new Error(`Failed to authenticate with eWeLink cloud: ${message}`);
  }

  const wsClient = new Ewelink.Ws({
    appId: config.ewelinkAppId,
    appSecret: config.ewelinkAppSecret,
    region: resolvedRegion
  });

  websocket = await wsClient.Connect.create(
    {
      region: resolvedRegion,
      at: webApi.at,
      userApiKey: webApi.userApiKey,
      appId: config.ewelinkAppId
    },
    () => {
      console.log('eWeLink websocket opened and listening for updates');
    },
    () => {
      console.error('eWeLink websocket closed');
      if (!shuttingDown && config.exitOnWsClose) {
        shutdown('WEBSOCKET_CLOSED', 1);
      }
    },
    (event) => {
      console.error('eWeLink websocket error:', event && event.message ? event.message : event);
    },
    (_ws, message) => {
      if (shuttingDown) return;
      const messageText = getWebsocketMessageText(message);

      if (config.verbose && messageText !== null) {
        console.log('eWeLink websocket message:', messageText);
      }

      const action = parseWebsocketPayload(messageText);
      if (!action) {
        return;
      }

      publishDeviceUpdate(action);
    }
  );

  if (shuttingDown) {
    if (typeof websocket.close === 'function') websocket.close();
    else if (typeof websocket.terminate === 'function') websocket.terminate();
    return;
  }
  console.log('Bridge is running. Waiting for device updates...');
}

async function shutdown(signal, requestedExitCode = 0) {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  const deadline = setTimeout(() => process.exit(requestedExitCode), 5000);

  console.log(`Received ${signal}, shutting down...`);

  if (websocket) {
    try {
      if (typeof websocket.close === 'function') {
        websocket.close();
      } else if (typeof websocket.terminate === 'function') {
        websocket.terminate();
      }
    } catch (err) {
      console.error('Failed to close websocket cleanly:', err.message || err);
    }
  }

  await Promise.all(outputs.map(output => output.stop(4500)));
  clearTimeout(deadline);
  process.exit(requestedExitCode);
}

process.on('SIGINT', () => shutdown('SIGINT', 0));
process.on('SIGTERM', () => shutdown('SIGTERM', 0));

process.on('unhandledRejection', (reason) => {
  console.error('Unhandled rejection:', reason);
  shutdown('UNHANDLED_REJECTION', 1);
});

process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
  shutdown('UNCAUGHT_EXCEPTION', 1);
});

outputs.forEach(output => output.start());
startBridge().catch((err) => {
  console.error('Failed to start bridge:', err.message || err);
  shutdown('STARTUP_FAILURE', 1);
});
