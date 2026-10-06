# eWeLink Bridge

`ewelink-bridge` connects to one eWeLink cloud websocket and republishes live device updates to one or more [MQTT](https://mqtt.org/) and [NATS](https://nats.io/) brokers. Outputs may use either protocol in the same process. Data flows from eWeLink to brokers; the bridge does not subscribe to commands.

Each update is converted once into individual parameter publications. Every output has its own connection, bounded in-memory FIFO, delivery worker, and reconnect lifecycle. Unavailable outputs do not delay eWeLink startup or healthy outputs.

## Configuration

Configuration uses environment variables. Copy `.env.example` to `.env` and fill in credentials and broker settings. Node does not automatically load `.env`; use `node --env-file=.env bridge.js`.

Without `BRIDGE_OUTPUTS`, set exactly one of `MQTT_URL` or `NATS_SERVERS`. Both selectors, neither selector, or an empty selected address are invalid. Addresses have no implicit defaults.

For named outputs, set `BRIDGE_OUTPUTS` and exactly one of `MQTT_<NAME>_URL` or `NATS_<NAME>_SERVERS` per output:

```ini
BRIDGE_OUTPUTS=home,events
MQTT_HOME_URL=mqtt://home-broker:1883
MQTT_HOME_USER=bridge
MQTT_HOME_PASS=secret
NATS_EVENTS_SERVERS=nats://events-one:4222,nats://events-two:4222
NATS_EVENTS_TOKEN=secret

TOPIC_PREFIX=ewelink
SUBJECT_PREFIX=ewelink
MQTT_BUFFER_LIMIT=100000
NATS_BUFFER_LIMIT=100000
NATS_EVENTS_BUFFER_LIMIT=50000
```

Names start with a letter and contain letters, digits, or underscores. Lookup is case-insensitive; duplicate names and an empty list are invalid. Named mode ignores unnamed addresses, credentials, certificates, and connection names.

### Shared settings

| Variable | Default | Description |
| --- | --- | --- |
| `EWELINK_ACCOUNT` | required | Email or phone number; `EWELINK_EMAIL` is a fallback alias. |
| `EWELINK_PASSWORD` | required | Account password. |
| `EWELINK_APP_ID` | required | Developer app ID. |
| `EWELINK_APP_SECRET` | required | Developer app secret. |
| `EWELINK_REGION` | `us` | Region: `us`, `eu`, `cn`, or `as`; login follows region redirects. |
| `EWELINK_AREA_CODE` | `+1` | Area code used for login. |
| `BRIDGE_OUTPUTS` | unset | Comma-separated named outputs. |
| `VERBOSE` | `false` | Log all eWeLink websocket packets, which may contain sensitive data. |
| `EXIT_ON_WEBSOCKET_CLOSE` | `true` | Exit with failure on cloud closure, for container restart. |

Obtain your app ID and secret by registering and creating an application on the [eWeLink Developer Platform](https://dev.ewelink.io/). See the [eWeLink API library](https://www.npmjs.com/package/ewelink-api-next) for details.

### MQTT settings

| Variable | Default | Description |
| --- | --- | --- |
| `MQTT_URL` | required for MQTT | Broker URL using `mqtt`, `mqtts`, `ws`, or `wss`. |
| `MQTT_USER`, `MQTT_PASS` | empty | Authentication credentials. |
| `TOPIC_PREFIX` | `ewelink` | Nonempty topic prefix without wildcards. |
| `MQTT_QOS` | `1` | Device publication QoS: 0, 1, or 2. |
| `MQTT_RETAIN` | `true` | Retain device state. |
| `MQTT_BUFFER_LIMIT` | `100000` | Positive safe integer; maximum queued publications including the one in flight. |

Named settings are `MQTT_<NAME>_URL`, `USER`, `PASS`, `TOPIC_PREFIX`, `QOS`, `RETAIN`, and `BUFFER_LIMIT` with that same prefix. URL and credentials have no fallback. Prefix, QoS, retain, and buffer limit inherit shared defaults above. TLS broker connections use `mqtts://` and runtime trust configuration.

### NATS settings

| Variable | Default | Description |
| --- | --- | --- |
| `NATS_SERVERS` | required for NATS | Comma-separated nonempty server addresses. |
| `NATS_USER`, `NATS_PASS` | empty | Username/password authentication; password requires username. |
| `NATS_TOKEN` | empty | Token authentication. |
| `NATS_CREDS` | empty | Full multiline JWT/NKey credentials contents. |
| `NATS_NAME` | `ewelink-bridge` | Connection name; named outputs default to `ewelink-bridge-<name>`. |
| `NATS_TLS` | `false` | Require TLS using runtime trust roots. |
| `NATS_TLS_CA` | empty | PEM CA contents; enables TLS. |
| `NATS_TLS_CERT`, `NATS_TLS_KEY` | empty | PEM client certificate/key contents; set both. |
| `NATS_CONNECT_TIMEOUT` | `10000` | Positive connection timeout in milliseconds. |
| `NATS_RECONNECT_TIME_WAIT` | `3000` | Nonnegative reconnect delay in milliseconds; native reconnect adds jitter. |
| `NATS_MAX_RECONNECT_ATTEMPTS` | `-1` | Reconnect attempts per server; -1 unlimited, 0 disables retries. |
| `NATS_BUFFER_LIMIT` | `100000` | Positive safe integer; maximum queued publications including the one in flight. |
| `SUBJECT_PREFIX` | `ewelink` | Nonempty dot-separated prefix without whitespace or wildcards. |

Named settings use `NATS_<NAME>_` followed by any suffix in the table, including `SUBJECT_PREFIX`. Reconnect settings, timeout, TLS enable flag, subject prefix, and buffer limit inherit shared defaults. Servers, authentication, certificate contents, and connection names are configured per output. Choose exactly one authentication method: user/password, token, or credentials.

Booleans accept `true/false`, `1/0`, `yes/no`, and `on/off`. Empty optional settings use their fallback. Defaults for active protocols are validated even when overridden; settings for unused protocols are ignored. All configuration is validated before connections open.

## Publications and status

| Protocol | Device state | Bridge status |
| --- | --- | --- |
| MQTT | `ewelink/<deviceId>/state/<key>` | `ewelink/bridge/status` |
| NATS | `ewelink.<deviceId>.state.<key>` | `ewelink.bridge.status` |

For example, temperature 22.5 and humidity 48 produce two publications with payloads `22.5` and `48`. Scalars are text, objects/arrays are JSON, and null is empty text. MQTT wildcard characters, slashes, and NUL in device IDs/keys become underscores. NATS dots, whitespace, wildcards, and NUL become underscores.

MQTT status is retained, uses QoS 1 independently of device settings, and has an `offline` last will. Each NATS output publishes `online` and flushes on connection/reconnection before replaying device data. Asynchronous server permission errors are monitored separately because a successful flush does not confirm publish authorization. Allow publishing to status and device subjects.

Core NATS has no retention, MQTT QoS, last will, or subscriber acknowledgements. Late subscribers receive no previous state; abrupt termination cannot publish `offline`. The bridge does not configure JetStream.

## Running

Requires Node.js 22 or newer:

```sh
npm ci
cp .env.example .env
# Fill in eWeLink credentials and select your broker.
node --env-file=.env bridge.js
```

On PowerShell use `Copy-Item .env.example .env`. Alternatively export environment variables and run `npm start`.

Build and run with Docker:

```sh
docker build -t ewelink-bridge:local .
docker run -d --name ewelink-bridge --restart unless-stopped \
  --env-file .env ewelink-bridge:local
```

For a published image use `codejive/ewelink-bridge:<version>` with the release tag including its `v` prefix. Set the image tag in `docker-compose.yml`, then run `docker compose up -d`. Addresses must be reachable from the container; its `127.0.0.1` refers to the container itself. Use host networking only when your platform requires it.

Docker `--env-file` does not support multiline values. Inject multiline NATS credentials/certificates through environment variables or Compose's supported quoted multiline `.env` values.

## Operational behavior

- Every FIFO counts individual publications, including one in flight. Repeated parameter values stay separate and ordered.
- Overflow drops the oldest pending publication. The in-flight head stays pinned; at capacity one, incoming data is dropped while the head is in flight.
- MQTT callbacks and NATS flushes advance queues. NATS flushes confirm server round trips, not subscriber receipt. Stalled publications/status flushes time out after 15 seconds. Disconnects or failed operations keep unconfirmed data for retry; retries may duplicate messages.
- MQTT connection attempts time out after 15 seconds and retry every three seconds. NATS uses its configured connection/reconnect policy. Initial NATS failures also retry independently; finite budgets are applied per server. Exhaustion marks that output failed until process restart. Other outputs and eWeLink ingestion continue, and its queue remains bounded.
- Buffers are in RAM and disappear on exit. Count limits do not bound bytes; size limits according to payload sizes and available memory. MQTT retained replay sends historical values before retaining the latest delivered value.
- SIGINT/SIGTERM stops ingestion, attempts `offline`, and closes all outputs concurrently within five seconds. NATS drains transport data; application queues are not drained. A connection finishing after shutdown is closed.
- Logs identify the transport/output and report queue depth, in-flight count, drops, failures, and reconnects every minute. Repeated connection/outage/overflow logs are rate-limited. Transport errors use generic descriptions to avoid exposing credentials.
- eWeLink authentication errors stop the process. Disabling `EXIT_ON_WEBSOCKET_CLOSE` leaves it running after cloud closure without automatically reconnecting the cloud websocket.

## Validation

`npm test` runs configuration, buffering, adapter, and MQTT integration tests without eWeLink access. Real NATS integration tests run when `NATS_SERVER_BINARY` points to a local `nats-server` executable; they launch isolated test servers and run in CI.

## License

Apache License 2.0; see [LICENSE](LICENSE).
