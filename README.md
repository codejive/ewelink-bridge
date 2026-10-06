# eWeLink to MQTT Bridge

`ewelink-mqtt-bridge` connects to one eWeLink cloud websocket and republishes device updates to one or more [MQTT](https://mqtt.org/) brokers. Data flows only from eWeLink to MQTT; the bridge does not subscribe to broker commands.

This is useful when your eWeLink devices can't connect directly to your MQTT broker. For example because your broker is on a public server and uses SSL, which the eWeLink devices don't support.

Looking for the NATS version of this bridge? Check out [ewelink-nats-bridge](https://github.com/codejive/ewelink-nats-bridge).

## How It Works

1. Authenticate to eWeLink cloud using app credentials and account credentials.
2. Open a persistent websocket to receive live device update events.
3. Convert each update once into individual publications, then enqueue them independently for every output.
4. Each output uses its own MQTT connection, bounded in-memory FIFO, delivery worker, and reconnect cycle. An unavailable broker does not block other outputs or eWeLink updates.

## MQTT Topics

Default topic layout:

- `ewelink/<deviceId>/state/<key>`
- `ewelink/bridge/status`

Examples:

- `ewelink/1000abcdef/state/temperature` -> `22.5`
- `ewelink/1000abcdef/state/humidity` -> `48`
- `ewelink/bridge/status` -> `online` or `offline`

Topic prefix is configurable via `TOPIC_PREFIX`.

Each output publishes its own retained `<prefix>/bridge/status=online` on successful connection, separately from buffered device data. Its retained last will is `offline`. Shutdown stops accepting updates, attempts `offline` on connected outputs, and closes all outputs concurrently under a five-second process deadline without waiting to drain queues. Availability always uses QoS 1 and retain, independently of device publication settings.

## Environment Variables

The bridge is configured only through environment variables.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `EWELINK_ACCOUNT` | Yes | - | eWeLink account identifier (email or phone number). |
| `EWELINK_PASSWORD` | Yes | - | eWeLink account password used for cloud login. |
| `EWELINK_APP_ID` | Yes | - | eWeLink developer app ID used for API authentication. |
| `EWELINK_APP_SECRET` | Yes | - | eWeLink developer app secret used for API authentication. |
| `EWELINK_REGION` | No | `us` | eWeLink region for your account. Valid values: `us`, `eu`, `cn`, `as`. |
| `EWELINK_AREA_CODE` | No | `+1` | Area code used during login (required by eWeLink login API). |
| `MQTT_URL` | No | `mqtt://127.0.0.1:1883` | MQTT broker URL. Examples: `mqtt://broker:1883`, `mqtts://broker:8883`. |
| `MQTT_USER` | No | empty | Username for MQTT authentication. |
| `MQTT_PASS` | No | empty | Password for MQTT authentication. |
| `TOPIC_PREFIX` | No | `ewelink` | Prefix used for all published topics. |
| `VERBOSE` | No | `false` | If `true`, logs every websocket message received from eWeLink, including non-update packets. |
| `MQTT_RETAIN` | No | `true` | If `true`, publishes state messages with retain flag enabled. |
| `MQTT_QOS` | No | `1` | MQTT QoS level for published messages. Allowed: `0`, `1`, `2`. |
| `MQTT_BUFFER_LIMIT` | No | `100000` | Maximum individual device publications per output, including the one in flight. Positive safe integer. |
| `MQTT_OUTPUTS` | No | unset | Comma-separated output names, e.g. `home,remote`. Enables named configuration below. |
| `EXIT_ON_WEBSOCKET_CLOSE` | No | `true` | If `true`, process exits when eWeLink websocket closes (recommended in containers with restart policy). |

Boolean variables (`VERBOSE`, `MQTT_RETAIN`, `EXIT_ON_WEBSOCKET_CLOSE`) accept: `true/false`, `1/0`, `yes/no`, `on/off`. Invalid booleans, QoS, limits, topic prefixes, URLs, region, missing eWeLink credentials, and invalid named outputs fail validation before any connection is opened.

### Named outputs and precedence

Without `MQTT_OUTPUTS`, existing `MQTT_URL`, `MQTT_USER`, `MQTT_PASS`, `TOPIC_PREFIX`, `MQTT_QOS`, and `MQTT_RETAIN` deployments continue to work, with the new bounded buffer. With `MQTT_OUTPUTS`, the legacy broker URL and credentials are ignored. Each named output requires its own URL; credentials are optional and do not inherit legacy credentials.

```ini
MQTT_OUTPUTS=home,remote
MQTT_HOME_URL=mqtt://home-broker:1883
MQTT_HOME_USER=bridge
MQTT_HOME_PASS=secret
MQTT_REMOTE_URL=mqtts://remote-broker:8883
MQTT_REMOTE_USER=bridge
MQTT_REMOTE_PASS=secret

TOPIC_PREFIX=ewelink
MQTT_QOS=1
MQTT_RETAIN=true
MQTT_BUFFER_LIMIT=100000
MQTT_HOME_BUFFER_LIMIT=50000
```

Names start with a letter and contain letters, digits, or underscores. Names are case-insensitive for environment variable lookup and must be unique. An explicitly empty `MQTT_OUTPUTS` is invalid. The output name is uppercased in variable names.

| Per-output variable | Fallback |
| --- | --- |
| `MQTT_<NAME>_URL` | Required; no fallback |
| `MQTT_<NAME>_USER`, `MQTT_<NAME>_PASS` | No credentials |
| `MQTT_<NAME>_TOPIC_PREFIX` | `TOPIC_PREFIX`, then `ewelink` |
| `MQTT_<NAME>_QOS` | `MQTT_QOS`, then `1` |
| `MQTT_<NAME>_RETAIN` | `MQTT_RETAIN`, then `true` |
| `MQTT_<NAME>_BUFFER_LIMIT` | `MQTT_BUFFER_LIMIT`, then `100000` |

Empty optional values use their fallback. Shared defaults are validated even when outputs override them. URLs support `mqtt://`, `mqtts://`, `ws://`, and `wss://`. Topic prefixes must be nonempty and contain no MQTT wildcards. All settings remain ordinary environment variables, usable with `docker run -e`, `--env-file`, or Compose `env_file`; Node does not automatically load `.env`.

## Getting App Credentials

This bridge requires your own eWeLink app credentials:

1. **Register an eWeLink Developer Account**
   - Visit [eWeLink Developer Platform](https://dev.ewelink.io/)
   - Create an account or log in

2. **Create an Application**
   - Navigate to the applications/credentials section
   - Create a new application
   - You'll receive an `APP_ID` and `APP_SECRET`

3. **Configure the Bridge**
   - Add `EWELINK_APP_ID` and `EWELINK_APP_SECRET` to your `.env` file or pass them as environment variables
   - Set `EWELINK_ACCOUNT` to your eWeLink email or phone number

If you're unable to obtain credentials, check the [eWeLink API Next documentation](https://www.npmjs.com/package/ewelink-api-next) or the [eWeLink community forums](https://www.ewelink.cc/).

## Usage

Copy `.env.example` to `.env`, fill in your credentials, and set `MQTT_URL` to a broker address reachable from the container (not `127.0.0.1`). Replace `<version>` below with the exact tag of a [published release](https://github.com/codejive/ewelink-mqtt-bridge/releases), including the `v` prefix. Normally, use the latest release:

```sh
docker run -d --name ewelink-mqtt-bridge --restart unless-stopped \
  --env-file .env codejive/ewelink-mqtt-bridge:<version>
```

## Developing

### Quick Start (Node.js)

1. Install dependencies:

```bash
npm install
```

1. Run with required variables:

```bash
EWELINK_ACCOUNT="you@example.com" \
EWELINK_PASSWORD="your-password" \
EWELINK_REGION="eu" \
EWELINK_AREA_CODE="+1" \
EWELINK_APP_ID="your-app-id" \
EWELINK_APP_SECRET="your-app-secret" \
MQTT_URL="mqtt://127.0.0.1:1883" \
node bridge.js
```

### Docker

Build image:

```bash
docker build -t ewelink-mqtt-bridge:local .
```

Run container:

```bash
docker run -d \
  --name ewelink-mqtt-bridge \
  --restart unless-stopped \
  -e EWELINK_ACCOUNT="you@example.com" \
  -e EWELINK_PASSWORD="your-password" \
  -e EWELINK_REGION="eu" \
  -e EWELINK_AREA_CODE="+1" \
  -e EWELINK_APP_ID="your-app-id" \
  -e EWELINK_APP_SECRET="your-app-secret" \
  -e MQTT_URL="mqtt://broker:1883" \
  -e MQTT_USER="mqtt-user" \
  -e MQTT_PASS="mqtt-pass" \
  ewelink-mqtt-bridge:local
```

Use host networking only if your platform/setup requires it.

For two outputs, put eWeLink credentials in `.env` and pass the output configuration directly, or put all variables in the same env file:

```sh
docker run -d --name ewelink-mqtt-bridge --restart unless-stopped \
  --env-file .env \
  -e MQTT_OUTPUTS=home,remote \
  -e MQTT_HOME_URL=mqtt://home-broker:1883 \
  -e MQTT_HOME_USER=bridge -e MQTT_HOME_PASS=secret \
  -e MQTT_REMOTE_URL=mqtts://remote-broker:8883 \
  -e MQTT_REMOTE_USER=bridge -e MQTT_REMOTE_PASS=secret \
  -e MQTT_BUFFER_LIMIT=100000 -e MQTT_HOME_BUFFER_LIMIT=50000 \
  ewelink-mqtt-bridge:local
```

Docker `-e` settings override values from `--env-file`. Use addresses reachable from the container for every output.

### Docker Compose

1. Copy `.env.example` to `.env` and fill in your values.
2. Update image in `docker-compose.yml` to your published image (or build locally and point to that tag).
3. Start:

```bash
docker compose up -d
```

## Operational Notes

- If websocket connectivity drops, the bridge can exit and rely on container restart policy.
- MQTT retained messages are convenient for Home Assistant and similar consumers, but can be disabled with `MQTT_RETAIN=false`.
- The bridge manages `ewelink/bridge/status` itself: `online` on MQTT connect, `offline` on shutdown, and `offline` via MQTT last-will if the process stops unexpectedly.
- If your broker requires TLS, use an `mqtts://` URL and ensure trust/cert settings are provided by your runtime environment.
- Each broker has a FIFO measured in individual publications, not updates or topics. Repeated values for the same topic remain separate and ordered. Pending and in-flight publications together cannot exceed that output's limit.
- On overflow, the oldest pending publication is dropped and counted. The in-flight publication cannot be withdrawn: it stays at the head while the oldest publication behind it is discarded. At capacity one with a publication in flight, the incoming publication is dropped instead because no existing pending publication can be removed.
- Successful publish callbacks remove publications. Disconnects, publish errors, and callbacks stalled for 15 seconds replace that output's transport and retry buffered data in order. Connection attempts also time out after 15 seconds; retries are independent and spaced three seconds apart. Retries can produce duplicates, including when a broker received a message before confirmation was lost. QoS 0 callbacks confirm local sending only, not broker receipt; QoS 1/2 use MQTT acknowledgments and do not make bridge retries exactly once.
- Buffers exist only in RAM and are lost on process exit, restart, or shutdown. They provide no persistence. Memory grows with payload and topic sizes and queue depth across all outputs; 100,000 publications is a count limit, not a byte limit. As an illustration, 100-byte payloads alone account for about 10 MB per full output, before topic strings, FIFO nodes, JavaScript overhead, and MQTT transport storage. Size limits and available container memory accordingly.
- With `MQTT_RETAIN=true`, replay sends historical buffered values to active subscribers. Retained storage ultimately holds the last delivered value for each topic. Disabling retain does not disable FIFO buffering.
- Logs include the output name and report queue depth, in-flight count, cumulative overflow drops, publication failures, and reconnect attempts every minute and on relevant events. Repeated outage, reconnect, connection, and overflow logs are limited to one per category per output every 30 seconds. Broker URL credentials and query strings are redacted; broker errors use generic descriptions to avoid exposing credentials.

Run `npm test` for configuration and FIFO unit tests, transport callback tests, and local two-broker integration tests. The integration tests cover matching publication order, disconnects, stalled acknowledgments, reconnect replay, overflow, in-flight accounting, unavailable-broker startup/shutdown, and legacy single-broker configuration. They require no eWeLink credentials or external broker.

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE).
