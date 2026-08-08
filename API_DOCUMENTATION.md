# API Documentation

Base URL for local runs: `http://localhost:8082`. Swagger/OpenAPI UI is
available at `http://localhost:8082/agent/swagger-ui.html` once the app is
running (see `SETUP_GUIDE.md`).

All examples below use PowerShell-friendly `curl.exe` syntax. On
macOS/Linux, drop `.exe` and adjust quoting as needed.

---

## Item Producer

### `POST /item-kafka/app/publish-items/v1`
Reads up to 100 `Item` rows from the configured source database
(`ItemRepository`) and publishes them as JSON messages to the shared Kafka
topic (`Item_Topic` by default) via `ItemProducerService`. Roughly the first
half of the batch is tagged for the `item_group` consumer group and the rest
for `manual-item-group`.

Implemented by: `ItemProducerController.createItemKafkaTopic()`

```powershell
curl.exe -X POST "http://localhost:8082/item-kafka/app/publish-items/v1" `
  -H "Content-Type: application/json"
```

Response: `200 OK` with the body `Items sent to Kafka topic successfully!`
(or an error message if the source table is empty/unreachable).

---

## Item grid / pagination (used by the React front end)

### `GET /item-kafka/app/items/v1?page=0&size=15`
Returns a single page of `Item` rows (sorted by `item_id` ascending) as a
Spring Data `Page<Item>` JSON object (`content`, `totalElements`,
`totalPages`, `number`, `size`, etc.). Powers the React UI's Item grid/pager.

Implemented by: `ItemController.listItems()`

```powershell
curl.exe "http://localhost:8082/item-kafka/app/items/v1?page=0&size=15"
```

### `GET /item-kafka/app/items/count/v1`
Returns the total number of `Item` rows in the source table as a plain number.

Implemented by: `ItemController.countItems()`

```powershell
curl.exe "http://localhost:8082/item-kafka/app/items/count/v1"
```

---

## Item Consumer

### `GET /item-kafka/consumer/consume-status/v1`
Reports whether a continuous listener-based consumer is active. This POC
only demonstrates manual/on-demand polling, so this currently always reports
not running (`ItemConsumerService.isRunning()` is a placeholder for wiring in
a real `@KafkaListener` health check).

Implemented by: `ItemConsumerController.checkConsumerStatus()`

```powershell
curl.exe "http://localhost:8082/item-kafka/consumer/consume-status/v1"
```

### `POST /item-kafka/consumer/manual-consume/v1`
Opens a short-lived Kafka consumer (up to ~30 seconds) using the supplied
consumer group id, polls the shared Item topic, deserializes each record
back into an `Item`, and commits offsets synchronously.

Implemented by: `ItemConsumerController.manualConsumeItem()` ->
`ItemConsumerService.manualConsume(groupId)`

```powershell
curl.exe -X POST "http://localhost:8082/item-kafka/consumer/manual-consume/v1" `
  -H "Content-Type: application/json" `
  -d '{ "groupId": "item_group", "msg": "manual poll test" }'
```

Valid `groupId` values: `item_group` or `manual-item-group` (case
insensitive). Any other value returns a validation message instead of
polling Kafka.

---

## Gateway / JWT test flow

### `POST /item-kafka/app/send-items/v1`
Builds a signed JWT (via `JwtTokenUtil`, using the key configured under
`jwt.private-key` / `ITEM_JWT_PRIVATE_KEY`) and prepares an
`Authorization: Bearer <token>` request destined for the configured
downstream gateway (`gateway.endpoint.url` / `ITEM_GATEWAY_URL`). If no JWT
key is configured, the request is still prepared but sent without an
`Authorization` header.

Implemented by: `MsgConsumerController.sendItemsToKafka()` ->
`MsgRoutingServiceImpl.processSentMsgRequest()`

```powershell
curl.exe -X POST "http://localhost:8082/item-kafka/app/send-items/v1" `
  -H "Content-Type: application/json" `
  -d '{ "msg": "hello from send-items" }'
```

### `GET /item-kafka/app/consume-items/v1`
Simulates the "receiving" side of the same test flow; logs the message body
(and an optional `Authorization` header if present) and returns success.

Implemented by: `MsgConsumerController.consumeItemsFromKafka()` ->
`MsgRoutingServiceImpl.processReceivedMsgRequest()`

```powershell
curl.exe -X GET "http://localhost:8082/item-kafka/app/consume-items/v1" `
  -H "Content-Type: application/json" `
  -H "Authorization: Bearer <optional-token>" `
  -d '{ "msg": "hello from consume-items" }'
```

---

## Flink job orchestration

All Flink endpoints are implemented by `FlinkJobController`, which delegates
to `FlinkJobService`. Jobs 1 and 2 run asynchronously in a background thread
and return immediately; the simple demo job runs synchronously.

### `POST /flink/start-job1`
Triggers `MssqlItemToKafkaJob`: reads a batch of `Item` rows from the
configured MS SQL Server source table and publishes them as JSON to the
shared Kafka topic. Fire-and-forget - check `/flink/job-status` afterwards.

```powershell
curl.exe -X POST "http://localhost:8082/flink/start-job1"
```

### `POST /flink/start-job2`
Triggers `KafkaItemToMysqlJob`: starts an unbounded Flink stream that
consumes from the shared Kafka topic and continuously upserts rows into the
configured MySQL `ITEM` table. This keeps running until the process is
stopped or the job is cancelled - it is not a one-shot batch like Job 1.

```powershell
curl.exe -X POST "http://localhost:8082/flink/start-job2"
```

### `POST /flink/start-simple-job`
Runs `FlinkWordStreamDemoJob`, a trivial "Hello/World/Flink" word-count style
job with no external dependencies. Useful to confirm the Flink runtime
itself works before troubleshooting the real Item pipeline jobs. Runs
synchronously and returns once complete.

```powershell
curl.exe -X POST "http://localhost:8082/flink/start-simple-job"
```

### `GET /flink/job-status?jobName=<name>`
Returns the last known status (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`)
recorded for a job, keyed by its display name.

Valid `jobName` values: `Flink Job 1`, `Flink Job 2`, `Flink Simple Job`.

```powershell
curl.exe "http://localhost:8082/flink/job-status?jobName=Flink%20Job%201"
```

---

## Quick end-to-end smoke test

```powershell
# 1) Confirm the app is up and Flink runtime works
curl.exe -X POST "http://localhost:8082/flink/start-simple-job"

# 2) Publish sample Items from the source DB onto Kafka
curl.exe -X POST "http://localhost:8082/item-kafka/app/publish-items/v1"

# 3) Manually consume them back off Kafka (Spring consumer path)
curl.exe -X POST "http://localhost:8082/item-kafka/consumer/manual-consume/v1" `
  -H "Content-Type: application/json" -d '{ "groupId": "item_group" }'

# 4) OR replicate them into MySQL continuously via Flink (Flink path)
curl.exe -X POST "http://localhost:8082/flink/start-job2"

# 5) Check status
curl.exe "http://localhost:8082/flink/job-status?jobName=Flink%20Job%202"
```

