# Item Kafka Producer / Consumer / Flink POC

This repository is a Spring Boot proof of concept that demonstrates three related flows for retail `Item` records:

1. **Read items from a database** using Spring Data JPA.
2. **Publish items to Kafka** on the `Item_Topic` topic.
3. **Consume items from Kafka** either with a Spring consumer or with an Apache Flink job that writes to MySQL.

The codebase uses the namespace `com.antontech.itemkafka_poc`.

## 📖 New here? Start with the Developer Guide

**[`DEVELOPER_GUIDE.md`](./DEVELOPER_GUIDE.md)** is the single, self-contained,
step-by-step walkthrough for running this *entire* system - backend, Kafka,
database and the React UI - in **either**:
- **Developer Machine Mode** (everything installed natively), or
- **Docker Mode** (`docker compose up`, including a Dockerfile for the backend
  and a full `docker-compose.full.yml` covering MySQL + Kafka + the backend), or
- **AWS EKS Mode** (Kubernetes manifests and deployment commands for EKS).

It also explains exactly which endpoints to test in each mode and clears up
whether you need Docker Desktop's dashboard to use the app (short answer: no -
see its "Do you need a Docker UI to access the app?" section). It ends with a
**documentation map** linking every other doc below to the specific question
it answers.

## Related documentation

- **`DEVELOPER_GUIDE.md`** - ⭐ start here - native **and** Docker setup, step by step, plus a full documentation map.
- **`SETUP_GUIDE.md`** - every environment variable mapped to its config property (Kafka, MySQL, SQL Server, TLS keystore/truststore, JWT key).
- **`KAFKA_SETUP.md`** - deep-dive reference for installing/configuring Apache Kafka (native KRaft or Docker) and creating the `Item_Topic` topic.
- **`EKS_README.md`** - AWS deployment guide for Amazon EKS, including Kubernetes manifests, ECR image publishing, pod autoscaling, and ECS/Fargate comparison notes.
- **`DATABASE_SETUP.md`** - full MS SQL Server and MySQL schema/table creation scripts generated from the `Item` entity, plus how Windows Integrated Authentication works and how to set it up (or use MySQL instead if you don't have SQL Server).
- **`API_DOCUMENTATION.md`** - every REST endpoint with curl examples, including how to trigger the Flink jobs.
- **`ARCHITECTURE.md`** - end-to-end architecture diagrams (Mermaid), why Apache Flink is used over a plain Kafka consumer, exactly which endpoints are synchronous vs. asynchronous, and how CORS is configured for the React front end.
- **`Architecture_ConfluentCloud_Kafka_POC.docx`** - the same architecture document with high-resolution rendered diagrams, ready to share/print.
- **`ReactJS_UI_User_Guide.docx`** - step-by-step guide for using the companion React UI to test every backend feature (also included in the React repo).
- **`sql-scripts/`** - 200-row dummy `Item` seed scripts for both MS SQL Server and MySQL, plus the MySQL sink/consumed-records table DDL.
- **`Dockerfile`** / **`docker-compose.full.yml`** - containerise and run the whole backend + MySQL + Kafka stack with one command.
- **`docker-compose.kafka.yml`** - a lighter-weight compose file for just a local Kafka broker.

## React test client

A companion React + Bootstrap UI that exercises every endpoint below (including a paginated
Item grid, 15 records per page) lives in a separate repository:
**[ReactJS-UI-For-Item-Kafka-Producer-POC](https://github.com/antonboshoff67-tech/ReactJS-UI-For-Item-Kafka-Producer-POC)**.
It calls this backend over CORS (see `CorsConfig.java` / `cors.allowed-origins`).

## What the project currently does

### 1) Item producer
- `ItemProducerController` reads records through `ItemRepository`.
- `ItemRepository` targets the `Item` entity mapped to the `ITEM` table.
- `ItemProducerService` serializes the items to JSON and publishes them to Kafka topic `Item_Topic`.
- Messages are keyed with either `item_group_<itemId>` or `manual-item-group_<itemId>`.

### 2) Spring Kafka consumer
- `ItemConsumerService` demonstrates manual polling from `Item_Topic`.
- `ItemConsumerController` exposes a manual consume endpoint.
- The current Spring consumer path deserializes records and logs them. It does **not** persist to MySQL yet.

### 3) Apache Flink jobs
- `MssqlItemToKafkaJob` reads a batch of rows from the configured MS SQL Server source table and publishes them as JSON to `Item_Topic`.
- `KafkaItemToMysqlJob` continuously consumes `Item_Topic` and upserts rows into the configured MySQL `ITEM` table using JDBC.
- `FlinkWordStreamDemoJob` is a dependency-free smoke test for the Flink runtime itself.
- All three are started on demand via `FlinkJobController` and orchestrated by `FlinkJobService` - see `API_DOCUMENTATION.md`.

## Kafka topic and consumer groups

### Topic
- `Item_Topic`

### Consumer groups
- `item_group`
- `manual-item-group`

## API endpoints

Base path: `item-kafka/app/`

### Publish items to Kafka
- `POST /item-kafka/app/publish-items/v1`

### Prepare a send request
- `POST /item-kafka/app/send-items/v1`

### Consume items request
- `GET /item-kafka/app/consume-items/v1`

### Consumer status
- `GET /item-kafka/consumer/consume-status/v1`

### Manual consume
- `POST /item-kafka/consumer/manual-consume/v1`

Swagger UI is configured at:
- `/agent/swagger-ui.html`

## Required infrastructure

- Java 11
- Maven
- Kafka broker
- MySQL 8+
- Optional SQL Server source database if you want to keep the legacy JPA source path
- Apache Flink runtime if you want to run the Flink jobs separately

## Configuration and secrets

The repository no longer stores real secrets. Provide values through environment variables or local overrides.

Recommended environment variables:

- `ITEM_KAFKA_BOOTSTRAP_SERVERS`
- `ITEM_MYSQL_URL`
- `ITEM_MYSQL_USERNAME`
- `ITEM_MYSQL_PASSWORD`
- `ITEM_MSSQL_URL`
- `ITEM_SSL_KEYSTORE`
- `ITEM_SSL_KEYSTORE_PASSWORD`
- `ITEM_SSL_TRUSTSTORE`
- `ITEM_SSL_TRUSTSTORE_PASSWORD`
- `ITEM_JWT_PRIVATE_KEY`
- `ITEM_JWT_ISSUER`
- `ITEM_JWT_EXPIRY_MINUTES`
- `ITEM_GATEWAY_URL`
- `SYSLOG_HOST`

## Local startup

### 1) Start Kafka and MySQL
Make sure Kafka and MySQL are running locally or reachable from your machine.

### 2) Set environment variables
Example PowerShell session:

```powershell
$env:ITEM_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:ITEM_MYSQL_URL = "jdbc:mysql://localhost:3306/item_poc?useSSL=false&allowPublicKeyRetrieval=true"
$env:ITEM_MYSQL_USERNAME = "root"
$env:ITEM_MYSQL_PASSWORD = "change-me"
$env:ITEM_JWT_PRIVATE_KEY = "<your RSA private key body or PEM>"
```

### 3) Run the Spring Boot app

```powershell
mvn spring-boot:run -Dspring-boot.run.profiles=dev
```

### 4) Use the APIs
- Publish records to Kafka.
- Manually consume them.
- Run `MssqlItemToKafkaJob` / `KafkaItemToMysqlJob` via `FlinkJobController` if you want the Flink-based replication path instead.

## Creating the Kafka topic

See `KAFKA_SETUP.md` for full install/configure instructions (download links, KRaft mode, Docker one-liner). Once a broker is running:

```powershell
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic Item_Topic --partitions 1 --replication-factor 1
```

If you are on Windows and using the Kafka scripts that ship with the distribution, run the corresponding `.bat` command instead.

## Database tables

See `DATABASE_SETUP.md` for full DDL scripts (both MS SQL Server and MySQL) and Windows Integrated Authentication setup. Short version:

### Source table for the producer
The JPA entity `Item` maps to the `ITEM` table.

For a local demo, either:
1. point `ITEM_MSSQL_URL` at a database that already has the `ITEM` table (`DATABASE_SETUP.md` section 1), or
2. use the MySQL alternative for the source table (`DATABASE_SETUP.md` section 2) if you don't have SQL Server available.

### Sink table for Flink
`KafkaItemToMysqlJob` writes to MySQL using the same `Item` column set (`DATABASE_SETUP.md` section 3).

Use a MySQL database such as `item_poc` and create a matching `ITEM` table. The column names must line up with the fields in `src/main/java/com/antontech/itemkafka_poc/model/Item.java` and the JDBC `INSERT` statement in `KafkaItemToMysqlJob`.

If you want the fastest possible local demo, let Hibernate create the source table first by keeping:

```yaml
spring:
  jpa:
    hibernate:
      ddl-auto: update
```

Then seed a few rows manually.

## Flink jobs

The project includes three Flink entry points, all under `com.antontech.itemkafka_poc.flink.jobs`:

- `MssqlItemToKafkaJob` - batch read from MS SQL Server, publish to Kafka.
- `KafkaItemToMysqlJob` - continuous streaming read from Kafka, upsert into MySQL. This is the main Kafka-to-MySQL flow.
- `FlinkWordStreamDemoJob` - dependency-free smoke test.

## Security notes for sharing this project

- Do not commit real JWT keys, keystore passwords, truststore passwords, or database passwords.
- Keep your actual secrets in environment variables or a local file excluded from Git.
- The sample `fnb.co.za`-style keystore reference has been replaced with the safer `antontech.co.za` namespace in configuration.

See `SETUP_GUIDE.md` for the full environment-variable-to-config mapping and step-by-step setup instructions.

## 📜 License

**© 2026 Anton Boshoff. All Rights Reserved.**

This repository is public for **portfolio/demonstration purposes only** — it is **not** open-source. See the [`LICENSE`](./LICENSE) file for full terms. In short: you're welcome to view/clone it to see how it works, but copying, redistributing, reselling, or building derivative products from this code requires the Author's written permission. Recruiters/hiring managers: browse away — that's exactly what this is here for!


