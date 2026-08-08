# Developer Guide

A single, self-contained, step-by-step guide to running this entire system -
backend, Kafka, database and the React UI - in **three** ways:

1. **Developer Machine Mode** - everything installed natively on your own machine.
2. **Docker Mode** - everything running in containers, one `docker compose` command.
3. **AWS EKS Mode** - Kubernetes deployments on Amazon EKS.

If you only want a quick reference for one topic, jump straight to it using
the [documentation map](#documentation-map) at the end of this file.

---

## Table of contents

1. [Architecture at a glance](#1-architecture-at-a-glance)
2. [Developer Machine Mode](#2-developer-machine-mode)
   - 2.1 [Prerequisites](#21-prerequisites)
   - 2.2 [Install and configure Kafka in KRaft mode](#22-install-and-configure-kafka-in-kraft-mode)
   - 2.3 [Create the `Item_Topic` topic](#23-create-the-item_topic-topic)
   - 2.4 [Set up the database](#24-set-up-the-database)
   - 2.5 [Run the Spring Boot backend](#25-run-the-spring-boot-backend)
   - 2.6 [Run the React UI](#26-run-the-react-ui)
   - 2.7 [Test it end to end](#27-test-it-end-to-end)
3. [Docker Mode](#3-docker-mode)
   - 3.1 [Install Docker Desktop](#31-install-docker-desktop)
   - 3.2 [Install/verify Docker Compose](#32-installverify-docker-compose)
   - 3.3 [Run Kafka only in Docker](#33-run-kafka-only-in-docker)
   - 3.4 [Run the full backend stack in Docker](#34-run-the-full-backend-stack-in-docker-mysql--kafka--spring-boot)
   - 3.5 [Run the React UI in Docker](#35-run-the-react-ui-in-docker)
   - 3.6 [Do you need a "Docker UI" to access the app?](#36-do-you-need-a-docker-ui-to-access-the-app)
   - 3.7 [Which endpoints to test in Docker mode](#37-which-endpoints-to-test-in-docker-mode)
   - 3.8 [Useful Docker commands](#38-useful-docker-commands)
   - 3.9 [Docker troubleshooting](#39-docker-troubleshooting)
4. [AWS EKS Mode](#4-aws-eks-mode)
   - 4.1 [Why use EKS for this project](#41-why-use-eks-for-this-project)
   - 4.2 [What gets deployed to EKS](#42-what-gets-deployed-to-eks)
   - 4.3 [Deploy backend and frontend to EKS](#43-deploy-backend-and-frontend-to-eks)
   - 4.4 [Horizontal autoscaling (pods)](#44-horizontal-autoscaling-pods)
   - 4.5 [ECS/Fargate option](#45-ecsfargate-option)
5. [Documentation map](#documentation-map)

---

## 1. Architecture at a glance

```
 [ React UI ]  --HTTP/CORS-->  [ Spring Boot backend :8082 ]
                                     |            |
                                     |            +--> [ MySQL / SQL Server ]  (source + sink tables)
                                     |
                                     +--> [ Apache Kafka :9092 ]  (Item_Topic)
                                                |
                                     [ Apache Flink jobs, triggered on demand by the backend ]
```

Full diagrams and narrative: `ARCHITECTURE.md` / `Architecture_ConfluentCloud_Kafka_POC.docx`.

---

## 2. Developer Machine Mode

Run the backend, Kafka and the database natively on your machine (no Docker
required for this path, although you *can* still use Docker just for Kafka -
see [3.3](#33-run-kafka-only-in-docker)).

### 2.1 Prerequisites

| Tool | Version | Check with |
|---|---|---|
| Java JDK | 11+ | `java -version` |
| Maven | 3.8+ | `mvn -v` |
| Node.js | 18+ | `node -v` |
| npm | 9+ | `npm -v` |
| MySQL Server | 8.0+ | `mysql --version` |
| Apache Kafka | 3.x | (installed in step 2.2) |

### 2.2 Install and configure Kafka in KRaft mode

KRaft mode means Kafka runs **without ZooKeeper** - simpler for local development.

**Step 1 - Download.**
Go to <https://kafka.apache.org/downloads> and download the latest stable
**binary** release (Scala 2.13 build), e.g. `kafka_2.13-3.8.0.tgz`. You do
not need the source release.

**Step 2 - Extract.**
```powershell
# Windows - extract to a short path to avoid MAX_PATH issues
tar -xzf kafka_2.13-3.8.0.tgz -C C:\
Rename-Item C:\kafka_2.13-3.8.0 C:\kafka
```
```bash
# macOS/Linux
tar -xzf kafka_2.13-3.8.0.tgz
cd kafka_2.13-3.8.0
```

**Step 3 - Generate a cluster ID and format storage (run once).**
```powershell
cd C:\kafka
$uuid = & .\bin\windows\kafka-storage.bat random-uuid
.\bin\windows\kafka-storage.bat format -t $uuid -c .\config\kraft\server.properties
```
```bash
UUID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format -t "$UUID" -c config/kraft/server.properties
```

**Step 4 - Review the config (optional).**
Open `config/kraft/server.properties` and confirm:
```properties
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092
num.partitions=1
```

**Step 5 - Start the broker** (leave running in its own terminal):
```powershell
.\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
```
```bash
bin/kafka-server-start.sh config/kraft/server.properties
```

**Step 6 - Verify it's up:**
```powershell
.\bin\windows\kafka-broker-api-versions.bat --bootstrap-server localhost:9092
```
Any output listing API versions confirms success.

> Prefer not to install Kafka natively at all? Skip straight to
> [3.3 Run Kafka only in Docker](#33-run-kafka-only-in-docker) and come back
> here for the rest of Developer Machine Mode - the backend doesn't care
> whether Kafka itself is native or containerised, only that
> `localhost:9092` is reachable.

### 2.3 Create the `Item_Topic` topic

This is the single shared topic used by the producer, the manual consumer,
and both Flink jobs.

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic Item_Topic --partitions 3 --replication-factor 1
```
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic Item_Topic --partitions 3 --replication-factor 1
```

Confirm it exists:
```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic Item_Topic
```

Optional - watch messages live in a spare terminal while testing:
```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic Item_Topic --from-beginning
```

Full reference with troubleshooting: **`KAFKA_SETUP.md`**.

### 2.4 Set up the database

1. Install MySQL 8+ (or use SQL Server - see `DATABASE_SETUP.md` for both).
2. Run the seed scripts in `sql-scripts/` against your instance:
   ```powershell
   mysql -u root -p < sql-scripts\02_mysql_item_source_seed_200.sql
   mysql -u root -p < sql-scripts\03_mysql_item_sink_and_consumed_tables.sql
   ```
   This creates `item_poc_source.ITEM` (200 dummy rows, the **source** table)
   and `item_poc.ITEM` / `item_poc.ITEM_CONSUMED` (the **sink**/audit tables,
   left empty - populated by running the pipeline).

Full DDL, both MySQL and SQL Server, plus Windows Integrated Authentication
notes: **`DATABASE_SETUP.md`**.

### 2.5 Run the Spring Boot backend

```powershell
cd ConfluentCloud_Kafka-Producer-Consumer-POC
$env:ITEM_MYSQL_URL = "jdbc:mysql://localhost:3306/item_poc?useSSL=false&allowPublicKeyRetrieval=true"
$env:ITEM_MYSQL_USERNAME = "root"
$env:ITEM_MYSQL_PASSWORD = "<your-mysql-password>"
$env:ITEM_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
mvn spring-boot:run
```

The backend starts on **http://localhost:8082**. Swagger UI:
`http://localhost:8082/agent/swagger-ui.html`.

If you want SQL Server as source instead, set `ITEM_MSSQL_URL` as documented in `SETUP_GUIDE.md`.

Full environment-variable reference: **`SETUP_GUIDE.md`**.

### 2.6 Run the React UI

```powershell
cd ReactJS-UI-For-Item-Kafka-Producer-POC
npm install
npm run dev
```
Open **http://localhost:5173**. The backend already allows this origin via
CORS by default (see `ARCHITECTURE.md`, "CORS" section, and `CorsConfig.java`).

### 2.7 Test it end to end

Use the React UI directly, or curl/Postman against the endpoints listed in
`API_DOCUMENTATION.md`. Step-by-step UI walkthrough:
**`ReactJS_UI_User_Guide.docx`** (in both repos).

---

## 3. Docker Mode

Everything - MySQL, Kafka, and the Spring Boot backend - running in
containers, so nothing needs to be installed natively except Docker itself.

### 3.1 Install Docker Desktop

1. Download from <https://www.docker.com/products/docker-desktop/> (Windows, macOS, or Linux).
2. **Windows:** the installer will prompt to enable the **WSL2 backend** -
   accept this (it's faster and is the modern default). You may need to
   enable the "Windows Subsystem for Linux" and "Virtual Machine Platform"
   Windows features first; the installer will guide you if so.
3. Run the installer, then **restart your machine** if prompted.
4. Launch **Docker Desktop** from the Start Menu/Applications folder and
   wait for the whale icon in the system tray/menu bar to stop animating -
   that means the Docker *engine* (daemon) is ready.
5. Verify from a terminal:
   ```powershell
   docker version
   docker ps
   ```
   `docker ps` should return an (possibly empty) table with no connection
   errors.

### 3.2 Install/verify Docker Compose

Docker Compose (the `docker compose` subcommand, v2) ships **bundled** with
Docker Desktop - no separate install needed on Windows/macOS. Verify with:
```powershell
docker compose version
```
(On some Linux distributions installing the Docker Engine package manually,
you may need the separate `docker-compose-plugin` package - see
<https://docs.docker.com/compose/install/linux/>.)

### 3.3 Run Kafka only in Docker

If you want to run the backend natively but Kafka in a container, use the
lightweight compose file that ships with this repo:
```powershell
docker compose -f docker-compose.kafka.yml up -d
docker exec item-kafka-broker kafka-topics --bootstrap-server localhost:9092 --create --topic Item_Topic --partitions 1 --replication-factor 1
docker exec item-kafka-broker kafka-topics --bootstrap-server localhost:9092 --list
docker exec item-kafka-broker kafka-topics --bootstrap-server localhost:9092 --describe --topic Item_Topic
```
Stop it later with `docker compose -f docker-compose.kafka.yml down` (add
`-v` to also wipe topic data). Full detail: `KAFKA_SETUP.md`.

### 3.4 Run the full backend stack in Docker (MySQL + Kafka + Spring Boot)

This repo includes **`docker-compose.full.yml`**, which builds the backend
image from the included `Dockerfile` and wires it up to its own MySQL and
Kafka containers automatically.

**Step 1 - Build and start everything:**
```powershell
cd ConfluentCloud_Kafka-Producer-Consumer-POC
docker compose -f docker-compose.full.yml up -d --build
```
This will:
- Build the Spring Boot backend image (multi-stage Maven + JRE build).
- Start a MySQL 8 container, automatically seeding `item_poc_source.ITEM`
  (200 rows) and creating `item_poc.ITEM` / `item_poc.ITEM_CONSUMED` from
  `sql-scripts/` the **first time** the MySQL data volume is created.
- Start a Kafka broker (KRaft mode) with topic auto-creation enabled.
- Start the backend container, wired to talk to the `mysql` and `kafka`
  containers by their Docker Compose service names (not `localhost`).

**Step 2 - Watch it come up:**
```powershell
docker compose -f docker-compose.full.yml logs -f backend
```
Look for `Started KafkaItemProducerPocApplication`, then press Ctrl+C to
stop following logs (the containers keep running).

**Step 3 - Confirm the topic exists** (auto-create means this is optional,
but it's good practice to check explicitly):
```powershell
docker exec item-kafka-broker kafka-topics --bootstrap-server localhost:9092 --list
docker exec item-kafka-broker kafka-topics --bootstrap-server localhost:9092 --describe --topic Item_Topic
```

**Step 4 - The backend is now reachable exactly like native mode**, because
`docker-compose.full.yml` publishes port 8082 to your host machine:
```
http://localhost:8082/agent/swagger-ui.html
http://localhost:8082/item-kafka/app/items/v1
```

### 3.5 Run the React UI in Docker

The React repo also includes a `Dockerfile` (multi-stage Vite build served
by Nginx):
```powershell
cd ReactJS-UI-For-Item-Kafka-Producer-POC
docker build -t item-kafka-ui --build-arg VITE_API_BASE_URL=http://localhost:8082 .
docker run -d --name item-kafka-ui -p 8080:80 item-kafka-ui
```
Open **http://localhost:8080**. `VITE_API_BASE_URL` is baked in at *build*
time (Vite is a static-site tool), so rebuild the image if the backend's URL
changes.

> Alternatively, just run the UI natively with `npm run dev` (see
> [2.6](#26-run-the-react-ui)) even while the backend runs in Docker - both
> approaches work identically from the UI's point of view, since the
> backend's port is published to `localhost` either way.

### 3.6 Do you need a "Docker UI" to access the app?

**No.** This is a common point of confusion, so to be explicit:

- The **application itself** (the REST API on port 8082, the React UI on
  port 80/8080/5173) is accessed exactly the same way whether it's running
  natively or inside Docker - via your normal web browser, curl, Postman, or
  the React UI, hitting `http://localhost:<port>`. Docker's port publishing
  (`-p 8082:8082` / the `ports:` section in the compose files) makes the
  containerised service appear on `localhost` just like a native process
  would.
- **Docker Desktop's Dashboard** (the graphical app that installs alongside
  the engine) is a *management* tool, not a requirement for using the
  application. It's useful for:
  - Seeing which containers are running and their resource usage.
  - Viewing container logs in a scrollable window (equivalent to
    `docker logs -f <container>`).
  - Opening a shell **inside** a container (equivalent to
    `docker exec -it <container> sh`) - e.g. to run `mysql` commands
    directly inside the `item-mysql` container.
  - Starting/stopping/removing containers with a click instead of the CLI.
- You never need to "go into" Docker Desktop's dashboard to reach the
  application's own UI/API - only use it (or the CLI commands in
  [3.8](#38-useful-docker-commands)) if you want to inspect/manage the
  containers themselves (e.g. checking logs when something isn't working).

### 3.7 Which endpoints to test in Docker mode

Identical list to native mode - Docker mode changes *where* the process
runs, not what it exposes. See **`API_DOCUMENTATION.md`** for full
request/response examples of every endpoint, or drive them from the React
UI per **`ReactJS_UI_User_Guide.docx`**. Quick smoke test:
```powershell
# Paginated Item grid (reads from MySQL - the fastest way to confirm everything is healthy)
curl http://localhost:8082/item-kafka/app/items/v1?page=0&size=5

# Publish 100 items to Kafka
curl -X POST http://localhost:8082/item-kafka/app/publish-items/v1 -H "Content-Type: application/json" -d "{}"

# Trigger Flink Job 1, then poll its status
curl -X POST http://localhost:8082/flink/start-job1
curl "http://localhost:8082/flink/job-status?jobName=Flink%20Job%201"
```

### 3.8 Useful Docker commands

```powershell
# See what's running
docker compose -f docker-compose.full.yml ps

# Tail logs for one service
docker compose -f docker-compose.full.yml logs -f backend
docker compose -f docker-compose.full.yml logs -f mysql
docker compose -f docker-compose.full.yml logs -f kafka

# Open a shell inside a running container
docker exec -it item-mysql mysql -uroot -pPassword@1
docker exec -it item-kafka-broker bash

# Stop everything (keeps data volumes)
docker compose -f docker-compose.full.yml down

# Stop everything AND wipe the database/Kafka data (start completely fresh)
docker compose -f docker-compose.full.yml down -v

# Rebuild just the backend image after a code change
docker compose -f docker-compose.full.yml up -d --build backend
```

### 3.9 Docker troubleshooting

| Symptom | Fix |
|---|---|
| `docker ps` errors with "the docker daemon is not running" | Launch Docker Desktop and wait for the whale icon to stop animating |
| `backend` container keeps restarting / exits immediately | `docker compose -f docker-compose.full.yml logs backend` - almost always a DB connection issue; confirm `mysql` is healthy first (`docker compose ps`) |
| MySQL seed scripts didn't run | Init scripts only run against an **empty** data volume - if you'd already started the stack before, run `docker compose -f docker-compose.full.yml down -v` first, then `up -d --build` again |
| Kafka container exits with a "Cluster ID... not a valid UUID" error | The `CLUSTER_ID` value in the compose file must be a valid base64-encoded UUID (already fixed in the shipped compose files) - don't hand-edit it to a plain string |
| Port already in use (8082/3306/9092/8080) | Something else on your machine is already using that port - stop it, or change the left-hand side of the `ports:` mapping, e.g. `"18082:8082"` |

---

## 4. AWS EKS Mode

This mode is for cloud-native, production-style deployment using Kubernetes on AWS.

### 4.1 Why use EKS for this project

- You want multi-node orchestration, rolling updates, and health-based restarts.
- You want horizontal scaling across pods under load (HPA).
- You need one deployment model shared by multiple environments (dev/int/qa/prod).
- You want managed Kubernetes control plane operations handled by AWS.

### 4.2 What gets deployed to EKS

- Backend Spring Boot app (`item-kafka-backend`) as a `Deployment` + `Service` + optional `HPA`.
- Frontend React/Nginx app (`item-kafka-ui`) as a `Deployment` + `Service` + optional `HPA`.
- Config and secrets via `ConfigMap` + `Secret` manifests (do not commit real credentials).

For this POC, Kafka/MySQL are usually external AWS services in EKS mode:

- Kafka: Amazon MSK (recommended) or self-managed Kafka.
- Database: Amazon RDS for MySQL (recommended).

### 4.3 Deploy backend and frontend to EKS

Complete step-by-step instructions and commands are in `EKS_README.md`.

At a high level:

1. Create EKS cluster and node group.
2. Build and push backend/frontend images to ECR.
3. Apply manifests in each repo's `k8s/` folder.
4. Expose services via `LoadBalancer` or Ingress (ALB).
5. Validate pods and endpoint health.

### 4.4 Horizontal autoscaling (pods)

Both repos include sample HPA manifests. In EKS:

- `Deployment` sets baseline replica count.
- `HorizontalPodAutoscaler` scales replicas based on CPU utilization.
- `Cluster Autoscaler` (or Karpenter) can add/remove worker nodes when pods cannot be scheduled.

This is how pod-level and node-level autoscaling work together.

### 4.5 ECS/Fargate option

If you prefer not to operate Kubernetes, ECS/Fargate is a simpler AWS container path:

- ECS handles task scheduling; Fargate runs tasks serverlessly.
- Good for teams that want managed containers without Kubernetes APIs.
- EKS is still better if you need Kubernetes-native tooling/portability.

This project documents both options, but the Kubernetes manifests in `k8s/` target EKS.

---

## Documentation map

| Question | Read |
|---|---|
| What does this system do and why? | `README.md`, `ARCHITECTURE.md` |
| Full architecture with diagrams (printable) | `Architecture_ConfluentCloud_Kafka_POC.docx` |
| How do I set up Kafka from scratch? | `KAFKA_SETUP.md` (deep reference) and section 2.2/3.3 above (step-by-step) |
| How do I set up the database (MySQL or SQL Server)? | `DATABASE_SETUP.md` |
| Every environment variable, mapped to config | `SETUP_GUIDE.md` |
| Every REST endpoint, with curl examples | `API_DOCUMENTATION.md` |
| How do I run everything - local native, local Docker, or AWS EKS? | **This file** |
| Full AWS EKS deployment steps and commands | `EKS_README.md` |
| How do I use the React UI to test the backend, click by click? | `ReactJS_UI_User_Guide.docx` |
| How does the React UI's code work internally? | JSDoc comments in `src/api/apiClient.js`, `src/components/*.jsx`, `src/pages/*.jsx` in the React repo |
| 200-row dummy data / sink table DDL | `sql-scripts/` |
