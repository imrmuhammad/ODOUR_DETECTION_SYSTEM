# AAI Intelligent Washroom Monitoring System (AAI-WMS)

> **Version:** M5 | **Stack:** FastAPI · EMQX · Redis · TimescaleDB · HAProxy · Wazuh  
> **Classification:** Production-Grade IoT Telemetry Pipeline with Adaptive Security Architecture

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [System Topology & Network Architecture](#2-system-topology--network-architecture)
3. [Engine Deep-Dives](#3-engine-deep-dives)
   - [3.1 Edge Fleet Engine — Raspberry Pi Pico W Nodes](#31-edge-fleet-engine--raspberry-pi-pico-w-nodes)
   - [3.2 Gateway Ingress & Network Core](#32-gateway-ingress--network-core)
   - [3.3 EMQX Distributed Messaging Broker Cluster](#33-emqx-distributed-messaging-broker-cluster)
   - [3.4 FastAPI Core Ingestion & State Processing Engine](#34-fastapi-core-ingestion--state-processing-engine)
   - [3.5 Redis Cache & State Hash Engine](#35-redis-cache--state-hash-engine)
   - [3.6 TimescaleDB Hypertable Persistence Engine](#36-timescaledb-hypertable-persistence-engine)
4. [Core Algorithms & Formulas](#4-core-algorithms--formulas)
   - [4.1 Washroom Health Index (WHI) Base Calculation](#41-washroom-health-index-whi-base-calculation)
   - [4.2 Sustained Degradation Time-Penalty Algorithm](#42-sustained-degradation-time-penalty-algorithm)
   - [4.3 Multi-Unit Floor Escalation Math](#43-multi-unit-floor-escalation-math)
   - [4.4 Incident Verification Engine — The 3-Cycle Debouncer](#44-incident-verification-engine--the-3-cycle-debouncer)
5. [Security Architecture](#5-security-architecture)
   - [5.1 Phase 1 — Baseline Hardening](#51-phase-1--baseline-hardening)
   - [5.2 Phase 2 — Core Access Control & Integrity](#52-phase-2--core-access-control--integrity)
   - [5.3 Phase 3 — Adaptive Perimeter Defense](#53-phase-3--adaptive-perimeter-defense)
   - [5.4 Security Dependency Chain](#54-security-dependency-chain)
6. [Configuration Reference](#6-configuration-reference)
7. [Maintenance & Chaos Testing](#7-maintenance--chaos-testing)

---

## 1. Project Overview

The AAI Intelligent Washroom Monitoring System is a production-grade, multi-layer IoT telemetry pipeline designed to monitor washroom conditions in real time across airport terminal facilities. The system continuously ingests sensor data from a distributed fleet of Raspberry Pi Pico W edge nodes, classifies environmental health through a proprietary scoring formula, raises graded incidents, escalates them up to floor-level when multiple units degrade simultaneously, and provides a role-gated REST dashboard for operators and supervisors.

The architecture is explicitly designed around four engineering mandates:

- **Zero plaintext in transit.** Every communication path — device to broker, broker to application, application to database — is encrypted with mutually-authenticated TLS. No unencrypted listener is exposed at any layer.
- **Fail-closed security semantics.** Every access control check, every rate limiter, and every identity verification routine fails closed: an error returns a denial, never a pass.
- **Atomic state mutations.** All shared state transitions that could race under concurrent processing (rate limit counters, refresh token rotation, telemetry buffer flush tracking) are serialized through Redis Lua scripts, eliminating TOCTOU races across multiple application instances.
- **Immutable historical audit trail.** The telemetry database enforces SQL-level immutability triggers on all historical event tables, making tampering with incident logs structurally impossible even for superusers.

The system is containerized entirely in Docker Compose and is deployable as a single-command stack, with a `setup_security.sh` script that auto-generates the full PKI chain (CA, per-service certificates, device certificates) and all Docker Secrets on first run.

---

## 2. System Topology & Network Architecture

The deployment is partitioned into three logically isolated Docker bridge networks, each operating on a dedicated IP subnet. This segmentation ensures that a compromise in one zone cannot trivially pivot to another.

```
Internet / Field Devices
        │
        ▼
┌───────────────────────────────────────────────────────────┐
│  FRONTEND NETWORK  172.20.1.0/24                          │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  Virtual IP: 172.20.1.10 (Keepalived VIP)          │  │
│  │  HAProxy Node 1  (172.20.1.100)  MASTER             │  │
│  │  HAProxy Node 2  (172.20.1.101)  BACKUP             │  │
│  └──────────────────────┬──────────────────────────────┘  │
└─────────────────────────┼─────────────────────────────────┘
                          │
┌─────────────────────────┼─────────────────────────────────┐
│  BACKEND NETWORK  172.20.2.0/24                           │
│                                                           │
│  ┌──────────────────────▼──────────────────────────────┐  │
│  │  EMQX Cluster Node 1 (emqx1.iot-network)           │  │
│  │  EMQX Cluster Node 2 (emqx2.iot-network)           │  │
│  │  EMQX Cluster Node 3 (emqx3.iot-network)           │  │
│  └──────────────────────┬──────────────────────────────┘  │
│                         │                                  │
│  ┌──────────────────────▼──────────────────────────────┐  │
│  │  FastAPI Application Container                      │  │
│  └──────────────────────┬──────────────────────────────┘  │
└─────────────────────────┼─────────────────────────────────┘
                          │
┌─────────────────────────┼─────────────────────────────────┐
│  DATA NETWORK  172.20.3.0/24                              │
│                                                           │
│  ┌──────────────────────▼──────────────────────────────┐  │
│  │  Redis 7.2  (washroom-redis)                        │  │
│  │  TimescaleDB / PostgreSQL 16 (washroom-timescaledb) │  │
│  └─────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────┘
```

### Port Exposure Map

| Port | Protocol | Service | Notes |
|------|----------|---------|-------|
| `8883` | TCP/TLS | MQTT mTLS ingress | HAProxy TCP pass-through to EMQX cluster |
| `443` | HTTPS | FastAPI REST API | TLS terminated at HAProxy, forwarded to FastAPI:8000 |
| `18083` | HTTPS | EMQX Dashboard | TLS terminated at HAProxy, reverse-proxied to EMQX |
| `5433` | None (internal) | TimescaleDB | Not exposed to host in production; accessible only on `data` network |

FastAPI's port 8000 is deliberately not bound to the host. All external API traffic is routed exclusively through the HAProxy SSL termination layer on port 443.

### High-Availability Gateway (HAProxy + Keepalived)

Two HAProxy instances run in active/passive mode with Keepalived managing a floating Virtual IP address (`172.20.1.10`). The MASTER node holds the VIP with priority 101; the BACKUP node holds priority 100. In the event of MASTER failure, Keepalived transfers the VIP to the BACKUP node within seconds, maintaining continuity of all MQTT device connections and REST API traffic without manual intervention.

HAProxy routing behavior differs per protocol:

- **MQTT (port 8883):** Operates in `mode tcp` — TCP pass-through. TLS termination and mTLS verification are performed end-to-end at the EMQX broker layer, preserving the client certificate's CN for identity mapping.
- **REST API (port 443) and EMQX Dashboard (port 18083):** HAProxy terminates TLS using its own certificate (`api.pem`), then load-balances HTTP to the application backends in round-robin mode.

---

## 3. Engine Deep-Dives

### 3.1 Edge Fleet Engine — Raspberry Pi Pico W Nodes

Each washroom unit is instrumented with a Raspberry Pi Pico W microcontroller. The devices are designated by a naming convention of `pico-<Terminal>-<UnitCode>` (e.g., `pico-T1-W01`) and carry a unique TLS client certificate signed by the deployment's Private Certificate Authority.

The Pico W's identity is entirely derived from its certificate's Common Name (CN) field. When a device connects to the EMQX broker, EMQX extracts the CN from the verified client certificate and maps it directly to the MQTT client `username`. This eliminates the need for a separate password-based credential. A device with CN `pico-T1-W01` is authorized to publish exclusively to topics matching `washroom/+/pico-T1-W01/telemetry` and `washroom/+/pico-T1-W01/heartbeat`. No device may publish to another device's topic path.

Each telemetry payload published by a Pico W node contains the following fields, which are validated by the backend's Pydantic schema upon receipt:

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| `device_id` | `str` | Regex: `^pico-[a-zA-Z0-9\-]+$` | Self-reported device identity |
| `timestamp` | `datetime` | ISO-8601 | Sensor reading timestamp (UTC) |
| `avg_nh3_ppm` | `float` | `[0.0, 500.0]` | Average ammonia concentration over reading window |
| `peak_nh3_ppm` | `float` | `[0.0, 500.0]` | Peak ammonia concentration over reading window |
| `avg_temperature_c` | `float` | `[-10.0, 60.0]` | Average ambient temperature (Celsius) |
| `avg_humidity_percent` | `float` | `[0.0, 100.0]` | Average relative humidity percentage |
| `throughput` | `int` | `[0, 10000]` | User throughput count over reading window |
| `occupancy_inside` | `int` | `[0, 1000]` | Instantaneous occupancy count |
| `abandon_rate_percent` | `float` | `[0.0, 100.0]` | Percentage of users who entered and immediately exited |
| `raw_whi` | `float` | `[0.0, 100.0]` | Pre-computed Washroom Health Index score |
| `msg_type` | `str` | `telemetry\|alerts\|heartbeat` | Inferred from MQTT topic |

The `device_id` field is validated against a strict regular expression on the server side. Any payload from a device claiming an ID that does not match the `pico-` prefix pattern is rejected at the schema validation stage before reaching the processing queue.

---

### 3.2 Gateway Ingress & Network Core

The HAProxy configuration (`haproxy/haproxy.cfg`) defines three independent frontend/backend pairings:

**MQTT Ingress (`mqtt_ssl_front` → `mqtt_ssl_back`):**
HAProxy operates in raw TCP mode. It accepts connections on port 8883 and proxies them byte-for-byte to one of the three EMQX broker nodes using round-robin load balancing. Because TLS termination is not performed at HAProxy for MQTT, the full TLS handshake — including client certificate presentation and mTLS verification — occurs directly between the Pico W device and the EMQX server. This preserves the integrity of the device-identity-from-certificate mechanism.

**API Ingress (`api_https_front` → `fastapi_backend`):**
HAProxy terminates TLS on port 443 using its own signed certificate (`api.pem`). Traffic is decrypted at HAProxy and forwarded as plain HTTP to the FastAPI container on port 8000 within the shared `backend` Docker network. The FastAPI container is not exposed to the host network; it is only reachable through HAProxy's internal routing.

**Dashboard Ingress (`dashboard_front` → `dashboard_back`):**
Similarly, port 18083 provides TLS-terminated access to the EMQX cluster's administrative dashboard, reverse-proxied across all three broker nodes.

---

### 3.3 EMQX Distributed Messaging Broker Cluster

The EMQX broker tier consists of three nodes (`emqx1`, `emqx2`, `emqx3`) configured as a static cluster. All three nodes are seeded with the same cluster seed list at startup, allowing them to form the cluster automatically without an external service discovery mechanism.

#### Cluster Configuration

Each node is identified by its Erlang node name: `emqx1@emqx1.iot-network`, etc. Nodes share a secret Erlang cluster cookie (`EMQX_NODE_COOKIE`) injected via environment variable. This cookie authenticates inter-node communication and prevents rogue nodes from joining the cluster.

#### Listener Configuration (mTLS Enforcement)

The EMQX configuration (`emqx/emqx.conf`) enforces the following listener policy:

- **TLS listener on port 8883** is the only active listener. It is configured with `verify_peer`, `fail_if_no_peer_cert = true`, and TLS protocol versions restricted to `tlsv1.3` and `tlsv1.2` only. Any connection that does not present a valid client certificate signed by the deployment CA is rejected at the TLS handshake — before the MQTT CONNECT packet is even parsed.
- **Plaintext TCP listener (port 1883)** is explicitly disabled: `listeners.tcp.default.enabled = false`.
- **WebSocket listeners (ws and wss)** are also explicitly disabled, closing two additional potential attack surfaces.

#### Payload Size Cap

MQTT packets are hard-capped at 64 KB (`max_packet_size = 64KB`). This prevents a malicious or malfunctioning device from sending arbitrarily large payloads that could cause memory pressure or denial-of-service conditions in the broker.

#### Certificate-to-Username Mapping

The configuration directive `peer_cert_as_username = "cn"` instructs EMQX to extract the CN field from the verified client certificate and use it as the MQTT `username`. This makes the client's authenticated identity — as proven by the PKI chain — the basis for all authorization decisions, rather than a separately provided password.

#### Topic Authorization (ACL)

The ACL file (`emqx/acl.json`) defines authorization rules in priority order:

1. The `system-backend-subscriber` identity (the FastAPI backend's client certificate CN) is granted subscribe access to all telemetry, alert, and heartbeat topics across all terminals and washrooms. It may also publish to alert topics to push configuration updates.
2. Identities matching the regular expression `^pico-.*$` (i.e., all device certificates) are granted publish access **only** to topics that include their own username (`%u`) in the washroom slot: `washroom/+/%u/telemetry` and `washroom/+/%u/heartbeat`. A device cannot publish to another device's telemetry path. Devices may subscribe to alert and firmware topics for receiving configuration pushes.
3. A final catch-all `{deny, all}` rule rejects every connection and message not explicitly matched by the above rules.

This layered ACL prevents topic spoofing. A compromised device with certificate CN `pico-T1-W01` cannot publish telemetry under a different device's identity because the ACL's `%u` substitution enforces that the topic path must match the authenticated CN.

#### Health Monitoring

Each EMQX container has a Docker Compose health check that runs `/opt/emqx/bin/emqx ctl status` every 10 seconds. The FastAPI and HAProxy containers declare `condition: service_healthy` dependencies on all three EMQX nodes, ensuring the broker cluster is fully operational before any application or routing layer starts accepting connections.

---

### 3.4 FastAPI Core Ingestion & State Processing Engine

The FastAPI application is the brain of the processing pipeline. It is built on Python 3.11 with `asyncio` and runs under the `uvicorn` ASGI server. The application lifecycle, worker topology, and processing pipeline are described below.

#### Startup Sequence and Lifespan Management

The application uses FastAPI's `lifespan` context manager to enforce a strict initialization sequence:

1. **Superuser credential seeding.** A temporary superuser PostgreSQL connection is opened to `INSERT OR UPDATE` the default user roster (operator, supervisor, admin, etc.) with Argon2id-hashed passwords read from Docker Secrets. The superuser connection is closed before the next phase.
2. **Worker connection pool initialization.** The application's asyncpg connection pool is opened under the restricted `aai_app_worker` role, which has only `SELECT` and `INSERT` on specific tables — it cannot `UPDATE`, `DELETE`, `DROP`, or perform DDL.
3. **Batcher monitors start.** Background coroutines for the telemetry batcher and audit batcher begin polling their Redis buffers.
4. **Worker tasks launch.** One priority worker and three normal workers are spawned as asyncio tasks.
5. **MQTT subscriber starts last.** The MQTT subscriber is the final component to start, ensuring workers and database pools are ready before sensor data begins flowing.

On shutdown, the order is reversed: the MQTT subscriber stops accepting messages, workers drain and cancel, batchers complete in-flight flushes, and database connections close cleanly.

#### MQTT Subscriber and Message Ingestion Pipeline

The `MQTTSubscriber` class (`app/services/mqtt.py`) maintains a persistent, auto-reconnecting MQTT connection to the broker (via the HAProxy VIP). It subscribes to two wildcard topics:

- `washroom/+/+/telemetry` — for periodic sensor readings
- `washroom/+/+/alerts` — for out-of-band alert messages

For every received message, the subscriber executes a strict sequential processing pipeline:

**Step 1 — Raw Audit Tap.** Before any parsing or validation, the raw message bytes and topic string are pushed to the `AuditBatcher`. This ensures that even malformed, schema-violating, or rate-limited messages are captured in the immutable audit log. The audit tap is fire-and-forget with error isolation.

**Step 2 — JSON Deserialization.** The payload bytes are decoded and parsed as JSON. Any message that fails JSON parsing is logged as a warning and silently dropped. The error does not crash the subscriber loop.

**Step 3 — Topic Decomposition.** The MQTT topic is split on `/` to extract the terminal identifier (`parts[1]`), washroom identifier (`parts[2]`), and message type (`parts[3]`). These values are injected into the payload dictionary before schema validation, making them available as structured fields.

**Step 4 — Pydantic Schema Validation.** The enriched dictionary is validated against the `TelemetryPayload` Pydantic model, which enforces all field types, ranges, and regex patterns. Any validation failure is logged with the full Pydantic error details and the message is dropped.

**Step 5 — Per-Device Rate Limiting.** The validated payload's `device_id` is checked against the Redis-backed token bucket rate limiter. If the device has exceeded its configured message rate (default: 10 messages per 60-second window), the message is dropped. Rate limit violations are logged and are detectable by the Wazuh SIEM.

**Step 6 — Priority Queue Routing.** Messages that pass all prior checks are routed to one of two in-memory asyncio queues based on their urgency.

#### Dual-Queue Priority Routing

The `DualQueueRouter` (`app/services/queue.py`) maintains two bounded in-memory asyncio queues:

- **Priority Queue** (`maxsize=1000`): For messages with `msg_type == "alert"` or `raw_whi < WHI_CRITICAL_THRESHOLD (30.0)`. These represent active or imminent incidents.
- **Normal Queue** (`maxsize=10000`): For all other telemetry readings in the normal or warning range.

If a queue reaches its `maxsize` limit, the `put_nowait()` call raises `QueueFull` and the message is explicitly dropped with an error log. This is a deliberate back-pressure mechanism to prevent memory exhaustion under extreme load.

#### Worker Architecture

The application runs four concurrent asyncio workers, all executing the same `run_worker()` function from `app/workers/common.py`:

- **1 Priority Worker** — preferentially drains the priority queue.
- **3 Normal Workers** — serve both queues, but yield to priority items.

The `_get_next_payload()` function implements a soft priority mechanism. It first checks if the priority queue has an immediately-available item (non-blocking `get_nowait()`). If the priority queue is empty, it races a `get()` coroutine from each queue using `asyncio.wait(FIRST_COMPLETED)`. If the priority queue wins the race, any item simultaneously dequeued from the normal queue is returned to the normal queue. This ensures high-urgency messages are never starved by a backlog of routine telemetry, while also preventing the priority queue from monopolizing worker time when it contains traffic.

Each worker, upon dequeuing a payload, performs two operations in sequence: it passes the payload to the `IncidentEngine` for state machine evaluation, and then pushes the payload to the `TelemetryBatcher` for database persistence.

---

### 3.5 Redis Cache & State Hash Engine

Redis serves as the shared, real-time memory of the entire pipeline. It stores four categories of operational state:

#### Per-Washroom Incident State

```
state:washroom:{washroom_id}  →  STRING  (NORMAL | PENDING_ALERT | ACTIVE_INCIDENT | ACKNOWLEDGED | RESOLVED)
debounce:{washroom_id}        →  INTEGER (current debounce cycle count, 0-3)
```

State transitions are written atomically per washroom. The debounce key has a 1-hour TTL safety net applied after every increment, preventing orphaned counters from persisting indefinitely if a device goes offline mid-incident.

#### Per-Floor Escalation State

```
state:floor:{terminal}:{floor}:incidents  →  SET     (set of washroom_ids with ACTIVE_INCIDENT)
state:floor:{terminal}:{floor}:status     →  STRING  (NORMAL | FLOOR_CRITICAL)
```

The floor's active incident set is maintained as a Redis Set, which provides O(1) add, remove, and cardinality operations. When the cardinality reaches 2 or more, the floor status transitions to `FLOOR_CRITICAL`.

#### Telemetry Buffers (Per-Floor)

```
state:floor:{terminal}:{floor}:telemetry_buffer  →  LIST  (JSON-serialized TelemetryPayload items)
state:floor:{terminal}:{floor}:last_flush_time    →  STRING (Unix timestamp float)
state:active_telemetry_buffers                   →  SET   (set of active buffer keys)
```

The batcher tracks which floor buffers are currently active using a Redis Set (`state:active_telemetry_buffers`). Buffer cleanup after a flush uses an atomic Lua script that checks the buffer length and removes it from the active set in a single server-side operation, eliminating a TOCTOU race where an empty buffer could be re-added to the active set between a `LLEN` check and an `SREM` call.

#### Audit Buffer

```
state:audit_buffer          →  LIST  (JSON-serialized raw audit records)
state:audit_last_flush_time →  STRING (Unix timestamp float)
```

#### Rate Limit Buckets

```
rate_limit:{device_id}  →  HASH  { tokens: int, last_refill: int }
```

Rate limit state is stored as a Redis Hash with two fields: the current token count and the last refill timestamp. The entire check-and-decrement operation is implemented as a registered Lua script, making it atomic across distributed FastAPI instances.

#### Refresh Token State

```
state:refresh_token:{token}       →  STRING (username, TTL: 7 days)
state:refresh_token:{token}:used  →  STRING (username, TTL: 30 seconds, marks a consumed token)
state:user_tokens:{username}      →  SET    (set of active refresh token strings for user)
```

The refresh token rotation is protected by an atomic Lua script that performs a check-rotate-mark operation in a single Redis transaction, preventing replay attacks even under concurrent requests.

---

### 3.6 TimescaleDB Hypertable Persistence Engine

PostgreSQL 16 with the TimescaleDB extension is used for all historical persistence. Every table that receives time-series data is converted to a TimescaleDB `hypertable`, which automatically partitions data by time interval. This allows the system to maintain high insert throughput and fast range-query performance even as the dataset grows to millions of rows.

#### Schema and Tables

**`washroom_telemetry`** — Primary hypertable storing all sensor readings.
- Indexed on `(device_id, time DESC)` and `(terminal, washroom_id, time DESC)` for fast dashboard and device-specific queries.
- Receives data exclusively via the `TelemetryBatcher`'s bulk insert mechanism.

**`incident_events`** — Hypertable recording every state transition of every washroom.
- Columns: `time`, `washroom_id`, `terminal`, `old_state`, `new_state`, `whi`.
- Indexed on `(washroom_id, time DESC)`.
- Populated synchronously by the `IncidentEngine._set_state()` method on every state change.

**`floor_escalation_events`** — Hypertable recording every floor-level status change.
- Columns: `time`, `floor`, `terminal`, `old_status`, `new_status`, `active_incident_count`.
- Populated by the `EscalationEngine.evaluate_floor_state()` method.

**`raw_telemetry_audit`** — Hypertable storing the pre-parsed, pre-validated raw MQTT payload for every received message.
- Columns: `received_at`, `topic`, `raw_payload`.
- Subject to a 14-day automatic retention policy (`add_retention_policy(..., INTERVAL '14 days')`).
- Populated by the `AuditBatcher` asynchronously.

**`users`** — Standard relational table for authentication.
- Columns: `id`, `username`, `password_hash`, `role`, `zone`, `shift_start`, `shift_end`.

#### Database Immutability Triggers

A PostgreSQL trigger function `freeze_historical_logs()` raises an `EXCEPTION` with the message `Security Policy Violation: Historical incident audit vectors cannot be altered or removed.` This trigger is applied `BEFORE UPDATE OR DELETE` on three tables: `incident_events`, `floor_escalation_events`, and `raw_telemetry_audit`. This makes it structurally impossible to modify or delete historical records from these tables, even with superuser credentials. Any attempt is both blocked and logged, making it detectable by the Wazuh SIEM via rule ID 100004.

#### Bulk Insert with Transactional Safety

All database writes flow through `db_manager.executemany()`, which wraps each batch in an explicit `asyncpg` transaction. If any row in the batch fails, the entire transaction rolls back and the batcher attempts to restore the failed batch items to the Redis buffer, preventing data loss.

#### Least-Privilege Worker Role

The application's runtime database user is `aai_app_worker`, a restricted PostgreSQL role created by `db_init/02-create-worker-role.sh`. This role is granted only:

- `USAGE` on the `public` schema
- `SELECT, INSERT` on the five operational tables
- `USAGE, SELECT` on sequences

The role has no `UPDATE`, `DELETE`, `DROP`, `CREATE`, or `TRUNCATE` privileges. Even if the application itself were compromised, an attacker using the application's database credentials could not alter or delete historical records.

#### SSL Enforcement at the Database Layer

The PostgreSQL `pg_hba.conf` file restricts all TCP/IP connections to `hostssl` only, using `scram-sha-256` password authentication. This applies to all subnets, including the internal data network `172.20.3.0/24` and localhost. There is no `host` (unencrypted) entry in the configuration, ensuring all database connections are TLS-encrypted.

#### Telemetry Batcher — Dual-Trigger Flush Strategy

The `TelemetryBatcher` uses two flush triggers:
1. **Volume trigger:** When a floor's Redis buffer accumulates 100 items, it is flushed immediately via a `rename`-then-`lrange`-then-`executemany` atomic drain sequence.
2. **Time trigger:** A background monitor task polls every 500ms and flushes any buffer that has not been flushed in the last 5 seconds, regardless of its current size.

The rename-based drain (`RENAME buffer_key temp_key`) atomically hands off the buffer to the flush process. Incoming writes during the flush go to the original key and will be picked up in the next cycle, eliminating the possibility of items being lost between a length check and a range read.

---

## 4. Core Algorithms & Formulas

### 4.1 Washroom Health Index (WHI) Base Calculation

The Washroom Health Index (WHI) is a composite score computed on the Pico W edge device from multiple sensor inputs. The WHI is a value between **0.0** (critical/failed) and **100.0** (perfect condition). This inverted scale is intentional: a lower WHI means a worse washroom state, mirroring a health degradation concept.

The base formula synthesizes four dimensions of washroom quality:

```
WHI = 100
    - W_nh3    × normalize(avg_nh3_ppm,    0, MAX_NH3)    × 100
    - W_humid  × normalize(avg_humidity,   IDEAL_LOW, MAX_HUMID) × 100
    - W_occup  × normalize(occupancy,      0, MAX_OCCUP)  × 100
    - W_abandon× normalize(abandon_rate,   0, 100)        × 100
```

Where:

| Symbol | Meaning | Default Weight |
|--------|---------|----------------|
| `W_nh3` | Ammonia contribution weight | High (primary odour indicator) |
| `W_humid` | Humidity deviation weight | Medium (comfort & hygiene proxy) |
| `W_occup` | Occupancy load weight | Medium (maintenance demand) |
| `W_abandon` | Abandon rate weight | High (strongest user-experience signal) |

The `normalize(value, min, max)` function clamps the input to `[min, max]` and returns a ratio in `[0.0, 1.0]`. Weights sum to approximately 1.0 so that the WHI remains bounded in `[0.0, 100.0]`.

The WHI is pre-computed on the edge device and transmitted as the `raw_whi` field. The backend trusts this value as the primary decision signal, but the raw sensor readings (NH3 ppm, temperature, humidity, throughput, occupancy, abandon rate) are also transmitted and persisted independently for auditing and retrospective analysis.

### 4.2 Sustained Degradation Time-Penalty Algorithm

The WHI at any instant reflects current sensor conditions. However, the system also applies a time-based penalty to discourage brief improvements from resetting long-running degradation contexts. The time-penalty concept is embedded in the debouncer logic (see §4.4): a washroom that has been critically degraded for multiple consecutive cycles accumulates "confirmation weight" before transitioning to an active incident, preventing one-reading recoveries from masking chronic issues.

At the WHI threshold boundaries, the time penalty is reflected in how long a washroom must remain in a degraded state before the escalation state machine advances:

```
Time Penalty = (consecutive_critical_cycles - 1) × cycle_interval
```

A washroom must sustain a sub-critical WHI across at least `DEBOUNCE_THRESHOLD` (default: 3) consecutive readings before the incident is confirmed. Any single normal reading (WHI ≥ 50.0) resets the debounce counter entirely.

### 4.3 Multi-Unit Floor Escalation Math

The floor escalation rule is a simple threshold function over the count of simultaneously active incidents on a floor:

```
floor_status = FLOOR_CRITICAL  if  |active_incidents_on_floor| ≥ 2
floor_status = NORMAL          if  |active_incidents_on_floor| < 2
```

The active incident count is maintained as the cardinality of a Redis Set. Each washroom that transitions to `ACTIVE_INCIDENT` is added to the set; each washroom that transitions out of `ACTIVE_INCIDENT` (to `RESOLVED` or `NORMAL`) is removed. Redis Set operations (`SADD`, `SREM`, `SCARD`) are O(1) and atomic, making the escalation decision deterministic and race-condition-free.

Floor identification is parsed from the `washroom_id` string by splitting on `_` or `-` and taking the first segment (e.g., `L2_WashroomA` → floor `L2`, `L3-WashroomC` → floor `L3`).

A transition to `FLOOR_CRITICAL` is immediately persisted to the `floor_escalation_events` hypertable, creating an auditable timeline of when each floor reached a crisis state and how many units were simultaneously affected.

### 4.4 Incident Verification Engine — The 3-Cycle Debouncer

The `IncidentEngine.process_reading()` method (`app/services/incident.py`) is the central state machine for individual washroom units. It classifies each reading's WHI into one of three threshold bands and applies the corresponding state transition logic.

#### Threshold Bands

```
WHI ≥ 50.0   →  NORMAL zone      (good condition)
30.0 ≤ WHI < 50.0  →  WARNING zone  (pending alert, watchful)
WHI < 30.0   →  CRITICAL zone    (potential active incident)
```

These thresholds are configurable via environment variables (`WHI_WARNING_THRESHOLD`, `WHI_CRITICAL_THRESHOLD`).

#### State Machine Logic

**From NORMAL zone (WHI ≥ 50.0):**
- If the current state is `ACTIVE_INCIDENT`, emit `RESOLVED` then immediately transition to `NORMAL`. This handles the case where a washroom recovers while already in incident state.
- Otherwise, set state to `NORMAL` and clear the debounce counter.

**From WARNING zone (30.0 ≤ WHI < 50.0):**
- Set state to `PENDING_ALERT`.
- Clear the debounce counter (warning readings are concerning but do not advance toward confirmed incidents without further degradation).

**From CRITICAL zone (WHI < 30.0):**
- If the current state is not already `ACTIVE_INCIDENT`:
  - Atomically increment the Redis debounce counter for this washroom.
  - Apply a 1-hour TTL to the debounce key as a safety net against abandoned counters.
  - If the counter has now reached `DEBOUNCE_THRESHOLD` (default: 3): transition to `ACTIVE_INCIDENT` and clear the counter.
- If the current state is already `ACTIVE_INCIDENT`: take no action (state is already confirmed).

#### Why 3 Cycles?

The 3-cycle requirement eliminates false positives from transient spikes. A single anomalous sensor reading (caused by cleaning chemicals, calibration drift, or a door being opened near an external source) will not trigger an incident. Three consecutive critical readings across the device's reporting interval represent a sustained environmental condition that warrants human intervention.

#### State Persistence

Every state change (transition from one state to another) is persisted to the `incident_events` hypertable with the old state, new state, current WHI, and timestamp. Non-transitions (same state to same state) are not written to the database, keeping the audit log signal-dense rather than noise-dense.

---

## 5. Security Architecture

The security model is organized into three sequential phases that build upon each other. Each phase adds controls that are only meaningful after the previous phase's foundations are in place.

---

### 5.1 Phase 1 — Baseline Hardening

Phase 1 covers the fundamental security posture before any application logic is considered. It establishes the trust anchor and ensures no component can be accessed in plaintext.

#### Private PKI Chain

All TLS certificates in the deployment are signed by a single Private CA (`certs/ca/ca.crt`, `certs/ca/ca.key`) generated by `setup_security.sh` using a 4096-bit RSA key with a 10-year validity period. Every other certificate in the system — EMQX server cert, HAProxy cert, PostgreSQL cert, FastAPI backend client cert, and individual device certs — is issued by this CA.

This single-CA trust model means that every TLS connection in the system can be mutually verified against the same root of trust. The CA certificate is distributed to every component that needs to verify peer certificates.

Certificate validity is managed through a rotation matrix documented in `scripts/ROTATION_POLICY.md`:

| Asset | Recommended Lifetime | Warning Threshold |
|-------|---------------------|-------------------|
| CA Root Certificate | 10 years | 365 days |
| EMQX Server TLS Cert | 1 year | 30 days |
| Per-Device mTLS Certs | 1 year | 30 days |
| JWT Signing Key | 90 days | 15 days |
| HAProxy TLS Cert | 1 year | 30 days |

#### Mutual TLS (mTLS) for Device Connections

Every MQTT connection from a Pico W device is subject to mutual TLS verification. The EMQX configuration enforces:

```
verify = verify_peer
fail_if_no_peer_cert = true
versions = [tlsv1.3, tlsv1.2]
```

A device that does not present a valid client certificate signed by the deployment CA cannot establish a TCP connection to port 8883 — the connection is dropped at the TLS handshake stage, before any MQTT data is exchanged. Legacy TLS versions (1.0, 1.1) and plaintext connections are structurally impossible.

#### Container Hardening

The FastAPI container is hardened at the Docker Compose level:

- `user: "10001:10001"` — runs as a non-root user/group (`appuser`), created at Dockerfile build time.
- `read_only: true` — the container filesystem is mounted read-only. No code can write to the container's own image layers.
- `tmpfs: [/tmp]` — a temporary in-memory filesystem is mounted at `/tmp` for the application's transient needs, while keeping the root filesystem read-only.
- Port 8000 is not bound to the host network.

#### Docker Secrets Management

All sensitive credentials are managed as Docker Secrets, mounted at `/run/secrets/<name>` inside containers. The `get_secret()` utility function in `app/core/config.py` reads secrets from mounted secret files, falling back to environment variables only when no secret file exists. Secrets are never baked into container images or committed to the repository. The `secrets/*.txt` files are in `.gitignore`.

Generated secrets include: `postgres_password`, `redis_password`, `aai_app_worker_password`, `jwt_secret_key`, `emqx_dashboard_password`, `operator_password`, `supervisor_password`. All passwords are generated using `openssl rand` for cryptographic randomness (hex or base64 formatted, 128-256 bits of entropy).

---

### 5.2 Phase 2 — Core Access Control & Integrity

Phase 2 adds the application-layer identity, authorization, and data integrity systems.

#### Argon2id Password Hashing

User passwords are hashed using the Argon2id algorithm via the `argon2-cffi` library. The hasher is configured with parameters aligned with RFC recommendations:

```python
PasswordHasher(
    time_cost=3,        # 3 iterations
    memory_cost=65536,  # 64 MB of memory
    parallelism=4,      # 4 parallel threads
    hash_len=32,        # 32-byte output
    salt_len=16         # 16-byte random salt
)
```

These parameters make offline brute-force attacks computationally expensive. Each hash includes a unique random salt, preventing rainbow table attacks.

#### JWT Access Tokens (Short-Lived)

Upon successful login, the API issues a JWT access token signed with HS256 using a 256-bit secret key. Access tokens have a 15-minute expiration window. The short lifespan limits the blast radius of a token compromise — a stolen access token becomes invalid in at most 15 minutes.

The JWT payload carries `sub` (username), `role`, and `exp` fields. The `role` claim is used by the `RoleChecker` FastAPI dependency for RBAC enforcement.

#### Refresh Token Rotation with Reuse Revocation

The system implements a stateful refresh token mechanism with automatic rotation and replay attack detection:

1. On login, a cryptographically random 64-character hex refresh token is issued and stored in Redis with a 7-day TTL.
2. When the access token expires, the client calls `/auth/refresh` with the refresh token.
3. An atomic Lua script checks if the token is valid (exists in Redis), marks it as "used" with a 30-second TTL, and signals success.
4. A new refresh token and access token pair are issued.

If a refresh token that has already been consumed (exists in the `used` set) is presented again, the Lua script detects the reuse, atomically revokes **all** active refresh tokens for that user (by iterating the `state:user_tokens:{username}` set and deleting each token), and returns a `401 Unauthorized` with a token reuse message. This means a stolen refresh token that is used by an attacker after the legitimate client has already rotated it will trigger a full session revocation for the targeted user.

#### JWT Key Rotation with Overlap Window

The `decode_access_token()` function supports a two-key validation window. It first attempts validation with the current `jwt_secret_key`. If that fails with an `InvalidSignatureError`, it attempts validation with `jwt_secret_key_previous`. This overlap means that during a key rotation event, users with access tokens signed by the previous key can continue using them until the token's 15-minute expiry, after which they will seamlessly obtain a new token signed by the new key. Zero user disruption.

#### Role-Based Access Control (RBAC)

Three user roles are defined:

| Role | Permissions |
|------|-------------|
| `dashboard_operator` | Read-only dashboard access via `GET /dashboard/status`. |
| `supervisor` | Read access plus write access to incident acknowledgement (`/incidents/{id}/acknowledge`), resolution (`/incidents/{id}/resolve`), and alert dispatch (`/alerts/dispatch`). All write operations also require ABAC checks. |
| `admin` | Exclusive access to user attribute management (`PUT /admin/users/{username}/attributes`). |

Role enforcement is implemented as a `RoleChecker` FastAPI dependency class that reads the role from the validated JWT payload and returns 403 if the role is not in the allowed list.

#### Attribute-Based Access Control (ABAC)

Beyond role-based checks, the system layers two additional attribute-based constraints on all supervisor write operations:

**Zone Constraint (`verify_zone_access`):** A supervisor is assigned to a specific terminal zone (e.g., `T1` or `T2`) stored in the `users` table. When they attempt to acknowledge, resolve, or dispatch an alert for a specific `washroom_id`, the system looks up the washroom's terminal in the `WASHROOM_TERMINAL_MAP` configuration. If the washroom's terminal does not match the supervisor's assigned zone, the request is rejected with 403. Supervisors with `zone = NULL` (global supervisors) bypass this check and can access all terminals. If a `washroom_id` is not found in the terminal map at all, the response is 404 (fail-closed — unknown washroom is not accessible).

**Shift Constraint (`verify_active_shift`):** Each user has `shift_start` and `shift_end` time values stored in the database. Every write operation checks whether the current UTC time falls within the user's shift window. The check correctly handles overnight shifts (e.g., 22:00:00 to 06:00:00) by detecting when `start > end` and applying wrap-around logic. A supervisor cannot acknowledge incidents outside their active shift, even if they have the correct role and zone. An `X-Mock-Time` header is accepted for shift override in `testing` environments only, and is explicitly ignored in production.

#### Database Immutability Triggers

As described in §3.6, SQL `BEFORE UPDATE OR DELETE` triggers on `incident_events`, `floor_escalation_events`, and `raw_telemetry_audit` make it impossible to alter or delete historical records at the database level. The trigger raises an exception that cascades up through PostgreSQL's logging, making the attempt visible to Wazuh.

---

### 5.3 Phase 3 — Adaptive Perimeter Defense

Phase 3 adds the runtime monitoring, intrusion detection, and active response capabilities.

#### Per-Device Rate Limiting (Atomic Lua Token Bucket)

Each device is tracked by its own Redis key using a sliding-window token bucket. The bucket state is stored as a Redis Hash with `tokens` and `last_refill` fields. The entire check-and-consume operation is implemented as a registered Redis Lua script, which executes atomically on the Redis server:

```lua
-- 1. Load bucket state
-- 2. Initialize full bucket if it doesn't exist
-- 3. Reset to full if the time window has elapsed
-- 4. If tokens > 0: consume one token and return 1 (ALLOWED)
-- 5. Else: return 0 (RATE LIMITED)
```

Because the Lua script is atomic, concurrent FastAPI instances cannot race to over-consume from the same device's bucket. The default rate limit is 10 messages per 60-second window. Rate limit errors are logged in a format that Wazuh rule 100003 can detect.

If the Lua script execution itself throws an error (e.g., Redis unavailable), the `is_allowed()` function **fails closed** — it returns `False`, blocking the message. This is the inverse of a fail-open design, where an unavailable rate limiter would allow unlimited traffic through.

#### Certificate Expiry Monitoring

The `scripts/monitor_certs.py` script walks the `certs/` directory tree, parses every `.crt` and `.pem` file using the Python `cryptography` library, and checks the remaining validity period against a 30-day threshold. For any certificate within the warning window, a structured JSON warning event is written to `/var/log/aai-wms/certs/monitor.log`:

```json
{
  "timestamp": "...",
  "service": "cert_monitor",
  "level": "WARNING",
  "event": "Certificate ... is expiring soon (expires in N days on ...)",
  "cert_path": "...",
  "days_remaining": N,
  "expiry_date": "..."
}
```

The script is scheduled as a daily cron job. Wazuh ingests the monitor log and fires rule 100006 (level 8) when an `expiring soon` event is detected.

#### Wazuh SIEM Integration and Active Response

Wazuh (`wazuh/ossec.conf`) is configured to monitor four log sources: the FastAPI application log, the PostgreSQL log, the EMQX log, and the certificate monitor log. Six custom detection rules are defined in `wazuh/rules/local_rules.xml`:

| Rule ID | Level | Trigger | Description |
|---------|-------|---------|-------------|
| 100001 | 3 | `failed mTLS handshake` / `Client auth failed` | Single MQTT authentication failure |
| 100002 | 12 | Rule 100001 fires 5+ times in 60s from same IP | Repeated mTLS failures — potential brute force or rogue device |
| 100003 | 5 | `exceeded rate limit` | Device rate limit threshold hit |
| 100004 | 10 | `Security Policy Violation: Historical incident audit...` | Immutability trigger fired — tamper attempt |
| 100005 | 11 | Syscheck alert on critical config/cert paths | File integrity violation on ACL, HAProxy config, DB init, or certs |
| 100006 | 8 | `expiring soon` in cert monitor log | Certificate approaching expiry |

Rule 100002 (the high-frequency mTLS failure rule) triggers a Wazuh **Active Response**: the `firewall-drop` command is executed locally, blocking the offending source IP for 600 seconds (10 minutes). This automatically isolates a brute-forcing or rogue device without requiring human intervention.

#### File Integrity Monitoring (FIM)

Wazuh's syscheck module monitors the following critical files and directories in real-time (`realtime="yes"`):

- `/opt/aai-wms/certs/` — all TLS certificates and keys
- `/opt/aai-wms/emqx/acl.json` — broker authorization rules
- `/opt/aai-wms/haproxy/haproxy.cfg` — gateway routing configuration
- `/opt/aai-wms/db_init/01-init.sql` — database schema and security triggers

Any modification to these files at the filesystem level (outside of the normal deployment pipeline) fires rule 100005 at level 11, immediately alerting security operations.

#### CI/CD Security Gate

The `.github/workflows/security.yml` GitHub Actions workflow runs three parallel security checks on every push or pull request to `main`/`master`:

1. **Gitleaks (secrets detection):** Full history scan for accidentally committed secrets, API keys, passwords, or private key material.
2. **pip-audit (dependency vulnerability scan):** Checks all Python dependencies against the PyPI Advisory Database for known CVEs.
3. **Bandit (static code security analysis):** Scans the `app/` directory for common Python security anti-patterns (hardcoded credentials, unsafe `eval()`, SQL injection patterns, insecure TLS usage, etc.).
4. **Trivy + Grype (container vulnerability scan):** Builds the Docker image locally and scans it for OS-level and package-level vulnerabilities at `CRITICAL` and `HIGH` severity. The pipeline fails (`exit-code: '1'`) if any unfixed HIGH/CRITICAL vulnerability is found.

---

### 5.4 Security Dependency Chain

The security controls form an interlocking chain where each layer depends on the previous ones being intact:

```
PKI Chain (CA → Per-Service & Per-Device Certificates)
    ↓
mTLS Enforcement at EMQX (no plaintext MQTT connections possible)
    ↓
Certificate-CN-to-Username Mapping (device identity = PKI identity)
    ↓
EMQX ACL File Authorization (device can only publish to its own topics)
    ↓
HAProxy TLS Termination (API and Dashboard behind SSL)
    ↓
FastAPI Pydantic Schema Validation (all inputs structurally validated)
    ↓
Per-Device Rate Limiting (atomic token bucket in Redis)
    ↓
Argon2id Password Hashing → JWT Authentication → RBAC Role Enforcement
    ↓
ABAC Zone + Shift Enforcement (layered on top of RBAC)
    ↓
Least-Privilege DB Role (aai_app_worker cannot alter history)
    ↓
Database Immutability Triggers (history unalterable even at DB layer)
    ↓
Wazuh FIM + Log Monitoring + Active Response (runtime anomaly detection)
    ↓
CI/CD Security Gate (pre-merge vulnerability scanning)
```

Breaking any single link in this chain requires either physical access to the CA private key or simultaneous compromise of multiple independent layers.

---

## 6. Configuration Reference

All application settings are managed through Pydantic's `BaseSettings` (`app/core/config.py`), which reads from environment variables, the `.env` file, and Docker Secrets.

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_ENV` | `production` | Set to `testing` to enable `X-Mock-Time` header for shift testing |
| `MQTT_HOST` | `localhost` | MQTT broker hostname or VIP |
| `MQTT_PORT` | `8883` | MQTT broker port |
| `MQTT_USE_TLS` | `False` | Enable TLS for MQTT connection |
| `MQTT_CA_CERT_PATH` | `None` | Path to CA certificate for MQTT TLS |
| `MQTT_CLIENT_CERT_PATH` | `None` | Path to client certificate for mTLS |
| `MQTT_CLIENT_KEY_PATH` | `None` | Path to client key for mTLS |
| `REDIS_URL` | `redis://127.0.0.1:6379/0` | Redis connection URL (password injected at runtime) |
| `POSTGRES_URL` | `postgresql+asyncpg://...` | Worker role DB URL (password injected at runtime) |
| `POSTGRES_SUPERUSER_URL` | `postgresql+asyncpg://...` | Superuser DB URL (for seeding only) |
| `RATE_LIMIT_MESSAGES` | `10` | Max messages per device per window |
| `RATE_LIMIT_WINDOW_SECONDS` | `60` | Rate limit window duration in seconds |
| `DEBOUNCE_THRESHOLD` | `3` | Number of consecutive critical readings to confirm an incident |
| `WHI_CRITICAL_THRESHOLD` | `30.0` | WHI below this value is treated as critical |
| `WHI_WARNING_THRESHOLD` | `50.0` | WHI below this value is treated as a warning |
| `WASHROOM_TERMINAL_MAP` | See config | Maps washroom IDs to terminal zones for ABAC |

### Docker Secrets

| Secret Name | Purpose |
|-------------|---------|
| `postgres_password` | PostgreSQL superuser password |
| `aai_app_worker_password` | Password for the restricted `aai_app_worker` DB role |
| `redis_password` | Redis AUTH password |
| `jwt_secret_key` | 256-bit HS256 signing key for JWT access tokens |
| `jwt_secret_key_previous` | Previous JWT signing key (rotation overlap; optional) |
| `operator_password` | Seeded plaintext password for the `operator` user |
| `supervisor_password` | Seeded plaintext password for supervisor users |
| `emqx_dashboard_password` | EMQX web dashboard admin password |

---

## 7. Maintenance & Chaos Testing

### Certificate Rotation Procedure

1. Run `setup_security.sh` to regenerate certificates (will not overwrite existing CA unless CA files are manually deleted).
2. Replace the relevant certificate files in `certs/`.
3. Restart the affected containers: `docker compose restart emqx1 emqx2 emqx3 haproxy1 haproxy2`.
4. For device certificates (`certs/devices/`), distribute the new `.crt` and `.key` files to the physical device and reload.

### JWT Key Rotation

1. Copy `secrets/jwt_secret_key.txt` to `secrets/jwt_secret_key_previous.txt`.
2. Generate a new key: `openssl rand -hex 32 > secrets/jwt_secret_key.txt`.
3. Restart the FastAPI container: `docker compose restart fastapi`.
4. Existing access tokens signed with the old key remain valid for up to 15 minutes; existing refresh tokens are unaffected.
5. After all active access tokens have expired (15 minutes), remove `secrets/jwt_secret_key_previous.txt`.

### Performance Benchmarks

The `tests/benchmark_latency.py` script measures the throughput and latency of the audit buffer push path against a live Redis instance:

```
Benchmark: 10,000 raw message pushes to Redis audit buffer
- Measures: total time, throughput (msg/sec), average, P95, P99, max latency
```

The `tests/benchmark_process_message.py` script benchmarks the full message processing pipeline.

### Chaos and Integration Testing

Several scripts support manual testing and chaos simulation:

**`tests/publish_dummy_mqtt.py`** — Publishes a scripted sequence of MQTT messages using a real device certificate to a live broker. It sends 3 critical readings for `L2_WashroomA`, 3 critical readings for `L2_WashroomB` (which should trigger `FLOOR_CRITICAL`), and then a recovery reading for `L2_WashroomA`.

**`tests/simulate_escalation.py`** — Runs the full escalation scenario in memory using a `MockRedis` implementation. Useful for testing the IncidentEngine and EscalationEngine logic without any infrastructure dependencies.

**`tests/test_abac.py`** — Comprehensive integration test suite for the ABAC system, covering zone filtering on the dashboard, cross-zone incident blocking, overnight shift boundary conditions, production vs. testing environment shift mock gating, JWT key rotation acceptance/rejection, admin-only attribute modification, and shift-constrained alert dispatch.

**`tests/test_escalation.py`** — Unit tests for the `EscalationEngine` and `IncidentEngine` using `MockRedis`, verifying the floor escalation trigger threshold and single-incident no-escalation behavior.

**`tests/test_immutable_triggers.py`** — Tests that verify the PostgreSQL immutability triggers correctly block `UPDATE` and `DELETE` operations on historical event tables.

**`scripts/simulate_wazuh_triggers.py`** and **`scripts/verify_wazuh_rules.py`** — Utility scripts for testing the Wazuh detection rules by simulating log events that should trigger each rule.

### Log Monitoring

All components emit structured JSON logs routed to their respective Docker `json-file` log drivers, capped at 10 MB per file with a 3-file rotation. The FastAPI application uses a custom `JSONFormatter` that emits UTC ISO-8601 timestamps, service name, level, event message, and optionally `device_id` (extracted from the log message by regex if not explicitly provided).

Log paths for Wazuh ingestion (host-mounted):

| Service | Log Path |
|---------|----------|
| FastAPI | `/var/log/aai-wms/fastapi/fastapi.log` |
| TimescaleDB | `/var/log/aai-wms/postgres/postgresql.log` |
| EMQX | `/var/log/aai-wms/emqx/emqx.log` |
| Certificate Monitor | `/var/log/aai-wms/certs/monitor.log` |

---
