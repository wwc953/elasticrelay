# ElasticRelay - Multi-Source CDC Gateway to Elasticsearch

![ElasticRelay Screenshot](releases/download/asset/screenshot_02.png)

<p align="center">
  <a href="https://github.com/yogoosoft/ElasticRelay/releases"><img src="https://img.shields.io/badge/version-v1.4.4-blue.svg" alt="Version"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/go-1.25.2+-00ADD8.svg" alt="Go Version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-green.svg" alt="License"></a>
</p>
<p align="center">
  <a href="releases/download/asset/README.de.md">Deutsch</a> |
  <a href="releases/download/asset/README.fr.md">Français</a> |
  <a href="releases/download/asset/README.ja.md">日本語</a> |
  <a href="releases/download/asset/README.ru.md">Русский</a> |
  <a href="releases/download/asset/README.zh-CN.md">中文</a>
</p>

## Vision

ElasticRelay is a seamless, heterogeneous data synchronizer designed to provide real-time Change Data Capture (CDC) from major OLTP databases (MySQL, PostgreSQL, MongoDB) to Elasticsearch. It aims to be more user-friendly and reliable than existing solutions like Logstash or Flink.

## 🎉 v1.4.4 Highlights - Production-Ready CDC Platform with Transform Engine

**Three major database sources + enterprise data transformation:**

| Source | Status | Features |
|--------|--------|----------|
| **MySQL** | ✅ Complete | Binlog CDC + Initial Sync + Parallel Snapshots |
| **PostgreSQL** | ✅ Production-Hardened | Logical Replication + WAL Parsing + Stable Snapshot-to-CDC Handoff |
| **MongoDB** | ✅ Complete | Change Streams + Sharded Clusters + Resume Tokens |
| **Transform Engine** | ✅ Complete | Field Mapping + Data Masking + Type Conversion + Expression Engine |

## Key Features

- **Multi-Source CDC**: Full support for MySQL, PostgreSQL, and MongoDB with real-time change capture
- **Transform Engine**: Enterprise-grade data transformation with field mapping, data masking (phone, ID card, email, bank card), type conversion, expression evaluation, and conditional filtering — processing at 800,000+ ops/sec
- **Zero-Code Configuration**: JSON-based configuration with wizard-style GUI (in development)
- **Multi-Table Dynamic Indexing**: Automatically creates separate Elasticsearch indices for each source table with configurable naming patterns (e.g., `elasticrelay-users`, `elasticrelay-orders`)
- **Built-in Governance**: Handles data structuring, anonymization, type conversion, normalization, and enrichment
- **Reliability by Default**: Utilizes transaction log-level CDC, precise checkpointing for resuming, and idempotent writes to ensure data integrity
- **Dead Letter Queue (DLQ)**: Comprehensive failure handling with exponential backoff retry and persistent storage
- **Parallel Processing**: Advanced parallel snapshot processing with chunking strategies for large tables
- **Centralized Logging**: Runtime-configurable log levels (debug/info/warn/error) with thread-safe global control

## Technology Stack

- **Data Plane (Go)**: The core data synchronization logic is built in Go (1.25.2+) for high concurrency, low memory footprint, and simple deployment.
- **Control Plane & GUI (TypeScript/Next.js)**: A rich, interactive UI for configuration and monitoring (in development).
- **APIs (gRPC)**: Internal communication between components is handled via gRPC for high performance with complete service implementations.
- **Database Support**: 
  - **MySQL CDC**: Advanced binlog parsing with real-time synchronization (go-mysql library)
  - **PostgreSQL CDC**: Logical replication with WAL parsing, replication slots, publications, and production-hardened snapshot-to-CDC handoff
  - **MongoDB CDC**: Change Streams with replica set and sharded cluster support (mongo-driver)
- **Transform Engine**: Complete data transformation pipeline with field mapping, type conversion, data masking (4 strategies, 5 preset templates), expression engine (16 built-in functions), and conditional filtering (10 operators)
- **Elasticsearch Integration**: Official Elasticsearch Go client (v8) with bulk indexing support
- **Configuration**: JSON-based configuration with automatic format detection and migration
- **Reliability**: Comprehensive error handling, DLQ system, and checkpoint management
- **Logging**: Centralized log level control system with runtime configuration

## Architecture

The system is composed of several key components:

- **Source Connectors**: Capture changes from MySQL (binlog), PostgreSQL (logical replication), and MongoDB (change streams).
- **Durable Buffer**: Asynchronous CDC event queue decoupling source reads from downstream processing.
- **Transform Engine**: Enterprise-grade data transformation pipeline with field mapping, type conversion, data masking, expression evaluation, and conditional filtering.
- **ES Sink Writer**: Writes data to Elasticsearch in efficient batches with automatic index management.
- **Orchestrator**: Manages the lifecycle of synchronization tasks, supporting both legacy single-source and multi-source configurations.
- **Dead Letter Queue**: Handles failed events with exponential backoff retry and persistent storage.
- **Checkpoint Manager**: Persistent position tracking (binlog positions, PostgreSQL LSN, MongoDB resume tokens) for fault-tolerant resumption.
- **Control Plane**: The UI and configuration management backend (in development).

## Quick Start

To quickly get ElasticRelay up and running, follow these three simple steps:

### Step 1: Build
```sh
./scripts/build.sh
```

### Step 2: Configure

#### MongoDB Setup (Required for MongoDB CDC)
MongoDB requires replica set mode for Change Streams. Run the setup script:
```sh
./scripts/reset-mongodb.sh
```

Or manually:
```sh
docker-compose down
rm -rf ./data/mongodb/*
docker-compose up -d mongodb
docker-compose up mongodb-init
```

Verify MongoDB is ready:
```sh
./scripts/verify-mongodb.sh
```

📚 **See**: `QUICKSTART.md` for detailed MongoDB setup instructions.

#### PostgreSQL Setup
For PostgreSQL, ensure logical replication is enabled:
```sql
-- Enable logical replication in postgresql.conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10

-- Create user with replication privileges
CREATE USER elasticrelay_user WITH LOGIN PASSWORD 'password' REPLICATION;
GRANT CONNECT ON DATABASE your_database TO elasticrelay_user;
GRANT USAGE ON SCHEMA public TO elasticrelay_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO elasticrelay_user;
```

#### Configuration Files
Edit the configuration file `./config/parallel_config.json` and ensure the database and Elasticsearch connection information is correct.

### Step 3: Execute
```sh
./start.sh
```

After completing these steps, ElasticRelay will start monitoring database changes and synchronizing them to Elasticsearch.

---

## How to Run

### Prerequisites

- Go (1.25.2+)
- Protobuf Compiler (`protoc`)
- Elasticsearch (7.x or 8.x)
- **MySQL** (5.7+ or 8.x) with binlog enabled
- **PostgreSQL** (10+ recommended, 9.4+ minimum) with logical replication enabled
- **MongoDB** (4.0+) with replica set or sharded cluster configuration

### Installation

1.  **Install Go dependencies and tools**:
    ```sh
    go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
    ```

2.  **Install `protoc`**:
    On macOS with Homebrew:
    ```sh
    brew install protobuf
    ```

3.  **Tidy dependencies**:
    ```sh
    go mod tidy
    ```

### Building and Running the Server

#### Quick Build (Development)
```sh
# Simple build without version info
go build -o elasticrelay ./cmd/elasticrelay

# Run the server
./elasticrelay -config multi_config.json
```

#### Production Build (Recommended)
```sh
# Build with version information using Makefile
make build

# Run the versioned binary
./bin/elasticrelay -config multi_config.json
```

#### Version Management
ElasticRelay has comprehensive version management with build-time injection:

```sh
# View current version info with detailed build information
./bin/elasticrelay -version

# Check version info from Makefile
make version

# Development build (fast, no version injection)
make dev

# Production build (optimized with version info)
make release

# Cross-platform builds for multiple architectures
make build-all

# Build with custom version
VERSION="v1.3.0" make build

# Build all tools including migration utilities
make build-tools
```

The version system includes:
- **Git Integration**: Automatic version detection from git tags
- **Build Metadata**: Commit hash, build time, Go version, and platform information
- **Colorized Output**: Rich console output with version details and ASCII art logo
- **Cross-Platform**: Support for Linux, macOS (Intel/ARM), and Windows

The server will start and listen on port `50051` by default.

**Alternative**: You can also run directly without building:
```sh
go run ./cmd/elasticrelay -config multi_config.json
```

### Multi-Table Configuration

ElasticRelay supports both legacy single-config and modern multi-config formats with automatic detection and migration.

#### Modern Multi-Config Format (`multi_config.json`):

```json
{
  "version": "3.0",
  "data_sources": [
    {
      "id": "mysql-main",
      "type": "mysql",
      "host": "localhost",
      "port": 3306,
      "user": "elastic_user",
      "password": "password",
      "database": "elasticrelay",
      "server_id": 100,
      "table_filters": ["users", "orders", "products"]
    },
    {
      "id": "postgresql-main",
      "type": "postgresql",
      "host": "localhost",
      "port": 5432,
      "user": "elastic_user",
      "password": "password",
      "database": "elasticrelay",
      "table_filters": ["users", "orders", "products"],
      "options": {
        "ssl_mode": "disable",
        "slot_name": "elasticrelay_slot",
        "publication_name": "elasticrelay_publication",
        "batch_size": 1000,
        "max_connections": 10,
        "parallel_snapshots": true
      }
    },
    {
      "id": "mongodb-main",
      "type": "mongodb",
      "host": "localhost",
      "port": 27017,
      "user": "elasticrelay_user",
      "password": "password",
      "database": "elasticrelay",
      "table_filters": ["users", "orders", "products"],
      "options": {
        "auth_source": "admin",
        "replica_set": "rs0"
      }
    }
  ],
  "sinks": [
    {
      "id": "es-main",
      "type": "elasticsearch",
      "addresses": ["http://localhost:9200"],
      "options": {
        "index_prefix": "elasticrelay"
      }
    }
  ],
  "jobs": [],
  "global": {
    "log_level": "info",
    "grpc_port": 50051,
    "dlq_config": {
      "enabled": true,
      "storage_path": "dlq",
      "max_retries": 3,
      "retry_delay": "30s"
    }
  }
}
```

#### Legacy Config Format (`config.json`):

```json
{
  "db_host": "localhost",
  "db_port": 3306,
  "db_user": "elastic_user",
  "db_password": "password",
  "db_name": "elasticrelay",
  "server_id": 100,
  "table_filters": ["users", "orders", "products"],
  "es_addresses": ["http://localhost:9200"]
}
```

The system automatically detects configuration format and supports migration between formats. This creates separate indices:
- `elasticrelay-users` for the `users` table
- `elasticrelay-orders` for the `orders` table  
- `elasticrelay-products` for the `products` table

### Dead Letter Queue (DLQ) Support

ElasticRelay includes a comprehensive DLQ system for handling failed events:

- **Automatic Retry**: Failed events are automatically retried with exponential backoff
- **Persistent Storage**: DLQ items are persisted to disk with full state management
- **Deduplication**: Prevents duplicate events from being added to the queue
- **Status Tracking**: Complete lifecycle tracking (pending, retrying, exhausted, resolved, discarded)
- **Manual Management**: Support for manual item inspection and management
- **Automatic Cleanup**: Resolved items are automatically cleaned up after configurable duration

### PostgreSQL Support

ElasticRelay provides comprehensive PostgreSQL CDC capabilities with advanced features:

#### Core PostgreSQL Features
- **Logical Replication**: Uses PostgreSQL's native logical replication with `pgoutput` plugin
- **WAL Parsing**: Advanced Write-Ahead Log parsing for real-time change capture
- **Replication Slots**: Automatic creation and management of logical replication slots
- **Publications**: Dynamic publication management for table filtering
- **LSN Management**: Precise Log Sequence Number tracking for checkpoint/resume functionality

#### Advanced PostgreSQL Capabilities
- **Connection Pooling**: Intelligent connection pool management with configurable limits
- **Parallel Snapshots**: Multi-threaded initial data synchronization with chunking strategies
- **Type Mapping**: Comprehensive PostgreSQL to Elasticsearch type conversion including:
  - All numeric types (bigint, integer, real, double, numeric)
  - Text and character types (text, varchar, char)
  - Date/time types with timezone support (timestamp, timestamptz, date, time)
  - JSON/JSONB with native object mapping
  - Array types (integer arrays, text arrays)
  - Advanced types (UUID, bytea, inet, geometric types)
- **Performance Optimizations**: 
  - Adaptive scheduling for large tables
  - Streaming mode for memory efficiency
  - Configurable batch sizes and worker pools
  - Connection lifecycle management

#### PostgreSQL Configuration Options
```json
{
  "type": "postgresql",
  "options": {
    "ssl_mode": "disable|require|verify-ca|verify-full",
    "slot_name": "custom_replication_slot_name",
    "publication_name": "custom_publication_name",
    "batch_size": 1000,
    "max_connections": 10,
    "min_connections": 2,
    "parallel_snapshots": true,
    "enable_performance_monitoring": true
  }
}
```

#### PostgreSQL Troubleshooting Checklist

If PostgreSQL CDC does not fully catch up, use the following checklist before investigating Elasticsearch or transform rules.

**Common symptoms:**

- Elasticsearch count stops far below the inserted row count after a large PostgreSQL write
- Logs show errors such as `unsupported logical replication message` or `unknown copy data message type`
- Repeated document overwrites appear because CDC events use duplicate `_id` values
- `postgresql_checkpoints.json` advances, but Elasticsearch document count stalls early

**Recommended reset procedure for a clean re-run:**

1. Stop ElasticRelay.
2. Remove the old checkpoint file if you want a full PostgreSQL re-sync.
3. Delete the target Elasticsearch index or index prefix used for the test.
4. If you are rebuilding the PostgreSQL table from scratch, also verify that the old replication slot is not left behind in an inactive state.

```sql
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name LIKE 'elasticrelay_slot%';
```

Drop an inactive slot only when you intentionally want to restart from a clean state:

```sql
SELECT pg_drop_replication_slot('elasticrelay_slot_postgresql_to_es_cdc');
```

**What a healthy PostgreSQL validation run looks like:**

- Insert 10,000 rows into the PostgreSQL test table and Elasticsearch count also reaches `10000`
- No duplicate primary key warnings appear in logs
- No PostgreSQL replication parse errors appear during CDC
- `postgresql_checkpoints.json` continues to move forward with a real PostgreSQL LSN

**Practical validation tips:**

- Keep `table_filters` narrowed to the test table while validating CDC fixes
- Ensure the synchronized PostgreSQL table has a real primary key
- Use `force_initial_sync` when you intentionally want ElasticRelay to rebuild snapshot state from scratch
- If you manually reset source tables and checkpoints outside ElasticRelay, also clean up any inactive PostgreSQL replication slot left by the previous run

### MongoDB Support

ElasticRelay provides complete MongoDB CDC capabilities using Change Streams:

#### Core MongoDB Features
- **Change Streams**: Real-time CDC using MongoDB's native Change Streams API
- **Cluster Support**: Automatic detection and support for replica sets and sharded clusters
- **Resume Tokens**: Persistent resume token management for checkpoint/resume functionality
- **Operation Mapping**: Full support for INSERT, UPDATE, REPLACE, and DELETE operations

#### Advanced MongoDB Capabilities
- **Sharded Cluster Support**: 
  - Multi-shard monitoring via mongos
  - Migration awareness for consistency during chunk migrations
  - Chunk distribution monitoring
- **Type Conversion**: Complete BSON to JSON-friendly type conversion:
  - ObjectID → string (hex format)
  - DateTime → RFC3339 timestamp
  - Decimal128 → string (precision preserved)
  - Binary → base64 encoded
  - Nested documents with configurable flattening depth
- **Parallel Snapshots**: 
  - ObjectID-based chunking for standard collections
  - Numeric ID-based chunking for integer primary keys
  - Skip/Limit fallback for complex ID types

#### MongoDB Configuration Options
```json
{
  "type": "mongodb",
  "host": "localhost",
  "port": 27017,
  "user": "elasticrelay_user",
  "password": "password",
  "database": "your_database",
  "options": {
    "auth_source": "admin",
    "replica_set": "rs0",
    "read_preference": "primaryPreferred",
    "batch_size": 1000,
    "flatten_depth": 3
  }
}
```

#### MongoDB Setup Requirements
```sh
# MongoDB must run in replica set mode for Change Streams
# Use the provided setup script:
./scripts/reset-mongodb.sh

# Or with Docker Compose:
docker-compose up -d mongodb
docker-compose up mongodb-init

# Verify replica set is configured:
./scripts/verify-mongodb.sh
```

### Transform Engine

ElasticRelay includes a complete data transformation pipeline, configurable via a separate JSON file (`-transform-config`):

#### Field Mapping
- **Rename**: Change field names (e.g., `user_name` → `username`)
- **Copy**: Duplicate fields to new names while keeping originals
- **Nested Path Support**: Access and modify nested fields using dot notation (`user.profile.name`)
- **Field Exclusion**: Remove sensitive or unnecessary fields before indexing

#### Type Conversion

| Source Type | Target Types |
|-------------|--------------|
| string | int, int64, float64, bool, date, timestamp |
| int/int64 | string, float64, bool, timestamp |
| float64 | string, int, int64, bool |
| bool | string, int |
| time.Time | string (RFC3339), timestamp (Unix) |

#### Data Masking

| Template | Input | Output |
|----------|-------|--------|
| `phone` | `13812345678` | `138****5678` |
| `id_card` | `110101199001011234` | `1101**********1234` |
| `email` | `john@example.com` | `jo***@example.com` |
| `bank_card` | `6222021234567890` | `6222********7890` |
| `name` | `张三` | `张*` |

Masking strategies: `mask` (character masking), `hash` (SHA256/MD5), `token` (tokenization), `regex` (pattern replacement).

#### Expression Engine

Built-in functions for computed fields:

| Category | Functions |
|----------|-----------|
| String | `concat()`, `substr()`, `upper()`, `lower()`, `trim()`, `replace()`, `length()` |
| Math | `round()`, `abs()`, `floor()`, `ceil()`, `min()`, `max()` |
| Date | `now()`, `formatDate()`, `parseDate()` |
| Conditional | `ifNull()`, `ifEmpty()`, `coalesce()` |

Example expressions:
```javascript
$.age < 18 ? 'minor' : 'adult'
concat($.first_name, ' ', $.last_name)
round($.price * $.quantity, 2)
```

#### Conditional Filtering

| Operator | Description | Example |
|----------|-------------|---------|
| `eq` | Equal | `status == "active"` |
| `ne` | Not equal | `status != "deleted"` |
| `gt` / `gte` | Greater than (or equal) | `age > 18` |
| `lt` / `lte` | Less than (or equal) | `price < 100` |
| `in` / `nin` | In / not in list | `type in ["a", "b"]` |
| `regex` | Regex match | `email ~ ".*@example.com"` |
| `exists` | Field exists | `email exists` |

#### Transform Configuration

```sh
# Run with transform rules
./bin/elasticrelay -config multi_config.json -transform-config ./config/mysql_transform.json

# Run without transform (pass-through mode, default)
./bin/elasticrelay -config multi_config.json
```

#### Performance

| Operation | Throughput | Memory |
|-----------|-----------|--------|
| Full Transform Pipeline | ~800,000 ops/sec | 1,601 B/op |
| Field Mapping | ~4,500,000 ops/sec | 416 B/op |
| Type Conversion | ~22,000,000 ops/sec | 16 B/op |
| Filter Evaluation | ~5,000,000 ops/sec | ~200 B/op |
| Data Masking (4 fields) | ~1,000,000 ops/sec | ~500 B/op |

### Parallel Processing

Advanced parallel snapshot processing capabilities:

- **Chunking Strategies**: Support for ID-based, time-based, and hash-based chunking
- **Worker Pools**: Configurable worker pool sizes with adaptive scheduling
- **Progress Tracking**: Real-time progress monitoring and statistics
- **Large Table Support**: Optimized handling of large tables with intelligent chunking
- **Streaming Mode**: Memory-efficient streaming processing for large datasets
- **Primary Key Discovery**: Automatic detection of primary key columns for correct document IDs

## Current Status

**Current Version**: v1.4.4 | **Phase**: Phase 2 Complete ✅, Phase 3 In Progress (Transform Engine complete)

This project has completed its core multi-source CDC platform (Phase 2) and has delivered the Transform Engine as the first major Phase 3 milestone. PostgreSQL CDC has been production-hardened with extensive stability fixes.

### ✅ Completed Features (v1.4.4)
- **Multi-Source CDC Pipeline**: 
  - **MySQL CDC**: Full implementation with binlog-based real-time synchronization, consistent datetime handling
  - **PostgreSQL CDC**: Production-hardened logical replication with WAL parsing, replication slots, publications, stable snapshot-to-CDC handoff, async batch decoupling, and job-scoped replication slot management
  - **MongoDB CDC**: Full Change Streams implementation with replica set and sharded cluster support
- **Transform Engine** (v1.4.0+):
  - Field mapping (rename, copy, move) with nested path support
  - Type conversion (string, int, float, bool, date, timestamp, object)
  - Data masking (phone, ID card, email, bank card, name) with 4 strategies
  - Expression engine with 16 built-in functions
  - Conditional filtering with 10 operators and include/exclude/route actions
  - Priority-based multi-rule matching with table pattern wildcards
  - Performance: 800,000+ ops/sec (80x above design target)
- **Multi-Table Dynamic Indexing**: Automatic per-table Elasticsearch index creation and management with configurable naming
- **gRPC Architecture**: Complete service definitions and implementations (Connector, Orchestrator, Sink, Transform, Health)
- **Advanced Configuration Management**: 
  - Multi-source configuration system with legacy migration support
  - Configuration synchronization and hot-reload capabilities
  - Automatic format detection and migration tools
- **Elasticsearch Integration**: High-performance bulk writing with automatic index management and data cleaning
- **Checkpoint/Resume**: Persistent position tracking for fault tolerance with automatic recovery (binlog, LSN, resume tokens)
- **Dead Letter Queue (DLQ)**: 
  - Comprehensive DLQ system with exponential backoff retry (configurable max retries)
  - Persistent storage with deduplication and status tracking
  - Automatic cleanup of resolved items
  - Support for manual item management and inspection
- **Parallel Processing**: 
  - Advanced parallel snapshot processing with chunking strategies
  - Automatic primary key discovery for correct document ID generation
  - Configurable worker pools and adaptive scheduling
  - Progress tracking and statistics collection
  - Support for large table optimization (MySQL, PostgreSQL, MongoDB)
- **Version Management**: Complete version injection system with build-time metadata
- **Robust Error Handling**: Comprehensive error handling with fallback mechanisms
- **Log Level Control**: Centralized logging system (debug/info/warn/error) with runtime configuration and thread-safe global control

### 🚧 In Progress (Phase 3 Remaining)
- **Prometheus Metrics**: Complete observability with metrics export
- **HTTP REST API**: grpc-gateway integration with OpenAPI documentation
- **Health Check Enhancement**: Kubernetes-ready readiness/liveness probes

### 📋 Upcoming (Phase 4+)
- **Frontend Development**: Control Plane GUI (TypeScript/Next.js)
- **High Availability**: Multi-replica deployment with automatic failover
- **Security Enhancement**: mTLS, RBAC, and audit logging
- **Advanced Governance**: Rich data transformation rules and field-level governance

---

## 📄 License

ElasticRelay is licensed under the [Apache License 2.0](LICENSE).

```
Copyright 2024 上海悦高软件股份有限公司 (Yogoo Software Co., Ltd.)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

## 📞 Support

- 🐦 X (Twitter): [@ElasticRelay](https://x.com/ElasticRelay)
- 🌐 Official Website: [www.elasticrelay.com](http://www.elasticrelay.com)
- 📧 Email: support@yogoo.net
- 💬 Community: [GitHub Discussions](https://github.com/yogoosoft/ElasticRelay/discussions)
- 🐛 Bug Reports: [GitHub Issues](https://github.com/yogoosoft/ElasticRelay/issues)
- 📖 Documentation: [docs.elasticrelay.com](https://docs.elasticrelay.com)
