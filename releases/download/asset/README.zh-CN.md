# ElasticRelay - 多源 CDC 网关到 Elasticsearch

![ElasticRelay 截图](/releases/download/asset/screenshot_02.png)

<p align="center">
  <a href="https://github.com/yogoosoft/ElasticRelay/releases"><img src="https://img.shields.io/badge/version-v1.4.4-blue.svg" alt="版本"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/go-1.25.2+-00ADD8.svg" alt="Go 版本"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-green.svg" alt="许可证"></a>
</p>
<p align="center">
  <a href="/README.md">English</a> |
  <a href="README.de.md">Deutsch</a> |
  <a href="README.fr.md">Français</a> |
  <a href="README.ja.md">日本語</a> |
  <a href="README.ru.md">Русский</a> |
  <a href="README.zh-CN.md">中文</a>
</p>

## 愿景

ElasticRelay 是一个无缝的异构数据同步器，旨在从主要 OLTP 数据库（MySQL、PostgreSQL、MongoDB）实时捕获数据变更（CDC）并同步到 Elasticsearch。它的目标是比现有解决方案（如 Logstash 或 Flink）更加用户友好和可靠。

## 🎉 v1.4.4 亮点 - 生产就绪的 CDC 平台（含转换引擎）

**三大数据库源 + 企业级数据转换：**

| 数据源 | 状态 | 功能 |
|--------|------|------|
| **MySQL** | ✅ 完成 | Binlog CDC + 初始同步 + 并行快照 |
| **PostgreSQL** | ✅ 生产强化 | 逻辑复制 + WAL 解析 + 稳定的快照到 CDC 切换 |
| **MongoDB** | ✅ 完成 | Change Streams + 分片集群 + Resume Tokens |
| **转换引擎** | ✅ 完成 | 字段映射 + 数据脱敏 + 类型转换 + 表达式引擎 |

## 核心功能

- **多源 CDC**：完全支持 MySQL、PostgreSQL 和 MongoDB 的实时变更捕获
- **转换引擎**：企业级数据转换，支持字段映射、数据脱敏（手机号、身份证、邮箱、银行卡）、类型转换、表达式求值和条件过滤 — 处理性能达 800,000+ 操作/秒
- **零代码配置**：基于 JSON 的配置，带有向导式 GUI（开发中）
- **多表动态索引**：自动为每个源表创建单独的 Elasticsearch 索引，支持可配置的命名模式（例如 `elasticrelay-users`、`elasticrelay-orders`）
- **内置治理**：处理数据结构化、匿名化、类型转换、规范化和增强
- **默认可靠性**：利用事务日志级别的 CDC、精确的检查点恢复和幂等写入确保数据完整性
- **死信队列（DLQ）**：具有指数退避重试和持久存储的全面故障处理
- **并行处理**：具有分块策略的高级并行快照处理，适用于大型表
- **集中日志管理**：运行时可配置的日志级别（debug/info/warn/error），支持线程安全的全局控制

## 技术栈

- **数据平面（Go）**：核心数据同步逻辑使用 Go（1.25.2+）构建，具有高并发、低内存占用和简单部署的特点。
- **控制平面和 GUI（TypeScript/Next.js）**：用于配置和监控的丰富交互式 UI（开发中）。
- **API（gRPC）**：组件之间的内部通信通过 gRPC 处理，具有高性能和完整的服务实现。
- **数据库支持**：
  - **MySQL CDC**：具有实时同步的高级 binlog 解析（go-mysql 库）
  - **PostgreSQL CDC**：具有 WAL 解析、复制槽和发布的逻辑复制，以及生产强化的快照到 CDC 切换
  - **MongoDB CDC**：支持副本集和分片集群的 Change Streams（mongo-driver）
- **转换引擎**：完整的数据转换管道，支持字段映射、类型转换、数据脱敏（4种策略、5个预设模板）、表达式引擎（16个内置函数）和条件过滤（10个运算符）
- **Elasticsearch 集成**：官方 Elasticsearch Go 客户端（v8），支持批量索引
- **配置**：基于 JSON 的配置，支持自动格式检测和迁移
- **可靠性**：全面的错误处理、DLQ 系统和检查点管理
- **日志管理**：具有运行时配置的集中日志级别控制系统

## 架构

系统由以下关键组件组成：

- **源连接器**：从 MySQL（binlog）、PostgreSQL（逻辑复制）和 MongoDB（变更流）捕获变更。
- **持久缓冲区**：异步 CDC 事件队列，将源端读取与下游处理解耦。
- **转换引擎**：企业级数据转换管道，支持字段映射、类型转换、数据脱敏、表达式求值和条件过滤。
- **ES Sink Writer**：以高效批次将数据写入 Elasticsearch，并自动管理索引。
- **编排器**：管理同步任务生命周期，同时支持传统单源与多源配置。
- **死信队列**：对失败事件采用指数退避重试与持久化存储。
- **检查点管理器**：持久化位置跟踪（binlog 位点、PostgreSQL LSN、MongoDB resume token），支持容错恢复。
- **控制平面**：UI 与配置管理后端（开发中）。

## 快速开始

要快速启动并运行 ElasticRelay，请按照以下三个简单步骤操作：

### 步骤 1：构建
```sh
./scripts/build.sh
```

### 步骤 2：配置

#### MongoDB 设置（MongoDB CDC 必需）
MongoDB 需要副本集模式才能使用 Change Streams。运行设置脚本：
```sh
./scripts/reset-mongodb.sh
```

或手动执行：
```sh
docker-compose down
rm -rf ./data/mongodb/*
docker-compose up -d mongodb
docker-compose up mongodb-init
```

验证 MongoDB 已就绪：
```sh
./scripts/verify-mongodb.sh
```

📚 **参见**：`QUICKSTART.md` 获取详细的 MongoDB 设置说明。

#### PostgreSQL 设置
对于 PostgreSQL，确保启用逻辑复制：
```sql
-- 在 postgresql.conf 中启用逻辑复制
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10

-- 创建具有复制权限的用户
CREATE USER elasticrelay_user WITH LOGIN PASSWORD 'password' REPLICATION;
GRANT CONNECT ON DATABASE your_database TO elasticrelay_user;
GRANT USAGE ON SCHEMA public TO elasticrelay_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO elasticrelay_user;
```

#### 配置文件
编辑配置文件 `./config/parallel_config.json`，确保数据库和 Elasticsearch 连接信息正确。

### 步骤 3：执行
```sh
./start.sh
```

完成这些步骤后，ElasticRelay 将开始监控数据库变更并将其同步到 Elasticsearch。

---

## 如何运行

### 先决条件

- Go（1.25.2+）
- Protobuf 编译器（`protoc`）
- Elasticsearch（7.x 或 8.x）
- **MySQL**（5.7+ 或 8.x）并启用 binlog
- **PostgreSQL**（推荐 10+，最低 9.4+）并启用逻辑复制
- **MongoDB**（4.0+）配置副本集或分片集群

### 安装

1.  **安装 Go 依赖和工具**：
    ```sh
    go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
    ```

2.  **安装 `protoc`**：
    在 macOS 上使用 Homebrew：
    ```sh
    brew install protobuf
    ```

3.  **整理依赖**：
    ```sh
    go mod tidy
    ```

### 构建和运行服务器

#### 快速构建（开发）
```sh
# 简单构建，不包含版本信息
go build -o elasticrelay ./cmd/elasticrelay

# 运行服务器
./elasticrelay -config multi_config.json
```

#### 生产构建（推荐）
```sh
# 使用 Makefile 构建，包含版本信息
make build

# 运行带版本的二进制文件
./bin/elasticrelay -config multi_config.json
```

#### 版本管理
ElasticRelay 具有完整的版本管理和构建时注入：

```sh
# 查看当前版本信息和详细构建信息
./bin/elasticrelay -version

# 从 Makefile 检查版本信息
make version

# 开发构建（快速，无版本注入）
make dev

# 生产构建（优化并包含版本信息）
make release

# 多架构跨平台构建
make build-all

# 使用自定义版本构建
VERSION="v1.3.0" make build

# 构建所有工具，包括迁移实用程序
make build-tools
```

版本系统包括：
- **Git 集成**：从 git 标签自动检测版本
- **构建元数据**：提交哈希、构建时间、Go 版本和平台信息
- **彩色输出**：丰富的控制台输出，包含版本详情和 ASCII 艺术 logo
- **跨平台**：支持 Linux、macOS（Intel/ARM）和 Windows

服务器将启动并默认监听端口 `50051`。

**替代方案**：您也可以直接运行而不构建：
```sh
go run ./cmd/elasticrelay -config multi_config.json
```

### 多表配置

ElasticRelay 支持传统单配置和现代多配置格式，具有自动检测和迁移功能。

#### 现代多配置格式（`multi_config.json`）：

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

#### 传统配置格式（`config.json`）：

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

系统自动检测配置格式并支持格式之间的迁移。这将创建单独的索引：
- `elasticrelay-users` 用于 `users` 表
- `elasticrelay-orders` 用于 `orders` 表
- `elasticrelay-products` 用于 `products` 表

### 死信队列（DLQ）支持

ElasticRelay 包含一个全面的 DLQ 系统，用于处理失败的事件：

- **自动重试**：失败的事件会自动使用指数退避进行重试
- **持久存储**：DLQ 项目持久化到磁盘，具有完整的状态管理
- **去重**：防止重复事件被添加到队列
- **状态跟踪**：完整的生命周期跟踪（待处理、重试中、已耗尽、已解决、已丢弃）
- **手动管理**：支持手动项目检查和管理
- **自动清理**：已解决的项目在可配置的持续时间后自动清理

### PostgreSQL 支持

ElasticRelay 提供全面的 PostgreSQL CDC 功能，具有高级特性：

#### PostgreSQL 核心功能
- **逻辑复制**：使用 PostgreSQL 原生逻辑复制和 `pgoutput` 插件
- **WAL 解析**：用于实时变更捕获的高级预写日志解析
- **复制槽**：自动创建和管理逻辑复制槽
- **发布**：用于表过滤的动态发布管理
- **LSN 管理**：用于检查点/恢复功能的精确日志序列号跟踪

#### PostgreSQL 高级功能
- **连接池**：具有可配置限制的智能连接池管理
- **并行快照**：使用分块策略的多线程初始数据同步
- **类型映射**：全面的 PostgreSQL 到 Elasticsearch 类型转换，包括：
  - 所有数值类型（bigint、integer、real、double、numeric）
  - 文本和字符类型（text、varchar、char）
  - 支持时区的日期/时间类型（timestamp、timestamptz、date、time）
  - 具有原生对象映射的 JSON/JSONB
  - 数组类型（整数数组、文本数组）
  - 高级类型（UUID、bytea、inet、几何类型）
- **性能优化**：
  - 大型表的自适应调度
  - 内存效率的流式模式
  - 可配置的批量大小和工作池
  - 连接生命周期管理

#### PostgreSQL 配置选项
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

### MongoDB 支持

ElasticRelay 使用 Change Streams 提供完整的 MongoDB CDC 功能：

#### MongoDB 核心功能
- **Change Streams**：使用 MongoDB 原生 Change Streams API 的实时 CDC
- **集群支持**：自动检测和支持副本集和分片集群
- **Resume Tokens**：用于检查点/恢复功能的持久 resume token 管理
- **操作映射**：完全支持 INSERT、UPDATE、REPLACE 和 DELETE 操作

#### MongoDB 高级功能
- **分片集群支持**：
  - 通过 mongos 进行多分片监控
  - 分块迁移期间的迁移感知一致性
  - 分块分布监控
- **类型转换**：完整的 BSON 到 JSON 友好类型转换：
  - ObjectID → 字符串（十六进制格式）
  - DateTime → RFC3339 时间戳
  - Decimal128 → 字符串（保留精度）
  - Binary → base64 编码
  - 具有可配置展平深度的嵌套文档
- **并行快照**：
  - 标准集合的基于 ObjectID 的分块
  - 整数主键的基于数值 ID 的分块
  - 复杂 ID 类型的 Skip/Limit 回退

#### MongoDB 配置选项
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

#### MongoDB 设置要求
```sh
# MongoDB 必须以副本集模式运行才能使用 Change Streams
# 使用提供的设置脚本：
./scripts/reset-mongodb.sh

# 或使用 Docker Compose：
docker-compose up -d mongodb
docker-compose up mongodb-init

# 验证副本集已配置：
./scripts/verify-mongodb.sh
```

### 转换引擎

ElasticRelay 包含完整的数据转换管道，可通过单独的 JSON 配置文件（`-transform-config`）进行配置：

#### 字段映射
- **重命名**：更改字段名（例如 `user_name` → `username`）
- **复制**：将字段复制到新名称同时保留原字段
- **嵌套路径支持**：使用点号表示法访问和修改嵌套字段（`user.profile.name`）
- **字段排除**：在索引前移除敏感或不需要的字段

#### 类型转换

| 源类型 | 目标类型 |
|--------|----------|
| string | int, int64, float64, bool, date, timestamp |
| int/int64 | string, float64, bool, timestamp |
| float64 | string, int, int64, bool |
| bool | string, int |
| time.Time | string (RFC3339), timestamp (Unix) |

#### 数据脱敏

| 模板 | 输入 | 输出 |
|----------|-------|--------|
| `phone` | `13812345678` | `138****5678` |
| `id_card` | `110101199001011234` | `1101**********1234` |
| `email` | `john@example.com` | `jo***@example.com` |
| `bank_card` | `6222021234567890` | `6222********7890` |
| `name` | `张三` | `张*` |

脱敏策略：`mask`（字符掩码）、`hash`（SHA256/MD5）、`token`（令牌化）、`regex`（模式替换）。

#### 表达式引擎

用于计算字段的内置函数：

| 类别 | 函数 |
|----------|-----------|
| 字符串 | `concat()`、`substr()`、`upper()`、`lower()`、`trim()`、`replace()`、`length()` |
| 数学 | `round()`、`abs()`、`floor()`、`ceil()`、`min()`、`max()` |
| 日期 | `now()`、`formatDate()`、`parseDate()` |
| 条件 | `ifNull()`、`ifEmpty()`、`coalesce()` |

表达式示例：
```javascript
$.age < 18 ? 'minor' : 'adult'
concat($.first_name, ' ', $.last_name)
round($.price * $.quantity, 2)
```

#### 条件过滤

| 运算符 | 说明 | 示例 |
|----------|-------------|---------|
| `eq` | 等于 | `status == "active"` |
| `ne` | 不等于 | `status != "deleted"` |
| `gt` / `gte` | 大于（或等于） | `age > 18` |
| `lt` / `lte` | 小于（或等于） | `price < 100` |
| `in` / `nin` | 在列表中 / 不在列表中 | `type in ["a", "b"]` |
| `regex` | 正则匹配 | `email ~ ".*@example.com"` |
| `exists` | 字段存在 | `email exists` |

#### 转换配置

```sh
# 使用转换规则运行
./bin/elasticrelay -config multi_config.json -transform-config ./config/mysql_transform.json

# 不使用转换运行（直通模式，默认）
./bin/elasticrelay -config multi_config.json
```

#### 性能

| 操作 | 吞吐量 | 内存 |
|-----------|-----------|--------|
| 完整转换管道 | ~800,000 次/秒 | 1,601 B/次 |
| 字段映射 | ~4,500,000 次/秒 | 416 B/次 |
| 类型转换 | ~22,000,000 次/秒 | 16 B/次 |
| 过滤求值 | ~5,000,000 次/秒 | ~200 B/次 |
| 数据脱敏（4 个字段） | ~1,000,000 次/秒 | ~500 B/次 |

### 并行处理

高级并行快照处理功能：

- **分块策略**：支持基于 ID、时间和哈希的分块
- **工作池**：具有自适应调度的可配置工作池大小
- **进度跟踪**：实时进度监控和统计
- **大型表支持**：具有智能分块的大型表优化处理
- **流式模式**：用于大型数据集的内存高效流式处理
- **主键发现**：自动检测主键列以生成正确的文档 ID

## 当前状态

**当前版本**：v1.4.4 | **阶段**：第 2 阶段完成 ✅，第 3 阶段进行中（转换引擎已完成）

该项目已完成其核心多源 CDC 平台（第 2 阶段），并以转换引擎作为第 3 阶段的第一个重要里程碑交付。PostgreSQL CDC 已通过大量稳定性修复实现生产强化。

### ✅ 已完成功能（v1.4.4）
- **多源 CDC 管道**：
  - **MySQL CDC**：基于 binlog 的实时同步完整实现，具有一致的日期时间处理
  - **PostgreSQL CDC**：生产强化的逻辑复制，包含 WAL 解析、复制槽、发布、稳定的快照到 CDC 切换、异步批处理解耦以及按任务范围的复制槽管理
  - **MongoDB CDC**：具有副本集和分片集群支持的完整 Change Streams 实现
- **转换引擎**（v1.4.0+）：
  - 字段映射（重命名、复制、移动）及嵌套路径支持
  - 类型转换（string、int、float、bool、date、timestamp、object）
  - 数据脱敏（手机号、身份证、邮箱、银行卡、姓名）及 4 种策略
  - 表达式引擎，含 16 个内置函数
  - 条件过滤，含 10 个运算符及 include/exclude/route 动作
  - 基于优先级的多规则匹配与表模式通配符
  - 性能：800,000+ 操作/秒（为设计目标的 80 倍）
- **多表动态索引**：自动为每个表创建和管理 Elasticsearch 索引，支持可配置的命名
- **gRPC 架构**：完整的服务定义和实现（Connector、Orchestrator、Sink、Transform、Health）
- **高级配置管理**：
  - 支持传统迁移的多源配置系统
  - 配置同步和热重载功能
  - 自动格式检测和迁移工具
- **Elasticsearch 集成**：具有自动索引管理和数据清理的高性能批量写入
- **检查点/恢复**：用于容错的持久位置跟踪，支持自动恢复（binlog、LSN、resume tokens）
- **死信队列（DLQ）**：
  - 具有指数退避重试的全面 DLQ 系统（可配置的最大重试次数）
  - 具有去重和状态跟踪的持久存储
  - 已解决项目的自动清理
  - 支持手动项目管理和检查
- **并行处理**：
  - 具有分块策略的高级并行快照处理
  - 自动主键发现，用于生成正确的文档 ID
  - 可配置的工作池和自适应调度
  - 进度跟踪和统计收集
  - 支持大型表优化（MySQL、PostgreSQL、MongoDB）
- **版本管理**：具有构建时元数据的完整版本注入系统
- **强大的错误处理**：具有回退机制的全面错误处理
- **日志级别控制**：集中日志系统（debug/info/warn/error），支持运行时配置与线程安全的全局控制

### 🚧 开发中（第 3 阶段剩余）
- **Prometheus 指标**：具有指标导出的完整可观察性
- **HTTP REST API**：grpc-gateway 集成与 OpenAPI 文档
- **健康检查增强**：Kubernetes 就绪的 readiness/liveness 探针

### 📋 即将推出（第 4+ 阶段）
- **前端开发**：控制平面 GUI（TypeScript/Next.js）
- **高可用性**：具有自动故障转移的多副本部署
- **安全增强**：mTLS、RBAC 和审计日志
- **高级治理**：丰富的数据转换规则和字段级治理

---

## 📄 许可证

ElasticRelay 根据 [Apache License 2.0](LICENSE) 许可。

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

## 🤝 贡献

我们欢迎贡献！请参阅我们的[贡献指南](CONTRIBUTING.md)了解详情。

## 📞 支持

- 🐦 X (Twitter): [@ElasticRelay](https://x.com/ElasticRelay)
- 🌐 官方网站: [www.elasticrelay.com](http://www.elasticrelay.com)
- 📧 邮箱: support@yogoo.net
- 💬 社区: [GitHub Discussions](https://github.com/yogoosoft/ElasticRelay/discussions)
- 🐛 问题报告: [GitHub Issues](https://github.com/yogoosoft/ElasticRelay/issues)
- 📖 文档: [docs.elasticrelay.com](https://docs.elasticrelay.com)
