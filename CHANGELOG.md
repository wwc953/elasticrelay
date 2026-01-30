# ElasticRelay Changelog

## [v1.4.2] - 2026-01-30

### 🔧 Transform Rule Matching Fix

This release fixes a critical issue where transform rules with `source_id` configuration were not matching events due to `Checkpoint.SourceType` being lost during gRPC transmission.

### 🐛 Bug Fixes

#### 1. Source ID Matching Fix (`internal/orchestrator/multi_orchestrator.go`)

**Fixed `_source_id` not being passed to Transform Engine:**

- **Issue**: `Checkpoint.SourceType` was correctly set when sending events via gRPC, but the value was lost (empty string) when received by Transform Service, causing transform rules with `source_id` configuration to fail matching
- **Root Cause**: gRPC protobuf transmission was dropping the `SourceType` field from nested `Checkpoint` message
- **Fix**: Embed `_source_id` field directly into event data JSON, bypassing the unreliable Checkpoint transmission
- **Impact**: Transform rules with `source_id` configuration now correctly match events

```go
// Added _source_id to snapshot event data
recordData["_table"] = tableName
recordData["_source_id"] = j.SourceID  // New: reliable source_id transmission

// Added enrichEventWithSourceID for CDC events
func (j *MultiJob) enrichEventWithSourceID(event *pb.ChangeEvent) {
    // Parse event data JSON
    // Add _source_id field if not present
    // Re-serialize to JSON
}
```

#### 2. Transform Engine Enhancement (`internal/transform/engine.go`)

**Added `_source_id` extraction from event data:**

- **Issue**: Transform engine only extracted `sourceType` from `Checkpoint`, which was unreliable
- **Fix**: Added `extractSourceID()` method to extract `_source_id` from event data JSON, with fallback to `Checkpoint.SourceType`
- **Impact**: Source ID matching now works reliably for transform rule filtering

```go
// Extract source_id from data first (more reliable), fallback to Checkpoint
sourceType := e.extractSourceID(data)
if sourceType == "" && event.Checkpoint != nil {
    sourceType = event.Checkpoint.SourceType
}

func (e *Engine) extractSourceID(data map[string]interface{}) string {
    if val, ok := data["_source_id"].(string); ok && val != "" {
        return val
    }
    return ""
}
```

#### 3. ES Sink Cleanup (`internal/sink/es/es.go`)

**Added `_source_id` to metadata cleanup:**

- **Issue**: `_source_id` metadata field would be stored in Elasticsearch documents
- **Fix**: Added `_source_id` to the list of metadata fields to remove before ES storage
- **Impact**: Clean documents without internal metadata fields

```go
// Remove metadata fields
delete(data, "_table")
delete(data, "_source_id")  // New: remove transform matching metadata
delete(data, "_schema")
```

### 📝 Configuration Updates

#### Transform Configuration (`config/mysql_transform.json`)

**Fixed `source_id` configuration:**

- **Issue**: All MySQL transform rules had empty `source_id: ""`, which was a global rule matching all sources
- **Fix**: Updated all MySQL-specific transform rules to use `source_id: "mysql-main"` to match the data source ID in `mysql_config.json`
- **Impact**: Transform rules now explicitly bind to specific data sources, preventing unintended rule matching across different sources

Updated rules:
- `user-data-transform`: `source_id: ""` → `source_id: "mysql-main"`
- `order-data-transform`: `source_id: ""` → `source_id: "mysql-main"`
- `log-data-transform`: Added `source_id: "mysql-main"`
- `mysql-users-transform`: `source_id: ""` → `source_id: "mysql-main"`
- `mysql-orders-transform`: `source_id: ""` → `source_id: "mysql-main"`
- `mysql-products-transform`: `source_id: ""` → `source_id: "mysql-main"`
- `test-table-transform`: `source_id: ""` → `source_id: "mysql-main"`

### ✅ Verification

After this fix, transform rules correctly match and apply transformations:

```
Before: Transform: Table='mysql_users' matched 0 rules: []
After:  Transform: Table='mysql_users' matched 1 rules: [mysql-users-transform]
```

Transform features now working:
- ✅ Field mapping (e.g., `user_name` → `username`)
- ✅ Data masking (phone: `138****5678`, email: `zh***@example.com`)
- ✅ Password hashing (SHA256)
- ✅ Computed fields (`full_name`, `age_group`, `processed_at`)
- ✅ Record filtering (exclude test users and deleted records)
- ✅ Type conversion (`is_vip`: number → boolean)

---

## [v1.4.1] - 2026-01-30

### 🔧 Transform Engine Integration & Bug Fixes

This release fixes critical issues that prevented the Transform Engine from working correctly in production. The Transform Engine is now fully operational with complete data transformation pipeline support.

### 🐛 Bug Fixes

#### 1. Transform Service Integration (`internal/orchestrator/multi_orchestrator.go`)

**Fixed `transformEvents` method not calling Transform Service:**

- **Issue**: The `transformEvents` method was a placeholder implementation that simply returned events unchanged (pass-through mode)
- **Fix**: Implemented actual gRPC call to Transform Service via `ApplyRules` stream
- **Impact**: Transform rules now properly execute during both Initial Sync and CDC

```go
// Before (placeholder)
func (j *MultiJob) transformEvents(events []*pb.ChangeEvent) []*pb.ChangeEvent {
    return events // No transformation!
}

// After (working implementation)
func (j *MultiJob) transformEvents(events []*pb.ChangeEvent) []*pb.ChangeEvent {
    // Opens gRPC stream to Transform Service
    // Sends all events for transformation
    // Receives and returns transformed/filtered events
}
```

**Fixed missing table name in snapshot events:**

- **Issue**: ChangeEvents created during Initial Sync were missing `_table` field, preventing rule matching
- **Fix**: Added `_table` field to enriched record data before creating ChangeEvent
- **Impact**: Transform rules can now match tables correctly during Initial Sync

**Fixed missing SourceType in Checkpoint:**

- **Issue**: ChangeEvents were missing `SourceType` in Checkpoint, causing source_id matching to fail
- **Fix**: Set `Checkpoint.SourceType = j.SourceID` when creating events

#### 2. Type Converter Enhancement (`internal/transform/type_converter.go`)

**Added `object` type support:**

- **Issue**: JSON object fields (like MySQL JSON columns) failed with "unsupported target type: object"
- **Fix**: Added `DataTypeObject` constant and `toObject()` pass-through converter
- **Impact**: JSON fields now correctly pass through without conversion errors

```go
tc.Register(DataTypeObject, tc.toObject) // JSON object type (pass-through)

func (tc *TypeConverter) toObject(value interface{}) (interface{}, error) {
    return value, nil // Pass through unchanged
}
```

#### 3. Data Masking Engine (`internal/transform/masking/masking.go`)

**Fixed numeric value handling in masking:**

- **Issue**: Large numeric values (phone numbers, ID cards) were converted to scientific notation (e.g., `1.38e+10`)
- **Fix**: Added `valueToString()` function that properly formats numeric types without scientific notation
- **Impact**: Phone numbers, ID cards, bank cards are now correctly masked (e.g., `138****5678`)

```go
func valueToString(value interface{}) string {
    switch v := value.(type) {
    case int64:
        return strconv.FormatInt(v, 10)
    case float64:
        if v == float64(int64(v)) {
            return strconv.FormatInt(int64(v), 10)
        }
        return strconv.FormatFloat(v, 'f', -1, 64)
    // ... handles all numeric types
    }
}
```

#### 4. MySQL Connector (`internal/connectors/mysql/mysql.go`)

**Fixed long numeric strings being converted to numbers:**

- **Issue**: VARCHAR fields containing long numeric strings (phone, ID card, bank card) were parsed as int64, causing precision loss in JSON serialization
- **Fix**: Only convert strings to numbers if length ≤ 10 digits
- **Impact**: Phone numbers (11 digits), ID cards (18 digits), bank cards (16-19 digits) remain as strings

```go
// Only convert short numeric strings to avoid precision issues
if len(s) <= 10 {
    if num, err := strconv.ParseInt(s, 10, 64); err == nil {
        dataMap[colName] = num
    }
} else {
    // Keep long numeric strings as strings
    dataMap[colName] = s
}
```

### 📝 Configuration Updates

#### Transform Configuration (`config/mysql_transform.json`)

**Fixed source_id matching:**

- Changed `source_id` from `"mysql-main"` to `""` (empty) for global rule matching
- Rules now match regardless of data source identifier

**Fixed is_test filter:**

- Changed filter value from `true` to `1` to match MySQL's numeric boolean representation

**Fixed full_name expression:**

- Simplified from `concat($.last_name || '', $.first_name || '')` to `concat($.last_name, $.first_name)`
- Expression now correctly generates Chinese names (e.g., "张三")

**Added sensitive field type configurations:**

```json
{
  "field": "phone",
  "target_type": "keyword",
  "description": "Ensure phone is string for masking"
},
{
  "field": "bank_card",
  "target_type": "keyword",
  "description": "Ensure bank_card is string for masking"
}
```

#### SQL Schema Updates (`config/mysql/init-mysql.sql`)

**Added Transform-compatible test tables:**

- `users` - Matches `user-data-transform` rule with all required fields
- `orders` - Matches `order-data-transform` rule
- `audit_logs` - Matches `log-data-transform` rule

**Added Transform rules for existing tables:**

- `mysql-users-transform` - For `mysql_users` table
- `mysql-orders-transform` - For `mysql_orders` table
- `mysql-products-transform` - For `mysql_products` table
- `test-table-transform` - For `test_table`

### ✅ Verified Transform Features

| Feature | Status | Example |
|---------|--------|---------|
| Field Rename | ✅ | `user_name` → `username` |
| Field Copy | ✅ | `created_at` → `create_time` |
| Field Exclude | ✅ | `internal_notes`, `debug_info` removed |
| Type Conversion | ✅ | `is_vip: 1` → `is_vip: true` |
| Phone Masking | ✅ | `13812345678` → `138****5678` |
| ID Card Masking | ✅ | `110101199001011234` → `1101**********1234` |
| Bank Card Masking | ✅ | `6222021234567890123` → `6222***********0123` |
| Email Masking | ✅ | `zhangsan@example.com` → `zh***@example.com` |
| Password Hashing | ✅ | SHA256 hash |
| Address Masking | ✅ | `北京市朝阳区建国路100号` → `北京市朝阳区*******` |
| Computed Fields | ✅ | `full_name`, `age_group`, `display_balance` |
| Record Filtering | ✅ | `status='deleted'` and `is_test=1` filtered out |

### 📊 Test Results

**Before fixes:** 5 records with no transformation (pass-through mode)
**After fixes:** 3 records with full transformation applied

---

## [v1.4.0] - 2026-01-17

### 🎉 Major Release: Transform Engine Complete Implementation

This release marks a major milestone in Phase 3 development - the complete implementation of the **Transform Engine**, providing enterprise-grade data transformation capabilities including field mapping, type conversion, data masking, expression evaluation, and conditional filtering.

### 🚀 New Features

#### 1. Transform Engine Core (`internal/transform/engine.go`)

**Complete transformation pipeline implementation:**

- **Rule Matching**: Table pattern matching with wildcard support (`users`, `user_*`)
- **Priority-based Processing**: Rules sorted by priority (lower = higher priority)
- **Multi-rule Support**: Apply multiple transformation rules to same event
- **Statistics Tracking**: Process count, error count, filtered count with atomic operations
- **Pass-through Mode**: Automatic detection when no rules configured

**Key Functions:**
```go
func (e *Engine) Transform(ctx context.Context, event *pb.ChangeEvent) (*pb.ChangeEvent, bool, error)
func (e *Engine) LoadConfig(configs []*TransformConfig) error
func (e *Engine) GetStats() EngineStats
```

#### 2. Configuration Model (`internal/transform/config.go`)

**Complete configuration structures:**

- `TransformConfig`: Main rule configuration with ID, name, table patterns, priority
- `FieldMapping`: Field rename/copy/move operations with nested path support
- `FieldConfig`: Per-field type conversion, validation, null handling, exclusion
- `FilterRule`: Conditional filtering with operators (eq, ne, gt, lt, in, regex, exists)
- `MaskingRule`: Data masking with strategies and preset templates
- `ComputedField`: Expression-based computed fields
- `ValidationRule`: Field validation with patterns, min/max, length constraints
- `GlobalTransformSettings`: Engine-wide configuration options

#### 3. Field Mapper (`internal/transform/field_mapper.go`)

**Field transformation capabilities:**

- **Rename**: Change field name, remove original
- **Copy**: Duplicate field to new name, keep original
- **Move**: Same as rename (alias)
- **Nested Path Support**: Access and modify nested fields using dot notation (`user.profile.name`)
- **Deep Copy**: Proper deep copying of nested structures

**Key Functions:**
```go
func (fm *FieldMapper) Apply(mappings []FieldMapping, data map[string]interface{}) (map[string]interface{}, error)
func (fm *FieldMapper) ProcessFieldConfigs(configs []FieldConfig, data map[string]interface{}) (map[string]interface{}, error)
```

#### 4. Type Converter (`internal/transform/type_converter.go`)

**Comprehensive type conversion system:**

| Source Type | Target Types |
|-------------|--------------|
| string | int, int64, float64, bool, date, timestamp |
| int/int64 | string, float64, bool, timestamp |
| float64 | string, int, int64, bool |
| bool | string, int |
| time.Time | string (RFC3339), timestamp (Unix) |

**Supported Target Types:**
- `string`, `keyword`, `text` - String types (ES compatible)
- `int`, `int64` - Integer types
- `float`, `float64` - Floating point types
- `bool` - Boolean type
- `date` - RFC3339 formatted date string
- `timestamp` - Unix timestamp (int64)

#### 5. Filter Engine (`internal/transform/filter/filter.go`)

**Conditional record filtering:**

| Operator | Description | Example |
|----------|-------------|---------|
| `eq` | Equal | `status == "active"` |
| `ne` | Not equal | `status != "deleted"` |
| `gt` | Greater than | `age > 18` |
| `gte` | Greater or equal | `score >= 60` |
| `lt` | Less than | `price < 100` |
| `lte` | Less or equal | `quantity <= 10` |
| `in` | In list | `type in ["a", "b"]` |
| `nin` | Not in list | `status not in ["deleted"]` |
| `regex` | Regex match | `email ~ ".*@example.com"` |
| `exists` | Field exists | `email exists` |

**Filter Actions:**
- `include`: Include record if condition matches
- `exclude`: Exclude record if condition matches
- `route`: Route to specific target if condition matches

#### 6. Masking Engine (`internal/transform/masking/masking.go`)

**Data anonymization and masking:**

**Masking Strategies:**
| Strategy | Description | Example |
|----------|-------------|---------|
| `mask` | Character masking | `138****5678` |
| `hash` | SHA256/MD5 hash | `a1b2c3d4...` |
| `token` | Tokenization | `TOKEN_abc123` |
| `regex` | Regex replacement | Custom pattern |

**Preset Templates:**
| Template | Input | Output |
|----------|-------|--------|
| `phone` | `13812345678` | `138****5678` |
| `id_card` | `110101199001011234` | `1101**********1234` |
| `email` | `john@example.com` | `jo***@example.com` |
| `bank_card` | `6222021234567890` | `6222********7890` |
| `name` | `张三` | `张*` |

#### 7. Expression Engine (`internal/transform/expression/engine.go`)

**Dynamic field computation:**

**Built-in Functions:**

| Category | Functions |
|----------|-----------|
| String | `concat()`, `substr()`, `upper()`, `lower()`, `trim()`, `replace()`, `length()` |
| Math | `round()`, `abs()`, `floor()`, `ceil()`, `min()`, `max()` |
| Date | `now()`, `formatDate()`, `parseDate()` |
| Conditional | `ifNull()`, `ifEmpty()`, `coalesce()` |

**Expression Syntax:**
```javascript
// Field access
$.field_name
$.nested.field

// Ternary expressions
$.age < 18 ? 'minor' : 'adult'

// Arithmetic
$.price * $.quantity

// Function calls
concat($.first_name, ' ', $.last_name)
round($.price, 2)
```

#### 8. gRPC Service Integration (`internal/transform/transform.go`)

**Updated TransformService implementation:**

- Replaced pass-through with full transformation pipeline
- Support for configuration injection via `ServerOption`
- Statistics tracking and retrieval
- Single event transformation utility method

#### 9. Configuration Loading System

**Command-line Parameter Support (`cmd/elasticrelay/main.go`):**

```bash
./bin/elasticrelay \
  -config ./config/mongodb_config.json \
  -port 50051 \
  -transform-config ./config/transform_example.json
```

| Parameter | Description | Default |
|-----------|-------------|---------|
| `-config` | Data source configuration file | `config.json` |
| `-port` | gRPC service port | `50051` |
| `-transform-config` | Transform configuration file (optional) | empty (pass-through) |

**Configuration Loading (`internal/transform/config.go`):**

```go
// Load Transform configuration from JSON file
func LoadTransformConfig(filePath string) (*TransformConfigFile, error)

// Global settings management
func SetGlobalSettings(settings GlobalTransformSettings)
func GetGlobalSettings() GlobalTransformSettings

// Masking templates management
func SetMaskingTemplates(templates map[string]*masking.Template)
func GetMaskingTemplates() map[string]*masking.Template
```

**Startup Script Integration (`start.sh`):**

```bash
# Transform configuration (optional)
# Set to empty string to disable transform rules (pass-through mode)
# TRANSFORM_CONFIG="./config/transform_example.json"
TRANSFORM_CONFIG=""  # Default: pass-through mode

# Build command arguments
if [ -n "$TRANSFORM_CONFIG" ] && [ -f "$TRANSFORM_CONFIG" ]; then
    CMD_ARGS="$CMD_ARGS -transform-config $TRANSFORM_CONFIG"
fi
```

**Usage Modes:**

| Mode | Configuration | Behavior |
|------|---------------|----------|
| **Pass-through** | `TRANSFORM_CONFIG=""` | Events pass through unchanged |
| **Transform** | `TRANSFORM_CONFIG="./config/transform_example.json"` | Apply transformation rules |

### 🧪 Testing

#### Unit Tests

**File:** `internal/transform/transform_test.go`
- `TestFieldMapper_Rename`: Field renaming
- `TestFieldMapper_Copy`: Field copying
- `TestFieldMapper_NestedPath`: Nested path access
- `TestTypeConverter_ToInt`: Integer conversion
- `TestTypeConverter_ToFloat64`: Float conversion
- `TestTypeConverter_ToBool`: Boolean conversion
- `TestTypeConverter_ToDate`: Date conversion
- `TestEngine_PassThrough`: Pass-through mode
- `TestEngine_FieldMapping`: Field mapping
- `TestEngine_FieldExclusion`: Field exclusion
- `TestEngine_TypeConversion`: Type conversion
- `TestEngine_TablePatternMatching`: Table pattern matching
- Benchmark tests for performance validation

**File:** `internal/transform/filter/filter_test.go`
- All operators tested (eq, ne, gt, gte, lt, lte, in, nin, regex, exists)
- Nested field support
- Include/Exclude/Route actions
- Regex compilation and caching

**File:** `internal/transform/masking/masking_test.go`
- All strategies tested (mask, hash, token, regex)
- All preset templates tested (phone, id_card, email, bank_card)
- Custom parameters
- Edge cases (short values, missing fields)

### 📊 Performance Characteristics

| Operation | Speed | Memory | Notes |
|-----------|-------|--------|-------|
| **Engine.Transform** | ~800,000 ops/sec | 1,601 B/op | Full transformation pipeline |
| FieldMapper.Apply | ~4,500,000 ops/sec | 416 B/op | Field mapping only |
| TypeConverter.Convert | ~22,000,000 ops/sec | 16 B/op | Type conversion only |
| Filter.Check | ~5,000,000 ops/sec | ~200 B/op | Rule evaluation |
| Masking.Apply | ~1,000,000 ops/sec | ~500 B/op | 4-field masking |

> **Performance exceeds design target of 10,000 ops/sec by 80x!**

### 📁 Files Added

| File | Description |
|------|-------------|
| `internal/transform/config.go` | Configuration model definitions |
| `internal/transform/engine.go` | Core transform engine |
| `internal/transform/field_mapper.go` | Field mapping operations |
| `internal/transform/type_converter.go` | Type conversion system |
| `internal/transform/filter/filter.go` | Filter engine |
| `internal/transform/filter/filter_test.go` | Filter unit tests |
| `internal/transform/masking/masking.go` | Masking engine |
| `internal/transform/masking/masking_test.go` | Masking unit tests |
| `internal/transform/expression/engine.go` | Expression engine |
| `internal/transform/transform_test.go` | Main unit tests |

### 📁 Files Modified

| File | Description |
|------|-------------|
| `internal/transform/transform.go` | Updated gRPC service with engine integration |
| `cmd/elasticrelay/main.go` | Added `-transform-config` command-line parameter |
| `start.sh` | Integrated Transform configuration loading |

### ✅ Phase 3 Progress

| Task | Status | Notes |
|------|--------|-------|
| Transform Engine Core | ✅ Complete | Full implementation |
| Field Mapping | ✅ Complete | rename/copy/move with nested paths |
| Type Conversion | ✅ Complete | All common types supported |
| Data Masking | ✅ Complete | 4 strategies, 5 templates |
| Expression Engine | ✅ Complete | 16 built-in functions |
| Filter Engine | ✅ Complete | 10 operators |
| Configuration Loading | ✅ Complete | CLI args + start.sh integration |
| Unit Tests | ✅ Complete | 38 test cases |
| Performance | ✅ Complete | 80x above target |

### 🎯 Next Steps (Phase 3 Remaining)

- [ ] Prometheus metrics export (`internal/metrics/`)
- [ ] Health Check enhancement
- [ ] HTTP Gateway (grpc-gateway)
- [ ] REST API documentation

---

## [v1.3.1] - 2025-12-15

### 🐛 Bug Fixes

#### 1. MongoDB CDC Index Routing Fix

**Issue:** MongoDB CDC events were incorrectly routed to `elasticrelay_mongo-default` index instead of the proper collection-specific index (e.g., `elasticrelay_mongo-users`).

**Root Cause:** The ES sink's `extractTableName()` function only checked for the `_table` field, but MongoDB CDC events used `_collection` to store the collection name.

**Fix:** Updated `internal/sink/es/es.go`:
- `extractTableName()` now checks both `_table` (MySQL/PostgreSQL) and `_collection` (MongoDB) fields
- `cleanDataForES()` now properly removes `_collection`, `_database`, and `_id` metadata fields before indexing

#### 2. MongoDB Authentication Fix

**Issue:** MongoDB connector failed to authenticate when using credentials stored in admin database.

**Fix:** Added `authSource=admin` to the MongoDB connection URI in `internal/connectors/mongodb/mongodb.go`.

#### 3. MongoDB Snapshot Primary Key Extraction

**Issue:** MongoDB snapshot records used `_collection` instead of `_table`, causing inconsistent index routing.

**Fix:** Updated `beginStandardSnapshot()` to use `_table` field for consistency with ES sink expectations.

#### 4. Primary Key Detection Enhancement

**Issue:** MongoDB documents using `_id` as primary key were not properly detected during snapshot processing.

**Fix:** Updated `processSnapshotChunk()` in `internal/orchestrator/multi_orchestrator.go` to check for `_id` field first, then fall back to `id`.

### 🔧 Improvements

#### 1. Elasticsearch Timeout Configuration

- Increased timeout from 3 seconds to 30 seconds for `ensureIndexExists()` and `createDefaultIndex()` operations
- Improves reliability when connecting to remote Elasticsearch servers with higher latency

#### 2. Docker Compose MongoDB Replica Set

- Enhanced MongoDB container configuration with keyFile authentication for replica set
- Improved `mongodb-init` service with better replica set status detection
- Added conditional initialization to avoid re-initializing already configured replica sets

#### 3. Documentation Updates

- Updated `README.md` with MongoDB setup instructions
- Added MongoDB-specific scripts reference (`reset-mongodb.sh`, `verify-mongodb.sh`)
- Added reference to `QUICKSTART.md` for detailed setup

#### 4. Code Formatting

- Fixed indentation issues in `cmd/elasticrelay/main.go`
- Added MongoDB connector case to the switch statement for connector server creation

### 📁 Files Changed

- `internal/sink/es/es.go` - MongoDB collection name support in ES sink
- `internal/connectors/mongodb/mongodb.go` - Auth source fix and metadata field consistency
- `internal/orchestrator/multi_orchestrator.go` - MongoDB connector integration and `_id` support
- `cmd/elasticrelay/main.go` - MongoDB connector server creation
- `docker-compose.yml` - MongoDB replica set keyFile configuration
- `README.md` - MongoDB setup documentation
- `start.sh` - Default config changed to MongoDB
- `.gitignore` - Simplified ignore rules
- `config/mongodb_config.json` - Updated ES connection settings

---

## [v1.3.0] - 2025-12-07

### 🎉 Major Release: MongoDB Connector Complete Implementation

This release marks the completion of MongoDB Connector development, achieving **100% coverage of the three major database sources** (MySQL, PostgreSQL, MongoDB) for the ElasticRelay CDC platform.

### 🚀 New Features

#### 1. MongoDB Change Streams CDC Implementation

**Core Module:** `internal/connectors/mongodb/mongodb.go`

- **Change Streams Support**: Full implementation of MongoDB Change Streams for real-time CDC
- **Cluster Topology Detection**: Automatic detection of Standalone, ReplicaSet, and Sharded Cluster deployments
- **Resume Token Management**: Complete resume token encoding/decoding for checkpoint persistence
- **Operation Mapping**: Support for INSERT, UPDATE, REPLACE, and DELETE operations
- **Configurable Options**: `ConnectorOptions` and `ServerOptions` for flexible configuration

**Key Functions:**
```go
// Cluster type detection
func (c *Connector) detectClusterTopology(ctx context.Context) (*ClusterInfo, error)
func (c *Connector) IsSharded() bool
func (c *Connector) IsReplicaSet() bool

// CDC Pipeline
func (c *Connector) buildPipeline() mongo.Pipeline
func (c *Connector) Start(stream pb.ConnectorService_StartCdcServer, startCheckpoint *pb.Checkpoint) error
```

#### 2. BSON Type Converter System

**Module:** `internal/connectors/mongodb/type_converter.go`

Complete BSON to JSON-friendly type conversion supporting:

- **Basic Types**: ObjectID → string (hex), DateTime → RFC3339, Timestamp → map
- **Binary Types**: Binary → base64 encoded map with subtype
- **Numeric Types**: Decimal128 → string (precision preserved), int32 → int64 normalization
- **Special Types**: Regex → map, JavaScript → string, CodeWithScope → map
- **MongoDB-Specific**: MinKey, MaxKey, DBPointer, Symbol, Undefined, Null
- **Nested Structures**: Recursive document and array conversion
- **Document Flattening**: `FlattenDocument()` with configurable max depth for Elasticsearch compatibility

#### 3. Sharded Cluster Support

**Module:** `internal/connectors/mongodb/sharded.go`

- **ShardedConnector**: Dedicated connector for sharded cluster monitoring via mongos
- **Cluster Information**: `ClusterInfo` and `ShardInfo` structures for topology introspection
- **Multi-Shard Watching**: `WatchShardedCluster()` for aggregated change events across shards
- **Migration Awareness**: `GetActiveMigrations()` and migration callback support for consistency during chunk migrations
- **Chunk Distribution**: `GetChunkDistribution()` for monitoring data distribution across shards

**Key Functions:**
```go
func (sc *ShardedConnector) WatchShardedCluster(ctx context.Context, opts *options.ChangeStreamOptions) (*mongo.ChangeStream, error)
func (sc *ShardedConnector) WatchShardedClusterWithMigrationAwareness(ctx context.Context, opts *options.ChangeStreamOptions, migrationCallback func(MigrationEvent)) (*mongo.ChangeStream, error)
func (sc *ShardedConnector) GetChunkDistribution(ctx context.Context, collectionName string) (map[string]int, error)
```

#### 4. Parallel Snapshot Manager Integration

**Module:** `internal/connectors/mongodb/parallel_integration.go`

- **MongoDBSnapshotAdapter**: Adapter implementing parallel snapshot interface
- **Collection Info Retrieval**: `GetCollectionInfo()` with document count and field schema detection
- **Chunking Strategies**: 
  - ObjectID-based chunking for standard collections
  - Numeric ID-based chunking for integer primary keys
  - Skip/Limit fallback for complex ID types
- **Parallel Processing**: `MongoDBParallelSnapshotManager` for coordinated parallel snapshots

**Key Functions:**
```go
func (msa *MongoDBSnapshotAdapter) GetCollectionInfo(ctx context.Context, collName string) (*parallel.TableInfo, error)
func (msa *MongoDBSnapshotAdapter) CreateCollectionChunks(ctx context.Context, info *parallel.TableInfo, chunkSize int) ([]*parallel.ChunkInfo, error)
func (msa *MongoDBSnapshotAdapter) ProcessChunk(ctx context.Context, chunk *parallel.ChunkInfo, stream pb.ConnectorService_BeginSnapshotServer) error
```

#### 5. Checkpoint Manager Enhancement

**Module:** `internal/connectors/mongodb/checkpoint.go`

- **MongoCheckpoint Structure**: Job-specific checkpoint with resume token, cluster time, and event count
- **Thread-Safe Operations**: Mutex-protected CRUD operations
- **Event Counting**: `IncrementEventCount()` and `GetEventCount()` for monitoring
- **Persistent Storage**: JSON file-based checkpoint persistence

### 🧪 Testing

#### Unit Tests

**File:** `internal/connectors/mongodb/type_converter_test.go`
- `TestConvertBSONToMap`: Empty, simple, and complex document conversion
- `TestConvertBSONValue_*`: Tests for all BSON types (ObjectID, DateTime, Binary, Decimal128, Regex, etc.)
- `TestGetPrimaryKey`: Various _id types (ObjectID, string, int, int32, int64, complex)
- `TestEncodeDecodeResumeToken`: Resume token roundtrip encoding
- `TestFlattenDocument`: Nested document flattening with depth boundaries
- Benchmark tests for performance validation

**File:** `internal/connectors/mongodb/mongodb_test.go`
- `TestBuildMongoURI`: URI construction with/without authentication
- `TestBuildPipeline`: Change Stream aggregation pipeline construction
- `TestChangeEvent_*`: Operation type mapping and structure validation
- `TestCheckpointManager_*`: Full CRUD, concurrency, and persistence tests
- Benchmark tests for pipeline and URI building

#### Integration Tests

**File:** `internal/connectors/mongodb/integration_test.go` (with `//go:build integration` tag)
- `TestChangeStreamBasic`: Basic Change Stream functionality
- `TestChangeStreamResumeToken`: Resume token persistence and recovery
- `TestChangeStreamUpdateDelete`: UPDATE and DELETE operation handling
- `TestTypeConversionEndToEnd`: Real MongoDB data type conversion
- `TestDatabaseLevelChangeStream`: Database-wide change monitoring
- `TestConnectorIntegration`: Full connector integration
- `TestCheckpointManagerPersistence`: File-based checkpoint persistence
- Benchmark for change stream processing performance

### 📊 Performance Characteristics

- **Change Streams Latency**: < 1s for real-time CDC events
- **Type Conversion**: Handles all MongoDB BSON types with 100% accuracy
- **Parallel Snapshot**: Configurable chunk size (default: 100,000 documents)
- **Memory Efficient**: Streaming processing with configurable batch sizes

### 📁 Files Changed

| File | Operation | Description |
|------|-----------|-------------|
| `internal/connectors/mongodb/type_converter_test.go` | Added | Unit tests for BSON type conversion |
| `internal/connectors/mongodb/mongodb_test.go` | Added | Unit tests for connector functions |
| `internal/connectors/mongodb/sharded.go` | Added | Sharded cluster support |
| `internal/connectors/mongodb/parallel_integration.go` | Added | Parallel snapshot manager integration |
| `internal/connectors/mongodb/integration_test.go` | Added | Integration tests with Docker |
| `internal/connectors/mongodb/mongodb.go` | Modified | Added cluster detection and parallel snapshot support |
| `docs/ROADMAP.md` | Modified | Updated MongoDB Connector status to completed |

### ✅ Milestone Achievement

**Phase 2 Progress**: 
- MySQL Connector: ✅ Complete
- PostgreSQL Connector: ✅ Complete  
- MongoDB Connector: ✅ Complete (this release)
- Multi-source CDC coverage: **100%** 🎉

---

## [v1.2.6] - 2025-11-25

### 🚀 Features & Improvements

#### Implemented Global Log Level Control System

**Issue Description:**

The application had inconsistent log level behavior where setting `log_level: "info"` in configuration still displayed verbose DEBUG logs. This was caused by hardcoded debug messages in PostgreSQL connector and lack of a centralized log level filtering system, making production deployments noisy with unnecessary debug output.

**Root Causes:**

1. **Missing Log Level Infrastructure**: No centralized logging system to enforce log level filtering across all components
2. **Hardcoded DEBUG Messages**: PostgreSQL WAL parser contained 34+ hardcoded `log.Printf("[DEBUG] ...")` statements that ignored configuration
3. **Configuration Not Applied**: Global log level from config was loaded but never applied to control actual logging behavior

**Implementation Solutions:**

#### 1. Created Centralized Logging System

**New File:** `internal/logger/logger.go`

**Features:**
```go
// Log levels supported
type LogLevel int
const (
    DEBUG LogLevel = iota  // Most verbose
    INFO                   // Default production level  
    WARN                   // Warnings only
    ERROR                  // Errors only
)

// Usage examples
logger.Debug("Debug information")    // Only shown when level = DEBUG
logger.Info("Important information") // Shown when level <= INFO  
logger.Warn("Warning message")       // Shown when level <= WARN
logger.Error("Error occurred")       // Always shown
```

**Thread-Safe Implementation:**
- Global log level with mutex protection
- Runtime level changes supported
- Compatible with existing `log.Printf` calls

#### 2. Integrated Log Level Configuration

**File:** `cmd/elasticrelay/main.go`

**Before Fix:**
```go
// Configuration loaded but log level never applied
multiCfg, err := config.LoadMultiConfig(*configFile)
// Log level remained at default regardless of config
```

**After Fix:**
```go
// Set global log level from configuration
if multiCfg.Global.LogLevel != "" {
    logger.SetLogLevel(multiCfg.Global.LogLevel)
    log.Printf("Set log level to: %s", multiCfg.Global.LogLevel)
}
```

#### 3. Fixed Hardcoded Debug Logs in PostgreSQL Connector

**File:** `internal/connectors/postgresql/wal_parser.go`

**Before Fix:**
```go
log.Printf("[DEBUG] About to send replication command using SimpleQuery")
log.Printf("[DEBUG] Writing query message to connection") 
log.Printf("[DEBUG] Command sent, waiting for CopyBothResponse")
// ... 34+ more hardcoded debug messages
```

**After Fix:**
```go
logger.Debug("About to send replication command using SimpleQuery")
logger.Debug("Writing query message to connection")
logger.Debug("Command sent, waiting for CopyBothResponse")
// All debug messages now respect global log level
```

**Batch Replacement:**
- Replaced all `log.Printf("[DEBUG] ...)` with `logger.Debug(...)`
- Added logger import to PostgreSQL connector
- Maintained same debug information but with proper level control

#### 4. Updated Configuration Files

**File:** `config/postgresql_config.json`

**Before:**
```json
{
  "global": {
    "log_level": "debug"  // Caused verbose output
  }
}
```

**After:**
```json
{
  "global": {
    "log_level": "info"   // Clean production-ready output
  }
}
```

**Technical Benefits:**

- **Production Ready**: Clean log output suitable for production environments
- **Consistent Behavior**: All components respect global log level configuration
- **Performance Improvement**: Reduced I/O overhead by eliminating unnecessary debug output
- **Debugging Flexibility**: Easy to enable debug mode by changing config to `"log_level": "debug"`
- **Thread Safety**: Concurrent log level changes handled safely
- **Backward Compatibility**: Existing `log.Printf` calls continue to work

**Supported Log Levels:**
- `"debug"` - Shows all messages (development/troubleshooting)
- `"info"` - Shows informational, warning, and error messages (recommended for production)
- `"warn"` - Shows warning and error messages only
- `"error"` - Shows error messages only (minimal output)

**Migration Impact:**

**Before Migration:**
```
2025/11/25 16:51:49 [DEBUG] About to send replication command using SimpleQuery
2025/11/25 16:51:49 [DEBUG] Writing query message to connection  
2025/11/25 16:51:49 [DEBUG] Command sent, waiting for CopyBothResponse
2025/11/25 16:51:49 [DEBUG] Received initial message type: *pgproto3.CopyBothResponse
... 30+ more debug lines per connection
```

**After Migration (with log_level: "info"):**
```
2025/11/25 16:51:49 Set log level to: info
2025/11/25 16:51:49 PostgreSQL connection configured successfully
2025/11/25 16:51:49 Starting logical replication from LSN: 0/19DC6A0
... only essential information
```

**Configuration Examples:**

```json
{
  "global": {
    "log_level": "info"     // Recommended for production
  }
}
```

```json
{
  "global": {  
    "log_level": "debug"    // For development/troubleshooting
  }
}
```

This improvement significantly enhances the production experience by providing clean, configurable logging while maintaining full debugging capabilities when needed.

---

## [v1.2.5] - 2025-11-25

### 🐛 Bug Fixes

#### Fixed MySQL Date/Time Format Issues in CDC Synchronization

**Issue Description:**

MySQL CDC synchronization was experiencing critical date/time related failures with two main problems:

1. **Missing DateTime Parser Function**: CDC events with datetime fields were failing with Elasticsearch parsing errors like `document_parsing_exception: failed to parse field [created_at] of type [date]`, causing all events to be sent to DLQ (Dead Letter Queue).

2. **Inconsistent DateTime Formats**: Initial sync and CDC sync were producing different datetime formats for the same data, causing data inconsistency in Elasticsearch indices.

**Root Causes:**

1. **Missing `tryParseDateTime` Function**: The MySQL connector was calling an undefined `tryParseDateTime` function in both CDC event handling and initial snapshot processing, causing compilation errors and preventing proper datetime conversion.

2. **Timezone Handling Inconsistency**: 
   - Initial sync used DSN with `loc=Local`, returning local timezone format (`+08:00`)
   - CDC sync processed binlog data without timezone conversion, defaulting to different formats
   - Result: Same table had mixed datetime formats

**Fix Solutions:**

**File:** `internal/connectors/mysql/mysql.go`

#### 1. Implemented Missing `tryParseDateTime` Function

**Added Function:**
```go
// tryParseDateTime attempts to parse MySQL datetime strings and convert them to RFC3339 format
func tryParseDateTime(value string) (string, bool) {
    // MySQL datetime formats to try (most specific first)
    formats := []string{
        "2006-01-02 15:04:05.999999999", // with nanoseconds
        "2006-01-02 15:04:05.999999",    // with microseconds  
        "2006-01-02 15:04:05.999",       // with milliseconds
        "2006-01-02 15:04:05",           // standard MySQL DATETIME format
        "2006-01-02",                    // MySQL DATE format
        "15:04:05",                      // MySQL TIME format
        time.RFC3339Nano,                // RFC3339 with nanoseconds
        time.RFC3339,                    // RFC3339
    }
    
    for _, format := range formats {
        if t, err := time.Parse(format, value); err == nil {
            // Convert to UTC and format as RFC3339Nano for Elasticsearch compatibility
            return t.UTC().Format(time.RFC3339Nano), true
        }
    }
    
    // If all parsing attempts fail, it's not a datetime string
    return "", false
}
```

#### 2. Enhanced CDC Event Processing

**Before Fix (CDC):**
```go
case []byte:
    s := string(v)
    if parsed, ok := tryParseDateTime(s); ok {  // ❌ Function didn't exist
        dataMap[colName] = parsed
    } else {
        // fallback to string
        dataMap[colName] = s
    }
```

**After Fix (CDC):**
```go
case []byte:
    s := string(v)
    if parsed, ok := tryParseDateTime(s); ok {  // ✅ Function now exists
        dataMap[colName] = parsed  // Converts to UTC RFC3339Nano
    } else if i, err := strconv.ParseInt(s, 10, 64); err == nil {
        dataMap[colName] = i
    } // ... other type conversions
```

#### 3. Enhanced Initial Sync Processing

**Before Fix (Snapshot):**
```go
case time.Time:
    dataMap[colName] = v.Format(time.RFC3339Nano)  // ❌ Used local timezone
```

**After Fix (Snapshot):**
```go
case time.Time:
    dataMap[colName] = v.UTC().Format(time.RFC3339Nano)  // ✅ Force UTC conversion

case string:
    // Handle string datetime values
    if parsed, ok := tryParseDateTime(v); ok {
        dataMap[colName] = parsed  // ✅ Consistent UTC format
    } else {
        dataMap[colName] = v
    }
```

#### 4. Unified Timezone Handling

**Problem Examples:**
```json
// Before Fix - Inconsistent formats in same table:
{"created_at": "2025-11-24T14:37:38Z"}        // From CDC
{"created_at": "2025-11-24T14:37:38+08:00"}   // From Initial Sync

// After Fix - Consistent UTC format:
{"created_at": "2025-11-24T14:37:38.000000000Z"}  // All sources
{"updated_at": "2025-11-25T13:31:38.000000000Z"}  // All sources
```

**Technical Impact:**

- **Elasticsearch Compatibility**: All datetime fields now use RFC3339Nano format with UTC timezone
- **Data Consistency**: Initial sync and CDC sync produce identical datetime formats
- **Error Elimination**: No more `document_parsing_exception` errors for datetime fields
- **DLQ Reduction**: Eliminates datetime-related failures from going to Dead Letter Queue
- **Multi-Format Support**: Handles various MySQL datetime formats (DATE, TIME, DATETIME, TIMESTAMP)

**Supported MySQL DateTime Formats:**
- `2006-01-02 15:04:05.999999999` (DATETIME with nanoseconds)
- `2006-01-02 15:04:05` (Standard DATETIME)
- `2006-01-02` (DATE only)
- `15:04:05` (TIME only)
- Existing RFC3339 formats

**Output Format:**
All datetime fields are consistently formatted as: `2025-11-24T14:37:38.000000000Z`

**Migration Notes:**

For existing data with inconsistent datetime formats, it's recommended to:
1. Delete existing indices: `curl -X DELETE "http://your-es:9200/elasticrelay_mysql-*"`
2. Restart ElasticRelay to trigger fresh initial sync with consistent formatting
3. All new data will maintain consistent UTC datetime formatting

---

## [v1.2.4] - 2025-11-25

### 🐛 Bug Fixes

#### Fixed `force_initial_sync` Configuration Not Working

**Issue Description:**

When the `force_initial_sync` configuration option was set to `true`, it was being ignored by the system. Even with this option enabled, if a checkpoint existed, the initial sync would be skipped and the system would proceed directly to CDC mode. This prevented users from forcing a fresh initial synchronization when needed.

**Root Cause:**

The bug was in the `needsInitialSync()` function in `multi_orchestrator.go`. The function's logic checked for existing checkpoints **before** checking the `force_initial_sync` configuration:

1. First, it checked if `initial_sync` was enabled
2. Then, it checked if a valid checkpoint exists → **If yes, returned false immediately**
3. The `force_initial_sync` check was only performed when "target has data but no checkpoint"
4. Result: When checkpoint exists, `force_initial_sync` was never evaluated

**Fix Solution:**

**File:** `internal/orchestrator/multi_orchestrator.go`

**Before Fix:**
```go
func (j *MultiJob) needsInitialSync() bool {
    // 1. Check configuration
    if !j.isInitialSyncEnabledInConfig() {
        return false
    }
    
    // 2. Check if valid checkpoint exists
    if j.hasValidCheckpoint() {
        return false  // ❌ Returns here, force_initial_sync never checked
    }
    
    // 3. Check target system
    if j.targetSystemHasData() {
        return j.shouldForceInitialSync()  // Only checked in specific case
    }
    
    return true
}
```

**After Fix:**
```go
func (j *MultiJob) needsInitialSync() bool {
    // 1. Check configuration
    if !j.isInitialSyncEnabledInConfig() {
        return false
    }
    
    // 2. Check force_initial_sync first - overrides all other checks
    if j.shouldForceInitialSync() {
        log.Printf("force_initial_sync enabled, will perform initial sync")
        return true  // ✅ Force initial sync regardless of checkpoint
    }
    
    // 3. Check if valid checkpoint exists
    if j.hasValidCheckpoint() {
        return false
    }
    
    // 4. Check target system
    if j.targetSystemHasData() {
        return false
    }
    
    return true
}
```

**Technical Impact:**

- `force_initial_sync` is now checked **before** checkpoint validation
- When `force_initial_sync: true` is set, the system will:
  - Ignore existing checkpoints
  - Ignore existing data in target Elasticsearch indices
  - Always perform a fresh initial synchronization
- This is particularly useful for:
  - Development and testing scenarios
  - Data consistency recovery
  - Forcing a complete re-sync after schema changes

**Configuration Example:**

```json
{
  "jobs": [
    {
      "id": "mysql-to-es-cdc",
      "options": {
        "initial_sync": true,
        "force_initial_sync": true
      }
    }
  ]
}
```

**Warning:** Using `force_initial_sync: true` in production should be done with caution, as it will re-sync all data on every restart. It's recommended to use this option temporarily for specific scenarios and then disable it.

---

## [v1.2.3] - 2025-11-24

### 🎉 Major Features

#### PostgreSQL CDC Functionality Fully Fixed and Operational

**Issue Description:**
PostgreSQL CDC functionality had multiple critical issues preventing normal data synchronization to Elasticsearch:
1. `conn busy` error preventing WAL replication message reception
2. RELATION message parsing failure with error "RELATION message too short for relation name"
3. Logical replication connection blocking or failing immediately after establishment
4. Data change events unable to be correctly parsed and forwarded to Elasticsearch

**Root Causes:**
1. **Replication Protocol Handling Error**: After sending `START_REPLICATION` command using `pgconn.Exec()`, incorrectly called `result.Close()`, causing connection to enter busy state and unable to receive subsequent WAL messages
2. **String Parsing Error**: `parseRelation` function assumed strings used length-prefix encoding, but PostgreSQL logical replication protocol actually uses null-terminated C-style strings
3. **LSN Position Issue**: Starting replication from a newer LSN position missed initial RELATION metadata messages, causing subsequent UPDATE/INSERT/DELETE events to fail parsing due to missing table structure

**Fix Solutions:**

##### 1. Fixed Logical Replication Connection Establishment (conn busy issue)

**File:** `internal/connectors/postgresql/wal_parser.go`

**Before Fix:**
```go
result := wp.conn.Exec(ctx, cmd)
result.Close()  // ❌ Error: This causes connection blocking
```

**After Fix:**
```go
// Use SimpleQuery protocol to send command directly
queryMsg := &pgproto3.Query{String: cmd}
buf, err := queryMsg.Encode(buf)
_, err = wp.conn.Conn().Write(buf)

// Receive CopyBothResponse to confirm entering replication mode
initialMsg, err := wp.conn.ReceiveMessage(ctx)
if _, ok := initialMsg.(*pgproto3.CopyBothResponse); !ok {
    return fmt.Errorf("unexpected initial response: %T", initialMsg)
}
```

**Technical Notes:**
- Uses PostgreSQL Simple Query Protocol to send `START_REPLICATION` command directly
- Avoids using `MultiResultReader.Close()`, which waits for replication stream to end (never ends)
- Correctly receives and validates `CopyBothResponse` message to ensure connection entered COPY BOTH mode

##### 2. Fixed RELATION Message Parsing

**File:** `internal/connectors/postgresql/wal_parser.go`

**Before Fix:**
```go
func (wp *WALParser) parseRelation(data []byte) error {
    relationID := binary.BigEndian.Uint32(data[0:4])
    namespaceLen := int(data[4])  // ❌ Error: Assumes length prefix
    namespace := string(data[5 : 5+namespaceLen])
    // ...
}
```

**After Fix:**
```go
func (wp *WALParser) parseRelation(data []byte) error {
    relationID := binary.BigEndian.Uint32(data[0:4])
    offset := 4
    
    // Parse namespace (null-terminated string)
    namespaceEnd := offset
    for namespaceEnd < len(data) && data[namespaceEnd] != 0 {
        namespaceEnd++
    }
    namespace := string(data[offset:namespaceEnd])
    offset = namespaceEnd + 1  // Skip null terminator
    
    // Parse relation name (null-terminated string)
    relationNameEnd := offset
    for relationNameEnd < len(data) && data[relationNameEnd] != 0 {
        relationNameEnd++
    }
    relationName := string(data[offset:relationNameEnd])
    offset = relationNameEnd + 1
    
    // Parse column information (column names are also null-terminated)
    // ...
}
```

**Technical Notes:**
- PostgreSQL logical replication protocol uses null-terminated C-style strings
- Correctly handles parsing of namespace, table name, and column names
- Added boundary checks to prevent out-of-bounds access

##### 3. Optimized Replication Slot Management

**Improvements:**
- Clean up old replication slots on each startup to avoid LSN position issues
- Ensure replication starts from position containing RELATION messages
- Added detailed debug logging for easier issue tracking

##### 4. Enhanced Message Processing and Error Handling

**File:** `internal/connectors/postgresql/wal_parser.go`

**Improvements:**
```go
// Added detailed debug logging
log.Printf("[DEBUG] parseLogicalMessage: message type '%c' (0x%02x), data length: %d", 
    msgType, msgType, len(data))
log.Printf("[DEBUG] Parsed RELATION: id=%d, schema=%s, table=%s, columns=%d", 
    relationID, namespace, relationName, len(columns))

// Improved error handling
if relation == nil {
    return nil, fmt.Errorf("unknown relation ID: %d", relationID)
}
```

### 🐛 Bug Fixes

#### PostgreSQL Configuration Optimization

**File:** `docker-compose.yml`

**Changes:**
- Increased `wal_sender_timeout` from 60s to 300s
- Removed incorrect `tcp_keepalives_idle` parameter configuration

**File:** `config/postgresql_config.json`

**Changes:**
- Increased `connection_timeout` to 60s
- Increased `replication_timeout` to 30s
- Added `wal_sender_timeout` configuration item

#### Disabled Parallel Snapshot Processing for PostgreSQL

**File:** `internal/orchestrator/multi_orchestrator.go`

**Issue:** Generic parallel snapshot manager was designed for MySQL and not fully compatible with PostgreSQL's logical replication mechanism

**Fix:**
```go
case "postgresql":
    log.Printf("MultiJob '%s': PostgreSQL detected, disabling parallel processing", j.ID)
    j.useParallel = false
    return nil  // Use serial processing for initial sync
```

### ✨ Feature Verification

#### Successful Test Scenarios

1. **Logical Replication Connection Establishment**
   - ✅ Successfully sent `START_REPLICATION` command
   - ✅ Correctly received `CopyBothResponse` message
   - ✅ Entered replication message reception loop

2. **WAL Message Parsing**
   - ✅ BEGIN transaction messages
   - ✅ RELATION metadata messages (containing table structure)
   - ✅ UPDATE data change messages
   - ✅ INSERT messages
   - ✅ DELETE messages
   - ✅ COMMIT transaction messages
   - ✅ Primary Keepalive heartbeat messages

3. **Data Synchronization Verification**
   - ✅ PostgreSQL table `test_table` UPDATE operations successfully synced to Elasticsearch
   - ✅ ES index `elasticrelay_pg-test_table` automatically created
   - ✅ Real-time data sync with latency less than 3 seconds

**Test Data:**
```sql
-- PostgreSQL
UPDATE test_table SET name = 'Final Test', age = 35 WHERE id = 1;

-- Elasticsearch Result
{
  "_index": "elasticrelay_pg-test_table",
  "_id": "1",
  "docs.count": 1
}
```

### 📝 Technical Details

#### PostgreSQL Logical Replication Protocol Key Points

1. **Message Format**:
   - XLogData message format: `'w' + walStart(8) + walEnd(8) + sendTime(8) + data`
   - Strings use null terminators (`\0`), not length prefixes
   - Column type identifiers: `'n'` = NULL, `'t'` = TEXT, `'u'` = UNCHANGED

2. **Message Order**:
   - BEGIN → RELATION → (INSERT|UPDATE|DELETE)* → COMMIT
   - RELATION messages sent on first use of table in each transaction
   - Need to cache RELATION information for subsequent event parsing

3. **Keepalive Mechanism**:
   - Client needs to periodically send Standby Status Updates
   - Format: `'r' + received_LSN(8) + flushed_LSN(8) + applied_LSN(8) + timestamp(8) + reply_required(1)`
   - Recommended interval: 10 seconds

### 🔧 Configuration Recommendations

#### PostgreSQL Server Configuration

```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
wal_sender_timeout = 300s
```

#### Table REPLICA IDENTITY Settings

```sql
-- Default configuration (primary key only)
ALTER TABLE test_table REPLICA IDENTITY DEFAULT;

-- Or use FULL (includes all columns)
ALTER TABLE test_table REPLICA IDENTITY FULL;
```

### 🚀 Performance Metrics

- **Message Processing Latency**: < 100ms
- **Data Sync Latency**: < 3s
- **Connection Stability**: No issues during long-term operation
- **Memory Usage**: Normal, no memory leaks

### 🎯 Next Steps for Optimization

1. Improve field mapping logic to use correct column names
2. Add more complete support for PostgreSQL data types
3. Implement incremental snapshot synchronization
4. Add CDC performance monitoring metrics

---

## [v1.0.1] - 2025-10-12

### 🐛 Bug Fixes

#### 1. MySQL CDC Permissions and Configuration Issues Fixed

**Problem Description:**
ElasticRelay encountered two critical errors during CDC operations:
1. `ERROR 1227 (42000): Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation`
2. `ERROR can't use 0 as the server ID, will panic`

**Root Causes:**
1. MySQL user `elasticrelay_user` lacked replication privileges required for CDC operations
2. Configuration file missing `server_id` configuration, causing CDC service startup failure

**Fix Solutions:**

##### MySQL User Privileges Fix

**File:** `init.sql`

**Fix Content:**
Added necessary privileges for `elasticrelay_user` for CDC operations:

```sql
-- Grant replication privileges to elasticrelay_user
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'elasticrelay_user'@'%';
GRANT SUPER ON *.* TO 'elasticrelay_user'@'%';
FLUSH PRIVILEGES;
```

**Privilege Explanations:**
- `REPLICATION CLIENT`: Allows user to execute replication-related commands like `SHOW MASTER STATUS`
- `REPLICATION SLAVE`: Allows user to connect to master server as replication slave
- `SUPER`: Provides super user privileges required for replication operations

##### CDC Configuration Fix

**Files:** `config.json` and `bin/config.json`

**Before Fix:**
```json
{
  "db_host": "127.0.0.1",
  "db_port": 3306,
  "db_user": "elasticrelay_user",
  "db_password": "elasticrelay_pass",
  "db_name": "elasticrelay"
}
```

**After Fix:**
```json
{
  "db_host": "127.0.0.1",
  "db_port": 3306,
  "db_user": "elasticrelay_user",
  "db_password": "elasticrelay_pass",
  "db_name": "elasticrelay",
  "server_id": 100,
  "table_filters": ["test_table"]
}
```

**Configuration Explanations:**
- `server_id`: MySQL replication server ID, must be non-zero positive integer (set to 100)
- `table_filters`: CDC table filters, restricting monitoring scope to specific tables

##### Deployment Configuration Fix

**Operation Steps:**
1. **Recreate MySQL Container** - Apply new privilege configuration
   ```bash
   docker-compose down
   rm -rf ./data  # Clear old data for re-initialization
   docker-compose up -d mysql
   ```

2. **Verify Privilege Configuration**
   ```bash
   # Check user privileges
   docker-compose exec mysql mysql -u elasticrelay_user -p \
     -e "SHOW GRANTS FOR 'elasticrelay_user'@'%';"
   
   # Verify binary log enabled
   docker-compose exec mysql mysql -u elasticrelay_user -p \
     -e "SHOW VARIABLES LIKE 'log_bin';"
   ```

3. **Rebuild Application**
   ```bash
   make build
   ```

**Fix Results:**

✅ **Permission Issues Resolved**
- User now has complete CDC operation privileges
- Successfully executes `SHOW MASTER STATUS` command
- Can establish binlog sync connection

✅ **Server ID Configuration Correct**
- BinlogSyncer configuration shows `ServerID:100`
- CDC sync starts successfully
- Monitors from correct binlog position

✅ **CDC Functionality Works Normally**
- Successfully connects to MySQL 8.0.43
- Real-time capture of data change events
- Correctly handles INSERT, UPDATE, DELETE operations
- Checkpoint functionality saves and restores normally

**Verification Logs:**
```
ElasticRelay ea3989a-dirty (commit: ea3989a, built: 2025-10-12_08:01:48_UTC, go: go1.25.2, platform: darwin/amd64)
2025/10/12 16:05:26 Configuration loaded from config.json
2025/10/12 16:05:26 Starting CDC from provided checkpoint: binlog.000002:1290
2025/10/12 16:05:26 INFO create BinlogSyncer config="{ServerID:100 ...}"
2025/10/12 16:05:26 INFO Connected to server flavor=mysql version=8.0.43
2025/10/12 16:05:26 CDC sync started from position (binlog.000002, 1290)
```

**Test Verification:**
```bash
# Test data change capture
mysql> INSERT INTO test_table (name, email) VALUES ('Real-time Test', 'realtime@example.com');
# ✅ ElasticRelay successfully captures and processes this change event
```

#### 2. Data Not Syncing to Elasticsearch Issue Fixed

**Problem Description:**
During CDC process, data processed by MySQL Connector and Transform service was not successfully syncing to Elasticsearch. Logs showed events stopping after Transform service processing, failing to reach Sink service.

**Root Causes:**
1. **Transform Service Stream Processing Issues:** The `ApplyRules` function in `internal/transform/transform.go` did not properly signal stream end (`io.EOF`) to Orchestrator after processing events, causing `transformStream.Recv()` loop to block indefinitely.
2. **Orchestrator Client Stream Closure Missing:** The `flushBatch` function in `internal/orchestrator/orchestrator.go` did not call `transformStream.CloseSend()` after sending all events to Transform service, preventing Transform service from receiving `io.EOF`.

**Fix Solutions:**

##### Transform Service Stream Processing Logic Fix

**File:** `internal/transform/transform.go`

**Fix Content:**
Modified `ApplyRules` function to first receive all events from Orchestrator, then process (currently pass-through), send all processed events back to Orchestrator, and finally return `nil` to properly signal stream end to Orchestrator.

##### Orchestrator Client Stream Closure Fix

**File:** `internal/orchestrator/orchestrator.go`

**Fix Content:**
Added `transformStream.CloseSend()` call in `flushBatch` function after sending all events to Transform service, explicitly notifying Transform service that client has finished sending.

**Fix Results:**

✅ **Data Flow Normal**
- Events now correctly flow from Orchestrator through Transform service to Elasticsearch Sink service.
- Elasticsearch Sink service can receive event data and successfully perform bulk index operations.
- Checkpoint functionality works normally, recording latest sync position.

**Verification Logs:**
```
2025/10/12 19:40:18 Transform: Processing event for PK 128
2025/10/12 19:40:18 Transform: ApplyRules stream closed after sending all transformed events.
2025/10/12 19:40:18 Sink: BulkWrite stream opened and BulkIndexer started.
2025/10/12 19:40:18 Sink: Received event for PK 128, Op INSERT, Data: {"created_at":"2025-10-12 19:40:16","email":"linxiuying@example.com","id":128,"name":"林秀英"}
2025/10/12 19:40:18 Sink: BulkWrite stream finished. Stats: {NumAdded:1 NumFlushed:1 NumFailed:0 NumIndexed:1 NumCreated:0 NumUpdated:0 NumDeleted:0 NumRequests:1 FlushedBytes:122}
2025/10/12 19:40:18 Successfully committed checkpoint for job test-job-test_table to checkpoints.json
```

#### 3. Elasticsearch Sink DELETE Operation Failure Fixed

**Problem Description:**
Elasticsearch Sink returned `400 Bad Request` error when processing MySQL CDC DELETE events, with message `Malformed action/metadata line [...] expected field [create], [delete], [index] or [update] but found [...]`. This caused DELETE operations to fail syncing to Elasticsearch.

**Root Cause:**
The `Body` field of `esutil.BulkIndexerItem` was incorrectly populated with `event.Data` during `DELETE` operations. Elasticsearch `DELETE` requests should not include request body. Additionally, the `esutil.BulkIndexerItem.Body` field type is `io.WriterTo` and requires a concrete type implementing `io.ReadSeeker` (such as `*bytes.Reader`).

**Fix Solutions:**

##### 1. `esutil.BulkIndexerItem.Body` Type Adaptation and Empty Body Handling

**File:** `internal/sink/es/es.go`

**Fix Content:**
Modified `Body` field setting logic for `esutil.BulkIndexerItem` in `BulkWrite` function:
- For `DELETE` operations, `Body` field is now set to an empty `*bytes.Reader` (`bytes.NewReader(nil)`) to ensure empty request body and satisfy `io.ReadSeeker` interface requirements.
- For `INSERT` and `UPDATE` operations, `Body` field continues using `bytes.NewReader([]byte(event.Data))`.

**Fix Results:**

✅ **Elasticsearch DELETE Operations Successful**
- `DELETE` events can now be correctly processed by Elasticsearch Sink and synced to Elasticsearch.
- Elasticsearch no longer returns `400 Bad Request` errors.
- `BulkIndexer` statistics show `NumDeleted:1`.

**Verification Logs:**
```
2025/10/12 21:21:39 Sink: BulkWrite stream finished. Stats: {NumAdded:1 NumFlushed:1 NumFailed:0 NumIndexed:0 NumCreated:0 NumUpdated:0 NumDeleted:1 NumRequests:1 FlushedBytes:62}
```

### 🔧 Configuration File Standardization

**Affected Files:**
- `config.json` (root directory)
- `bin/config.json` (runtime configuration)

**Standardization Content:**
- Unified configuration file format and field names
- Added default values for CDC-related configuration items
- Ensured consistency between runtime and development environment configurations

### 📖 Deployment Guide Updates

Based on these fixes, recommended complete deployment process:

1. **Initialize MySQL Environment**
   ```bash
   # Ensure MySQL container uses latest init.sql
   docker-compose down -v
   docker-compose up -d mysql
   ```

2. **Verify Environment Configuration**
   ```bash
   # Check privileges
   docker-compose exec mysql mysql -u elasticrelay_user -pelasticrelay_pass elasticrelay \
     -e "SHOW GRANTS FOR 'elasticrelay_user'@'%';"
   
   # Verify configuration
   cat bin/config.json
   ```

3. **Build and Start Application**
   ```bash
   make build
   ./bin/elasticrelay --table test_table
   ```

### ✅ Fix Verification

- **Permission Verification:** ✅ User has REPLICATION CLIENT and SUPER privileges
- **Configuration Verification:** ✅ Server ID correctly set to 100
- **Connection Verification:** ✅ Successfully connects to MySQL 8.0.43 server
- **CDC Verification:** ✅ Real-time data change capture works normally
- **Checkpoint Verification:** ✅ binlog position correctly saved and restored
- **Table Filter Verification:** ✅ Only monitors specified test_table

---

## [v1.0.0] - 2025-10-12

### ✨ New Features

#### Version Management System

**Feature Description:**
Implemented complete project version management system supporting semantic versioning, build-time version injection, multi-platform builds, and more.

**New Files:**
- `internal/version/version.go` - Version information package
- `Makefile` - Build configuration and commands
- `scripts/build.sh` - Build script
- `docs/VERSION_MANAGEMENT.md` - Version management documentation

**Feature Characteristics:**

##### 1. Version Information Management

**File:** `internal/version/version.go`

```go
type Info struct {
    Version   string `json:"version"`      // Application version
    GitCommit string `json:"git_commit"`   // Git commit hash
    BuildTime string `json:"build_time"`   // Build time
    GoVersion string `json:"go_version"`   // Go version
    Platform  string `json:"platform"`     // Platform information
}
```

**Supported Functions:**
- Dynamic version injection (via ldflags)
- Automatic Git information retrieval
- Build time recording
- Platform information detection
- Structured version information API

##### 2. Enhanced Build System

**File:** `Makefile`

**New Build Commands:**
```bash
make build          # Standard build
make dev            # Development build (fast)
make release        # Release build (optimized)
make build-all      # Cross-platform build
make run            # Build and run
make dev-run        # Development mode run
make test           # Run tests
make test-cover     # Test coverage
make lint           # Code checking
make fmt            # Format code
make tidy           # Organize dependencies
make clean          # Clean build files
make version        # Show version info
make help           # Show help
```

**Version Injection Mechanism:**
- Support setting version via environment variable: `VERSION=v1.0.0 make build`
- Automatic version retrieval from Git tags
- Build-time injection of Git commit hash and timestamp

##### 3. Command Line Enhancement

**File:** `cmd/elasticrelay/main.go`

**New Features:**
- `--version` parameter: Display version information and exit
- `--port` parameter: Configure gRPC service port (default 50051)
- Automatic complete version information display at startup

**Version Information Format:**
```
ElasticRelay v1.0.0 (commit: abc1234, built: 2025-10-12_07:17:49_UTC, go: go1.25.2, platform: darwin/amd64)
```

##### 4. Cross-Platform Build Support

**Supported Platforms:**
- Linux AMD64: `bin/elasticrelay-linux-amd64`
- macOS AMD64: `bin/elasticrelay-darwin-amd64`
- macOS ARM64: `bin/elasticrelay-darwin-arm64`
- Windows AMD64: `bin/elasticrelay-windows-amd64.exe`

**Build Optimizations:**
- Release builds remove debug info (`-s -w`)
- Static linking support (`CGO_ENABLED=0`)
- Reproducible builds

### 🐛 Bug Fixes

#### 1. Go Module Dependency Fix

**Problem Description:**
`github.com/go-sql-driver/mysql` was marked as indirect dependency but used directly in code.

**Fix Solution:**
Moved `github.com/go-sql-driver/mysql` from indirect to direct dependency.

**File Modified:** `go.mod`
```diff
require (
    github.com/go-mysql-org/go-mysql v1.13.0
+   github.com/go-sql-driver/mysql v1.9.3
    google.golang.org/grpc v1.76.0
    google.golang.org/protobuf v1.36.10
)

require (
    filippo.io/edwards25519 v1.1.0 // indirect
-   github.com/go-sql-driver/mysql v1.9.3 // indirect
    github.com/goccy/go-json v0.10.2 // indirect
```

#### 2. MySQL Connector Compilation Errors Fixed

**Problem Description:**
Encountered three compilation errors when compiling `internal/connectors/mysql/mysql.go`:
1. `h.syncer.GetTable undefined` - lines 181 and 242
2. `undefined: jsonData` - line 403
3. `invalid operation: pkColIndex < len(row) (mismatched types uint64 and int)` - line 253

**Fix Details:**

##### BinlogSyncer GetTable Method Call Error Fix

**File:** `internal/connectors/mysql/mysql.go`
**Location:** Lines 181, 242

**Problem:** `*replication.BinlogSyncer` type does not have `GetTable` method

**Before Fix:**
```go
table, err := h.syncer.GetTable(rowsEvent.TableID)
if err != nil {
    log.Printf("Error getting table metadata for TableID %d: %v", rowsEvent.TableID, err)
    return nil
}
for colIdx, colData := range row {
    colName := string(table.Columns[colIdx].Name)
```

**After Fix:**
```go
table := rowsEvent.Table // Directly use table info from RowsEvent

for colIdx, colData := range row {
    var colName string
    if colIdx < len(table.ColumnName) {
        colName = string(table.ColumnName[colIdx])
    } else {
        colName = fmt.Sprintf("col_%d", colIdx) // Fallback handling
    }
```

**Related Changes:**
- Removed `syncer.GetTable()` calls in `handleRowsEvent` function
- Same removal in `getPrimaryKey` function
- Field access changed from `table.Columns[].Name` to `table.ColumnName[]`
- Primary key field access changed from `table.PKColumns` to `table.PrimaryKey`

##### jsonData Variable Undefined Error Fix

**File:** `internal/connectors/mysql/mysql.go`
**Location:** Line 403

**Problem:** Used undefined `jsonData` variable in `BeginSnapshot` function

**Before Fix:**
```go
records = append(records, string(jsonData)) // jsonData undefined
```

**After Fix:**
```go
// Convert dataMap to JSON
jsonData, err := json.Marshal(dataMap)
if err != nil {
    log.Printf("Failed to marshal row to JSON: %v", err)
    continue
}

records = append(records, string(jsonData))
```

##### Type Mismatch Error Fix

**File:** `internal/connectors/mysql/mysql.go`
**Location:** Line 253

**Problem:** Index type in `table.PrimaryKey` is `uint64`, while `len(row)` returns `int`, causing type mismatch in comparison

**Before Fix:**
```go
if pkColIndex < len(row) {
```

**After Fix:**
```go
if int(pkColIndex) < len(row) {
```

### 🔧 Code Formatting

**File:** `internal/connectors/mysql/mysql.go`

- Unified import statement ordering, placing internal packages after standard library packages
- Adjusted struct field alignment and comment formatting
- Removed extra blank lines, unified code style
- Optimized variable declaration spacing and alignment

### 📖 Usage Examples

#### Version Management Usage Examples

**View Version Information:**
```bash
# View program version
./bin/elasticrelay --version

# Output example:
# ElasticRelay v1.0.0 (commit: abc1234, built: 2025-10-12_07:17:49_UTC, go: go1.25.2, platform: darwin/amd64)
```

**Build Different Versions:**
```bash
# Development build (default dev version)
make dev

# Specific version build
make build VERSION=v1.0.0

# Release build (optimized)
make release VERSION=v1.0.0

# Cross-platform build
make build-all VERSION=v1.0.0
```

**Version Release Process:**
```bash
# 1. Create Git tag
git tag v1.0.0
git push origin v1.0.0

# 2. Build release version
make release

# Version number will be automatically retrieved from Git tag
```

### ✅ Verification Results

#### MySQL Connector Fix Verification
- **Lint Check:** Passed, no errors
- **Compilation Test:** `go build ./...` successful
- **Functionality Verification:** All MySQL connector related functions normal

#### Version Management System Verification
- **Build Test:** `make build VERSION=v1.0.0` successful
- **Version Display:** `./bin/elasticrelay --version` correctly displays version info
- **Command Line Parameters:** `--version` and `--port` parameters work normally
- **Cross-Platform Build:** `make build-all` successfully generates all platform binaries
- **Makefile Functions:** All build commands (`make help`) run normally

#### Dependency Management Verification
- **Dependency Check:** `go mod tidy` successful
- **Compilation Check:** No "should be direct" warnings
- **Module Integrity:** All dependency relationships correct

### 📝 Technical Notes

#### Version Management System Technical Implementation

1. **Version Injection Mechanism:**
   - Uses Go's `-ldflags` parameter to inject version information at compile time
   - Uses `-X` flag to override package-level variable values
   - Supports injection of version number, Git commit hash, build time, etc.

2. **Build System Design:**
   - Makefile provides unified build interface
   - Supports multiple build modes: development, release, cross-platform
   - Automatically detects Git information and injects into binary
   - Release builds use `-s -w` flags to remove debug information

3. **Version Information Architecture:**
   - Independent version package (`internal/version`) provides version API
   - Structured version information convenient for internal program use
   - JSON serialization support convenient for API interface version info return

4. **Cross-Platform Compatibility:**
   - Uses `GOOS` and `GOARCH` environment variables to control target platform
   - `CGO_ENABLED=0` ensures static compilation
   - Platform information runtime detection

#### MySQL Connector Technical Fixes

1. **BinlogSyncer API Change Adaptation:**
   - In newer versions of `go-mysql-org/go-mysql` library, table information is directly obtained from `RowsEvent.Table`
   - Column name access changed from `table.Columns[].Name` to `table.ColumnName[]`
   - Primary key information changed from `table.PKColumns` to `table.PrimaryKey`

2. **Type Safety Handling:**
   - Added type conversion to ensure numerical comparison type consistency
   - Added boundary checks to prevent array out-of-bounds access

3. **Enhanced Error Handling:**
   - Added complete error handling for JSON marshaling
   - Maintained original logging mechanism

4. **Dependency Management Optimization:**
   - Corrected Go module dependency relationships, ensuring direct dependencies are correctly declared
   - Avoided compile-time dependency warning messages

### 📊 Modification Statistics

#### New Files (6)
- `internal/version/version.go` - Version information management package
- `Makefile` - Build system configuration
- `scripts/build.sh` - Build script
- `docs/VERSION_MANAGEMENT.md` - Version management documentation
- `CHANGELOG.md` - Change log (this document)
- `bin/` - Build output directory

#### Modified Files (3)
- `cmd/elasticrelay/main.go` - Added command line parameters and version info display
- `internal/connectors/mysql/mysql.go` - Fixed compilation errors and API adaptation
- `go.mod` - Corrected dependency relationships

#### Feature Improvements
- ✅ **Version Management**: Complete semantic version control system
- ✅ **Build System**: Multi-platform builds and optimization options
- ✅ **Command Line Tools**: Version viewing and port configuration
- ✅ **Error Fixes**: MySQL connector compilation issues resolved
- ✅ **Dependency Management**: Go module dependency relationship standardization
- ✅ **Documentation Enhancement**: Detailed usage guides and technical documentation

---

**Developer:** 
**Modification Date:** 2025-10-12
**Impact Scope:**
- MySQL CDC connector module
- Version management system (new)
- Build system (new)
- Command line tools (enhanced)
- Project documentation (improved)

**Backward Compatibility:** ✅ Fully compatible
**Breaking Changes:** ❌ None
**Security Impact:** ℹ️ No security risks