# ElasticRelay 大表快照读取优化方案

> 针对 1 亿行以上大表的快照读取性能优化设计文档

---

## 一、问题分析

### 1.1 当前架构的 5 个致命问题

| # | 问题 | 代码位置 | 影响 |
|---|------|----------|------|
| 1 | `SELECT * FROM table` 全表扫描，无分页 | `mysql.go:390` | 长时间占用连接，可能 OOM |
| 2 | 逐行 `json.Marshal`，1 亿次序列化 | `mysql.go:474` | CPU 密集型，GC 压力大 |
| 3 | 单线程串行处理（读→转换→写） | `mysql.go:406-488` | 无法利用多核 |
| 4 | `FLUSH TABLES WITH READ LOCK` 锁表 | `mysql.go:501` | **阻塞所有写操作** |
| 5 | 非自增主键降级为单线程全表扫描 | `manager.go:217-218` | 大表无法并行 |

### 1.2 当前锁表流程的时间线

```
时间轴 ─────────────────────────────────────────────────────────►

  │                    │                                    │
  ▼                    ▼                                    ▼
获取锁              全表扫描开始                          UNLOCK TABLES
(1秒)              (可能 30 分钟)                          (扫描完成)

  │◄───────────────── 锁持续期间 ────────────────────────►│
  │                                                        │
  │  这段时间内：                                          │
  │  ├─ 所有写操作被阻塞（INSERT/UPDATE/DELETE）           │
  │  ├─ DDL 被阻塞（ALTER TABLE 等）                       │
  │  ├─ 事务提交被阻塞                                     │
  │  └─ 连接池可能被占满                                   │
```

### 1.3 `FLUSH TABLES WITH READ LOCK` 的影响链

```
FLUSH TABLES WITH READ LOCK
       │
       ▼
┌─────────────────────────────────────────────┐
│ 影响1: 写操作全部阻塞                        │
│ 业务 INSERT ──► 等待锁 ──► 超时/失败         │
│ 业务 UPDATE ──► 等待锁 ──► 超时/失败         │
│ 业务 DELETE ──► 等待锁 ──► 超时/失败         │
└─────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────┐
│ 影响2: 连接池耗尽                            │
│ 正常连接 ──► 等待锁释放 ──► 连接被占满       │
│ 新请求 ──► 无可用连接 ──► 服务不可用         │
└─────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────┐
│ 影响3: 级联故障                              │
│ 应用超时 ──► 重试 ──► 更多连接 ──► 雪崩     │
└─────────────────────────────────────────────┘
```

---

## 二、InnoDB MVCC 快照机制

### 2.1 RR 隔离级别下的快照创建规则

```
┌─────────────────────────────────────────────────────────────────┐
│  InnoDB RR 隔离级别：快照创建时机                                │
│                                                                 │
│  规则：第一次 SELECT 时创建 Read View（快照）                   │
│                                                                 │
│  BEGIN;                                                         │
│  -- 此时还没有快照                                              │
│                                                                 │
│  SELECT * FROM table;  ← 第一次 SELECT，创建 Read View         │
│  -- 后续所有 SELECT 都基于这个 Read View                        │
│                                                                 │
│  SELECT * FROM table;  ← 复用同一个 Read View                   │
│                                                                 │
│  COMMIT;                                                        │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 `START TRANSACTION WITH CONSISTENT SNAPSHOT`

```
MySQL 提供了一条特殊语句：

  START TRANSACTION WITH CONSISTENT SNAPSHOT;

  这条语句 = BEGIN + 立即创建 Read View

┌─────────────────────────────────────────────────────────────────┐
│  对比：                                                         │
│                                                                 │
│  普通 BEGIN:                                                    │
│    BEGIN;                  ← 不创建 Read View                   │
│    SELECT * FROM t;        ← 才创建 Read View                   │
│                                                                 │
│  WITH CONSISTENT SNAPSHOT:                                      │
│    START TRANSACTION WITH CONSISTENT SNAPSHOT;                  │
│    ↑ 这条语句执行时立即创建 Read View                            │
│    SELECT * FROM t;        ← 复用已创建的 Read View             │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 多连接一致性问题与解决方案

```
问题：
  连接1 BEGIN → 第一次 SELECT (创建 Read View A)
  连接2 BEGIN → 第一次 SELECT (创建 Read View B)

  Read View A ≠ Read View B （创建时间不同）

解决方案：快速连续创建事务

┌─────────────────────────────────────────────────────────────────┐
│  Phase 1: 获取 binlog 位置（短暂锁）                             │
│                                                                 │
│  时间点 T0: FLUSH TABLES WITH READ LOCK                          │
│  时间点 T0: SHOW MASTER STATUS → binlog_pos = 1000               │
│  时间点 T0: UNLOCK TABLES                                        │
│                                                                 │
│  Phase 2: 快速创建所有 Worker 事务                                │
│                                                                 │
│  时间点 T1: Worker 0: START TRANSACTION WITH CONSISTENT SNAPSHOT│
│  时间点 T1: Worker 1: START TRANSACTION WITH CONSISTENT SNAPSHOT│
│  时间点 T1: Worker 2: START TRANSACTION WITH CONSISTENT SNAPSHOT│
│  ...                                                            │
│  时间点 T1: Worker N: START TRANSACTION WITH CONSISTENT SNAPSHOT│
│                                                                 │
│  因为 T0 ~ T1 之间没有新的写入（或写入很少）                      │
│  所有 Worker 的 Read View 基本一致                                │
└─────────────────────────────────────────────────────────────────┘
```

---

## 三、改造方案

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                    改造后数据流                                   │
│                                                                 │
│  ┌─────────┐     ┌─────────────┐     ┌─────────────┐           │
│  │ Reader   │────►│ Transformer │────►│   Writer    │           │
│  │ Pipeline │     │  Pipeline   │     │  Pipeline   │           │
│  │          │     │             │     │             │           │
│  │ 游标分批 │     │ 批量类型转换 │     │ ES Bulk     │           │
│  │ 10000行  │     │ 批量序列化   │     │ 批量写入    │           │
│  └─────────┘     └─────────────┘     └─────────────┘           │
│       │                │                    │                   │
│       ▼                ▼                    ▼                   │
│   channel(5)      channel(5)          channel(5)                │
│   (背压控制)      (背压控制)          (背压控制)                 │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              N 个 Worker 并行                            │   │
│  │  ┌────────┐  ┌────────┐  ┌────────┐      ┌────────┐   │   │
│  │  │Worker 0│  │Worker 1│  │Worker 2│ ...  │Worker N│   │   │
│  │  └────────┘  └────────┘  └────────┘      └────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 多表并行快照架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                    多表并行快照架构                                   │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  Phase 1: 获取快照点（~1秒）                                 │   │
│  │                                                             │   │
│  │  ┌──────────┐    FLUSH+UNLOCK    ┌──────────┐              │   │
│  │  │ 临时连接  │ ─────────────────► │ binlog   │              │   │
│  │  │          │    < 1秒           │ pos:1000 │              │   │
│  │  └──────────┘                    └──────────┘              │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              │                                      │
│                              ▼                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  Phase 2: 创建 Worker 事务（~0.5秒）                         │   │
│  │                                                             │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │   │
│  │  │ Worker 0 │  │ Worker 1 │  │ Worker 2 │  │ Worker N │   │   │
│  │  │ BEGIN TX │  │ BEGIN TX │  │ BEGIN TX │  │ BEGIN TX │   │   │
│  │  │ (RR)     │  │ (RR)     │  │ (RR)     │  │ (RR)     │   │   │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │   │
│  │       │              │              │              │         │   │
│  │       ▼              ▼              ▼              ▼         │   │
│  │  独立连接+事务   独立连接+事务   独立连接+事务   独立连接+事务 │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              │                                      │
│                              ▼                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  Phase 3: 并行扫描（数分钟）                                 │   │
│  │                                                             │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │   │
│  │  │ Worker 0 │  │ Worker 1 │  │ Worker 2 │  │ Worker N │   │   │
│  │  │          │  │          │  │          │  │          │   │   │
│  │  │ 表A 分片0│  │ 表A 分片1│  │ 表B 分片0│  │ 表C 分片0│   │   │
│  │  │          │  │          │  │          │  │          │   │   │
│  │  │ SELECT * │  │ SELECT * │  │ SELECT * │  │ SELECT * │   │   │
│  │  │ FROM A   │  │ FROM A   │  │ FROM B   │  │ FROM C   │   │   │
│  │  │ WHERE id │  │ WHERE id │  │ WHERE id │  │ WHERE id │   │   │
│  │  │ BETWEEN  │  │ BETWEEN  │  │ BETWEEN  │  │ BETWEEN  │   │   │
│  │  │ 1~100000 │  │100001~   │  │ 1~100000 │  │ 1~100000 │   │   │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │   │
│  │       │              │              │              │         │   │
│  │       ▼              ▼              ▼              ▼         │   │
│  │   ┌─────────────────────────────────────────────────────┐   │   │
│  │   │              ES Bulk Index                          │   │   │
│  │   └─────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.3 改造点 1：游标分批读取

**适用所有表，无论主键是否自增：**

```go
// 核心思想：用主键游标替代 SELECT *

const batchSize = 10000  // 每次读取 1 万行
lastCursor := int64(0)

for {
    // 游标分页：WHERE pk > lastCursor ORDER BY pk LIMIT batchSize
    query := fmt.Sprintf(
        "SELECT * FROM %s WHERE %s > %d ORDER BY %s LIMIT %d",
        tableName, pkColumn, lastCursor, pkColumn, batchSize,
    )

    rows, err := tx.QueryContext(ctx, query)
    // ... 处理本批数据 ...

    if batchCount < batchSize {
        break  // 扫完了
    }
}
```

**优势：**
- 每次只处理 1 万行，内存占用恒定
- 不锁表，MVCC 读
- 随时可以暂停/恢复

### 3.4 改造点 2：非自增主键并行分片

```go
// 用 MOD(CRC32(pk), N) 分片，适用于任何主键类型

func (m *ParallelSnapshotManager) createCursorBasedChunks(
    task *TableTask,
    indexInfo *IndexInfo,
) ([]*ChunkTask, error) {

    chunkCount := int(task.TotalRows / int64(task.ChunkSize))
    if chunkCount > m.config.MaxConcurrentChunks {
        chunkCount = m.config.MaxConcurrentChunks
    }

    var chunks []*ChunkTask
    for i := 0; i < chunkCount; i++ {
        chunk := &ChunkTask{
            ID:       fmt.Sprintf("%s_chunk_%d", task.TableName, i),
            ModShard: i,        // 分片 ID
            ModTotal: chunkCount, // 总分片数
        }
        chunks = append(chunks, chunk)
    }
    return chunks, nil
}

// Worker 查询：
// SELECT * FROM table WHERE MOD(CRC32(pk), N) = i ORDER BY pk
```

### 3.5 改造点 3：流水线并行处理

```
┌──────────┐    ┌─────────────┐    ┌─────────────┐
│ Reader   │───►│ Transformer │───►│   Writer    │
│ Pipeline │    │  Pipeline   │    │  Pipeline   │
│          │    │             │    │             │
│ 游标分批 │    │ 类型转换     │    │ ES Bulk     │
│ 5000行   │    │ JSON序列化   │    │ 批量写入    │
└──────────┘    └─────────────┘    └─────────────┘
     │                │                    │
     ▼                ▼                    ▼
  channel(5)      channel(5)          channel(5)
  (背压控制)      (背压控制)          (背压控制)
```

```go
func (w *SnapshotWorker) processChunkPipeline(ctx context.Context, chunk *ChunkTask) error {
    rawChan := make(chan []*RawRow, 5)
    serializedChan := make(chan []*SerializedRecord, 5)

    // Stage 1: Reader
    go func() {
        defer close(rawChan)
        w.readerPipeline(ctx, chunk, 5000, rawChan)
    }()

    // Stage 2: Transformer
    go func() {
        defer close(serializedChan)
        w.transformerPipeline(ctx, rawChan, 5000, serializedChan)
    }()

    // Stage 3: Writer
    for records := range serializedChan {
        w.writerPipeline(ctx, records, chunk)
    }
    return nil
}
```

### 3.6 改造点 4：Worker 事务创建

```go
// 每个 Worker 独立连接 + 事务
func (w *SnapshotWorker) beginSnapshotTx(ctx context.Context) error {
    conn, err := w.db.Conn(ctx)
    if err != nil {
        return err
    }
    w.conn = conn

    // ★ 关键：立即创建 Read View
    _, err = conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
    if err != nil {
        conn.Close()
        return err
    }

    return nil
}

// 在事务内查询
func (w *SnapshotWorker) processChunk(ctx context.Context, chunk *ChunkTask) {
    // ★ 用 tx 查询，读取快照数据
    rows, err := w.tx.QueryContext(ctx, query)
    // ...
}
```

---

## 四、完整时序图

```
┌─────────────────────────────────────────────────────────────────────┐
│  时间轴                                                              │
│                                                                     │
│  T0          T1          T2          T3          T4          T5    │
│  │           │           │           │           │           │      │
│  ▼           ▼           ▼           ▼           ▼           ▼      │
│                                                                     │
│  ┌─────┐                                                                │
│  │LOCK │  ← 短暂锁（~1秒）                                              │
│  └──┬──┘                                                                │
│     │                                                                   │
│     ▼                                                                   │
│  ┌─────────────┐                                                        │
│  │SHOW MASTER  │ → binlog_pos = 1000                                   │
│  │STATUS       │                                                        │
│  └──┬──────────┘                                                        │
│     │                                                                   │
│     ▼                                                                   │
│  ┌─────┐                                                                │
│  │UNLOCK│  ← 立即释放                                                   │
│  └──┬──┘                                                                │
│     │                                                                   │
│     ▼                                                                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ 快速创建 Worker 事务（~0.5秒）                                   │   │
│  │                                                                 │   │
│  │ Worker0: START TRANSACTION WITH CONSISTENT SNAPSHOT → RV@T1    │   │
│  │ Worker1: START TRANSACTION WITH CONSISTENT SNAPSHOT → RV@T1    │   │
│  │ Worker2: START TRANSACTION WITH CONSISTENT SNAPSHOT → RV@T1    │   │
│  │ ...                                                             │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│     │                                                                   │
│     ▼                                                                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ 并行扫描（数分钟）                                               │   │
│  │                                                                 │   │
│  │ 所有查询都基于 RV@T1，看到一致的数据                              │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│     │                                                                   │
│     ▼                                                                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ 完成：逐个 ROLLBACK（只读事务，回滚很快）                        │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 五、性能对比

### 5.1 方案对比

```
┌──────────────────────────────────────────────────────────────────┐
│              1亿行表性能对比                                      │
│                                                                  │
│  方案                    │ 耗时估算    │ 对线上影响  │ 实现复杂度 │
│  ────────────────────────┼────────────┼────────────┼──────────  │
│  当前(全表扫描+逐行处理)  │ 30min~2h   │ 🔴 严重    │ —          │
│  + 游标分批              │ 10~20min   │ 🟡 中等    │ ★☆☆☆☆      │
│  + 游标分批 + 并行Worker │ 3~8min     │ 🟡 中等    │ ★★☆☆☆      │
│  + 流水线(读/转/写并行)  │ 2~5min     │ 🟡 中等    │ ★★★☆☆      │
│  + 事务快照替代锁表      │ 2~5min     │ 🟢 轻微    │ ★☆☆☆☆      │
│  + 读从库                │ 2~5min     │ 🟢 无影响  │ ★☆☆☆☆      │
│  + 批量序列化优化        │ 1.5~4min   │ 🟢 无影响  │ ★★☆☆☆      │
│  + 限流保护              │ 3~6min     │ 🟢 轻微    │ ★☆☆☆☆      │
│                                                                  │
│  最优组合: 游标分批 + 并行Worker + 事务快照 + 流水线 + 读从库     │
│  预期: 1亿行 2~5分钟，对线上业务几乎无感知                        │
└──────────────────────────────────────────────────────────────────┘
```

### 5.2 改造优先级

```
P0 — 立即改（低风险高收益）:
  ├─ ① 游标分批替代 SELECT *（解决全表扫描）
  ├─ ② 事务快照替代 FLUSH TABLES WITH READ LOCK（解决锁表）
  └─ ③ 非自增主键用 MOD 分片（解决单线程降级）

P1 — 短期改（中等收益）:
  ├─ ④ 流水线处理（读/转/写并行）
  └─ ⑤ 批量序列化优化（减少 GC）

P2 — 长期改（基础设施）:
  └─ ⑥ 支持读从库配置
```

---

## 六、关键知识点总结

### 6.1 事务快照与锁的对比

```
┌────────────────────┬──────────────┬──────────────┬──────────────┐
│                    │ 当前方案     │ 方案A        │ 方案B        │
│                    │ FLUSH+全表锁 │ 事务快照     │ 短暂锁       │
├────────────────────┼──────────────┼──────────────┼──────────────┤
│ 锁持续时间         │ 整个快照过程 │ 无锁         │ 1~2秒        │
│ 对写操作影响       │ 🔴 完全阻塞  │ 🟢 无影响    │ 🟢 几乎无影响│
│ 对读操作影响       │ 轻微         │ 🟢 无影响    │ 🟢 无影响    │
│ 数据一致性         │ ✅ 强一致    │ ✅ 快照一致  │ ⚠️ 弱一致*  │
│ 实现复杂度         │ 低           │ 低           │ 低           │
│ 适用引擎           │ 所有引擎     │ 仅 InnoDB    │ 所有引擎     │
└────────────────────┴──────────────┴──────────────┴──────────────┘

* 方案B的一致性说明：
  锁释放后到 SELECT 开始前的瞬间可能有新写入，
  但对于 CDC 场景，这部分写入会被后续的 CDC 捕获，
  不会丢失数据，只是可能有少量重复（幂等写入可处理）。
```

### 6.2 多连接一致性保证

| 问题 | 答案 |
|------|------|
| 如何基于快照点创建 RR 事务？ | `START TRANSACTION WITH CONSISTENT SNAPSHOT` |
| 为什么不用 `BEGIN`？ | `BEGIN` 不立即创建 Read View，第一次 SELECT 时才创建 |
| 多连接如何保证一致性？ | 所有连接快速连续执行 `WITH CONSISTENT SNAPSHOT`，Read View 创建时间接近 |
| 锁表时间多长？ | 仅获取 binlog 位置的 1~2 秒 |
| 扫描过程锁表吗？ | 不锁，MVCC 读不影响写 |
| 事务什么时候结束？ | 所有 Worker 完成后 `ROLLBACK` |

**核心原理**：`START TRANSACTION WITH CONSISTENT SNAPSHOT` 在语句执行时立即创建 Read View，所有后续查询都基于这个快照。多个连接快速连续执行该语句，它们的 Read View 创建时间接近，因此看到的数据基本一致。

---

## 七、改造代码示例

### 7.1 改造后的 BeginSnapshot（mysql.go）

```go
func (s *Server) BeginSnapshot(req *pb.BeginSnapshotRequest, stream pb.ConnectorService_BeginSnapshotServer) error {
    dsn := s.config.DSN()
    db, err := sql.Open("mysql", dsn)
    if err != nil {
        return err
    }
    defer db.Close()

    // ★ 改造1：用事务快照替代全局锁
    tx, err := db.BeginTx(stream.Context(), &sql.TxOptions{
        Isolation: sql.LevelRepeatableRead,
        ReadOnly:  true,
    })
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // ★ 改造2：获取 binlog 位置（短暂锁）
    masterPos, err := getMasterPositionWithBriefLock(stream.Context(), db)
    if err != nil {
        return err
    }

    // ★ 改造3：获取主键列
    pkColumn, err := getPrimaryKeyColumn(tx, req.TableName)
    if err != nil {
        return err
    }

    // ★ 改造4：游标分批扫描
    const batchSize = 10000
    lastCursor := int64(0)
    var records []string

    for {
        query := fmt.Sprintf(
            "SELECT * FROM %s WHERE %s > %d ORDER BY %s LIMIT %d",
            req.TableName, pkColumn, lastCursor, pkColumn, batchSize,
        )

        rows, err := tx.QueryContext(stream.Context(), query)
        if err != nil {
            return err
        }

        batchCount := 0
        for rows.Next() {
            record, pkValue, err := s.scanRowWithPK(rows, cols, req.TableName, pkColumn)
            if err != nil {
                continue
            }
            records = append(records, record)
            lastCursor = pkValue
            batchCount++
        }
        rows.Close()

        if batchCount < batchSize {
            break
        }

        if len(records) >= snapshotChunkSize {
            stream.Send(&pb.SnapshotChunk{
                Records:          records,
                SnapshotBinlogFile: masterPos.Name,
                SnapshotBinlogPos:  masterPos.Pos,
            })
            records = nil
        }
    }

    if len(records) > 0 {
        stream.Send(&pb.SnapshotChunk{
            Records:          records,
            SnapshotBinlogFile: masterPos.Name,
            SnapshotBinlogPos:  masterPos.Pos,
        })
    }

    return nil
}

// ★ 短暂锁获取 binlog 位置后立即释放
func getMasterPositionWithBriefLock(ctx context.Context, db *sql.DB) (mysql.Position, error) {
    db.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK")

    var file string
    var pos uint32
    err := db.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(&file, &pos, nil, nil, nil)

    db.ExecContext(ctx, "UNLOCK TABLES")  // 立即释放

    if err != nil {
        return mysql.Position{}, err
    }
    return mysql.Position{Name: file, Pos: pos}, nil
}
```

### 7.2 改造后的 Worker（parallel/worker.go）

```go
type SnapshotWorker struct {
    ID          int
    db          *sql.DB
    conn        *sql.Conn
    tx          *sql.Tx
    esClient    ESClient
    transformer *DataTransformer
}

func (w *SnapshotWorker) Run(ctx context.Context, chunkQueue <-chan *ChunkTask) {
    // 创建一致性快照事务
    if err := w.beginSnapshotTx(ctx); err != nil {
        log.Printf("Worker #%d: failed to begin snapshot tx: %v", w.ID, err)
        return
    }
    defer w.tx.Rollback()

    for {
        select {
        case chunk, ok := <-chunkQueue:
            if !ok {
                return
            }
            w.processChunk(ctx, chunk)
        case <-ctx.Done():
            return
        }
    }
}

func (w *SnapshotWorker) beginSnapshotTx(ctx context.Context) error {
    conn, err := w.db.Conn(ctx)
    if err != nil {
        return err
    }
    w.conn = conn

    // ★ 立即创建 Read View
    _, err = conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
    return err
}

func (w *SnapshotWorker) processChunk(ctx context.Context, chunk *ChunkTask) {
    query := w.buildChunkQuery(chunk)

    // ★ 在事务内查询
    rows, err := w.tx.QueryContext(ctx, query)
    if err != nil {
        return
    }
    defer rows.Close()

    var batch []*Record
    for rows.Next() {
        record, err := w.scanRow(rows, chunk)
        if err != nil {
            continue
        }
        batch = append(batch, record)

        if len(batch) >= 1000 {
            w.processBatch(ctx, batch)
            batch = batch[:0]
        }
    }

    if len(batch) > 0 {
        w.processBatch(ctx, batch)
    }
}
```

### 7.3 改造后的分片逻辑（parallel/manager.go）

```go
func (m *ParallelSnapshotManager) createTableChunks(task *TableTask) ([]*ChunkTask, error) {
    indexInfo, err := m.analyzeTableIndexes(task.TableName)
    if err != nil {
        return nil, err
    }

    task.PrimaryKeyColumn = indexInfo.PrimaryKeyColumn

    // ★ 改造：非自增主键也用 MOD 分片
    if indexInfo.HasAutoIncrementPK {
        return m.createIDBasedChunks(task, indexInfo)
    }
    return m.createCursorBasedChunks(task, indexInfo)
}

// 新增：基于 MOD 的通用分片
func (m *ParallelSnapshotManager) createCursorBasedChunks(
    task *TableTask,
    indexInfo *IndexInfo,
) ([]*ChunkTask, error) {

    chunkCount := int(task.TotalRows / int64(task.ChunkSize))
    if chunkCount == 0 {
        chunkCount = 1
    }
    if chunkCount > m.config.MaxConcurrentChunks {
        chunkCount = m.config.MaxConcurrentChunks
    }

    var chunks []*ChunkTask
    for i := 0; i < chunkCount; i++ {
        chunks = append(chunks, &ChunkTask{
            ID:        fmt.Sprintf("%s_chunk_%d", task.TableName, i),
            TableTask: task,
            ChunkID:   i,
            ModShard:  i,
            ModTotal:  chunkCount,
        })
    }
    return chunks, nil
}

// Worker 查询构建
func (w *SnapshotWorker) buildChunkQuery(chunk *ChunkTask) string {
    table := chunk.TableTask.TableName
    pk := chunk.TableTask.PrimaryKeyColumn

    // MOD 分片查询
    if chunk.ModTotal > 0 {
        return fmt.Sprintf(
            "SELECT * FROM %s WHERE MOD(CRC32(%s), %d) = %d ORDER BY %s",
            table, pk, chunk.ModTotal, chunk.ModShard, pk,
        )
    }

    // 自增主键范围分片
    return fmt.Sprintf(
        "SELECT * FROM %s WHERE %s >= %d AND %s < %d ORDER BY %s",
        table, pk, chunk.StartID, pk, chunk.EndID, pk,
    )
}
```

---

## 八、总结

### 核心改动清单

| 改动 | 文件 | 行数 | 风险 |
|------|------|------|------|
| 游标分批替代全表扫描 | `mysql.go` | ~50 | 低 |
| 事务快照替代锁表 | `mysql.go` | ~30 | 低 |
| 非自增主键 MOD 分片 | `manager.go` | ~40 | 低 |
| Worker 独立事务 | `worker.go` | ~60 | 中 |
| 流水线处理 | `worker.go` | ~80 | 中 |
| 批量序列化优化 | `mysql.go` | ~30 | 低 |

### 预期效果

```
改造前:
  - 1 亿行全表扫描: 30min~2h
  - 锁表时间: 全程阻塞
  - 对线上影响: 🔴 严重

改造后:
  - 1 亿行并行扫描: 2~5min
  - 锁表时间: 1~2 秒
  - 对线上影响: 🟢 几乎无感知
```

---

> 文档生成时间: 2025-05-24
> 适用版本: ElasticRelay v1.4.44