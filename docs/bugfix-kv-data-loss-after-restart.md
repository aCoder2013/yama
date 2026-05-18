# 修复: KV 存储重启后数据丢失

## 问题背景

`yama-example-raft` 模块基于 `yama-raft` 库实现了一个分布式 KV 存储示例。该示例使用 Raft 共识协议保证数据一致性，通过 RocksDB 持久化 WAL（Write-Ahead Log）和 `HardState`，并通过快照（Snapshot）机制进行日志压缩。

在实际使用中发现：**当应用重启后，之前通过 API 写入的所有 KV 数据全部丢失**，返回空值。这意味着 WAL 恢复机制存在缺陷，Raft 日志无法正确回放到状态机中。

## 复现方式

### 环境要求

- JDK 8
- Maven 3.x

### 复现步骤

```bash
# 1. 构建项目
mvn clean install -DskipTests

# 2. 启动应用（单节点模式）
mvn spring-boot:run -pl yama-example-raft

# 3. 写入数据
curl "http://localhost:9001/yama/raft/api/v1/put?key=name&value=alice"
curl "http://localhost:9001/yama/raft/api/v1/put?key=city&value=beijing"

# 4. 验证数据存在
curl "http://localhost:9001/yama/raft/api/v1/get?key=name"    # 返回: alice
curl "http://localhost:9001/yama/raft/api/v1/get?key=city"    # 返回: beijing

# 5. 关闭应用（Ctrl+C 或 kill 进程）

# 6. 重新启动
mvn spring-boot:run -pl yama-example-raft

# 7. 查询数据 —— 数据已丢失
curl "http://localhost:9001/yama/raft/api/v1/get?key=name"    # 返回: (空)
curl "http://localhost:9001/yama/raft/api/v1/get?key=city"    # 返回: (空)
```

## 根因分析

经代码审查发现共 3 个 bug 导致了此问题：

### Bug 1: 无快照时完全不读取 WAL

**位置**: `RaftNode.java` — `start()` 方法

**问题**: 启动时仅在快照文件存在的情况下才读取 WAL 日志：

```java
// 修复前
Snapshot snapshot = this.snapshotStorage.load();
if (snapshot != null) {
    Result<RaftStateRecord> result = this.commitLog.readAll(snapshot);
    // ... 恢复逻辑
}
// 如果 snapshot == null，什么也不做，直接从零开始！
```

快照只有在 `appliedIndex - snapshotIndex > snapCount`（默认 10）时才会触发创建。因此，当写入条目不足 10 条时不存在快照文件，导致 WAL 中持久化的日志条目在重启时被完全忽略，节点以全新状态启动。

**这是最常见的数据丢失场景。**

### Bug 2: `readAll()` 使用快照的 term 遍历后续条目

**位置**: `RocksDBCommitLog.kt` — `readAll()` 方法

**问题**: 日志条目以 `record-entry-{term}-{index}` 为 key 存储在 RocksDB 中，但 `readAll()` 使用快照的固定 `term` 来遍历所有后续条目：

```kotlin
// 修复前
val term = snap.term        // 固定使用快照的 term
var index = snap.index
while (true) {
    // 所有条目都用同一个 term 查找 —— 错误！
    val entryBytes = this.keyValueStorage.get(
        String.format(ENTRY_KEY_PREFIX, term, ++index).toByteArray()
    )
    if (entryBytes == null || entryBytes.isEmpty()) break
    // ...
}
```

当发生 Leader 选举后 term 会递增。例如快照在 term=2 生成，之后选举使 term 变为 3，新条目存储为 `record-entry-3-12`，但 `readAll()` 查找的是 `record-entry-2-12`，导致新 term 的条目全部丢失。

### Bug 3: 状态机恢复顺序错误

**位置**: `RaftNode.java` — `start()` 和 `publishEntries()` 方法

**问题**: 当快照和 WAL 条目同时存在时，恢复顺序有误：

```java
// 修复前
if (CollectionUtils.isNotEmpty(ents)) {
    this.raftStorage.append(ents);
    this.lastIndex = ents.get(ents.size() - 1).getIndex();
} else {
    this.stateMachine.loadSnapshot(snapshot);  // 只在没有条目时才加载快照！
}
```

快照数据（包含快照时刻的完整 KV 状态）没有被加载到状态机中。条目在空状态上重放，导致丢失快照中的基础数据。更糟糕的是，`publishEntries()` 中当回放到 `lastIndex` 时会调用 `loadSnapshot()`，用过时的快照数据覆盖已重放的条目：

```java
// 修复前 publishEntries() 中
if (entry.getIndex() == this.lastIndex) {
    this.stateMachine.loadSnapshot();  // 用快照覆盖已重放的数据！
}
```

正确的顺序应该是：先加载快照（基础状态），再在其上重放 WAL 条目（增量变更）。

## 修复方式

### 修改 1: Entry key 格式变更（`RocksDBCommitLog.kt`）

将条目存储 key 从 `record-entry-{term}-{index}` 改为 `record-entry-{index}`：

```kotlin
// 修复后
private const val ENTRY_KEY_PREFIX = "record-entry-%d"  // 仅使用 index

private fun saveEntry(entry: Entry): Result<Void> {
    this.keyValueStorage.put(
        String.format(ENTRY_KEY_PREFIX, entry.index).toByteArray(),
        entry.toByteArray()  // Entry protobuf 中已包含 term 信息
    )
}
```

这样 `readAll()` 可以按 index 顺序遍历，不再依赖 term。Entry 的 term 信息已经完整存储在 protobuf 序列化的 value 中，key 中无需冗余存储。

### 修改 2: 新增无参 `readAll()` 方法

在 `CommitLog` 接口和 `RocksDBCommitLog` 中新增不依赖快照的 `readAll()` 方法，从 index=1 开始遍历所有条目：

```kotlin
override fun readAll(): Result<RaftStateRecord> {
    // 读取 HardState
    // 从 index=1 开始遍历所有 Entry
    // 不需要快照参数
}
```

### 修改 3: 修复启动恢复逻辑（`RaftNode.java`）

```java
// 修复后
if (snapshot != null) {
    // 有快照: 读取 WAL → 恢复快照到 raftStorage → 恢复快照到状态机 → 追加条目
    this.raftStorage.applySnapshot(snapshot);
    this.raftStorage.setHardState(raftStateRecord.getHardState());
    this.stateMachine.loadSnapshot(snapshot);  // 始终先加载快照到状态机
    if (CollectionUtils.isNotEmpty(ents)) {
        this.raftStorage.append(ents);
    }
} else {
    // 无快照: 直接读取 WAL 条目和 HardState
    Result<RaftStateRecord> result = this.commitLog.readAll();
    // ... 恢复 HardState 和 entries
}

// 根据是否有已持久化状态决定启动模式
boolean hasExistingState = snapshot != null || this.lastIndex > 0;
if (hasExistingState) {
    this.node = new DefaultNode(raftConfiguration);  // 重启模式
} else {
    this.node = new DefaultNode(raftConfiguration, startPeers);  // 新建集群模式
}
```

同时移除了 `publishEntries()` 中 `lastIndex` 处错误的 `loadSnapshot()` 调用。

## 涉及文件

| 文件 | 模块 | 变更内容 |
|------|------|---------|
| `RocksDBCommitLog.kt` | yama-raft | Entry key 格式改为 index-only；新增无参 `readAll()` |
| `CommitLog.kt` | yama-raft | 接口新增 `readAll()` 方法签名 |
| `RaftNode.java` | yama-example-raft | 无快照时读取 WAL；正确的快照+条目恢复顺序；移除错误的 `loadSnapshot()` |

## 验证

修复后验证覆盖以下 3 种恢复路径：

1. **仅 WAL 恢复**（无快照，写入 < 10 条）：重启后数据完整保留 ✅
2. **快照 + WAL 条目恢复**（写入 > 10 条触发快照后重启）：数据完整保留 ✅
3. **多次重启累积数据**：每次重启后追加数据，再次重启后全部保留 ✅
