# KV 基于 Raft 故障测试指南

本文档说明 `yama-example-raft` 分布式 KV 示例的故障测试体系、已修复问题及运行方式。

## 架构概览

```
Client ──GET /put,/get──► ApiController
                              │
                              ▼
                        kVStateMachine (ConcurrentHashMap)
                              │ propose(JSON)
                              ▼
                          RaftNode
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
              RocksDB WAL   HTTP RPC   Snapshot
              (持久化)    (节点间)    (日志压缩)
```

- **写入**：`put` 将 KV 编码为 JSON 提案提交给 Raft Leader，异步返回 `ok`
- **读取**：`get` 直接读本地内存，不经过 `readIndex`（可能读到过期数据）
- **持久化**：`ReadyProcessor` 循环处理 `pullReady()`，写 WAL、复制、应用到状态机

## 测试分层

| 层级 | 模块 | 测试类 | 覆盖场景 |
|------|------|--------|----------|
| WAL 持久化 | `yama-raft` | `RocksDBCommitLogRecoveryTest` | 空 WAL、跨 term 条目、快照+增量条目、index-only key |
| KV 状态机 | `yama-example-raft` | `KVStateMachineTest` | JSON 编解码、快照往返、ConfChange 忽略 |
| 多节点故障 | `yama-example-raft` | `KvRaftFaultTest` | 复制、分区、Leader 切换、少数派、日志收敛 |
| **JSON 故障** | `yama-example-raft` | `JsonKvRaftFaultTest` | Unicode/转义/类 JSON 值在分区、Leader 切换下的一致性 |
| **集成故障（HTTP）** | `yama-example-raft` | `RaftKvFaultIntegrationTest` | 真实三节点 Spring Boot、Leader 宕机、网络分区、Follower 追赶 |
| 端到端重启 | `yama-example-raft` | `RaftKvRecoveryIntegrationTest` | 仅 WAL 重启、快照后重启 |
| Raft 协议 | `yama-raft` | `RaftFaultInjectionTest` | 非对称丢包、少数派、旧 Leader 重入（库层） |

## 故障场景与测试用例

### 1. 三节点正常复制 (`threeNodeReplication`)

Leader 写入后，所有节点 KV 一致。

### 2. 非对称丢包阻断复制 (`asymmetricDropBlocksReplication`)

Leader → Follower 链路断开时，新写入只在 Leader 本地提交（若无法形成 quorum 则不会提交），Follower 读不到新数据。

### 3. 分区恢复后收敛 (`partitionHealKvConverges`)

分区期间写入在恢复后通过日志复制同步到所有节点。

### 4. Leader 故障切换 (`leaderFailoverPreservesWrites`)

旧 Leader 被隔离后，新 Leader 选举成功，历史数据保留且新写入可继续。

### 5. 少数派无法选举 (`minorityPartitionCannotElectLeader`)

5 节点集群中 {4,5} 与多数派隔离后无法产生 Leader。

### 6. 旧 Leader 重入 (`oldLeaderRejoinsAndKvConverges`)

被隔离的旧 Leader 重新加入后，日志与 KV 与当前 Leader 收敛，集群仍只有一个 Leader。

### 7. 大量写入一致性 (`manyWritesStayConsistent`)

15 次连续写入（超过默认快照阈值 10），所有节点数据一致。

### 8. Follower 过期读 (`followerReadMayBeStaleBeforeSync`)

被隔离的 Follower 无法收到新写入，可读到隔离前的旧数据，读不到新 key——说明 `get` 是本地读，不保证线性一致。

### 9. WAL 仅恢复 (`walOnlyRestartPreservesKv`)

写入 < 10 条（不触发快照），进程重启后数据完整保留。

### 10. 快照恢复 (`snapshotRestartPreservesKv`)

写入 > 10 条触发快照后重启，快照基础数据 + WAL 增量条目均正确恢复。

## 集成故障测试（`RaftKvFaultIntegrationTest`）

基于真实 Spring Boot 三节点集群，通过 HTTP API 验证故障行为。测试基础设施：

- `RaftKvClusterHarness` — 启动 3 个 Spring Boot 实例，动态分配端口
- `FaultInjectingHttpMessagingService` — 可注入网络丢包/隔离（`integration-fault-test` profile）
- `FaultInjectionRegistry` — 跨节点共享的故障规则

| 用例 | 场景 |
|------|------|
| `threeNodeHttpReplication` | HTTP 写入 Leader，三节点 GET 一致 |
| `writeOnLeaderReadableOnFollowers` | Follower 可读 Leader 写入 |
| `leaderCrashFailoverAndContinueWrite` | 停止 Leader 进程，剩余节点选主并继续写入 |
| `crashedFollowerCatchesUpOnRestart` | Follower 宕机期间写入，重启后追赶日志 |
| `networkPartitionMakesFollowerStaleUntilHeal` | 隔离 Follower，验证过期读，恢复后收敛 |
| `isolatedOldLeaderMajorityReElects` | 隔离旧 Leader，多数派重新选主 |
| `manyWritesConsistentAcrossThreeNodes` | 12 次 HTTP 写入三节点一致 |
| `specialJsonValuesReplicateOverHttp` | HTTP 写入 Unicode/转义/类 JSON 值，三节点一致 |
| `specialJsonValuesConvergeAfterPartition` | 分区期间写入特殊 JSON 值，恢复后收敛 |

```bash
# 仅集成故障测试（约 1 分钟，每个用例独立 JVM）
mvn test -pl yama-example-raft -Dtest=RaftKvFaultIntegrationTest
```

## 测试基础设施

### `KvRaftCluster`（内存模拟）

位于 `yama-example-raft/src/test/.../support/`，提供：

- `newCluster(n)` — 创建 n 节点内存集群
- `electLeader(id)` / `put(id, key, val)` / `sync()`
- `drop(from, to, rate)` / `cut(a, b)` / `isolate(id)` / `recover()`
- `assertKvConsistent(key, expected)` — 断言所有节点 KV 一致

不依赖 Spring Boot 和 HTTP，测试速度快、确定性强。

### 端到端重启测试

`RaftKvRecoveryIntegrationTest` 通过 `SpringApplication` 启动真实应用：

```properties
com.song.yama.raft.data-dir=/tmp/yama-test-xxx   # 隔离测试数据目录
com.song.yama.raft.id=1
com.song.yama.raft.servers=127.0.0.1:9001
```

流程：启动 → 等待 Leader 选举 → 写入 → 关闭 → 再启动 → 验证。

## 已修复问题

### 历史问题（详见 `docs/bugfix-kv-data-loss-after-restart.md`）

1. 无快照时不读 WAL
2. WAL 条目 key 绑定 term 导致跨 term 丢失
3. 快照与 WAL 回放顺序错误

### 本次修复

| 问题 | 位置 | 修复 |
|------|------|------|
| 无快照重启时状态机未同步回放 WAL | `RaftNode.start()` | 新增 `replayEntriesToStateMachine()`，启动时直接回放 |
| `publishSnapshot` 从磁盘重载而非使用传入快照 | `RaftNode.publishSnapshot()` | 改为 `loadSnapshot(snapshotToSave)` |
| 节点关闭时 RocksDB JNI 崩溃 | `RaftNode.close()` | 先停止线程池再关闭 WAL；ReadyProcessor 检查 `running` |
| 多快照文件时加载旧快照 | `SimpleSnapshotStorage.load()` | 按 index 取最新快照 |
| 测试数据目录不可配置 | `RaftProperties` | 新增 `com.song.yama.raft.data-dir` 配置项 |

## 运行测试

```bash
# 全量测试
mvn test

# 仅 KV 故障测试
mvn test -pl yama-example-raft -Dtest=KvRaftFaultTest,JsonKvRaftFaultTest,KVStateMachineTest,RaftKvRecoveryIntegrationTest

# 仅 WAL 恢复测试
mvn test -pl yama-raft -Dtest=RocksDBCommitLogRecoveryTest

# 手动验证（单节点）
mvn spring-boot:run -pl yama-example-raft
curl "http://localhost:9001/yama/raft/api/v1/put?key=name&value=alice"
curl "http://localhost:9001/yama/raft/api/v1/get?key=name"
# 重启后再次 get，应返回 alice
```

## 已知限制与后续改进

| 限制 | 说明 | 建议 |
|------|------|------|
| 非线性一致读 | `get` 不经过 `readIndex` | 生产环境应使用 `node.readIndex()` + 等待 appliedIndex |
| 写入异步确认 | `put` 返回 `ok` 不代表已提交 | 客户端应轮询或使用回调/版本号 |
| HTTP 消息静默失败 | `HttpMessagingService` 吞掉 IO 异常 | 应上报 `reportUnreachable` 触发 Raft 重试 |
| 单节点需等待选举 | 启动后需等待 ~1s 才有 Leader | 可在启动后自动 `campaign()` |
| `preVote` 未启用 | 注释掉了 `setPreVote(true)` | 可减少分区恢复时的无效选举 |

## 多节点手动故障演练

```bash
# 终端 1
mvn spring-boot:run -pl yama-example-raft \
  -Dspring-boot.run.arguments="--server.port=9001 --com.song.yama.raft.id=1 --com.song.yama.raft.servers=127.0.0.1:9001;127.0.0.1:9002;127.0.0.1:9003"

# 终端 2
mvn spring-boot:run -pl yama-example-raft \
  -Dspring-boot.run.arguments="--server.port=9002 --com.song.yama.raft.id=2 --com.song.yama.raft.servers=127.0.0.1:9001;127.0.0.1:9002;127.0.0.1:9003"

# 终端 3
mvn spring-boot:run -pl yama-example-raft \
  -Dspring-boot.run.arguments="--server.port=9003 --com.song.yama.raft.id=3 --com.song.yama.raft.servers=127.0.0.1:9001;127.0.0.1:9002;127.0.0.1:9003"

# 写入后分别在三个节点 get，验证一致性
# 使用 iptables 或断开网络模拟分区（需在真实环境操作）
```

## 相关文件

| 文件 | 说明 |
|------|------|
| `yama-example-raft/.../KvRaftFaultTest.java` | 多节点故障测试（内存模拟） |
| `yama-example-raft/.../JsonKvRaftFaultTest.java` | JSON 编码值故障测试（内存模拟） |
| `yama-example-raft/.../RaftKvFaultIntegrationTest.java` | 集成故障测试（真实 HTTP 三节点） |
| `yama-example-raft/.../support/RaftKvClusterHarness.java` | 三节点 Spring Boot 集群管理 |
| `yama-example-raft/.../RaftKvRecoveryIntegrationTest.java` | 重启恢复集成测试 |
| `yama-example-raft/.../support/KvRaftCluster.java` | 内存集群模拟器 |
| `yama-raft/.../RocksDBCommitLogRecoveryTest.kt` | WAL 恢复单元测试 |
| `docs/bugfix-kv-data-loss-after-restart.md` | 历史数据丢失修复记录 |
