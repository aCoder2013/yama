# Yama Raft Jepsen 验证指南

[Jepsen](https://jepsen.io/) 用于在故障注入下验证分布式系统的线性一致性（linearizability）。本目录包含针对 `yama-example-raft` HTTP KV API 的 Jepsen 测试套件。

## 架构

```
Jepsen 控制进程 (Clojure)
    │
    ├── Client workers ──HTTP──► n1:19001 / n2:19002 / n3:19003
    │                              │
    │                              ▼
    │                        yama-example-raft (Raft KV)
    │
    └── Nemesis ──► 网络分区 / 进程故障 (SSH 模式)
```

- **写入**：`GET /yama/raft/api/v1/put?key=...&value=...`，客户端在写入后轮询 `get` 确认可见
- **读取**：`GET /yama/raft/api/v1/get?key=...`，503 记为 `:fail`（ReadIndex 不可用）
- **Checker**：`knossos` 线性一致性检查（`cas-register` 模型，独立 key）

## 快速开始（本地无 SSH）

### 前置条件

- JDK 8
- [Leiningen](https://leiningen.org/)（`lein`）
- 已构建 `yama-example-raft` jar

### 一键运行

```bash
cd jepsen-yama
chmod +x scripts/*.sh
./scripts/run-local-jepsen.sh
```

脚本会：

1. 构建 `yama-example-raft` fat jar（如不存在）
2. 在 `19001/19002/19003` 启动三节点集群
3. 运行 60 秒 Jepsen 线性一致性测试
4. 测试结束后停止集群

结果目录：`jepsen-yama/store/latest/`

### 分步运行

```bash
# 1. 构建 Java 应用
cd .. && mvn package -pl yama-example-raft -am -DskipTests

# 2. 启动集群
cd jepsen-yama
./scripts/local-cluster.sh start

# 3. 运行 Jepsen
lein run test --nodes n1,n2,n3 --local --time-limit 60 --concurrency 2n --ops-per-key 32

# 4. 停止集群
./scripts/local-cluster.sh stop
```

### 查看结果

```bash
lein run serve
# 浏览器打开 http://127.0.0.1:8080
```

关注 `:valid?` 字段：

- `:valid? true` — 未发现线性一致性违反
- `:valid? false` — 存在违反，查看 `store/latest/` 下 timeline 与 history

## SSH 多节点模式（完整故障注入）

在 3 台 Debian 节点上通过 SSH 部署（标准 Jepsen 流程）：

```bash
# 在控制机上构建 jar
mvn package -pl yama-example-raft -am -DskipTests
cp ../yama-example-raft/target/example-raft-0.0.1-SNAPSHOT.jar jepsen-yama/target/example-raft.jar

# 配置 cluster-ports.edn 中各节点端口
cat > jepsen-yama/cluster-ports.edn <<'EOF'
{:n1 9001, :n2 9002, :n3 9003}
EOF

# 运行（含随机网络分区 nemesis）
cd jepsen-yama
lein run test \
  --nodes n1,n2,n3 \
  --username root \
  --password root \
  --time-limit 120 \
  --concurrency 2n \
  --ops-per-key 64
```

Nemesis 会周期性执行 `partition-random-halves`（iptables 随机半数分区）。

## 文件说明

| 文件 | 说明 |
|------|------|
| `src/jepsen/yama/core.clj` | 测试入口、generator、checker、nemesis |
| `src/jepsen/yama/client.clj` | HTTP 客户端（read/write/cas） |
| `src/jepsen/yama/db.clj` | 节点部署与生命周期 |
| `src/jepsen/yama/ports.clj` | 节点端口映射 |
| `scripts/local-cluster.sh` | 本地三节点启停 |
| `scripts/run-local-jepsen.sh` | 本地一键验证 |

## 与现有 Java 故障测试的关系

| 层级 | 工具 | 验证目标 |
|------|------|----------|
| 单元/集成 | `KvRaftFaultTest`、`RaftKvFaultIntegrationTest` | 确定性场景、快速回归 |
| 形式化一致性 | **Jepsen** | 并发历史下的线性一致性证明/反例 |

## 已知限制

- 本地模式（`--local`）当前不注入网络分区，主要验证无故障下的并发读写历史
- `put` 异步提交，客户端通过轮询 `get` 确认写入可见
- Jepsen 0.1.19 在 teardown 阶段可能因 pure generator 状态更新报错而中断，操作历史仍会写入 `store/latest/jepsen.log`，完整 `results.edn` 生成仍在完善中
- 完整分区测试需 SSH 多节点环境（后续支持）
