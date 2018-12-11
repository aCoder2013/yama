# yama-raft

## 什么是Raft

**Raft**是一个分布式一致性算法，充分的利用了可复制状态机以及日志。其最核心的设计目标就是易于理解。在性能、fault-tolerance等方面来看有点类似**Paxos**，但不同
之处在于，论文较为清晰了描述了所有的主要流程以及其中一些细节问题，而Paxos我们知道非常难以理解。

当构建一个分布式系统时，一个非常重要的设计目标就是**fault tolerance**。如果系统基于Raft协议实现，那么当其中一个节点挂掉，或者
发生了网络分区等异常情况时，只要大多数节点仍然能够正常通讯，整个集群就能够正常对外提供服务而不会挂掉。

关于Raft更多的细节，这里建议直接阅读论文: "In Search of an Understandable Consensus Algorithm"
(https://ramcloud.stanford.edu/raft.pdf) by Diego Ongaro and John Ousterhout.

## 介绍

Etcd的Raft库已经在生产环境得到了非常广泛的应用，有力的支撑了etcd、K8S、Docker Swarm、TiDB/TiKV等系统。因此相比从零开始造轮子，我这次选择了移植etcd的库，
并用Java和Kotlin语言进行了重写。

## 特性

yama-raft实现了Raft协议的完整特性，包括:

- Leader election
- Log replication
- Log compaction
- Membership changes
- Leadership transfer extension
- Efficient linearizable read-only queries served by both the leader and followers
- More efficient lease-based linearizable read-only queries served by both the leader and followers

yama-raft也包含一些优化:

- Optimistic pipelining to reduce log replication latency
- Flow control for log replication
- Batching Raft messages to reduce synchronized network I/O calls
- Batching log entries to reduce disk synchronized I/O
- Writing to leader's disk in parallel
- Internal proposal redirection from followers to leader
- Automatic stepping down when the leader loses quorum

## 用法

TODO