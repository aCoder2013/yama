# AGENTS.md

## Cursor Cloud specific instructions

### Project Overview

Yama is a Java/Kotlin Maven multi-module project implementing a Raft consensus-based distributed service discovery and configuration platform. See `README.md` for a brief description.

### Prerequisites

- **JDK 8** is required (`JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64`). The project targets Java 8 source/target and uses Kotlin 1.3.11, which is incompatible with JDK 16+ due to module access restrictions (Lombok annotation processor fails).
- **Maven** (`mvn`) must be installed (e.g. `sudo apt-get install -y maven`).

### Build & Test

- Build all modules: `JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 mvn clean install -DskipTests`
- Run all tests: `JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 mvn test`
- The `RocksDBKeyValueStorageTest` in `yama-common` requires directory `/home/admin/rocksdb/test` to exist with write permissions. Create it before running tests: `sudo mkdir -p /home/admin/rocksdb/test && sudo chmod 777 /home/admin/rocksdb/test`
- The `ExampleRaftApplicationTests` in `yama-example-raft` starts a Spring Boot app on port 9001. If the app is already running on that port, exclude this module from tests: `mvn test -pl !yama-example-raft`

### Running the Application

The primary runnable application is `yama-example-raft`, a Spring Boot app that demonstrates the Raft KV store:

```
cd yama-example-raft
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 mvn spring-boot:run
```

It starts on port 9001 with a single-node Raft cluster. API endpoints:
- `GET /yama/raft/api/v1/put?key=<key>&value=<value>` — store a key-value pair
- `GET /yama/raft/api/v1/get?key=<key>` — retrieve a value by key

### Architecture Notes

- No external services (databases, message queues, Docker) are required. RocksDB is embedded via `rocksdbjni`.
- The project has 7 Maven modules: `yama-common`, `yama-storage`, `yama-messaging`, `yama-raft`, `yama-server` (skeleton), `yama-example` (empty parent), `yama-example-raft` (runnable demo).
