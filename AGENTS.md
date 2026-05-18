# AGENTS.md

## Cursor Cloud specific instructions

### Project overview

Yama is a distributed service discovery/configuration platform built on the Raft consensus algorithm. It is a Maven multi-module Java/Kotlin project with 7 modules: `yama-common`, `yama-messaging`, `yama-storage`, `yama-raft`, `yama-server`, `yama-example`, `yama-example-raft`.

### JDK requirement

This project **must** use JDK 8 (`JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64`). JDK 21 (the VM default) causes Lombok/maven-compiler-plugin module-access failures. The `~/.bashrc` is configured with the correct `JAVA_HOME` and `PATH`.

### Build / Test / Run

- **Build**: `mvn clean install -DskipTests` (from repo root)
- **Test**: `mvn test` (from repo root; all modules)
- **Run yama-example-raft**: `mvn spring-boot:run -pl yama-example-raft` — starts on port 9001, single-node Raft cluster with KV store API at `/yama/raft/api/v1/put?key=...&value=...` and `/yama/raft/api/v1/get?key=...`
- **Run yama-server**: Cannot use `mvn spring-boot:run` (plugin version not pinned in parent POM, Maven resolves to latest incompatible version). Instead run via classpath: `java -cp yama-server/target/classes:$(mvn dependency:build-classpath -pl yama-server -Dmdep.outputFile=/dev/stdout -q) com.song.yama.server.YamaServerApplication` — starts on port 8080.

### Gotchas

- The `RocksDBKeyValueStorageTest` in `yama-common` requires `/home/admin/rocksdb/test` directory to exist (hardcoded path). The update script creates this.
- The `yama-example-raft` module uses its own Spring Boot parent (`spring-boot-starter-parent:2.1.1.RELEASE`) separate from the main parent POM.
- No external services (databases, message queues, etc.) are required — RocksDB is embedded via JNI.
