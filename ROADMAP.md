# rstmdb Roadmap

## Current Features (v0.3.0)

### Core

- **State machine definitions** - JSON DSL for defining states, transitions, and guards
- **Instance lifecycle** - Create, read, and soft-delete state machine instances
- **Guard expressions** - Conditional transitions with boolean expressions (`ctx.field > 10 && ctx.enabled`)
- **Event application** - Atomic state transitions with payload support
- **Idempotency** - Client-provided keys for safe retries and exactly-once semantics

### Durability

- **Write-Ahead Logging** - All mutations persisted before acknowledgment
- **Segment management** - 64 MiB segments with automatic rotation
- **Configurable fsync** - Trade-off between durability and performance (every_write, every_n, every_ms, never)
- **Per-instance snapshots** - Point-in-time state capture
- **Automatic compaction** - Background WAL cleanup based on thresholds
- **Crash recovery** - Startup restores persisted machines and instance snapshots, then replays the remaining WAL

### Replication

- **WAL streaming** - Primary streams WAL entries to replicas, in async or sync mode
- **Read-only replicas** - Replicas serve reads and reject writes, with offset-based catch-up
- **Lag thresholds** - `max_lag_seconds` and `max_lag_entries`, with per-replica metrics
- **Replication auth** - Hashed replication tokens with rotation

### Backup and Restore

- **Offline backup** - `rstmdb backup`, `rstmdb restore` and `rstmdb verify` with the `.rstmbak` format
- **Hot backup** - `BACKUP_BEGIN`, `BACKUP_CHUNK` and `BACKUP_END` protocol operations on a running server

### Networking

- **RCP binary protocol** - Efficient TCP-based protocol with CRC32C validation
- **TLS/mTLS support** - Server certificates and optional client certificate verification
- **Token authentication** - Bearer tokens with SHA-256 hashing
- **Session management** - Connection state, feature negotiation, idle timeouts

### Streaming

- **WATCH_INSTANCE** - Real-time subscriptions to individual instance changes
- **WATCH_ALL** - Subscribe to all events with filtering (by machine, states, event type)
- **Event broadcasting** - Push-based notifications to all subscribers

### Observability

- **Prometheus metrics** - Request counts, latencies, WAL stats, connection gauges
- **Grafana dashboard** - Pre-built visualizations
- **Health endpoints** - `/health` and `/healthz` on the metrics server
- **Logging** - `tracing` output, filtered with `RUST_LOG`

### Tooling

- **Rust client library** - Async API with connection management
- **Client libraries** - [Python](https://github.com/rstmdb/rstmdb-py), [JavaScript/TypeScript](https://github.com/rstmdb/rstmdb-js), [Go](https://github.com/rstmdb/rstmdb-go), [Java](https://github.com/rstmdb/rstmdb-java) and [.NET](https://github.com/rstmdb/rstmdb-dotnet), each in its own repository
- **CLI with REPL** - Interactive shell for administration
- **Docker support** - Container images and compose files
- **Load testing** - Configurable benchmark scripts

---

## Planned Features

### Phase 1: Client Libraries

Expand ecosystem with official client libraries:

- [x] **Python client** - [rstmdb-py](https://github.com/rstmdb/rstmdb-py)
- [x] **TypeScript/Node.js client** - [rstmdb-js](https://github.com/rstmdb/rstmdb-js)
- [x] **Go client** - [rstmdb-go](https://github.com/rstmdb/rstmdb-go)
- [x] **Java client** - [rstmdb-java](https://github.com/rstmdb/rstmdb-java)
- [x] **.NET client** - [rstmdb-dotnet](https://github.com/rstmdb/rstmdb-dotnet)

### Phase 2: High Availability

Enable read scaling and fault tolerance without consensus:

- [x] **WAL streaming to replicas** - Real-time log shipping
- [x] **Read-only follower mode** - Serve reads from replicas
- [x] **Health checks** - Replica lag monitoring and alerts
- [ ] **External failover orchestration** - Integration with orchestrators (Kubernetes, Consul)

### Phase 3: Consensus & Automatic Failover

Full cluster support with automatic leader election:

- [ ] **Raft consensus integration** - Distributed agreement protocol
- [ ] **Automatic leader election** - No manual failover required
- [ ] **Cluster membership management** - Dynamic node addition/removal
- [ ] **Split-brain protection** - Quorum-based writes

### Phase 4: Operational Excellence

Production hardening and enterprise features:

- [ ] **Encryption at rest** - AES-256 for WAL and snapshots
- [ ] **Point-in-time recovery** - Restore to any WAL offset
- [ ] **OpenTelemetry integration** - Distributed tracing support
- [ ] **Configuration hot-reload** - Update settings without restart
- [ ] **Admin API** - HTTP endpoints for operations

### Phase 5: Performance & Scale

Horizontal scaling and optimization:

- [ ] **Sharding support** - Distribute instances across nodes
- [ ] **Read replicas** - Dedicated read-only nodes
- [ ] **WAL compression** - Reduce storage and I/O
- [ ] **Batch optimization** - Improved throughput for bulk operations
- [ ] **Memory-mapped WAL** - Optional mmap for read performance

---

## Non-Goals

These are explicitly out of scope for rstmdb:

| Feature | Rationale |
|---------|-----------|
| **Multi-instance transactions** | Use sagas or choreography patterns instead |
| **SQL query interface** | Focus on state machine operations, not general queries |
| **Secondary indexes** | Query by instance ID; external search for complex queries |
| **Embedded mode** | Server-based architecture only |
| **Pluggable storage engines** | WAL-based design is fundamental |

---

## Contributing

We welcome contributions. Areas where help is especially appreciated:

1. **Client libraries** - Improvements to the existing clients and additional languages
2. **Documentation** - Tutorials, examples, API reference improvements
3. **Testing** - Chaos testing, fuzz testing, load testing
4. **Integrations** - Kubernetes operators, Terraform providers

See [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

---

## Version History

| Version | Date | Highlights |
|---------|------|------------|
| 0.3.0 | 2026-10-14 | WAL streaming replication, backup and restore, hot backup, durable idempotency |
| 0.2.0 | 2026-03-18 | Client libraries, FLUSH_ALL, instance re-creation |
| 0.1.1 | 2026-02-07 | Instance listing with filtering and pagination, BSL-1.1 license |
| 0.1.0 | 2026-02-01 | Initial release with core features |

---

## Feedback

Have suggestions for the roadmap? Open an issue or discussion on GitHub.
