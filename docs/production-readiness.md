<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ClickHouse sink production-readiness plan

The connector provides at-least-once delivery: `SinkWriter.flush(false)` flushes pending JDBC
batches before a Flink checkpoint. A failure after ClickHouse accepts a batch but before the
checkpoint completes can replay that batch. Exactly-once is not claimed.

An ambiguous JDBC outcome (for example, a socket failure after ClickHouse accepted an insert but
before the client received the response) is treated as an at-least-once failure. A Flink restart or
a retry can therefore produce duplicates unless the target table and ingest design provide their
own deduplication.

## P0: correctness and failure propagation

- [x] Apply configured sink parallelism to `SinkV2Provider`.
- [x] Use the Sink V2 checkpoint callback instead of an ineffective `CheckpointedFunction` on the
  sink object.
- [x] Propagate final and asynchronous flush failures while still closing statements and
  connections.
- [x] Validate batch size, flush interval, retry count, and sink parallelism eagerly.
- [x] Copy retained `RowData` and reduce upsert records by primary-key value independent of
  `RowKind`.
- [x] Run legacy JUnit 4 tests on the JUnit Platform used by the build.
- [x] Verify checkpoint completion and recovery after a TaskManager restart with an unbounded SQL
  job and filesystem-backed checkpoint storage.
- [ ] Add fault-injection integration tests for failure before, during, and after `executeBatch`.
- [x] Define duplicate behavior for ambiguous JDBC outcomes.
- [ ] Add an integration test that injects an ambiguous post-commit JDBC failure.

## P1: resilience and operability

- [x] Retain owned rows and recreate connections/statements before replaying retryable failures,
  including direct-to-shard writers.
- [x] Classify retryable and permanent JDBC errors and add bounded jittered backoff.
- [x] Add records, estimated sent bytes, batches, latency, retries, failures, buffered-record, and
  buffered-byte metrics.
- [x] Add configurable connect and socket timeouts with safe defaults.
- [ ] Add query and close timeouts without interrupting an acknowledged insert.
- [x] Bound retained rows by `sink.batch-size`.
- [x] Trigger a flush at `sink.max-buffered-bytes` using estimated binary-row size. A single record
  can exceed the threshold and is still accepted as one batch.
- [ ] Add health checks for every shard connection and deterministic replica failover.

## P2: ClickHouse semantics and compatibility

- [ ] Prefer idempotent append designs and document `insert_deduplication_token` limitations.
- [x] Treat `ALTER UPDATE/DELETE` as asynchronous mutations and expose their operational cost.
- [ ] Test ReplicatedMergeTree and Distributed tables across topology changes.
- [x] Publish an explicit Flink, ClickHouse server, Java, and clickhouse-jdbc compatibility matrix.
- [ ] Replace reflective catalog metadata access with supported driver APIs.

## Release gates

Current verified baseline: 26 core tests plus four containerized append, changelog upsert, delete
mutation, and checkpoint/TaskManager-recovery tests pass with Java 17, Flink 2.1.0, ClickHouse
24.8, and clickhouse-jdbc 0.6.4. The remaining unchecked items below still block a
production-ready release.

- Unit tests must execute (a zero-test build is a failure).
- Container integration tests cover append, upsert, delete, sharding, checkpoint recovery, and
  object reuse.
- A soak test runs with checkpointing, backpressure, network interruption, and TaskManager restart.
- Delivery guarantees and unsupported combinations are documented in the connector options.
- CI runs formatting, checkstyle, RAT, unit tests, integration tests, and binary compatibility.
