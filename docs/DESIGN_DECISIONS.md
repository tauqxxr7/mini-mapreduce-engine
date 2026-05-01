# Design Decisions

## Master-Owned Coordination

The master owns all job and task metadata. Workers are intentionally stateless executors. This mirrors the original MapReduce model and makes worker failure recovery straightforward: if a worker disappears, its leased task can be handed to another worker.

## Pull-Based Scheduling

Workers request work instead of the master pushing tasks. Pull scheduling keeps the master simple, naturally handles workers joining late, and avoids needing worker-specific inbound servers.

## Task Leases

Every assigned task has a lease deadline. A late result is rejected if the lease is no longer active. This prevents stale workers from overwriting work after another worker has retried and completed the same task.

## Intermediate Partitioning

Map output is partitioned by reducer ID using a stable FNV hash. Each mapper writes `map-N-reduce-M.jsonl`, so reduce task `M` reads only the files assigned to it.

## JSONL Intermediate Format

JSONL is human-readable and simple to debug. It is not the fastest format, but it is excellent for a portfolio project because candidates can inspect intermediate files directly.

## No External Protoc Requirement

The repository includes `proto/mapreduce.proto` as the API contract and registers equivalent gRPC methods in Go with a JSON codec. This keeps local builds lightweight while still using real gRPC transport.

## Structured Logging

All binaries emit JSON logs through `log/slog`. Logs include component names, worker IDs, job IDs, task IDs, and task types, which makes Docker logs useful during demos.

## Known Production Extensions

- Persist master metadata to survive coordinator restarts.
- Add leader election for highly available masters.
- Replace local storage with object storage or a distributed filesystem.
- Add streaming RPCs for progress reporting.
- Add metrics such as task latency, retries, queue depth, and worker liveness.
