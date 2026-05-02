# Architecture

This repository models the core control plane and data plane of MapReduce using a small Go codebase.

```text
Client
  |
  | SubmitJob(input, output, reducers, chunk size)
  v
+-----------------------------+
| Master Coordinator          |
| - splits input              |
| - owns task state           |
| - leases work to workers    |
| - retries expired work      |
| - creates reduce tasks      |
+---------------+-------------+
                |
                | gRPC
                v
     +----------+----------+
     |          |          |
+----+---+ +----+---+ +----+---+
| Worker | | Worker | | Worker |
| map    | | map    | | reduce |
+----+---+ +----+---+ +----+---+
     |          |          |
     +----------+----------+
                |
                v
        Shared local storage
        chunks/intermediate/output
```

## Components

- `cmd/master`: starts the gRPC master service.
- `cmd/worker`: starts a worker process that registers, heartbeats, polls, executes, and reports.
- `cmd/submit`: small CLI for submitting Word Count jobs.
- `internal/master`: task scheduler and job state machine.
- `internal/worker`: worker execution loop and heartbeat loop.
- `internal/rpc`: gRPC service registration, client, codec, and request/response types.
- `internal/mapreduce`: map/reduce function interfaces, Word Count implementation, partitioning, and execution engine.
- `internal/storage`: line-preserving input splitting, intermediate JSONL files, grouped reads, and reducer output.

## Job State Machine

```text
SubmitJob
  -> split input
  -> create MAP tasks
  -> workers complete all MAP tasks
  -> create REDUCE tasks
  -> workers complete all REDUCE tasks
  -> job COMPLETED
```

Tasks move through:

```text
PENDING -> RUNNING -> COMPLETED
    ^         |
    |         v
    +----- timeout/error
```

## Data Flow

1. The master splits the input file into chunk files under `data/<job>/chunks`.
2. A worker maps one chunk into key/value pairs.
3. The mapper partitions each key using FNV hashing and writes one intermediate file per reducer.
4. A reducer reads all map outputs for its reducer ID, groups values by key, sorts keys, and writes `part-xxxxx.txt`.

## Why Shared Local Storage?

The goal is to show distributed scheduling, retries, and phase orchestration without requiring HDFS, GCS, or S3. Docker Compose gives every container access to the same named volume, which acts as shared cluster storage for this mini clone.
