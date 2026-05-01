# Mini MapReduce Engine

A recruiter-ready distributed systems mini clone of Google's MapReduce, written in Go. It runs a master node plus multiple worker nodes over gRPC, splits large input files, executes map tasks in parallel, shuffles intermediate key/value pairs, runs reducers, retries failed work, and produces deterministic Word Count output.

## Architecture

```text
                         +----------------+
                         |  submit client |
                         +-------+--------+
                                 |
                                 | SubmitJob
                                 v
                      +----------+-----------+
                      |   Master Coordinator |
                      |----------------------|
                      | split input          |
                      | schedule map tasks   |
                      | track task leases    |
                      | monitor heartbeats   |
                      | schedule reduce      |
                      +----------+-----------+
                                 |
               gRPC: Register / Heartbeat / RequestTask / SubmitResult
                                 |
         +-----------------------+-----------------------+
         |                       |                       |
         v                       v                       v
   +-----+------+          +-----+------+          +-----+------+
   | Worker 1   |          | Worker 2   |          | Worker 3   |
   | map/reduce |          | map/reduce |          | map/reduce |
   +-----+------+          +-----+------+          +-----+------+
         |                       |                       |
         +-----------------------+-----------------------+
                                 |
                                 v
                     Shared local storage volume
                    chunks / intermediate / output
```

## How It Works

1. A client submits a job with an input file, output directory, reducer count, and chunk size.
2. The master splits the input into line-preserving chunks.
3. Workers register with the master, heartbeat, and poll for work.
4. Map workers run Word Count and write one intermediate JSONL file per reducer partition.
5. The master waits for all map tasks, then creates reduce tasks.
6. Reducers group values by key, sort keys, aggregate counts, and write `part-xxxxx.txt`.
7. If a worker misses heartbeats or a task lease expires, the master reassigns the task.

## Repository Layout

```text
cmd/
  master/       master node entrypoint
  worker/       worker node entrypoint
  submit/       job submission CLI
internal/
  master/       scheduler, leases, job state machine
  worker/       worker runtime and heartbeat loop
  rpc/          gRPC service registration, client, DTOs
  mapreduce/    map/reduce engine, partitioning, Word Count
  storage/      chunks, intermediate files, reducer outputs
  utils/        structured logging
proto/          API contract
docs/           architecture and interview notes
examples/       demo input and expected output
test/           integration tests
```

## Local Development

```bash
make tidy
make fmt
make build
make test
make race
```

Run the cluster manually:

```bash
go run ./cmd/master -addr=:50051 -storage-root=data
```

In separate terminals:

```bash
go run ./cmd/worker -master=localhost:50051 -id=worker-1 -storage-root=data
go run ./cmd/worker -master=localhost:50051 -id=worker-2 -storage-root=data
go run ./cmd/worker -master=localhost:50051 -id=worker-3 -storage-root=data
```

Submit Word Count:

```bash
go run ./cmd/submit -master=localhost:50051 -input=examples/large.txt -output=data/output -reducers=3 -chunk-size=128
```

## Docker Demo

```bash
docker compose config
docker compose up --build
```

The compose cluster starts one master and three workers. The master auto-submits `examples/large.txt` and writes output under `/data/output` in the shared Docker volume.

Inspect output:

```bash
docker compose exec master sh -c "cat /data/output/part-*.txt | sort"
```

Compare with:

```bash
cat examples/expected-output.txt
```

## Example Output

```text
mapreduce    2
systems      3
workers      3
large        3
reduce       2
```

Output is partitioned across reducer files:

```text
data/output/part-00000.txt
data/output/part-00001.txt
data/output/part-00002.txt
```

## Log Examples

Logs are structured JSON via `log/slog`:

```json
{"component":"master","msg":"job submitted","job_id":"job-000001","chunks":4,"reducers":3}
{"component":"worker","worker_id":"worker-1","msg":"task started","task_id":"job-000001-map-00000","type":"MAP"}
{"component":"master","msg":"task reclaimed","task_id":"job-000001-map-00001","worker_timeout":true}
{"component":"master","msg":"job completed","job_id":"job-000001","output_path":"data/output"}
```

## Fault Tolerance

- Workers send heartbeats to the master.
- Every assigned task receives a lease deadline.
- Expired leases are returned to the pending queue.
- Worker error reports also return tasks to pending.
- Late stale results are rejected unless the worker still owns the active lease.

This mirrors the core recovery property of MapReduce: map and reduce tasks are deterministic, so failed work can be safely retried by another worker.

## Design Decisions

- **Pull scheduling:** workers ask for tasks, which keeps workers horizontally scalable.
- **Master-owned state:** task lifecycle is centralized and easy to reason about.
- **Partitioned intermediate files:** each map writes one file per reducer, creating clean shuffle boundaries.
- **JSONL intermediate data:** simple to inspect during demos and tests.
- **Shared local storage:** Docker Compose uses a named volume as a stand-in for a distributed filesystem.
- **gRPC transport:** the control plane uses real gRPC calls matching `proto/mapreduce.proto`.

## Interview Talking Points

- Explain how task leases recover work after worker crashes.
- Discuss why late task results must be rejected.
- Compare shared local storage here with HDFS/GCS/S3 in production.
- Explain the map, shuffle, sort, reduce boundaries.
- Describe how master persistence and leader election would make the system production-grade.

More detail:

- [Architecture](docs/ARCHITECTURE.md)
- [Design Decisions](docs/DESIGN_DECISIONS.md)
- [Fault Tolerance](docs/FAULT_TOLERANCE.md)
- [Interview Guide](docs/INTERVIEW_GUIDE.md)
