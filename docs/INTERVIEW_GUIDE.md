# Interview Guide

Use this project to tell a clear distributed systems story.

## One-Minute Pitch

This is a Go mini clone of MapReduce. A master splits input, leases map tasks to workers over gRPC, workers write partitioned intermediate files, the master transitions the job into reduce tasks, reducers aggregate sorted key groups, and task leases plus heartbeats recover from worker failures.

## What To Demo

1. Run `make test`.
2. Run `docker compose up --build`.
3. Show master logs for job submission and task completion.
4. Show worker logs for map and reduce execution.
5. Inspect `part-*.txt` output and compare it to `examples/expected-output.txt`.

## Strong Talking Points

- Pull-based scheduling makes workers simple and elastic.
- Task leases are a clean recovery primitive for crashed or slow workers.
- Late results are rejected, which avoids stale writes.
- Map output is partitioned by reducer ID, giving deterministic shuffle boundaries.
- Local shared storage is intentionally simple and can be swapped for GCS, S3, or HDFS.
- The code separates control plane, worker runtime, RPC contract, storage, and pure MapReduce logic.

## Trade-Offs To Acknowledge

- The master state is in memory.
- There is no speculative execution.
- There is no master leader election.
- JSONL intermediate files favor debuggability over raw speed.
- The worker currently ships with Word Count, though the engine accepts mapper and reducer functions.

## Good Extension Ideas

- Add persisted master metadata.
- Add a job registry with multiple mapper/reducer plugins.
- Add Prometheus metrics.
- Add streaming job status.
- Add cancellation and backpressure.
- Add benchmark tests for large inputs.
