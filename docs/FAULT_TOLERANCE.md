# Fault Tolerance

This project implements practical worker failure handling with heartbeats and task leases.

## Heartbeats

Workers send `Heartbeat(worker_id)` periodically. The master records the last heartbeat timestamp per worker.

If a worker misses heartbeats longer than `WorkerTimeout`, the master treats tasks assigned to that worker as reclaimable.

## Task Leases

When a task is assigned, the master records:

- assigned worker ID
- lease deadline
- attempt count

If the lease expires before a valid result arrives, the task returns to `PENDING`.

## Retry Semantics

Workers report either outputs or an error. On error, the master makes the task pending again. On timeout, another worker can pick up the same task.

Late results are rejected unless:

- the task is still `RUNNING`
- the reporting worker still owns the active lease

This prevents stale worker writes from winning after reassignment.

## Failure Scenarios

```text
Worker crashes during map
  -> heartbeat stops
  -> lease expires or worker timeout triggers
  -> map task returns to pending
  -> another worker remaps the input chunk

Worker crashes during reduce
  -> reduce task returns to pending
  -> another worker rereads deterministic intermediate files
  -> final part file is regenerated

Worker finishes after lease expiration
  -> SubmitTaskResult is rejected
  -> completed retry result remains authoritative
```

## Why This Is Enough For A Mini Clone

The system demonstrates the essential MapReduce recovery model: deterministic tasks, durable intermediate files, and coordinator-driven retry. It does not yet solve master failover, which is a natural next production extension.
