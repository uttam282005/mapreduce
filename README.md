# Simplified MapReduce (educational)

This repository contains a small, simplified implementation of the MapReduce programming model in Go. It is intended as an educational example to show the high-level components, RPC interactions, intermediate file layout, and basic worker/coordinator interactions. This is not a production-ready MapReduce system.

Contents
- `coordinator.go` — Coordinator implementation: creates jobs, assigns them to workers, monitors progress, and exposes RPC handlers.
- `worker.go` — Worker implementation: registers with the coordinator, requests jobs, runs map or reduce handlers, and writes intermediate / output files.
- `rpc.go` — Shared RPC types and common data structures (KeyValue, Job, Args/Replies, metadata).

Overview
--------

The implementation follows the typical MapReduce flow:

- A coordinator creates map jobs (one per input file). Each map job produces several intermediate files partitioned by reduce task id.
- Workers register with the coordinator and periodically request jobs via RPC.
- When given a `map` job a worker reads the input file, runs a user-provided `mapf(filename, contents) -> []KeyValue` and writes intermediate JSON-encoded `KeyValue` entries into files named `mr-<mapJobID>-<reduceID>`.
- After all map tasks are done the coordinator moves to the `reduce` phase. Each reduce worker reads the `mr-*-<reduceID>` files across all map tasks, groups values by key, calls a user-provided `reducef(key, []values) -> string` and writes final output files `mr-out-<reduceID>`.

Key types & RPCs
-----------------

Shared types are in `rpc.go`:

- `KeyValue{Key, Value}` — intermediate / map output pair.
- `Job{JobID, Type, FileName, StartTime}` — job descriptor used by the coordinator.
- `MetaData{Nreduce, Nmap}` — metadata the worker fetches on startup.

Important RPC handlers implemented on the Coordinator (in `coordinator.go`):

- `GetJob(args *GetJobArgs, reply *GetJobReply)` — Worker calls this to request a job. Coordinator returns job metadata (FileName, JobID, Type). If there are no idle jobs it sets `reply.State = "wait"`.
- `GetMetaData(args *GetMetaDataArgs, reply *GetMetaDataReply)` — Worker asks for `Nmap` and `Nreduce` values.
- `ReportJobDone(args *JobCompleteArgs, reply *JobCompleteReply)` — Coordinator handler that marks a job as done (deletes record and sets status).
- `RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply)` — Register a worker so the coordinator can record it.

How the worker writes intermediate & output files
-------------------------------------------------

- Map phase: each worker computes `[]KeyValue` and writes them to one file per reduce partition. File naming (intended): `mr-<mapTaskID>-<reduceID>` (JSON-encoded entries). The choice of reduce partition is `ihash(key) % Nreduce`.
- Reduce phase: each worker reads all `mr-<mapID>-<reduceID>` files for its reduce id, decodes JSON entries, sorts/group-by key, calls `reducef` and writes final output `mr-out-<reduceID>` (atomic rename from a temp file is used).

License & attribution
---------------------

This code is a simplified educational implementation and should be treated as such. If you use or modify it, keep attribution and document any additional changes.

Acknowledgements
----------------

This repo is a tiny reconstruction of the classical MapReduce lab used in distributed systems coursework. It focuses on clarity over completeness.
