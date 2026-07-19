# Idempotent Delta table writes in foreachBatch

A small lab that shows why `foreachBatch` writes to Delta tables can produce
duplicates, and how the `txnAppId` / `txnVersion` writer options fix that.

The scenario: a stream of zip files, each containing several JSON files.
Every JSON file is appended to a Delta table with its own write. When one
JSON file is malformed, the micro-batch fails halfway — some writes are
already committed, but the streaming checkpoint is not. A restart replays
the batch and duplicates the rows that did land.

This repo accompanies the blog post
[Idempotent Delta table writes in foreachBatch](https://klimenko.com/blog/2026/delta-idempotent-writes).

## Requirements

- [uv](https://docs.astral.sh/uv/)
- Java 8, 11 or 17 (for Spark 3.5)

## Run

```bash
uv sync

uv run python 01_the_problem.py    # naive pipeline: restart duplicates rows
uv run python 02_the_fix.py        # txnAppId/txnVersion: the replay is skipped
uv run python 03_the_helper.py     # the same fix behind a small wrapper
uv run python 04_under_the_hood.py # SetTransaction actions in _delta_log
```

Each script starts from a clean slate (it wipes the local `data/` folder)
and prints what happens at every step.

## Files

- `01_the_problem.py` — the naive zip-ingest pipeline; a failed batch plus a
  restart leaves duplicate rows in the table.
- `02_the_fix.py` — the same pipeline with `txnAppId`/`txnVersion` on every
  write; on the retry, Delta skips the writes that already committed.
- `03_the_helper.py` + `idempotent.py` — the fix extracted into a tiny
  `IdempotentWriter` wrapper.
- `04_under_the_hood.py` — batch writes with and without the options, and the
  `SetTransaction` actions they leave in `_delta_log`. Also shows that a plain
  `writeStream` into Delta records the same actions automatically, with the
  streaming query id as `txnAppId`.
- `helpers.py` — Spark session, zip fixtures, stream runner, log inspection.

## Docs

- [Delta docs: Idempotent table writes in foreachBatch](https://docs.delta.io/delta-streaming/#idempotent-table-writes-in-foreachbatch)
- [Databricks docs: Use foreachBatch for idempotent table writes](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/delta-lake#idempot-write)
