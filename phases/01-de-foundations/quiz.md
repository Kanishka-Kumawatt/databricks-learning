# Phase 1 Quiz — no notes

Answer all 20 in writing, **then** open the answer key. Score: 1 point per correct answer, ½ for partial.

## Basic (1–10)

1. In one sentence, what is data engineering?
2. OLTP or OLAP: (a) checkout database, (b) monthly revenue dashboard, (c) Delta Lake tables queried by a SQL warehouse.
3. Why is columnar storage faster for `SUM(amount)` over a billion rows?
4. What does the `_delta_log` folder contain, and what does it give a table?
5. Name one structured, one semi-structured and one unstructured data example.
6. ETL or ELT: ADF copies raw files to ADLS, then Databricks builds Silver/Gold.
7. What does `trigger(availableNow=True)` do?
8. What belongs in Bronze, and why is it append-only?
9. Define the grain of a fact table.
10. Define idempotency.

## Medium (11–17)

11. In a watermark load, why must the watermark be updated after the write?
12. Why does a timestamp-based incremental load miss hard deletes, and name one fix.
13. Why is a single 20 GB gzip CSV file slow to read in Spark?
14. `WHERE NOT (amount > 0)` doesn't find rows with NULL amount. Why?
15. Why use surrogate keys in dimensions? Give two reasons.
16. Which failures should be retried automatically, and what property must the task have?
17. What is a semi-additive measure? Example and the wrong way to aggregate it.

## Tricky (18–20)

18. A job is green, quality checks pass, but the dashboard shows yesterday's data. What failed and how do you detect it automatically?
19. A streaming job's checkpoint folder was deleted and the job restarted, writing with plain append. What happened to the target?
20. A restart framework runs `DELETE today` then `INSERT today` as two statements. Is it safe? Explain.

---

<details><summary><b>Answer key</b></summary>

1. Designing, building and operating systems that move data from sources into reliable, usable datasets for consumers — repeatedly, at scale, recoverably.
2. (a) OLTP, (b) OLAP, (c) OLAP.
3. Reads only the `amount` column (pruning), compresses it well, processes values vectorised.
4. JSON commit files (and Parquet checkpoints) listing added/removed data files per version → ACID commits, isolation, DML, schema enforcement, time travel, history.
5. e.g. SQL table / JSON API response / PDF invoice.
6. ELT.
7. Processes all data available at start (possibly several micro-batches), then stops — incremental batch with checkpoints.
8. Raw data as received + audit columns; append-only so history is preserved for replay and audit.
9. What one row represents (e.g. one order line).
10. Running once or many times with the same input yields the same final state.
11. If the watermark advances and the write fails, the window is skipped forever (data loss); write-first means a retry reprocesses it (safe with MERGE).
12. Deleted rows produce nothing to extract. Fixes: CDC, soft-delete flag, periodic key reconciliation (anti-join source keys vs target).
13. Gzip isn't splittable → one task reads the whole file on one core.
14. `NULL > 0` is NULL, `NOT NULL` is NULL → treated as not-true. Use `COALESCE(cond, FALSE)` or explicit `IS NULL`.
15. Decouple from source keys (changes, collisions across systems), enable SCD2 versions, support an Unknown (-1) member, compact joins.
16. Transient: network, throttling, capacity, lost spot nodes, some write conflicts. The task must be idempotent.
17. Summable across some dimensions but not time, e.g. account balance; summing daily balances over a month is wrong — use closing or average balance.
18. Freshness/timeliness (e.g. source file didn't arrive). Monitor last update / max event time vs SLA, and zero-row alerts.
19. Progress was lost → source reprocessed from the start → duplicates in the target.
20. End state repeats, but it's not atomic: a failure between the statements leaves the day empty, and readers can see it empty. Use one atomic `replaceWhere` overwrite or MERGE.

</details>