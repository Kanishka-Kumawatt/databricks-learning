# Phase 6 — Delta Lake

**Day 3 PM · 4.5 h · Level: ⭐ Master**

| # | Topic | Time |
|---|---|---|
| 6.1 | Parquet vs Delta Lake | 15 min |
| 6.2 | The transaction log (`_delta_log`), commits, checkpoints | 30 min |
| 6.3 | ACID & optimistic concurrency control | 25 min |
| 6.4 | INSERT / UPDATE / DELETE / MERGE — what happens to files | 40 min |
| 6.5 | Schema enforcement vs evolution | 20 min |
| 6.6 | Time travel, history, RESTORE | 20 min |
| 6.7 | OPTIMIZE & small files; VACUUM & retention | 25 min |
| 6.8 | Partitioning, data skipping, Z-order, liquid clustering | 30 min |
| 6.9 | Concurrent writes & conflict types | 20 min |
| 6.10 | Maintenance strategy (incl. predictive optimisation) | 10 min |
| — | Practical: trace `_delta_log` through a sequence of DML | 30 min |
| — | Quiz + revision | 15 min |

**Gate:** explain, file by file, what a MERGE does to the table and the log, and why VACUUM can break time travel.