# Phase 1 — Data Engineering Foundations

**Day 1 AM · ~6 h with practicals · Level: Master**

**Objective:** own the vocabulary every later phase builds on, and reason about pipeline design — correctness, re-runs, layers, modelling — before touching Spark internals.

**How to study each topic:** read A (concepts) → run B (practical) → answer C **out loud or in writing before opening any model answer** → read E (revision). Log answers in [`interview/answers-log.md`](../../interview/answers-log.md).

| # | Topic | Time | Lesson | Practical |
|---|---|---|---|---|
| 1.1 | What is data engineering | 35 min | [01-what-is-data-engineering.md](01-what-is-data-engineering.md) | [01_mini_pipeline.py](../../exercises/phase-01/01_mini_pipeline.py) |
| 1.2 | OLTP vs OLAP | 30 min | [02-oltp-vs-olap.md](02-oltp-vs-olap.md) | in lesson (column pruning demo) |
| 1.3 | Data lake vs warehouse vs lakehouse | 35 min | [03-lake-warehouse-lakehouse.md](03-lake-warehouse-lakehouse.md) | in lesson (Parquet vs Delta demo) |
| 1.4 | Structured / semi-structured / unstructured | 20 min | [04-structured-semi-unstructured.md](04-structured-semi-unstructured.md) | in lesson (nested JSON) |
| 1.5 | ETL vs ELT | 20 min | [05-etl-vs-elt.md](05-etl-vs-elt.md) | in lesson (ELT in SQL) |
| 1.6 | Batch vs streaming | 25 min | [06-batch-vs-streaming.md](06-batch-vs-streaming.md) | in lesson (batch vs `availableNow`) |
| 1.7 | Full vs incremental load | 40 min | [07-full-vs-incremental.md](07-full-vs-incremental.md) | [07_incremental_watermark.py](../../exercises/phase-01/07_incremental_watermark.py) |
| 1.8 | Medallion architecture | 35 min | [08-medallion-architecture.md](08-medallion-architecture.md) | [08_medallion_end_to_end.py](../../exercises/phase-01/08_medallion_end_to_end.py) |
| 1.9 | Data quality, lineage, metadata, governance | 35 min | [09-quality-lineage-metadata-governance.md](09-quality-lineage-metadata-governance.md) | [09_data_quality_quarantine.py](../../exercises/phase-01/09_data_quality_quarantine.py) |
| 1.10 | Facts, dimensions, star schema | 40 min | [10-dimensional-modelling.md](10-dimensional-modelling.md) | [10_star_schema.sql](../../exercises/phase-01/10_star_schema.sql) |
| 1.11 | Orchestration, scheduling, monitoring | 30 min | [11-orchestration-monitoring.md](11-orchestration-monitoring.md) | [11_daily_revenue.job.yml](../../exercises/phase-01/11_daily_revenue.job.yml) |
| 1.12 | Idempotency, fault tolerance, scalability, reliability | 40 min | [12-idempotency-fault-tolerance.md](12-idempotency-fault-tolerance.md) | [12_idempotency_demo.py](../../exercises/phase-01/12_idempotency_demo.py) |
| — | **Phase quiz (no notes)** | 25 min | [quiz.md](quiz.md) | |
| — | **Revision sheet** | 10 min | [revision-sheet.md](revision-sheet.md) | |

**Running the practicals:** they target a Databricks workspace with Unity Catalog. Change `CATALOG, SCHEMA = "main", "de_prep"` to a catalog/schema you can write to. Never run them against shared or production catalogs.

**Gate to Phase 2:** quiz ≥ 80 % on basic, ≥ 60 % on medium; and you can explain — without notes — medallion, idempotency (with two techniques), the watermark ordering rule, and grain.