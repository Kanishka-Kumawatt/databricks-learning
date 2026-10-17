# Phase 1 Revision Sheet

| Topic | Remember |
|---|---|
| **DE** | Design, build, **operate**: source → ingest → store → transform → serve. Undercurrents: orchestration, quality, monitoring, idempotency, security, cost. Success ≠ correct. |
| **OLTP vs OLAP** | OLTP runs the business (tiny txns, normalised, row store, indexes, ms). OLAP analyses it (scans, denormalised, columnar, data skipping). Columnar wins via pruning + compression + vectorisation. Delta = ACID at file level, not a checkout DB. |
| **Lake / warehouse / lakehouse** | Lake: cheap, any format, schema-on-read, swamp (no atomicity/isolation/DML/enforcement/history). Warehouse: reliable, schema-on-write, costly. Lakehouse: Parquet + **transaction log** + governance. Writers write files then commit the log; readers trust the log. Storage/compute separated. |
| **Data types** | Structured / semi-structured (JSON, XML, Avro) / unstructured (images, PDFs). Parquet columnar; gzip CSV not splittable. Declare schemas; `from_json`, dot paths, `explode_outer`. |
| **ETL vs ELT** | Difference = where transform runs. ELT lands raw then transforms in platform → **replay**; risk: raw PII → governance. ADF Copy + Databricks = ELT. |
| **Batch vs streaming** | Bounded vs unbounded. Micro-batches, offsets, **checkpoint per query**, state. Triggers: default, processingTime, **availableNow**. Event time ≠ processing time. |
| **Full vs incremental** | Full: simple, self-healing, catches deletes, doesn't scale. Incremental: watermark / CDC / new files / snapshot diff. Rules: **write then watermark**, watermark = **max processed**, **MERGE**, **overlap**, dedupe, pinned snapshot. Timestamp loads miss **deletes**. |
| **Medallion** | Bronze raw + audit cols, append-only. Silver typed/deduped/validated/conformed + quarantine. Gold consumption-ready. For replay, traceability, quality gates, reuse, access. Left join + Unknown, reconcile counts. |
| **Quality & governance** | Completeness, validity, accuracy, consistency, uniqueness, timeliness, integrity. Warn / drop / quarantine / fail-over-threshold. **NULL trap.** Delta NOT NULL + CHECK. Lineage (UC automatic), metadata (technical/operational/business), governance (grants, least privilege, masks, tags, audit). |
| **Modelling** | **Grain first.** Facts: transaction, periodic snapshot, accumulating, factless. Measures: additive, semi-additive (not over time), non-additive. Surrogate keys, Unknown member, conformed, degenerate, date dim. Star > snowflake. |
| **Orchestration** | Scheduling = when; orchestration = what/order/inputs/compute/failures. DAG, fan-in/out, triggers (time, file arrival), params, retries (transient + idempotent only), timeouts, `max_concurrent_runs`, backfills. Monitor jobs **and** data (freshness, volume, schema, distribution). |
| **Reliability** | Idempotent: replaceWhere, MERGE, dedupe, txnAppId/txnVersion, deterministic inputs. Exactly-once = at-least-once + idempotent sink + checkpoint. Spark: task retries, lineage recompute. Delta: atomic commits. Scale-out killers: collect, skew, non-splittable files, tiny files, row-by-row Python. SLI → SLO → SLA. |

**Answer framework:** Define → Why → How → Example → Trade-off.