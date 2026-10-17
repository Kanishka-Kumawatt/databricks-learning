# Databricks Data Engineer — 5-Day Assessment Prep

Intensive, interactive preparation for a Databricks Data Engineer technical assessment (Full-Time conversion).
The goal is real understanding — explaining concepts from first principles, writing and debugging code, and
reasoning through unfamiliar questions — not memorised answers.

## How every topic is taught

| Step | What happens |
|---|---|
| **A. Concept deep dive** | What it is, why it exists, how it works internally, when to use it, misconceptions |
| **B. Practical** | Code / SQL / design exercise with sample input and expected output, explained line by line |
| **C. Interview questions** | 🟢 Basic (concepts) → 🟡 Medium (how it works, scenarios) → 🔴 Tricky (traps, edge cases) — answered *before* any solution is shown |
| **D. Evaluation** | Each answer graded ✅ / ⚠️ partial / ❌, gaps explained, model answer given |
| **E. Revision** | Short summary to re-read before sleeping |

**Readiness gate per phase:** ≥ 80 % on 🟢, ≥ 60 % on 🟡, and a reasoned attempt at every 🔴.

## Answering an unfamiliar question

1. **Define** — "At its core, X is…"
2. **Why** — "It exists because the problem was…"
3. **How** — "Internally it works by…"
4. **Example** — "For instance, in an orders pipeline…"
5. **Trade-off** — "The downside / when not to use it is…"

## Five-day roadmap

| Day | Objective | Phases | End-of-day gate |
|---|---|---|---|
| **1** | Foundations + how Spark thinks | 1, 2 | Explain driver → job → stage → task flow from memory |
| **2** | Spark internals + PySpark/SQL fluency | 3, 4 | Write dedup + window queries without notes; read an `explain()` plan |
| **3** | Databricks platform + Delta Lake | 5, 6 | Explain what happens on disk during a MERGE |
| **4** | Ingestion, streaming, orchestration, CDC | 7, 8, 9 | Build SCD2 by hand and justify every column |
| **5** | ADF, production, project, final assessment | 10, 11, 12 + mock | Scorecard + personalised revision plan |

## Phase map

⭐ = must master (explain, code, debug). Others = understand conceptually.

| Phase | Day | Hours | Topics | Level |
|---|---|---|---|---|
| [1. DE foundations](phases/01-de-foundations/) | D1 AM | 3.5 | What DE is · OLTP vs OLAP · lake / warehouse / lakehouse · data types · ETL/ELT · batch/streaming · full/incremental · medallion · facts/dims/star schema · orchestration · idempotency, fault tolerance | Master |
| [2. Spark core](phases/02-spark-core/) | D1 PM | 5 | Why Spark · distributed computing · driver/executors/cluster manager · SparkSession · jobs/stages/tasks/partitions · DAG · lazy evaluation · transformations/actions · narrow/wide · shuffle | ⭐ |
| [3. Spark internals](phases/03-spark-internals/) | D2 AM | 4 | Catalyst · logical/physical plans · AQE · join strategies · skew/spill/memory · caching · checkpointing · Spark UI · RDD vs DataFrame vs Dataset | ⭐ core |
| [4. PySpark & SQL](phases/04-pyspark-sql/) | D2 PM | 5 | Schemas/types · read/write · nulls · aggregations · joins · dedup · window functions · dates · nested JSON/explode · UDFs · union/unionByName · repartition/coalesce · CTEs · MERGE | ⭐ code without notes |
| [5. Databricks platform](phases/05-databricks-platform/) | D3 AM | 3.5 | Workspace · compute types (verified vs current docs) · access modes · all-purpose/job/serverless · SQL warehouses · sizing & cost · Unity Catalog · managed/external · volumes · secrets | ⭐ |
| [6. Delta Lake](phases/06-delta-lake/) | D3 PM | 4.5 | Parquet vs Delta · `_delta_log` · ACID/OCC · DML & MERGE internals · schema enforcement/evolution · time travel/RESTORE · OPTIMIZE/VACUUM · partitioning/Z-order/liquid clustering · conflicts | ⭐ |
| [7. Streaming & Auto Loader](phases/07-streaming-autoloader/) | D4 AM | 3 | Structured Streaming · triggers · checkpoints · event time/watermarks · Auto Loader/cloudFiles · schema location · rescued data · file discovery | ⭐ |
| [8. Workflows & Declarative Pipelines](phases/08-workflows-sdp/) | D4 mid | 2.5 | Jobs/tasks/params/retries/task values · SDP vs imperative · streaming tables vs materialized views · current `dp` API vs legacy `dlt` · expectations · quarantine · full refresh | Master Jobs, deep SDP concepts |
| [9. CDC & SCD](phases/09-cdc-scd/) | D4 PM | 3 | CDC types · ordering/sequencing · duplicates/late data · SCD1/SCD2 · MERGE-based CDC · AUTO CDC · deletes · replay | ⭐ |
| [10. Azure Data Factory](phases/10-adf/) | D5 AM | 1.5 | Pipelines/activities/linked services/IR · Copy · Lookup/ForEach/If · params · triggers · watermark pattern · ADF → Databricks | Understand |
| [11. Production & troubleshooting](phases/11-production/) | D5 | 4.5 | Diagnosis framework · OOM/skew/small files · MERGE perf · failure recovery · reconciliation · CI/CD · capstone | ⭐ |
| [12. Etihad project learnings](phases/12-etihad-project/) | D5 eve | 2.5 | Architecture walkthrough · orchestration · frameworks · SCD/CDC as built · HMS → UC migration · security & CI/CD · compute · "what I'd improve" | ⭐ "tell me about your project" |

The final mock interview (Day 5) includes a **project round** on Phase 12.

## Repo layout

```
phases/          one folder per phase: README (topics, gate) + one notes file per topic
exercises/       runnable PySpark/SQL practice code, grouped by phase
data/            small sample datasets (customers, orders, transactions, CDC events)
interview/       answers log with grades, mock interviews, final scorecard
PROGRESS.md      phase/topic tracker with scores
```

Exercises target a Databricks workspace (Unity Catalog + Volumes); most also run on local PySpark with paths changed.