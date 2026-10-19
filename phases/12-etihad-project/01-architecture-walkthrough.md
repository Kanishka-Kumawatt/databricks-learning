# 12.1 Architecture Walkthrough

**Time:** 30 min · **Prerequisites:** 1.3, 1.8, Phase 5 (Unity Catalog), Phase 10 (ADF)

**After this topic you can:**
- draw the project architecture from memory in under a minute (sources → landing → raw → staging → curated → consumers);
- explain *why* each layer and each tool exists, not just *what* it is;
- map every box on the diagram to a generic concept from Phases 1–11;
- deliver a 2-minute pitch and survive "why not X?" follow-ups.

> Confidentiality: describe **patterns**, never internal names, paths, hostnames, catalog names or volumes.
> "An airline enterprise data platform on Azure" is enough context.

---

## A. Concept / story

### 1. The one-sentence version

> "Our team built and ran an enterprise lakehouse on Azure: **ADF** pulled data from many source systems into an
> **ADLS Gen2 landing zone**, **Databricks** notebooks and pipelines refined it through **raw → staging → curated**
> layers in Delta tables governed by **Unity Catalog** (one catalog per business domain), and BI / downstream
> applications consumed the curated layer."

### 2. The diagram you should be able to draw

```
 SOURCES                    INGEST (ADF)                 STORAGE + COMPUTE (ADLS Gen2 + Databricks)              CONSUME
 ───────                    ────────────                 ───────────────────────────────────────────              ───────
 SFTP / partner files ─┐                                  ┌───────────── per-domain UC catalog ─────────────┐
 SQL Server / RDBMS ───┤    Copy activity                 │                                                  │
 SAP / ERP extracts ───┼──► (metadata-driven ──► LANDING ─┼─► RAW ───────► STAGING ─────────► CURATED ───────┼──► Power BI / SQL warehouse
 SharePoint / Excel ───┤     Lookup + ForEach)  (files,   │   (as-is,       (cleaned, typed,   (business      │──► downstream apps / APIs
 REST APIs ────────────┤                         by date) │    Delta,       deduped, SCD,      models, aggs,  │──► outbound files (encrypted)
 Event Hub (streams) ──┼──────────── Databricks streaming / Auto Loader ───►│  audit cols)    conformed)      │
 Cosmos DB / NoSQL ────┘                                  └──────────────────────────────────────────────────┘
                                  ▲                                     ▲
                    ADF master → wrapper → child pipelines       audit / DQ / GDPR frameworks, Key Vault secrets,
                    (schedule triggers, alerts)                   job clusters, CI/CD (bundles + ARM export)
```

### 3. Box by box — what, why, and which generic lesson it maps to

| Box | What it is on the project (generic) | Why it exists | Generic lesson |
|---|---|---|---|
| Sources | Files over SFTP, relational DBs, ERP extracts, office files, REST APIs, event streams, NoSQL | An airline has dozens of operational systems (bookings, flight ops, loyalty, cargo, crew, finance) | [1.4](../01-de-foundations/04-structured-semi-unstructured.md) |
| ADF Copy | Moves bytes from source to the lake, mostly unchanged; self-hosted IR for on-prem sources | ADF has 90+ connectors and handles network/on-prem connectivity; Spark is not the right tool to SFTP a file | [Phase 10](../10-adf/) |
| Landing | Files as delivered, in date-partitioned folders | Replayability: if a downstream bug is found you can reprocess from the original bytes | [1.8](../01-de-foundations/08-medallion-architecture.md) |
| Raw | Delta copy of the source, source schema, plus audit columns (load timestamp, file name, run id) | "Bronze": cheap to re-derive everything else; isolates source format quirks | [1.8](../01-de-foundations/08-medallion-architecture.md) |
| Staging | Typed, cleaned, deduped; SCD1/SCD2 history; CDC applied | "Silver": one trustworthy version of each entity | [Phase 9](../09-cdc-scd/) |
| Curated | Business models — facts/dimensions, aggregates, data products per domain | "Gold": shaped for the consumer, stable contracts | [1.10](../01-de-foundations/10-dimensional-modelling.md) |
| Unity Catalog | `catalog.schema.table`; one catalog per domain, schemas per layer, managed/external tables, volumes for files | Central governance, lineage, fine-grained grants, no workspace-local metastore | [Phase 5](../05-databricks-platform/) |
| Frameworks | Audit/control tables, DQ checks, GDPR retention | Reconciliation, restartability, compliance — the "boring" parts that make it production | [12.3](03-frameworks.md) |
| Orchestration | ADF master → wrapper → child pipelines; newer flows as Databricks Jobs | Dependencies, schedules, retries, alerting | [12.2](02-orchestration.md), [Phase 8](../08-workflows-sdp/) |
| Consumers | Power BI, SQL warehouses, outbound partner files | The reason the platform exists | — |

### 4. Why layers are named raw / staging / curated (and not bronze / silver / gold)

Same idea, different vocabulary. Many enterprise teams pre-date the medallion naming. In the interview, say both:
*"Our raw/staging/curated layers are a medallion architecture — raw is bronze, staging is silver, curated is gold."*

### 5. Per-domain catalogs — the design decision worth explaining

```
domain_a_catalog              domain_b_catalog            (one catalog per business domain, per environment
 ├─ raw schema                 ├─ raw                        or an environment suffix/prefix resolved at runtime)
 ├─ staging schema             ├─ staging
 └─ curated schema             └─ curated
```

**Why per domain:** ownership and grants follow the business (a domain team owns its catalog), blast radius is
limited, and catalogs can be bound to specific workspaces. **Trade-off:** cross-domain joins need 3-part names
and grants across catalogs; shared reference data needs a clear home (a shared/common catalog).

### 6. How data moves — the four ingestion patterns you can name

| Pattern | Used for | Mechanics |
|---|---|---|
| **File drop** | Partner files over SFTP, office files | ADF Copy → landing folder by date → notebook reads that folder → raw Delta |
| **Incremental DB extract** | Relational sources | ADF Copy with a watermark query (`WHERE modified_ts > @last`) → landing → MERGE into staging |
| **Full snapshot** | Small reference tables, sources with no change column | Overwrite raw; compare snapshots or SCD2 in staging |
| **Streaming** | Event Hub topics | Structured Streaming / Auto Loader with checkpoints into raw Delta |

### 7. The 2-minute pitch (practise out loud, time yourself)

1. **Context (15 s):** "An airline's enterprise data platform on Azure Databricks and ADF, serving several business
   domains — commercial, operations, customer, finance."
2. **Flow (40 s):** sources → ADF Copy → ADLS landing → raw / staging / curated Delta in per-domain UC catalogs → BI.
3. **What makes it production (30 s):** metadata-driven orchestration, an audit framework with source/target
   counts and restart, DQ checks with alerts, GDPR retention rules, Key Vault secrets, CI/CD.
4. **The big change (20 s):** "During my time we migrated from the Hive metastore to Unity Catalog and started
   moving new pipelines to declarative pipelines and Databricks Jobs deployed with Asset Bundles."
5. **My part (15 s):** one or two sentences from your *Personal contributions* table — use "I" only there.

### 8. Misconceptions and traps

1. **"ADF does the transformations."** On this kind of platform ADF mostly *moves* and *orchestrates*; Spark transforms.
2. **"Landing and raw are the same thing."** Landing = files as delivered; raw = Delta table with audit columns.
3. **"Medallion means exactly three layers."** It's a pattern — some domains had extra sub-layers (e.g. a separate
   area for outbound extracts); what matters is that each layer has a contract.
4. **Over-claiming.** Interviewers detect "I built the whole platform". Be precise about scope.

---

## B. Practical — draw and narrate

1. On paper, draw the diagram in §2 **from memory** in < 90 s. Label every arrow with the tool that moves the data.
2. For each layer, write one line: *grain, write mode (append / overwrite / MERGE), who reads it.* Example:

| Layer | Grain | Write mode | Readers |
|---|---|---|---|
| raw.bookings | one row per source record per load | append (+ `_run_id`, `_ingest_ts`) | staging jobs only |
| staging.bookings | one row per booking (SCD1) | MERGE on booking key | curated jobs, analysts |
| staging.passenger_dim | one row per passenger *version* (SCD2) | atomic MERGE | curated |
| curated.daily_route_revenue | route × flight date | `replaceWhere` per date | BI |

3. A generic re-creation of the layer flow on sample airline data (runs on any UC workspace):

```python
from pyspark.sql import functions as F
CATALOG, SCHEMA = "main", "de_prep"
raw = (spark.createDataFrame(
        [("BK1", "AUH-LHR", "2026-10-01", 820.0, "2026-10-02T01:00:00"),
         ("BK1", "AUH-LHR", "2026-10-01", 860.0, "2026-10-02T05:00:00"),   # later update of same booking
         ("BK2", "AUH-JFK", "2026-10-01", 1250.0, "2026-10-02T01:00:00")],
        "booking_id STRING, route STRING, flight_date STRING, fare DOUBLE, source_ts STRING")
       .withColumn("_ingest_ts", F.current_timestamp()))
raw.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.arch_raw_bookings")

from pyspark.sql.window import Window
w = Window.partitionBy("booking_id").orderBy(F.col("source_ts").desc())
staging = (spark.table(f"{CATALOG}.{SCHEMA}.arch_raw_bookings")
           .withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
           .withColumn("flight_date", F.to_date("flight_date")))
staging.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.arch_stg_bookings")

curated = staging.groupBy("route", "flight_date").agg(F.sum("fare").alias("revenue"), F.count("*").alias("bookings"))
curated.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.arch_cur_route_revenue")
```

Expected curated output: `AUH-LHR, 2026-10-01, 860.0, 1` and `AUH-JFK, 2026-10-01, 1250.0, 1` — the staging
dedup kept only the latest version of BK1.

---

## C. Interview questions

### 🟢 Basic

**B1.** Walk me through your project's architecture in 2 minutes.

<details><summary>Model answer</summary>

"It's an airline enterprise data platform on Azure. Source systems — partner files over SFTP, SQL Server and ERP
extracts, SharePoint files, REST APIs, Event Hub streams — are ingested by ADF Copy activities into an ADLS Gen2
landing zone. Databricks then loads them into raw Delta tables with audit columns, cleans, dedupes and applies
SCD logic into staging, and builds business models in curated. Each business domain has its own Unity Catalog
catalog with raw/staging/curated schemas. ADF orchestrates through master/wrapper/child pipelines driven by
metadata; an audit framework records source/target counts and supports restarts; DQ checks alert by email; GDPR
retention rules anonymise or purge old personal data. Secrets come from Key Vault and code is deployed through
CI/CD. The biggest change while I was there was the Hive-metastore-to-Unity-Catalog migration. My part was …"
</details>

**B2.** Why keep a landing zone *and* a raw layer?

<details><summary>Model answer</summary>

Landing holds the bytes exactly as delivered — CSV, JSON, Excel, Parquet — so we can always replay. Raw is the
first Delta table: it adds schema, ACID, time travel and audit columns like ingest timestamp, file name and run
id. If a parsing bug is found later, we fix the notebook and re-load raw from landing without going back to the
source system, which may no longer have the data.
</details>

**B3.** Why did ADF do the copy and Databricks the transformation?

<details><summary>Model answer</summary>

ADF is strong at connectivity — many connectors, self-hosted integration runtime for on-prem sources, SFTP and
SharePoint — and is cheap for pure data movement. Spark is strong at distributed transformation. Using each for
what it's good at kept notebooks free of connector code and kept heavy logic out of ADF expressions.
</details>

### 🟡 Medium

**M1.** Why one catalog per domain rather than one catalog per layer?

<details><summary>Model answer</summary>

Ownership and access follow the business: a domain team owns its catalog and grants on it; it limits blast
radius and lets us bind catalogs to workspaces. Layers became schemas inside the catalog. A catalog per layer
would mix all domains together and make grants coarse. The trade-off is cross-domain queries need three-part
names and cross-catalog grants, and shared reference data needs a common home.
</details>

**M2.** Which layer would you expose to BI and why not staging?

<details><summary>Model answer</summary>

Curated. It has stable, documented contracts, business naming and pre-aggregated or modelled data. Staging
mirrors entities and can change when sources change; exposing it couples dashboards to source quirks. Analysts
may get read access to staging for exploration, but production reports point at curated.
</details>

**M3.** How did streaming sources fit into a mostly-batch platform?

<details><summary>Model answer</summary>

Event Hub data was read with Structured Streaming into raw Delta with a checkpoint, often with
`availableNow`-style scheduled runs so it behaved like an incremental batch. From raw onwards the same
staging/curated logic applied. Delta is the unifying layer: a batch job can read a table written by a stream.
</details>

### 🔴 Tricky

**T1.** If you rebuilt this platform today, what would you change architecturally?

<details><summary>Model answer</summary>

Use Auto Loader or Lakeflow Connect for file and DB ingestion where possible instead of a Copy activity plus a
notebook; declarative pipelines with expectations and AUTO CDC for staging; Databricks Jobs deployed with Asset
Bundles for orchestration with ADF kept only for connectivity it uniquely provides; UC managed tables with
predictive optimisation and liquid clustering; and one parameterised framework instead of per-domain copies.
I'd keep the domain-catalog design and the landing/raw replayability.
</details>

**T2.** The interviewer says: "Your medallion has three layers plus landing — isn't that four copies of the data? Too expensive."

<details><summary>Model answer</summary>

Storage is the cheapest part of the platform; compute and engineer time dominate. Each copy buys something:
landing = replay from original bytes, raw = schema + audit + time travel, staging = trusted entities, curated =
consumer shape. We controlled cost with retention — landing files and raw history aged out per policy, VACUUM
on Delta — and by not materialising curated tables nobody reads. Where a layer adds nothing (a clean source),
staging can be a view.
</details>

**T3.** What's the weakest part of the architecture you described?

<details><summary>Model answer</summary>

Honest answer: the two-system orchestration (ADF calling notebooks, notebooks calling notebooks) made
end-to-end lineage of a run and failure diagnosis harder, and frameworks were copied per domain so fixes had to
be applied many times. Our team was addressing both by moving new pipelines to Jobs and declarative pipelines in
bundles. See 12.8 for how I'd roll that out.
</details>

---

## E. Revision summary

- Sources → **ADF Copy** → **landing** (files) → **raw** (Delta + audit cols) → **staging** (clean, SCD) → **curated** (business) → BI.
- raw/staging/curated = bronze/silver/gold. Say both.
- **One UC catalog per domain**, schemas per layer; trade-off = cross-catalog joins/grants.
- ADF = connectivity + orchestration; Databricks = transformation.
- Production-ness comes from frameworks: audit, DQ, GDPR, secrets, CI/CD.
- Pitch = context → flow → production features → the UC migration → *your* part.