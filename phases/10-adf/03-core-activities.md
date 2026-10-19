# 10.3 Copy, Lookup, Get Metadata, ForEach, If Condition, Execute Pipeline (+ Until, Wait, Set Variable, Web, Fail)

**Time:** 15 min · **Prerequisites:** 10.2

**After this topic you can:**
- explain how the Copy activity moves data and which knobs control its speed and fault tolerance (DIUs, parallel copy, staging, skip incompatible rows);
- use Lookup and Get Metadata outputs in later activities (and know their limits);
- build a metadata-driven pipeline with ForEach + Execute Pipeline;
- branch and loop with If Condition, Until, Wait, Set Variable; call APIs with Web; fail on purpose with Fail.

---

## A. Concept deep dive

### 1. The simplest explanation

| Activity | Does | Typical output used later |
|---|---|---|
| **Copy** | Reads a source dataset, writes a sink dataset | `rowsCopied`, `rowsRead`, `dataWritten`, `rowsSkipped` |
| **Lookup** | Runs a query / reads a small file and returns rows | `output.firstRow.<col>` or `output.value[]` + `output.count` |
| **Get Metadata** | Returns metadata of a file/folder/table | `output.exists`, `output.childItems`, `output.lastModified`, `output.structure` |
| **ForEach** | Runs inner activities for each item of an array | — (use `@item()` inside) |
| **If Condition** | Evaluates a boolean expression; runs the true or false branch | — |
| **Execute Pipeline** | Calls a child pipeline with parameters | `output.pipelineRunId`, `output.pipelineReturnValue.<key>` |
| **Until** | Loops until an expression is true (with a timeout) | — |
| **Wait** | Pauses N seconds | — |
| **Set Variable** | Sets a pipeline variable (or the pipeline return value) | `variables('name')` |
| **Web** | Calls a REST endpoint (GET/POST/PUT/PATCH/DELETE) | `output.<json field>` |
| **Fail** | Fails the pipeline on purpose with your message and error code | — |

### 2. Copy activity — how it works internally

```
            Integration Runtime (Azure IR: N DIUs  |  SHIR: your nodes)
 ┌────────┐   read (parallel threads,   ┌──────────────────────┐   write (parallel   ┌────────┐
 │ source │ ── partitions/files) ─────▶ │ serialize / convert  │ ── threads) ──────▶ │  sink  │
 │ SQL    │                             │ types, column mapping│                     │ Parquet│
 └────────┘                             │ (translator)         │                     │ ADLS   │
                                        └──────────────────────┘
          optional: enableStaging → source ─▶ staging Blob/ADLS ─▶ sink (two hops, auto-cleaned)
```

| Knob | JSON property | What it does | Notes |
|---|---|---|---|
| **Data Integration Units** | `dataIntegrationUnits` | CPU/memory/network power of the Azure IR for this copy | 4–256; *Auto* by default; only Azure IR (not SHIR); billed DIUs × duration |
| **Parallel copy** | `parallelCopies` | Max parallel threads reading/writing | Auto by default; for files = file-level parallelism; for SQL sources only effective with partitioning |
| **Source partitioning** | `partitionOption` (`None`, `PhysicalPartitionsOfTable`, `DynamicRange`) + `partitionSettings` | Splits a big SQL read into ranges read in parallel | Without it a big SQL table is read by one query |
| **Staged copy** | `enableStaging` + `stagingSettings` (`linkedServiceName`, `path`) | Copies via interim Blob/ADLS | Needed for Synapse PolyBase/COPY, Snowflake; helps when only 443 may be opened; ADF cleans up staging |
| **Fault tolerance (tabular)** | `enableSkipIncompatibleRow` + `logSettings` | Skips rows with type conversion errors, column-count mismatch, PK violation (SQL sinks); logs them | Off by default → copy fails on first bad row |
| **Fault tolerance (binary files)** | `skipErrorFile` (`fileMissing`, `fileForbidden`, `dataInconsistency`, `invalidFileName`) | Skips problem files | Binary copy between file stores |
| **Consistency check** | `validateDataConsistency` | Verifies size/checksum after copy | Binary/file copies |
| **Column mapping** | `translator` (`TabularTranslator`) | Explicit source → sink column mapping and type conversion | Without it ADF maps by name (case-sensitive for some formats) |

**Rule of thumb for the lakehouse:** land raw data **as-is** in Parquet (no fancy mapping), let Databricks handle types and bad rows. If you enable `enableSkipIncompatibleRow`, **monitor `rowsSkipped`** — silently dropped rows are data loss.

### 3. Lookup — the "read a small thing" activity

- Returns **up to 5,000 rows** and **4 MB** of output; activity fails if output exceeds 4 MB. Timeout max 24 h.
- `firstRowOnly: true` (default) → `@activity('LookupWatermark').output.firstRow.last_value`.
- `firstRowOnly: false` → `@activity('LookupTables').output.value` (array, feed to ForEach) and `output.count`.
- The query must return **exactly one result set**.
- Perfect for: watermarks, config/control tables, lists of tables to load. Not for: moving data.

### 4. Get Metadata — "does it exist / what's in it"

- `fieldList`: `exists`, `itemName`, `itemType`, `size`, `lastModified`, `created`, `childItems`, `contentMD5`, `structure`, `columnCount` (support differs by connector).
- With `exists` in the list, the activity **does not fail** when the object is missing — it returns `exists: false`. Without it, a missing file fails the activity.
- `childItems` lists only the top level (name + type) — no wildcards, no recursion; output max 4 MB.

### 5. ForEach, Execute Pipeline — fan-out

| Property | Meaning | Default / limit |
|---|---|---|
| `items` | Expression returning an array, e.g. `@activity('LookupTables').output.value` | max 100,000 items |
| `isSequential` | `true` = one at a time | default `false` (parallel) |
| `batchCount` | Max parallel iterations when not sequential | default 20, **max 50** |
| `@item()` | Current element inside the loop | `@item().table_name` |

Limits that shape designs:
- **No ForEach inside ForEach** (or inside Until) → put the inner loop in a **child pipeline** called via **Execute Pipeline**.
- **Set Variable inside a parallel ForEach is unsafe** — variables are pipeline-scoped, not per iteration. Use a child pipeline (its own variables) or Append Variable for collecting results.
- Execute Pipeline with `waitOnCompletion: true` makes the parent wait and inherit the child's success/failure; with `false` it's fire-and-forget (the parent can't react to failures).
- A child can return values to the parent through the **pipeline return value** (Set Variable with the system variable) → `@activity('ExecChild').output.pipelineReturnValue.rows_copied`.

### 6. If Condition, Until, Wait, Set Variable, Web, Fail

- **If Condition**: `expression` must evaluate to boolean; `ifTrueActivities` / `ifFalseActivities`. Example: `@greater(activity('CopyOrders').output.rowsCopied, 0)` → run Databricks only if rows arrived.
- **Until**: `expression` + `timeout`; inner activities run at least once, until the expression is true. Combine with **Wait** for polling ("wait for a file", "poll a Databricks run state").
- **Set Variable**: types String, Bool, Array (and Integer in newer factories); a variable can't reference itself in its own Set Variable (use a temp variable).
- **Web**: REST call; response must be JSON; default timeout **1 minute** (`httpRequestTimeout` up to 10 min); response ≤ 4 MB; auth: Basic, client certificate, **managed identity (system/user-assigned)**, service principal. Can run on SHIR for private URLs.
- **Fail**: `message` + `errorCode` (both can be expressions) → the pipeline run fails with your message. Used after an alert so the run still shows **Failed** (10.5).

### 7. ShopCo example — metadata-driven copy

ShopCo keeps a control table `etl.load_config(source_schema, source_table, landing_folder, enabled)`.

```
 LookupTables (firstRowOnly=false)
      │ output.value = [{orders}, {customers}, {payments}, ...]
      ▼
 ForEach (isSequential=false, batchCount=4)
      └── ExecutePipeline PL_Child_CopyTable (waitOnCompletion=true)
             params: schema=@item().source_schema, table=@item().source_table, folder=@item().landing_folder
             child: Copy (AzureSqlSource → ParquetSink)  →  SetVariable pipelineReturnValue rows_copied
```

Adding a new table = inserting one row in `etl.load_config`. No deployment.

### 8. Misconceptions

1. **"Lookup can read my 2-million-row table."** Max 5,000 rows / 4 MB — use Copy.
2. **"Parallel ForEach + Set Variable gives one value per iteration."** Variables are global to the pipeline → race conditions.
3. **"More DIUs always = faster."** A single source file or an unpartitioned SQL query can't use them; the source/sink may be the bottleneck.
4. **"Skip incompatible rows is a safe default."** It hides data loss unless you log and alert on `rowsSkipped`.
5. **"Web activity waits for my Databricks job to finish."** It returns when the HTTP call returns (unless the endpoint uses the 202 async pattern); a `jobs/run-now` call returns immediately with a run ID — you must poll (10.7).

---

## B. Practical — control-flow pipelines in JSON

**File:** [`exercises/phase-10/03_control_flow_activities.json`](../../exercises/phase-10/03_control_flow_activities.json) (study bundle, abbreviated): master/child metadata-driven copy, and a "wait for a file" pipeline with Until + Get Metadata + If.

The child Copy, explained:

```json
{
  "name": "CopyTableToLanding",
  "type": "Copy",
  "policy": { "timeout": "0.02:00:00", "retry": 2, "retryIntervalInSeconds": 120 },
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "sqlReaderQuery": {
        "value": "SELECT * FROM [@{pipeline().parameters.schemaName}].[@{pipeline().parameters.tableName}]",
        "type": "Expression"
      },
      "partitionOption": "DynamicRange",
      "partitionSettings": { "partitionColumnName": "@{pipeline().parameters.partitionColumn}" }
    },
    "sink": {
      "type": "ParquetSink",
      "storeSettings": { "type": "AzureBlobFSWriteSettings" },
      "formatSettings": { "type": "ParquetWriteSettings" }
    },
    "dataIntegrationUnits": 8,
    "parallelCopies": 4
  },
  "inputs":  [ { "referenceName": "DS_AzureSql_Query", "type": "DatasetReference" } ],
  "outputs": [ { "referenceName": "DS_LandingParquet", "type": "DatasetReference",
                 "parameters": { "folderPath": "@{pipeline().parameters.landingFolder}/run_id=@{pipeline().RunId}" } } ]
}
```

| Part | Explanation |
|---|---|
| `policy.retry: 2` | Transient failures retried twice, 120 s apart. Safe because each run writes its own folder |
| `AzureSqlSource.sqlReaderQuery` | Query instead of table; `@{...}` is **string interpolation** of expressions inside text |
| `partitionOption: DynamicRange` | Splits the read into ranges of `partitionColumn` and reads them in parallel |
| `ParquetSink` + `AzureBlobFSWriteSettings` | Write Parquet files to ADLS Gen2 |
| `dataIntegrationUnits`, `parallelCopies` | Explicit caps (normally leave *Auto*; set to protect a fragile source) |
| `folderPath` with `@{pipeline().RunId}` | Run-specific folder → re-runs never overwrite or mix with earlier runs' files |

The "wait for a file" pattern:

```json
{
  "name": "WaitForPartnerFile",
  "type": "Until",
  "typeProperties": {
    "expression": { "value": "@variables('fileFound')", "type": "Expression" },
    "timeout": "0.02:00:00",
    "activities": [
      { "name": "CheckFolder", "type": "GetMetadata",
        "typeProperties": { "dataset": { "referenceName": "DS_LandingFolderBinary", "type": "DatasetReference",
                                         "parameters": { "folderPath": "partner/@{formatDateTime(utcnow(),'yyyy-MM-dd')}" } },
                            "fieldList": [ "exists" ] } },
      { "name": "SetFileFound", "type": "SetVariable", "dependsOn": [ { "activity": "CheckFolder", "dependencyConditions": [ "Succeeded" ] } ],
        "typeProperties": { "variableName": "fileFound", "value": { "value": "@activity('CheckFolder').output.exists", "type": "Expression" } } },
      { "name": "Wait5Min", "type": "Wait", "dependsOn": [ { "activity": "SetFileFound", "dependencyConditions": [ "Succeeded" ] } ],
        "typeProperties": { "waitTimeInSeconds": 300 } }
    ]
  }
}
```

`fieldList: ["exists"]` makes the check return `false` instead of failing. After the Until, an **If Condition** on `@variables('fileFound')` either continues or runs a **Fail** activity ("partner file not received by 06:00"). (A storage event trigger, 10.4, is the event-driven alternative to polling.)

### Common mistakes

- Referencing `output.firstRow` when `firstRowOnly` is `false` (it's `output.value[0]` then) — or the reverse.
- Forgetting `waitOnCompletion: true` on Execute Pipeline, so the parent "succeeds" while children fail.
- Building SQL with `@{...}` from untrusted values (SQL injection via config tables) — keep control tables locked down; prefer stored procedures with parameters for writes.
- Setting `batchCount` 50 against a small source database and overloading it.

---

## C. Interview questions

### 🟢 Basic

**B1.** What are the limits of the Lookup activity, and what is `firstRowOnly`?

<details><summary>Model answer</summary>

Up to 5,000 rows and 4 MB output (fails above 4 MB); one result set; max 24 h. `firstRowOnly: true` (default) returns `output.firstRow`; `false` returns `output.value` (array) and `output.count`, typically fed to a ForEach.
</details>

**B2.** What does Get Metadata with `exists` in the field list do differently?

<details><summary>Model answer</summary>

It returns `exists: true/false` instead of failing when the file, folder or table doesn't exist. Without `exists`, a missing object makes the activity fail.
</details>

**B3.** What do `isSequential` and `batchCount` control in ForEach?

<details><summary>Model answer</summary>

`isSequential: true` runs iterations one at a time; `false` (default) runs them in parallel with at most `batchCount` concurrent iterations (default 20, max 50).
</details>

### 🟡 Medium

**M1.** A Copy from a 500 GB Azure SQL table to ADLS is slow even with 64 DIUs. What do you check?

<details><summary>Model answer</summary>

Whether the read is parallel at all: without `partitionOption` (physical partitions or `DynamicRange` on an indexed column) it's one query on one thread, so DIUs are idle. Then source capacity (DTU/vCore throttling), network/IR location (cross-region), sink file size, and whether an incremental extract (watermark) would avoid copying 500 GB in the first place.
</details>

**M2.** Why can't you nest ForEach loops, and what's the standard workaround?

<details><summary>Model answer</summary>

ADF doesn't allow a ForEach inside a ForEach (or Until). Put the inner loop into a child pipeline and call it with Execute Pipeline from the outer ForEach (`waitOnCompletion: true`), passing `@item()` values as parameters. The child also has its own variables, avoiding the pipeline-scoped variable race in parallel loops.
</details>

**M3.** When would you enable staged copy?

<details><summary>Model answer</summary>

Loading Azure Synapse with PolyBase/COPY, copying to/from Snowflake (and some sources like Redshift/HDFS) where staging is required for performance; when corporate firewall allows only 443 from on-prem (SHIR writes to staging over HTTPS, then Azure loads the sink); or to compress data over slow hybrid links. ADF manages both hops and cleans the staging data (needs delete permission).
</details>

### 🔴 Tricky

**T1.** Copy has `enableSkipIncompatibleRow: true`. The run is green but Gold revenue is 2 % low. What happened and how do you prevent it?

<details><summary>Model answer</summary>

Rows that failed type conversion or column-count checks were skipped silently — the activity succeeds with `rowsSkipped > 0`. Prevent: log skipped rows (`logSettings`), add an If Condition after Copy (`@greater(activity('Copy').output.rowsSkipped, 0)`) that alerts/fails, reconcile counts (`rowsRead` vs `rowsCopied` vs Silver), or land raw as-is and let Databricks quarantine bad rows visibly.
</details>

**T2.** A parallel ForEach (batchCount 10) sets a variable `lastTable` in each iteration and a later activity logs it. The logged value is random. Why?

<details><summary>Model answer</summary>

Variables are pipeline-scoped and not thread-safe; parallel iterations overwrite the same variable in nondeterministic order. Use a sequential loop, Append Variable for collecting values, or move per-iteration logic into a child pipeline (own variables) and return values via the pipeline return value.
</details>

**T3.** Your Lookup reads a config table that suddenly grows to 8,000 rows. What happens and how do you redesign?

<details><summary>Model answer</summary>

Lookup returns only the first 5,000 rows (and fails if output > 4 MB), so 3,000 tables silently stop loading. Redesign: two-level pipeline — outer Lookup returns groups/batches (e.g. distinct `load_group`), ForEach calls a child pipeline that looks up only that group's tables (< 5,000 rows each). Also alert when `output.count` is near the limit.
</details>

---

## E. Revision summary

- **Copy**: IR reads/writes; speed via **DIUs** (4–256, Azure IR only), **parallelCopies**, **source partitioning**; **staging** for Synapse/Snowflake/firewalls; **skip incompatible rows** only with logging + alerts on `rowsSkipped`.
- **Lookup**: ≤ 5,000 rows, ≤ 4 MB; `firstRow` vs `value[]`.
- **Get Metadata**: `exists` avoids failure on missing; `childItems` top-level only.
- **ForEach**: parallel by default, `batchCount` ≤ 50, no nesting → child pipeline via **Execute Pipeline** (`waitOnCompletion: true`); no Set Variable in parallel loops.
- **If / Until / Wait / Set Variable / Web / Fail** for branching, polling, state, REST calls, deliberate failure.