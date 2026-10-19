# 11.4 Schema Mismatch and Corrupt Input

**Time:** 15 min · **Prerequisites:** 11.0, Phase 4 (read/write, schemas), Phase 6 (schema enforcement), Phase 7 (Auto Loader)

**After this topic you can:**
- distinguish read-time problems (malformed records, type mismatches, new columns) from write-time problems (Delta schema enforcement);
- choose between PERMISSIVE, DROPMALFORMED, FAILFAST, the rescued data column and `badRecordsPath`;
- explain Auto Loader's schema evolution modes and what "the stream stopped with UnknownFieldException" means;
- design a pipeline that never silently loses bad records.

---

## A. Concept deep dive

### 1. The problem

Sources change and send garbage: a new column appears, a number arrives as `"abc"`, a file is truncated mid-write, a field is renamed `Amount`. The pipeline must decide: **fail, drop, or keep-and-flag** — and that decision must be deliberate.

### 2. Where it happens

| Stage | What can go wrong | Mechanism |
|---|---|---|
| **Read** (parse) | Malformed JSON/CSV line; value doesn't match the declared type; unexpected/missing/renamed columns | Spark's parser can't build a row matching the schema |
| **Inference** | Types inferred from a sample differ between days (int one day, string the next) | Schema inference is data-dependent → unstable |
| **Write** (Delta) | DataFrame schema ≠ table schema (extra column, different type) | Delta **schema enforcement** rejects the write |
| **Downstream** | Column renamed upstream → silently NULL after `select` of the old name with permissive logic | Logical error, no exception |

### 3. Read-time options (file sources)

| Option | Behaviour | Engine |
|---|---|---|
| `mode=PERMISSIVE` (default) | Bad fields → NULL; with `columnNameOfCorruptRecord` (must be in the schema for CSV; default `_corrupt_record`) the raw line is kept | Spark |
| `mode=DROPMALFORMED` | Drops rows that don't parse | Spark |
| `mode=FAILFAST` | Throws on the first bad row | Spark |
| `rescuedDataColumn` | Keeps the row; fields that are **missing from the schema, mismatched in type, or case-mismatched** go into a JSON column with the source file path. With it set, type mismatches don't drop/fail rows in DROPMALFORMED/FAILFAST — only truly malformed records do | **Databricks** |
| `badRecordsPath` | Writes bad records/unreadable files as JSON under `<path>/<timestamp>/bad_records|bad_files/`; non-transactional; transient errors counted as bad | **Databricks** |
| Auto Loader `_rescued_data` | On by default for Auto Loader when it infers the schema | **Databricks** |

Gotcha: since Spark 2.3, a query on a raw JSON/CSV scan that references **only** the corrupt-record column is disallowed — cache or save the parsed result first.

### 4. Auto Loader schema evolution (Databricks)

With `cloudFiles.schemaLocation`, Auto Loader infers and **stores** the schema (JSON/CSV columns are strings unless `cloudFiles.inferColumnTypes=true`). `cloudFiles.schemaHints` pins types. `cloudFiles.schemaEvolutionMode`:

| Mode | On a new column |
|---|---|
| `addNewColumns` (**default**) | Stream **stops with `UnknownFieldException`** after updating the stored schema; **restart** picks up the new column. In jobs, a task retry handles this automatically |
| `addNewColumnsWithTypeWidening` | As above, plus safe type widening (int → long); incompatible changes → rescued column (newer runtimes — verify) |
| `rescue` | Schema never changes; new columns go to `_rescued_data`; stream keeps running |
| `failOnNewColumns` | Stream fails and stays failed until you change the schema or remove the file |
| `none` | New columns ignored; no failure |

### 5. Write-time: Delta schema enforcement

A write whose columns/types don't match the table **fails** (that's the point: Bronze/Silver contracts). Options:
- `mergeSchema = true` on the write (or `spark.databricks.delta.schema.autoMerge.enabled` for MERGE/session) — adds new columns; **reviewed, intentional** evolution.
- `overwriteSchema = true` with overwrite — replaces the schema (breaking for consumers).
- Type changes: safe widening via **type widening** table feature on newer runtimes (verify); otherwise rewrite the table or add a new column.
- `MERGE … WITH SCHEMA EVOLUTION` (Databricks SQL, newer runtimes — verify) for evolution inside MERGE.

### 6. How to recognise it — evidence

| Symptom | Evidence | Likely cause |
|---|---|---|
| Stream stopped, retry succeeded | Task error mentions `UnknownFieldException` / new fields in file | Auto Loader `addNewColumns` doing its job |
| Write failed: schema mismatch | Error class for metadata/schema mismatch (e.g. `DELTA_METADATA_MISMATCH`, verify) listing table vs data schema | Upstream added/changed a column |
| Counts dropped, no error | Bronze count ≫ Silver count; DROPMALFORMED used or inner filters; `_rescued_data` non-null | Silent drops |
| Column suddenly all NULL | `count(col)` dropped to 0 from one date; `_rescued_data` contains the field under a new name | Rename / case change upstream |
| FAILFAST error | `Malformed records are detected in record parsing` with the bad line | Corrupt file |

### 7. Fixes, ranked

1. **Bronze keeps everything**: schema-on-read with strings + `_rescued_data` (Auto Loader); never drop at ingestion.
2. **Silver validates explicitly**: `try_cast`, rules, **quarantine** rows with reasons; DQ thresholds fail the job when bad rows exceed a limit (11.7, capstone).
3. Choose evolution policy per layer: Bronze `addNewColumns` (with retries) or `rescue`; Silver/Gold explicit schemas; evolve via reviewed `mergeSchema`.
4. Alert on `_rescued_data IS NOT NULL` counts and schema changes (schema drift is a production event).
5. FAILFAST only where any bad record must stop everything (financial files with checksums).

### 8. Misconceptions

1. **"PERMISSIVE means no data loss."** Without a corrupt-record or rescued column, bad values become NULL and the raw value is gone.
2. **"DROPMALFORMED is cleaner."** It's silent data loss.
3. **"mergeSchema everywhere for robustness."** It lets any upstream change flow into curated tables; enforcement exists for a reason.
4. **"Auto Loader failing on a new column is a bug."** It's the default `addNewColumns` behaviour; restart (or task retry) continues with the evolved schema.

---

## B. Practical — read one bad file five ways

**Runnable notebook:** [`exercises/phase-11/04_schema_corrupt_input.py`](../../exercises/phase-11/04_schema_corrupt_input.py)

Input (6 lines): 2 good, 1 type mismatch (`"amount": "abc"`), 1 extra column (`coupon`), 1 case mismatch (`Amount`), 1 truncated line.

| Read | Expected result |
|---|---|
| PERMISSIVE + `_corrupt_record` | 6 rows; type-mismatch and malformed rows carry the raw text in `_corrupt_record`; extra column ignored silently |
| DROPMALFORMED | Fewer rows, no error — demonstrates silent loss |
| FAILFAST | Exception on first bad row |
| `rescuedDataColumn` [DBX] | 6 rows; `abc`, `coupon`, `Amount` captured as JSON with the file path |
| `badRecordsPath` [DBX] | Bad records written as JSON under the path; good rows returned |
| Delta write with extra column | Fails (enforcement) → succeeds with `mergeSchema` → type change still fails |
| Quarantine split | `good + quarantined == total` |

Key lines:

```python
permissive = permissive.cache()
display(permissive.where("_corrupt_record IS NOT NULL"))
```
Without the cache, Spark raises an error because the query references only the internal corrupt-record column of a raw scan.

```python
.option("rescuedDataColumn", "_rescued_data")
```
Databricks keeps the row and stores the non-conforming fields as JSON — the safest choice for Bronze.

**Common mistakes:** using DROPMALFORMED in production; forgetting the corrupt-record column must be declared in the schema (for CSV); treating `UnknownFieldException` as a code bug instead of configuring retries; running `mergeSchema` on Gold.

---

## C. Interview questions

### 🟢 Basic

**B1.** What are the three parse modes for Spark file sources?

<details><summary>Model answer</summary>

PERMISSIVE (default: bad fields → NULL, optionally keep the raw line in a corrupt-record column), DROPMALFORMED (drop rows that don't parse), FAILFAST (throw on the first malformed row).
</details>

**B2.** What is the rescued data column?

<details><summary>Model answer</summary>

A Databricks feature (default in Auto Loader, optional `rescuedDataColumn` for batch reads) that stores fields not matching the schema — missing from the schema, wrong type, or case-mismatched — as JSON in one column, with the source file path, so the row is loaded and nothing is lost.
</details>

**B3.** What does Delta schema enforcement do?

<details><summary>Model answer</summary>

Rejects writes whose schema doesn't match the table (unknown columns, incompatible types), so bad or unexpected data can't corrupt the table. Evolution must be explicit (`mergeSchema`, `overwriteSchema`, ALTER TABLE).
</details>

### 🟡 Medium

**M1.** An Auto Loader job fails every few weeks with `UnknownFieldException` and succeeds on the next run. Explain and improve.

<details><summary>Model answer</summary>

Symptom: intermittent failure that self-heals. Evidence: error says new fields were found; the schema location now contains a newer schema version. Root cause: default `addNewColumns` mode — on a new column it updates the stored schema and stops the stream so the next start uses it. Fix: expected behaviour → configure task retries (1–2) so it's automatic; alert on schema change so Silver owners review the new column; or use `rescue` mode if Bronze must never stop. Verify the new column appears in Bronze and nothing was lost.
</details>

**M2.** Silver row count dropped 15 % today, with no errors. How do you investigate?

<details><summary>Model answer</summary>

Reconcile Bronze vs Silver for the date; check quarantine counts and reasons; check `_rescued_data` non-null counts in Bronze (type changes/renames); compare null rates per column vs yesterday (a renamed column becomes all NULL → rule failures or inner-join drops); check whether DROPMALFORMED or a filter removes rows; check source file counts (partial delivery). Root cause at the step where counts diverge; fix the mapping/rules; backfill the date; add volume and null-rate alerts.
</details>

**M3.** When would you choose FAILFAST over PERMISSIVE + quarantine?

<details><summary>Model answer</summary>

When partial loads are worse than no load and any malformed record signals a corrupt delivery — e.g. regulatory/financial files with control totals, where you must reject the whole file and request a resend. Otherwise PERMISSIVE + rescued/corrupt column + quarantine + thresholds keeps the pipeline running while preserving bad records for investigation.
</details>

### 🔴 Tricky

**T1.** Upstream renamed `amount` to `Amount`. Your PERMISSIVE read shows correct values in dev but NULLs in prod. Why could they differ?

<details><summary>Model answer</summary>

Column matching depends on case sensitivity: with `spark.sql.caseSensitive=false` (default) the JSON reader may map `Amount` to `amount`; if prod sets case sensitivity on, or uses Auto Loader/rescued data (which treats case mismatches as rescued), the field ends up NULL with the value in `_rescued_data`. Evidence: compare configs and `_rescued_data` contents. Fix: explicit mapping in Silver, schema hints, alert on rescued data.
</details>

**T2.** A MERGE into Silver fails after a source added a column, but a plain append with `mergeSchema` worked yesterday. Why?

<details><summary>Model answer</summary>

`mergeSchema` is a write option for append/overwrite; MERGE uses session-level `spark.databricks.delta.schema.autoMerge.enabled` (or `MERGE WITH SCHEMA EVOLUTION` on newer runtimes) and only evolves for `UPDATE SET *`/`INSERT *` clauses. With explicit column lists, new source columns are simply not written. Decide whether the column belongs in Silver; evolve deliberately.
</details>

**T3.** Using `badRecordsPath`, today's run reported 200 bad records but a re-run of the same files reported 0. How?

<details><summary>Model answer</summary>

`badRecordsPath` is non-transactional and treats transient errors (e.g. a temporary I/O/network failure reading a file) as bad files. The first run hit a transient error. That's why the docs warn it can be inconsistent; prefer rescued data/quarantine tables for reproducibility, and always reconcile counts.
</details>

---

## E. Revision summary

- Read problems: malformed, type mismatch, extra/missing/renamed columns. Write problems: Delta enforcement.
- Modes: PERMISSIVE (+ `columnNameOfCorruptRecord`), DROPMALFORMED (silent loss), FAILFAST.
- [DBX] `rescuedDataColumn` keeps rows and captures mismatched fields; `badRecordsPath` non-transactional.
- Auto Loader: schemaLocation; default `addNewColumns` → stop with `UnknownFieldException` → restart/retry; `rescue`, `failOnNewColumns`, `none`.
- Evolve deliberately (`mergeSchema`, autoMerge for MERGE); quarantine bad rows; alert on rescued data and schema drift.