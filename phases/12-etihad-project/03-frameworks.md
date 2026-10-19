# 12.3 Frameworks — Audit/Control Tables, Restartability, Data Quality, GDPR Retention

**Time:** 20 min · **Prerequisites:** 1.9, 1.12, Phase 11 (reconciliation)

**After this topic you can:**
- design an audit/control-table framework (config → run → activity → error) and explain each table;
- explain how restartability worked and why "delete-then-insert" is idempotent but **not atomic**;
- describe a configurable DQ framework and its alerting, and write a single-pass null check;
- explain rules-table-driven GDPR retention: purge vs anonymise, and the Delta-specific catch (VACUUM).

---

## A. Concept / story

### 1. The audit framework — tables and flow

```
CONFIG (static)                         RUNTIME (one row per run / activity)
┌──────────────────┐                    ┌───────────────────────┐   ┌─────────────────────────┐
│ pipeline_master  │──┐                 │ pipeline_run          │──►│ activity_run            │
│ (id, name, src,  │  │  daily seeding  │ (run_id, pipeline_id, │   │ (run_id, activity_id,   │
│  is_active)      │  ├────────────────►│  start, end, status   │   │  source_obj, target_obj,│
├──────────────────┤  │                 │  I/R/S/F, active_run) │   │  source_cnt, target_cnt,│
│ activity_master  │──┘                 └───────────────────────┘   │  start, end, status)    │
│ (activity, src → │                               │                └─────────────────────────┘
│  target object)  │                               └──────────────► error_log (run_id, activity_id,
└──────────────────┘                                                   seq, message, timestamp)
```

Status codes in such frameworks are typically **I**nitialised → **R**unning → **S**ucceeded / **F**ailed. Each run:

1. **Seed** today's expected pipeline/activity rows from config (status `I`).
2. **Start**: mark the pipeline row `R`, set start time, generate a run id.
3. **Per activity**: record source count (rows read / file rows) and target count (rows written), status.
4. **End**: `S` with end time, or `F` with a pointer to the error log.
5. **Reconcile**: `source_cnt` vs `target_cnt` — a mismatch beyond tolerance is a DQ failure.

### 2. Restartability

On restart the framework looked at today's rows: activities already `S` were **skipped**, failed ones re-run.
For re-run activities it **deleted the target rows tagged with that run/date, then re-inserted**.

| Property | Delete-then-insert | `replaceWhere` overwrite / MERGE |
|---|---|---|
| Idempotent (same end state on rerun) | ✅ | ✅ |
| Atomic (readers never see a gap) | ❌ two commits | ✅ one commit |
| Crash between the two steps | Slice missing until rerun | Not possible |

```python
# atomic: one Delta commit replaces exactly the run's slice
(df.write.mode("overwrite")
   .option("replaceWhere", f"load_date = '{run_date}'")
   .saveAsTable(target))
```

Generic theory: [1.12 Idempotency](../01-de-foundations/12-idempotency-fault-tolerance.md).

### 3. DQ framework

A rules table drove checks per table: **not null**, **uniqueness** of business keys, **referential** checks,
**allowed values**, **freshness**, and **count reconciliation** from the audit tables. Failures were written to a
DQ results table and an **email alert** was sent to the owning team. Severity decided whether the load stopped
(blocking) or continued with a warning.

| rule_id | table | column | rule_type | threshold | severity |
|---|---|---|---|---|---|
| 1 | staging.bookings | booking_id | not_null | 0 | block |
| 2 | staging.bookings | booking_id | unique | 0 | block |
| 3 | staging.bookings | cabin_class | allowed: F,J,Y | 0.01 | warn |

**The performance lesson:** the original null check ran one `count()` **per column** — N full scans. Rewritten as one
aggregation it is one scan ([03_single_pass_dq.py](../../exercises/phase-12/03_single_pass_dq.py)). In newer
pipelines, the same rules become **expectations** (`expect`, `expect_or_drop`, `expect_or_fail`) — [Phase 8](../08-workflows-sdp/).

### 4. GDPR retention / anonymisation

A **retention rules table** lists, per table: the date column that defines age, the retention period, and the
action — **purge** (delete rows) or **anonymise** (null/hash personal columns, keep the non-personal facts for
analytics).

| table | date_col | retain_months | action | pii_columns |
|---|---|---|---|---|
| staging.passengers | last_activity_date | 84 | anonymise | first_name, last_name, email, phone |
| raw.partner_contacts | ingest_date | 24 | purge | — |

A scheduled job reads the rules and generates `UPDATE … SET col = NULL/sha2(...)` or `DELETE … WHERE date_col < cutoff`.

**The Delta catch:** `DELETE`/`UPDATE` create new files; the old files with personal data **remain** for time travel
until `VACUUM` removes them after the retention window. Compliance requires running VACUUM (and reviewing
`delta.deletedFileRetentionDuration`), and remembering landing files, raw history and downstream copies.
Deletion vectors also mean data is only physically rewritten after purge/OPTIMIZE — then VACUUM.

### 5. Misconceptions

1. **"Counts match, so the data is right."** Counts catch loss/duplication, not wrong values.
2. **"DELETE in Delta removes the data."** Not physically until VACUUM.
3. **"Hashing is anonymisation."** A hash of an email is still linkable (pseudonymisation); use salted hashes or null.

---

## B. Practical

**Runnable notebooks:**
- [`exercises/phase-12/02_audit_framework.py`](../../exercises/phase-12/02_audit_framework.py) — run/activity/error
  tables, counts, status transitions, and atomic restart with `replaceWhere`.
- [`exercises/phase-12/03_single_pass_dq.py`](../../exercises/phase-12/03_single_pass_dq.py) — N-scan vs one-scan null check with timing.

A rules-driven retention sketch (generic):

```python
from pyspark.sql import functions as F
rules = [("main.de_prep.passengers", "last_activity_date", 84, "anonymise", ["email", "phone"])]
for table, date_col, months, action, pii in rules:
    cutoff = f"add_months(current_date(), -{months})"
    if action == "purge":
        spark.sql(f"DELETE FROM {table} WHERE {date_col} < {cutoff}")
    else:
        sets = ", ".join(f"{c} = NULL" for c in pii)
        spark.sql(f"UPDATE {table} SET {sets} WHERE {date_col} < {cutoff} AND {pii[0]} IS NOT NULL")
# then (after the retention window agreed with compliance): VACUUM table
```
(Identifiers here come from a governed rules table, not user input; validate them against `information_schema` before building SQL.)

---

## C. Interview questions

### 🟢 Basic

**B1.** What does an audit framework record?

<details><summary>Model answer</summary>

Per pipeline run: run id, start/end time, status and remarks; per activity: source and target object, source and
target counts, timings and status; and an error log with the message for failures. Config tables define which
pipelines and activities exist and are active.
</details>

**B2.** Why reconcile source and target counts?

<details><summary>Model answer</summary>

To detect silent data loss or duplication — a load can "succeed" and still drop rows due to a filter, a join or a
bad parse. Comparing rows read with rows written per activity, with a tolerance, turns that into an alert.
</details>

**B3.** Purge vs anonymise?

<details><summary>Model answer</summary>

Purge deletes the rows; anonymise removes or irreversibly masks personal columns but keeps the record for
non-personal analytics — e.g. keep that a booking happened on a route, drop who made it.
</details>

### 🟡 Medium

**M1.** How did restart work, and where is it weak?

<details><summary>Model answer</summary>

On restart, succeeded activities were skipped; failed ones deleted the target rows for that run/date and reloaded.
That's idempotent, but it's two transactions: a crash after the delete leaves the slice empty and readers in
between see missing data. I'd use one atomic `replaceWhere` overwrite or a MERGE, and add activity retries.
</details>

**M2.** You DELETE a passenger's data for GDPR. Is it gone?

<details><summary>Model answer</summary>

Not yet. Delta keeps the old data files for time travel until VACUUM removes files older than the retention
threshold. With deletion vectors, rows are marked deleted but files are rewritten later. So: delete, (optionally
`REORG … APPLY (PURGE)`), then VACUUM after the agreed window — and also handle landing files, backups, and
downstream copies.
</details>

**M3.** How would you make DQ checks cheap on wide tables?

<details><summary>Model answer</summary>

Compute all checks in one aggregation pass — `sum(col IS NULL)` for every column in one `select`, plus
`count` vs `countDistinct` for keys — instead of one `count()` per column. In declarative pipelines, expectations
are evaluated during the write itself.
</details>

### 🔴 Tricky

**T1.** Your audit row says Succeeded, counts match, but the business says yesterday's revenue is wrong. Possible causes?

<details><summary>Model answer</summary>

Counts don't validate values: a wrong join (right count, wrong match), a timezone or currency conversion issue,
a late-arriving correction not yet applied, an SCD join on the wrong version, or the source itself being wrong.
Add value-level checks (sum of fares vs source totals, distributions), and test business rules.
</details>

**T2.** Two runs of the same pipeline start at the same time (manual rerun while the schedule fires). What protects you?

<details><summary>Model answer</summary>

Our framework had an `active_run` flag that a run checks/sets — a simple lock, but not atomic if two runs check
at once. Better: orchestrator-level concurrency limits (max concurrent runs = 1 on a Job), and Delta writes that are
idempotent (MERGE/replaceWhere) so that even if both run, the result is correct; Delta's optimistic concurrency
will fail one conflicting writer rather than corrupt the table.
</details>

**T3.** Is a SHA-256 hash of an email address anonymised data?

<details><summary>Model answer</summary>

No — it's pseudonymised: anyone with a list of emails can hash and match them. For anonymisation, null the field
or replace with a random token not stored elsewhere; if a stable join key is needed, use a keyed hash (HMAC) with
the key in Key Vault, and treat it as personal data.
</details>

---

## E. Revision summary

- Audit = **config** (pipeline/activity master) + **runtime** (pipeline run, activity run with counts) + **error log**.
- Restart = skip succeeded, rerun failed; delete-then-insert is **idempotent but not atomic** → use `replaceWhere`/MERGE.
- DQ = rules table → checks → results + email; do it in **one pass**; newer = expectations.
- GDPR = rules table → purge or anonymise; **VACUUM** to physically remove; hashing ≠ anonymising.