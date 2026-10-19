# 12.8 "What I Would Improve"

**Time:** 20 min · **Prerequisites:** 12.1–12.7, Phases 3, 6, 9, 11

**After this topic you can:**
- name 8 concrete weaknesses in a real platform without sounding negative about the team;
- for each: problem → why it matters → better approach → how you would sell and roll out the change;
- prioritise them (risk × effort) when asked "if you had one quarter, what would you do first?".

> Tone: "The platform worked and delivered; these are the next steps I'd push for." Criticise patterns, never people.
> Many of these were legacy choices made before newer features existed.

---

## A. The improvements

### 1. Two-statement SCD2 → single atomic MERGE / AUTO CDC

- **Problem:** expire with MERGE, then INSERT new versions — two commits.
- **Why it matters:** a failure between them leaves keys with no current row; curated joins drop them silently.
- **Better:** one MERGE with a staged union ([12.4](04-delta-cdc-scd-as-built.md)), identity surrogate keys; new pipelines on AUTO CDC.
- **Sell / roll out:** show the failure with a 10-row demo ([`04_atomic_scd2.py`](../../exercises/phase-12/04_atomic_scd2.py)); same table shape so
  no downstream change; convert the most-changed dimensions first behind a parallel run; add the "one current row per key" check as a DQ rule.

### 2. Heavy `collect()` / `toPandas()` → keep work distributed

- **Problem:** data pulled to the driver for loops, transformations or writes.
- **Why:** driver OOM as volumes grow, no parallelism, long runtimes.
- **Better:** DataFrame operations; `collect()` only for small control data (config rows, a max watermark); for
  pandas-style logic use pandas UDFs / `applyInPandas`.
- **Sell:** pick the slowest job, show the Spark UI (one task, driver memory), rewrite, report the runtime drop. Add a code-review checklist item.

### 3. Per-column DQ count → one aggregation pass

- **Problem:** `count()` per column — N scans for N columns.
- **Why:** wide tables (100+ columns) → minutes of wasted compute every run.
- **Better:** one `select` of `sum(col IS NULL)` per column ([`03_single_pass_dq.py`](../../exercises/phase-12/03_single_pass_dq.py)); expectations in new pipelines.
- **Sell:** it's a shared function — change once, every domain benefits; show timing before/after.

### 4. Restart via delete-then-insert, no retries → atomic overwrite + retries

- **Problem:** non-atomic restart; failures need manual intervention.
- **Why:** readers see empty slices; on-call effort; delayed SLAs.
- **Better:** `replaceWhere` overwrite of the run's slice or MERGE; activity/task retries with backoff for
  transient errors; timeouts; Job repair runs.
- **Sell:** count last quarter's manual reruns; propose a pilot on one wrapper pipeline.

### 5. External Parquet tables + retry loops for concurrent writes → Delta

- **Problem:** some tables were plain Parquet; concurrent writers handled by sleeping and retrying.
- **Why:** no ACID — partial writes are visible, concurrent writes can corrupt, no time travel, no MERGE.
- **Better:** convert to Delta (`CONVERT TO DELTA` in place) — atomic commits and optimistic concurrency; partition
  or cluster so concurrent writers touch disjoint files; idempotent writes.
- **Sell:** in-place conversion is cheap and reversible-ish (Parquet files stay); start with the tables that had incidents.

### 6. Static VACUUM list, little clustering → predictive optimisation, liquid clustering

- **Problem:** maintenance ran from a hand-maintained list; new tables were missed; few tables clustered.
- **Why:** small files and stale files accumulate; queries scan too much.
- **Better:** UC **managed tables** with **predictive optimisation** (automatic OPTIMIZE/VACUUM/ANALYZE);
  **liquid clustering** (`CLUSTER BY`) on common filter keys instead of over-partitioning.
- **Sell:** show file counts and query times on 3 big tables before/after; low risk, enable per schema.

### 7. SQL built by string formatting → parameterised SQL / DataFrame API

- **Problem:** `spark.sql(f"... WHERE id = '{value}'")` everywhere, including values from config/widgets.
- **Why:** injection risk, quoting bugs, hard to test.
- **Better:** `spark.sql("... WHERE id = :id", args={"id": value})`; `IDENTIFIER(:tbl)` for table names; DataFrame API.
- **Sell:** add a lint rule in the PR gate; fix on touch rather than a big-bang rewrite.

### 8. Many near-duplicate linked services and notebook copies → parameterisation + Git

- **Problem:** similar linked services per source/environment; backup copies of notebooks in the workspace.
- **Why:** fixes applied in one copy only; confusion about which version runs; drift.
- **Better:** **parameterised linked services** (server/database as parameters), global parameters; Git as the only
  history — delete copies; bundles so deployed code = repo code.
- **Sell:** inventory + "which copy is in production?" exercise usually convinces people; remove in waves.

### Prioritisation (one quarter)

| Priority | Item | Why first |
|---|---|---|
| 1 | Atomic SCD2 + atomic restart (1, 4) | Correctness risk, small change |
| 2 | Parquet → Delta for concurrently written tables (5) | Correctness risk |
| 3 | Single-pass DQ, collect() hotspots (2, 3) | Cost/runtime, easy wins |
| 4 | Predictive optimisation / clustering (6) | Cost/performance, low risk |
| 5 | Parameterised SQL, consolidation (7, 8) | Maintainability; fix-on-touch |

---

## B. Practical — prepare your "improve" answer

1. Pick **three** items you can talk about with evidence from your own work.
2. For each write: *the symptom I saw* (one sentence) · *the root cause* · *the fix* · *how I'd measure success*.
3. Practise the 60-second version: "Three things I'd improve: … The first because it's a correctness risk …"

Parameterised SQL example:

```python
spark.sql("SELECT * FROM IDENTIFIER(:tbl) WHERE flight_date = :d",
          args={"tbl": f"{CATALOG}.{SCHEMA}.flights", "d": "2026-10-01"})
```

---

## C. Interview questions

### 🟢 Basic

**B1.** What would you improve in your project?

<details><summary>Model answer</summary>

"Three things, in order of risk. First, our SCD2 was two statements, so a failure in between could leave keys
without a current row — I'd make it a single MERGE or AUTO CDC. Second, restart was delete-then-insert; I'd use
`replaceWhere` and retries. Third, performance hygiene: single-pass DQ checks, avoiding large `collect()`, and
predictive optimisation with liquid clustering. Each has a small pilot and a measurable outcome."
</details>

**B2.** When is `collect()` acceptable?

<details><summary>Model answer</summary>

When the result is small and bounded and needed on the driver: config rows, a single max watermark, a list of
partitions to process, a count for the audit table. Not for data you then transform or write.
</details>

**B3.** Why is string-formatted SQL a problem?

<details><summary>Model answer</summary>

Injection risk if any part comes from outside, quoting/escaping bugs, and it's harder to test. Use named
parameter markers with `args=` and `IDENTIFIER()` for object names, or the DataFrame API.
</details>

### 🟡 Medium

**M1.** Why would a team keep external Parquet over Delta — and what does it cost them?

<details><summary>Model answer</summary>

Habit, other tools reading the files directly, or "it's always worked". Cost: no atomic commits (readers see
partial writes), unsafe concurrent writes (hence retry loops), no MERGE/UPDATE/DELETE, no time travel, manual
small-file handling. Delta can be read by other engines too (or via UniForm/Iceberg readers), so the reason
usually doesn't hold.
</details>

**M2.** How would you convince the team to change the SCD2 pattern?

<details><summary>Model answer</summary>

Show, don't argue: a small reproducible demo where a failure between the statements leaves no current row and a
report misses passengers; propose a drop-in replacement with the same columns; run in parallel for a week with an
automated comparison; then roll out table by table.
</details>

**M3.** What's predictive optimisation?

<details><summary>Model answer</summary>

A Databricks feature for UC managed tables that automatically runs maintenance — OPTIMIZE, VACUUM, ANALYZE —
based on usage, so you don't maintain a static list of tables and schedules.
</details>

### 🔴 Tricky

**T1.** You have one quarter. Which improvement first and why?

<details><summary>Model answer</summary>

Correctness before cost: atomic SCD2 and atomic restart, because they can silently produce wrong data and the
fix is small and contained. Next, Delta for tables with concurrent writers. Then the cost/performance items,
which are measurable quick wins. Consolidation and parameterised SQL I'd do on touch.
</details>

**T2.** "If those problems existed, why didn't you fix them?"

<details><summary>Model answer</summary>

"Some we did — for example in the UC migration we replaced RDD code and mounts. Others were in code owned by
other teams or in stable pipelines where the migration was the priority; we raised them and some were in the
backlog. In a delivery project, changing working production code needs a business case and a test plan — which
is why I describe each with a pilot and a measure."
</details>

**T3.** Liquid clustering vs partitioning for a 2 TB bookings table filtered by flight_date and route?

<details><summary>Model answer</summary>

Partitioning by date works if each partition is large (≥ ~1 GB) and queries always filter by it; partitioning by
route too would create too many small partitions. Liquid clustering on (flight_date, route) handles both filters,
avoids small files, and keys can change later without rewriting the table layout manually. I'd choose liquid
clustering on a managed table with predictive optimisation.
</details>

---

## E. Revision summary

- Eight items: **atomic SCD2**, **no big collect**, **single-pass DQ**, **atomic restart + retries**, **Parquet → Delta**,
  **predictive optimisation + liquid clustering**, **parameterised SQL**, **consolidation + Git**.
- Each answer = problem → why → better → how to sell (demo, pilot, measure).
- Prioritise **correctness → reliability → cost → maintainability**.