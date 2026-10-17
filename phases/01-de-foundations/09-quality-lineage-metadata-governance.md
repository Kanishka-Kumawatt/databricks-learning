# 1.9 Data Quality, Lineage, Metadata and Governance

**Time:** 35 min · **Prerequisites:** 1.8

**After this topic you can:**
- name the data-quality dimensions and write a check for each;
- choose between warn / drop / quarantine / fail;
- explain lineage, the three kinds of metadata, and what governance means in practice;
- describe what Unity Catalog provides for each.

---

## A. Concept deep dive

### 1. The simplest explanation

- **Data quality** — is the data *fit for its purpose*: correct, complete, on time?
- **Lineage** — where did this data come from, and what depends on it?
- **Metadata** — data about data: schema, owner, meaning, freshness.
- **Governance** — the rules and controls for who can access what, how data is classified, and how compliance is proven.

They are linked: you can't govern what you can't describe (metadata), can't trust what you can't trace (lineage), and can't use what isn't correct (quality).

### 2. Data-quality dimensions

| Dimension | Question | Example check (ShopCo) |
|---|---|---|
| **Completeness** | Is required data present? | `order_id IS NOT NULL`; today's row count ≥ 50 % of 7-day average |
| **Validity** | Does it match format/domain rules? | `status IN ('PLACED','SHIPPED','CANCELLED')`; email matches a pattern |
| **Accuracy** | Does it reflect reality? | Totals reconcile with the finance system |
| **Consistency** | Do related data agree? | Order total = sum of line amounts; same customer_id format across sources |
| **Uniqueness** | No unwanted duplicates? | One row per `order_id` in Silver |
| **Timeliness / freshness** | Is it recent enough? | `max(order_ts)` within the last 2 hours; table updated before 07:00 |
| **Integrity** | Do references resolve? | Every `customer_id` in orders exists in customers |

### 3. Where to check — quality at every layer

| Layer | Typical checks |
|---|---|
| Ingestion / Bronze | File arrived? Readable? Expected columns present? Row count not zero? |
| Silver | Row-level rules (not null, ranges, domains, formats), uniqueness, referential integrity |
| Gold | Business rules, reconciliation against Silver and source totals, freshness SLAs |

### 4. What to do when a check fails

| Action | When | Risk |
|---|---|---|
| **Warn** (log metric, keep row) | Soft rules, monitoring trends | Bad data reaches consumers |
| **Drop** | Truly irrelevant rows (test records) | Silent loss if used carelessly |
| **Quarantine** (write row + reason to a separate table) | Row-level problems where the rest of the batch is valuable | Needs a process to review/reprocess |
| **Fail the pipeline** | Problems that invalidate the dataset (schema broken, > X % bad, key nulls everywhere) | Late data, but no wrong data |

Production pattern: **quarantine rows + fail if the bad ratio exceeds a threshold + alert**.

**NULL trap:** in SQL, `NULL > 0` is `NULL`, not `FALSE`. A filter `WHERE amount > 0` silently removes nulls; a "failed rules" check written as `NOT (amount > 0)` also returns `NULL` and **doesn't flag** the row. Always treat a `NULL` rule result as a failure (`COALESCE(condition, FALSE)`).

**Built-in enforcement in Databricks:**
- Delta **`NOT NULL`** and **`CHECK` constraints** — every write that violates them fails as a whole.
- Lakeflow Spark Declarative Pipelines **expectations** (`expect`, `expect_or_drop`, `expect_or_fail`) — Phase 8.

### 5. Lineage

**Table-level:** `bronze_orders → silver_orders → gold_daily_revenue → Revenue dashboard`.
**Column-level:** `gold.revenue` is computed from `silver_orders.amount`, which comes from `bronze_orders.amount`.

| Use | Example |
|---|---|
| **Impact analysis** | "If I rename `amount`, which tables and dashboards break?" |
| **Root-cause analysis** | "Revenue is wrong — which upstream table changed?" |
| **Compliance** | "Show every table derived from customer emails" (GDPR) |
| **Trust** | Consumers can see where a number comes from |

**Unity Catalog captures lineage automatically** (table and column level) for queries run on Unity Catalog-enabled compute — no extra code. You view it in Catalog Explorer or query system tables.

### 6. Metadata — three kinds

| Kind | Examples | Where in Databricks |
|---|---|---|
| **Technical** | Schema, types, location, format, partitions, size, file count | `DESCRIBE TABLE EXTENDED`, `DESCRIBE DETAIL`, `information_schema` |
| **Operational** | Last update time, row counts per run, job durations, versions | `DESCRIBE HISTORY`, job run history, system tables, your audit tables |
| **Business** | Meaning of "active customer", owner, steward, sensitivity | Table/column **comments**, **tags**, ownership in Unity Catalog |

### 7. Governance

Governance answers: **who can do what with which data, and can we prove it?**

| Capability | Unity Catalog feature |
|---|---|
| Central permissions | `GRANT SELECT ON TABLE … TO group` — one model for all workspaces |
| Least privilege | Grant to groups, minimum needed, at catalog/schema/table level |
| Fine-grained access | **Row filters** and **column masks** |
| Classification | Tags (e.g. `contains_pii = true`) |
| Audit | Audit logs / system tables: who read or changed what, when |
| Lineage | Automatic |
| Ownership | Every securable object has an owner |

Governance is also **process**: data owners, retention rules (GDPR "right to be forgotten"), approval flows.

### 8. Misconceptions

1. **"Quality = no nulls."** Quality has many dimensions; freshness and reconciliation failures are common and silent.
2. **"Drop bad rows and move on."** Silent loss — quarantine with reasons and monitor.
3. **"Lineage is documentation someone writes."** Manual lineage is always out of date; use automatic capture.
4. **"Governance slows teams down."** Good governance (self-service with clear permissions) speeds access up safely.

---

## B. Practical — rule-driven quarantine, thresholds, constraints

**Runnable notebook:** [`exercises/phase-01/09_data_quality_quarantine.py`](../../exercises/phase-01/09_data_quality_quarantine.py)

```python
RULES = {
    "order_id_not_null":    "order_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL",
    "amount_positive":      "amount > 0",
    "status_allowed":       "status IN ('PLACED', 'SHIPPED', 'CANCELLED')",
    "order_ts_present":     "order_ts IS NOT NULL",
    "email_format":         "email RLIKE '^[^@]+@[^@]+\\\\.[^@]+$'",   # \\\\ in Python -> \\ in SQL literal -> \. in regex
}

def apply_rules(df, rules):
    reasons = [F.when(~F.coalesce(F.expr(cond), F.lit(False)), F.lit(name))
               for name, cond in rules.items()]
    return df.withColumn("_dq_errors",
                         F.filter(F.array(*reasons), lambda x: x.isNotNull()))
```

| Part | Explanation |
|---|---|
| `RULES` dict | Rules are **data**, not code — easy to review, extend, or load from a config table |
| `F.expr(cond)` | Turns the SQL string into a column expression |
| `F.coalesce(..., False)` | **NULL → failed** (the NULL trap) |
| `~` | NOT: we want the name of rules that are *not* satisfied |
| `F.when(..., name)` | Rule name if it failed, otherwise null |
| `F.filter(array, not null)` | Keeps only failed rule names → `_dq_errors` array |

### Input → result

| order_id | customer_id | amount | status | order_ts | email | `_dq_errors` |
|---|---|---|---|---|---|---|
| 101 | C1 | 250.0 | PLACED | 2026-10-16 09:15 | asha@… | `[]` → **valid** |
| 102 | C2 | null | SHIPPED | 2026-10-16 10:02 | ben@… | `[amount_positive]` |
| null | C3 | 40.0 | PLACED | 2026-10-16 11:30 | chen@… | `[order_id_not_null]` |
| 104 | C1 | -5.0 | DELIVERED | 2026-10-16 14:45 | asha@… | `[amount_positive, status_allowed]` |
| 105 | null | 60.0 | PLACED | null | not-an-email | `[customer_id_not_null, order_ts_present, email_format]` |

1 valid, 4 quarantined (80 %) — in real life that should **fail** the run; the demo threshold is set high so you can see the outputs.

The notebook also adds a `NOT NULL` + `CHECK` constraint, comments and tags, shows a write rejected by the constraint, and includes (commented) `GRANT` and a **column mask**:

```sql
CREATE OR REPLACE FUNCTION main.de_prep.mask_email(email STRING)
  RETURN CASE WHEN is_account_group_member('pii_readers') THEN email ELSE '***@***' END;
ALTER TABLE main.de_prep.dq_orders ALTER COLUMN email SET MASK main.de_prep.mask_email;
```

### Common mistakes

- Writing rules that let NULLs pass.
- Quarantining without a reason column → nobody can fix the data.
- No threshold → a completely broken file "succeeds" with 100 % quarantined.
- Checking only rows, never totals/freshness.

---

## C. Interview questions

### 🟢 Basic

**B1.** Name five data-quality dimensions with one example check each.

<details><summary>Model answer</summary>

Completeness (`order_id IS NOT NULL`, row count vs average), validity (`status` in allowed set), uniqueness (one row per `order_id`), consistency (order total = sum of lines), timeliness (table updated before 07:00 / max timestamp recent), accuracy (totals reconcile with finance), integrity (every customer_id exists in customers).
</details>

**B2.** What is data lineage and why is it useful?

<details><summary>Model answer</summary>

The record of how data flows from sources through transformations to its consumers, at table and column level. Used for impact analysis before changes, root-cause analysis when numbers are wrong, compliance (where does PII go), and trust.
</details>

### 🟡 Medium

**M1.** Quarantine vs fail the pipeline — how do you decide?

<details><summary>Model answer</summary>

Quarantine row-level issues when the rest of the batch is still useful, record the reason, and alert. Fail when the problem invalidates the dataset or the consequences of partial data are worse than late data (schema broken, key fields null everywhere, bad ratio above threshold, financial reporting). Commonly: quarantine + fail above a threshold.
</details>

**M2.** Your rule is `WHERE NOT (amount > 0)` to find bad rows. Rows with `amount = NULL` aren't found. Why?

<details><summary>Model answer</summary>

SQL three-valued logic: `NULL > 0` is NULL and `NOT NULL` is still NULL, which a WHERE treats as not-true, so the row isn't selected. Use `NOT COALESCE(amount > 0, FALSE)` or check `amount IS NULL OR amount <= 0`.
</details>

**M3.** What's the difference between technical, operational and business metadata?

<details><summary>Model answer</summary>

Technical: structure and storage (schema, types, location, format). Operational: how the data behaves in production (last update, row counts, run durations, versions). Business: meaning and ownership (definitions, owner, sensitivity), usually captured as comments and tags.
</details>

### 🔴 Tricky

**T1.** All quality checks pass, every job is green, but the dashboard shows yesterday's numbers at 10:00. Which dimension failed and how would you detect it automatically?

<details><summary>Model answer</summary>

Timeliness/freshness. Possibly the upstream file never arrived, so the job processed nothing "successfully". Detect with freshness checks: max event timestamp or last commit time of the table vs an SLA, and row-count-is-zero alerts — data checks, not just job status.
</details>

**T2.** A Delta `CHECK (amount > 0)` constraint exists. A batch of 1 million rows contains one negative amount. What happens, and is that what you want?

<details><summary>Model answer</summary>

The whole write fails — constraints are all-or-nothing per transaction. Good as a last line of defence (guarantees the table never contains invalid data), but you usually want row-level handling *before* the write (quarantine), so one bad row doesn't block a million good ones; the constraint then catches logic bugs.
</details>

**T3.** Legal asks: "Which dashboards use customer email addresses?" How do you answer quickly?

<details><summary>Model answer</summary>

Use column-level lineage in Unity Catalog starting from the email column of the customer source table, following downstream tables, views and their consumers (dashboards/notebooks/jobs). Tags such as `contains_pii` make it searchable. Without automatic lineage you'd be grepping code — slow and unreliable.
</details>

---

## E. Revision summary

- Quality dimensions: completeness, validity, accuracy, consistency, uniqueness, timeliness, integrity.
- Actions: warn / drop / **quarantine with reason** / **fail above threshold**.
- **NULL trap**: a NULL rule result must count as failure.
- Lineage → impact analysis, root cause, compliance; Unity Catalog captures it automatically.
- Metadata: technical, operational, business. Governance: permissions, least privilege, masks/filters, tags, audit — Unity Catalog.