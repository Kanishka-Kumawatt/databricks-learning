# 4.2 Reading and Writing CSV, JSON, Parquet and Delta

**Time:** 20 min · **Prerequisites:** 4.1

**After this topic you can:**
- read CSV/JSON with an explicit schema and the right options (`header`, `multiLine`, `mode`, `columnNameOfCorruptRecord`);
- predict exactly which rows PERMISSIVE, DROPMALFORMED and FAILFAST return for a messy file;
- write with the right save mode and `partitionBy`, and explain the files it produces;
- explain why Delta is the default target format on Databricks.

---

## A. Concept deep dive

### 1. The simplest explanation

`spark.read` turns files/tables into a DataFrame; `df.write` turns a DataFrame into files/tables. Both are configured by **format + options + (schema)**:

```python
spark.read.format("csv").option("header", True).schema(ddl).load(path)    # generic form
spark.read.csv(path, header=True, schema=ddl)                               # shortcut, same thing
df.write.format("delta").mode("append").saveAsTable("cat.sch.tbl")
```

### 2. The formats compared

| | CSV | JSON | Parquet | Delta |
|---|---|---|---|---|
| Layout | Row text | Row text (nested OK) | **Columnar** binary | Parquet files + `_delta_log` |
| Schema in file | No (header names only) | No (inferred per record) | **Yes** | **Yes**, enforced on write |
| Types | Everything is a string until parsed | Strings/numbers/bools/nesting | Full Spark types | Full Spark types |
| Column pruning / predicate pushdown | No | No | Yes | Yes + data skipping stats |
| Splittable when compressed | Not with gzip | Not with gzip | Yes | Yes |
| ACID, MERGE/UPDATE/DELETE, time travel | No | No | No | **Yes** |
| Typical layer | Landing/Bronze input | Landing/Bronze input | Exchange format | **Bronze/Silver/Gold tables** |

### 3. Read options you must know

| Option | Formats | Default | Meaning |
|---|---|---|---|
| `header` | CSV | `false` | First line = column names |
| `inferSchema` | CSV | `false` | Infer types — **requires one extra pass over the data** |
| `samplingRatio` | CSV, JSON | `1.0` | Fraction of rows used for inference |
| `sep` / `delimiter` | CSV | `,` | Field separator |
| `quote`, `escape` | CSV | `"`, `\` | Quoted values containing the separator: `"Wu, Chen"` |
| `nullValue` | CSV | none | String to treat as NULL (e.g. `"NA"`) |
| `dateFormat`, `timestampFormat` | CSV, JSON | ISO patterns | Parsing patterns |
| `multiLine` | CSV, JSON | `false` | JSON: one record spans many lines / a top-level array. CSV: quoted values contain newlines |
| `mode` | CSV, JSON | `PERMISSIVE` | What to do with corrupt records (below) |
| `columnNameOfCorruptRecord` | CSV, JSON | `_corrupt_record` (from `spark.sql.columnNameOfCorruptRecord`) | Column that receives the raw malformed record in PERMISSIVE mode |
| `recursiveFileLookup`, `pathGlobFilter` | file sources | off | Read nested folders / filter file names |

### 4. Parse modes — exactly what they do

| Mode | Malformed record | Use when |
|---|---|---|
| `PERMISSIVE` (default) | Kept; malformed fields set to NULL; raw text put in the corrupt-record column **only if that column is in your schema** | Bronze/Silver ingestion — keep and quarantine |
| `DROPMALFORMED` | Whole record silently dropped | Rarely — silent data loss |
| `FAILFAST` | Read fails with an exception | Strict contracts where any bad row must stop the pipeline |

Subtleties (from the Spark docs):
- **CSV: a row with fewer or more tokens than the schema is not "corrupt"** — missing fields become NULL, extra tokens are dropped.
- Spark parses only the **columns the query needs** (CSV column pruning), so which rows count as corrupt can depend on the columns you select.
- A query that references **only** the corrupt-record column of a raw CSV/JSON read is disallowed; cache/persist the parsed DataFrame first or select it together with other columns.
- On Databricks, Auto Loader and `read_files` add a **`_rescued_data`** column by default instead (Phase 7) — more robust than `_corrupt_record`.

### 5. Writing: save modes and layout

| Mode | If target exists | Note |
|---|---|---|
| `errorifexists` / `error` (default) | Fail | Protects against accidents |
| `append` | Add rows | Not idempotent on re-run (duplicates) |
| `overwrite` | Replace all data | Delta: atomic, old version still in history. Plain Parquet: deletes then writes — readers can see an empty/partial folder, a failure can lose data |
| `ignore` | Do nothing | Silent — rarely what you want |

- **Number of files** written ≈ number of DataFrame partitions (× number of `partitionBy` folders each one touches). This is why you `repartition`/`coalesce` before writing (4.11).
- **`partitionBy("col")`** writes one folder per value: `.../channel=app/part-...parquet`. The partition column is **not stored inside the files**; it's rebuilt from folder names on read (partition discovery). Choose low-cardinality columns that queries filter on (date, region) — never `customer_id`. On Databricks Delta, **liquid clustering** is now usually recommended over partitioning for new tables (Phase 6).
- **Delta overwrite of a slice**: `.option("replaceWhere", "order_date = '2026-03-01'")` atomically replaces only matching rows — the idempotent daily-reload pattern.
- `saveAsTable` registers a **managed** table in Unity Catalog; `.save(path)` writes files only (external location / volume).

### 6. How Spark executes reads and writes

- A read with a declared schema runs **no job** until an action. `inferSchema` (CSV) and JSON inference run an **immediate job** to scan the files.
- File scans are split into input partitions by size (`spark.sql.files.maxPartitionBytes`, default 128 MB), except non-splittable files (gzip CSV/JSON → one task per file).
- Parquet/Delta: **column pruning** reads only selected columns; **predicate pushdown** skips row groups/files using min/max statistics.
- A write is an action: one task per DataFrame partition writes its files; the driver then **commits** (Delta: a single atomic JSON commit in `_delta_log`).

### 7. Misconceptions

1. **"PERMISSIVE keeps the bad text for me."** Only if your schema contains the corrupt-record column (or inference added it for JSON).
2. **"`mode('overwrite')` on Parquet is safe."** It isn't atomic. Delta's is.
3. **"`partitionBy` makes everything faster."** High-cardinality partitions create millions of tiny files and slow everything.
4. **"JSON inference gives me a map for key/value objects."** It infers a `STRUCT` with one field per key seen. Declare `MAP<STRING,STRING>` if keys vary.

---

## B. Practical

**Runnable notebook:** [`exercises/phase-04/01_dataframes_io.py`](../../exercises/phase-04/01_dataframes_io.py)

### Example 1 — one messy CSV, three parse modes

Input file `/Volumes/main/de_prep/raw/customers_csv/customers_messy.csv` (created by the setup notebook):

```
customer_id,name,signup_date,credit_limit
C1,Asha Rao,2025-01-10,5000
C2,Ben Ali,2025-03-15,abc
C3,"Wu, Chen",2025-06-20,3000
C4,Dana Lee,not-a-date,2500
C5,Eve Khan,2025-11-11
```

```python
PATH = "/Volumes/main/de_prep/raw/customers_csv/customers_messy.csv"
SCHEMA = "customer_id STRING, name STRING, signup_date DATE, credit_limit INT, _corrupt_record STRING"   # 1

permissive = (spark.read
    .option("header", True)                                                   # 2
    .option("mode", "PERMISSIVE")                                             # 3
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(SCHEMA)
    .csv(PATH)
    .cache())                                                                 # 4
display(permissive)

dropped = spark.read.option("header", True).option("mode", "DROPMALFORMED") \
    .schema("customer_id STRING, name STRING, signup_date DATE, credit_limit INT").csv(PATH)   # 5
display(dropped)

failfast = spark.read.option("header", True).option("mode", "FAILFAST") \
    .schema("customer_id STRING, name STRING, signup_date DATE, credit_limit INT").csv(PATH)
failfast.count()                                                              # 6 → raises
```

Equivalent SQL on Databricks (`read_files`, DBR 13.3 LTS+; note it also adds a `_rescued_data` column by default):

```sql
SELECT * FROM read_files('/Volumes/main/de_prep/raw/customers_csv/customers_messy.csv',
  format => 'csv', header => true, mode => 'PERMISSIVE',
  schema => 'customer_id STRING, name STRING, signup_date DATE, credit_limit INT, _corrupt_record STRING');
```

Expected — **PERMISSIVE** (5 rows):

| customer_id | name | signup_date | credit_limit | _corrupt_record |
|---|---|---|---|---|
| C1 | Asha Rao | 2025-01-10 | 5000 | null |
| C2 | Ben Ali | 2025-03-15 | null | C2,Ben Ali,2025-03-15,abc |
| C3 | Wu, Chen | 2025-06-20 | 3000 | null |
| C4 | Dana Lee | null | 2500 | C4,Dana Lee,not-a-date,2500 |
| C5 | Eve Khan | 2025-11-11 | null | null |

Expected — **DROPMALFORMED** (3 rows): C1, C3, C5. Expected — **FAILFAST**: the action fails with a malformed-record error (the first bad row is C2).

| # | Explanation |
|---|---|
| 1 | Explicit schema, plus a `STRING` column to receive malformed lines |
| 2 | First line holds column names, not data |
| 3 | PERMISSIVE: keep everything; unparseable *fields* become NULL and the raw line goes to `_corrupt_record` |
| 4 | Cache so we can safely filter on `_corrupt_record` alone later (`permissive.filter("_corrupt_record IS NOT NULL")`) |
| 5 | DROPMALFORMED silently discards C2 and C4 — 40 % of the file gone without an error |
| 6 | FAILFAST is lazy too: the error appears at the first action, not at `.csv()` |

Notes on the rows:
- `"Wu, Chen"` is quoted, so its comma is data, not a separator.
- **C5 has only 3 tokens: not corrupt** — `credit_limit` is simply NULL. Missing data and malformed data are different problems; validate both.

**Execution:** one scan job per action; no shuffle. `cache()` materialises on the first action.

### Example 2 — `inferSchema` on the same file

```python
spark.read.option("header", True).option("inferSchema", True).csv(PATH).printSchema()
```

Expected:

```
root
 |-- customer_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- signup_date: string (nullable = true)
 |-- credit_limit: string (nullable = true)
```

One bad value per column (`not-a-date`, `abc`) is enough to make the whole column `string`. And a job ran just to discover that. This is why production reads declare schemas.

### Example 3 — JSON lines vs multi-line JSON

```python
VOL = "/Volumes/main/de_prep/raw"
lines = spark.read.json(f"{VOL}/events_json/events.jsonl")            # 1 one JSON object per line
print(lines.columns)

bad = spark.read.json(f"{VOL}/events_multiline/events.json")         # 2 a pretty-printed array
good = spark.read.option("multiLine", True).json(f"{VOL}/events_multiline/events.json")   # 3
display(good)
```

Expected:
- `lines.columns` → `['_corrupt_record', 'customer', 'event_id', 'event_ts', 'event_type', 'items', 'props']` — 7 rows. Inference sorts fields alphabetically, added `_corrupt_record` because line E7 is malformed, inferred `event_ts` as **string** (timestamp inference is off by default), `items.qty` as `bigint` (JSON integers infer as long), and `props` as a **struct** `<campaign, page>` (not a map).
- `bad` has only a `_corrupt_record` column — each physical line was parsed as a separate record.
- `good` (2 rows):

| customer | event_id | event_type |
|---|---|---|
| {"id": "C1"} | M1 | view |
| {"id": "C2"} | M2 | purchase |

| # | Explanation |
|---|---|
| 1 | Default JSON = JSON Lines (one record per line) — splittable, parallel |
| 2 | Without `multiLine`, a pretty-printed document is garbage to the reader |
| 3 | `multiLine` reads each **file** as a whole — one task per file, so avoid for huge single files |

### Example 4 — writing: modes and `partitionBy`

Input: `orders` (10 rows; `channel` = app ×4, web ×6).

```python
OUT = f"{VOL}/out/orders_by_channel"
orders = spark.table("main.de_prep.orders")

(orders.write.mode("overwrite")                  # 1
       .partitionBy("channel")                   # 2
       .parquet(OUT))

display(dbutils.fs.ls(OUT))                      # 3
back = spark.read.parquet(OUT)
back.printSchema()                               # 4
back.filter("channel = 'app'").count()           # 5 → 4

orders.write.mode("errorifexists").parquet(OUT)  # 6 → fails: path already exists

(orders.write.format("delta").mode("overwrite")  # 7
       .saveAsTable("main.de_prep.orders_copy"))
```

SQL equivalent for the Delta write: `CREATE OR REPLACE TABLE main.de_prep.orders_copy AS SELECT * FROM main.de_prep.orders;`

Expected:
- Listing shows folders `channel=app/` and `channel=web/` (plus `_SUCCESS` and possibly `_committed_*`/`_started_*` marker files on Databricks).
- `back.printSchema()`: `channel` appears **last** — it came back from the folder names, not the files.
- Filter on `channel` reads only `channel=app/` (partition pruning) → 4.

| # | Explanation |
|---|---|
| 1 | Replace whatever is at `OUT` (non-atomic for Parquet) |
| 2 | One folder per distinct `channel` value; rows of each value go into that folder |
| 3 | Inspect the physical layout |
| 4 | Partition discovery adds `channel` from the paths |
| 5 | Partition pruning: only matching folders are listed and read |
| 6 | Default save mode refuses to overwrite |
| 7 | Delta managed table: atomic overwrite, new version in history, schema enforced on later appends |

### Common mistakes

- Using `inferSchema` in production, or forgetting `header` (the header becomes a data row and every typed column fails).
- PERMISSIVE without the corrupt-record column in the schema → malformed rows look like "rows with some NULLs" and nobody notices.
- `DROPMALFORMED` in Silver → silent data loss.
- `mode("append")` in a job that may be retried → duplicates (use MERGE or `replaceWhere`).
- `partitionBy` on a high-cardinality column → small-file explosion.
- Reading a pretty-printed JSON file without `multiLine`.

---

## C. Interview questions

### 🟢 Basic

**B1.** Name the three CSV/JSON parse modes and what each does with a malformed row.

<details><summary>Model answer</summary>

PERMISSIVE (default): keeps the row, sets malformed fields to NULL, and puts the raw record in the corrupt-record column if the schema has it. DROPMALFORMED: drops the whole row silently. FAILFAST: throws an exception at the first malformed record.
</details>

**B2.** Write code to read a headered CSV with a declared schema and keep malformed lines.

<details><summary>Model answer</summary>

```python
df = (spark.read.option("header", True)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .schema("id STRING, amount DECIMAL(10,2), ts TIMESTAMP, _corrupt_record STRING")
      .csv(path)
      .cache())
bad = df.filter("_corrupt_record IS NOT NULL")
good = df.filter("_corrupt_record IS NULL").drop("_corrupt_record")
```
</details>

**B3.** What are the four save modes and the default?

<details><summary>Model answer</summary>

`errorifexists`/`error` (default — fail if target exists), `append`, `overwrite`, `ignore` (do nothing if exists).
</details>

### 🟡 Medium

**M1.** Why is Parquet faster than CSV for `SELECT SUM(amount) FROM t WHERE order_date = '2026-03-01'`?

<details><summary>Model answer</summary>

Parquet is columnar: Spark reads only the `amount` and `order_date` columns (pruning), uses row-group min/max statistics to skip data (predicate pushdown), and reads typed, compressed, splittable data. CSV must read and parse every byte of every row as text.
</details>

**M2.** You write `df.write.partitionBy("customer_id").parquet(p)` for 5 million customers. What happens?

<details><summary>Model answer</summary>

Millions of folders, each with one or more tiny files: very slow writes and metadata listing, terrible read performance, possible driver memory pressure. Partition only by low-cardinality filter columns (date, region), or use Delta liquid clustering / Z-order for high-cardinality keys.
</details>

**M3.** In PERMISSIVE mode, a CSV row has 3 values but the schema has 4 columns. Is it corrupt?

<details><summary>Model answer</summary>

No. Per the Spark CSV docs, a record with fewer tokens than the schema is not corrupt — the missing trailing fields are set to NULL (extra tokens are dropped). It won't appear in `_corrupt_record`, so completeness must be validated separately (null checks).
</details>

### 🔴 Tricky

**T1.** `df.filter("_corrupt_record IS NOT NULL").count()` on a fresh CSV read throws an error. Why, and what's the fix?

<details><summary>Model answer</summary>

Spark disallows queries over raw CSV/JSON files that reference only the internal corrupt-record column, because with column pruning the parser would only parse the columns needed and the corrupt-record result would be inconsistent. Fix: `cache()`/persist the parsed DataFrame first (or save it), then filter.
</details>

**T2.** A daily job does `df.write.mode("overwrite").parquet(path)` and fails halfway. What state is the data in? How would Delta differ?

<details><summary>Model answer</summary>

With plain Parquet the old files may already be deleted and only some new files written — readers see partial or empty data, and yesterday's data may be gone. With Delta, data files are written first and the overwrite becomes visible only when the single commit file is written; a failure before commit leaves the previous version intact (orphan files are cleaned by VACUUM), and you can time-travel to earlier versions.
</details>

**T3.** The same DROPMALFORMED read returns 98 rows for `df.count()` and 95 rows for `df.select("amount").count()`... or the other way round. How is that possible?

<details><summary>Model answer</summary>

CSV column pruning (`spark.sql.csv.parser.columnPruning.enabled`, on by default): Spark parses only the columns the query requires, so a row whose malformed value is in an unselected column isn't detected as malformed — and isn't dropped. The set of "corrupt" rows depends on the projection. That's another reason to avoid DROPMALFORMED: its result isn't even stable.
</details>

---

## E. Revision summary

- Read = format + options + **schema**. Declare schemas; `inferSchema` costs an extra pass and guesses badly.
- PERMISSIVE (default) + corrupt-record column in the schema → keep and quarantine. DROPMALFORMED = silent loss. FAILFAST = stop.
- CSV: short rows aren't corrupt; quoted commas are data; column pruning changes which rows look malformed.
- JSON Lines is the default; pretty-printed → `multiLine`. JSON inference: alphabetical fields, objects → structs, adds `_corrupt_record`.
- Save modes: errorifexists (default), append, overwrite, ignore. Parquet overwrite isn't atomic; Delta's is. `replaceWhere` for idempotent slice reloads.
- `partitionBy` → one folder per value, column restored from path; low cardinality only.