# 4.1 DataFrames, Schemas and Spark Data Types

**Time:** 20 min · **Prerequisites:** Phase 2 (lazy evaluation, transformations vs actions), [setup notebook](../../exercises/phase-04/00_setup_datasets.py) run once

**After this topic you can:**
- explain what a DataFrame really is (a lazy, typed plan — not data in memory);
- declare a schema both as a DDL string and as a `StructType`, and say when each is better;
- choose the right type for money (`DECIMAL`, not `DOUBLE`), for time (`TIMESTAMP` vs `TIMESTAMP_NTZ` vs `DATE`) and for IDs;
- explain what ANSI mode changes and use `try_cast` / `try_divide` safely.

---

## A. Concept deep dive

### 1. The simplest explanation

A **DataFrame** is a table with **named, typed columns** that Spark knows how to compute. It is **not** the data itself: it is a **recipe** (a logical plan) plus a **schema** (column names + types). Nothing is read or computed until an **action** (`show`, `count`, `write`, `collect`) runs.

```
spark.table("orders")          ──▶  plan: Scan orders                          (no data read yet)
  .filter("status = 'PAID'")   ──▶  plan: Filter ← Scan
  .select("order_id")          ──▶  plan: Project ← Filter ← Scan
  .count()                     ──▶  ACTION: Catalyst optimises the plan, then tasks run on executors
```

### 2. Why schemas exist

| Without a declared schema | With a declared schema |
|---|---|
| Spark must **infer** types → an extra pass over the files (CSV/JSON) | No inference pass — faster startup |
| One bad value (`"abc"`) silently turns a whole column into `string` | Bad values are caught (null + corrupt record, or an error) |
| Types can change from one day's file to the next → downstream breaks | Contract is fixed; drift is detected |
| `"00123"` inferred as `123` (leading zeros lost) | ID kept as `STRING` |

In production **always declare schemas** for file sources (or use Auto Loader schema hints, Phase 7). Inference is fine for exploration.

### 3. Two ways to write a schema

| | DDL string | `StructType` |
|---|---|---|
| Example | `"order_id STRING, amount DECIMAL(10,2)"` | `StructType([StructField("order_id", StringType()), ...])` |
| Pros | Short, readable, same syntax as SQL `CREATE TABLE` | Programmatic (build from config), set `nullable=False`, attach metadata |
| Cons | Less control over nullability/metadata | Verbose |
| Use for | Most notebooks and reads | Frameworks generating schemas from metadata |

Both produce the same `StructType` internally — `df.schema` is always a `StructType`, and `df.schema.simpleString()` / `df.schema.json()` convert back.

### 4. Spark data types that matter for a DE

| Type (SQL / Python class) | Use for | Watch out |
|---|---|---|
| `STRING` / `StringType` | IDs, codes, free text | Keep IDs as strings (leading zeros, mixed formats) |
| `INT` / `IntegerType` | Counts, quantities | 32-bit: max 2,147,483,647 — overflow errors in ANSI mode |
| `BIGINT` / `LongType` | Sequence numbers, large counters, epoch millis | `count()` returns `BIGINT` |
| `DOUBLE` / `DoubleType` | Measurements, ML features, ratios | **Binary floating point: not exact** — never for money |
| `DECIMAL(p,s)` / `DecimalType(p,s)` | **Money**, exact quantities | `p` ≤ 38; arithmetic widens precision (e.g. `SUM` of `DECIMAL(10,2)` → `DECIMAL(20,2)`) |
| `BOOLEAN` | Flags | Three-valued: true / false / NULL |
| `DATE` | Calendar day (no time) | `to_date(ts)` truncates |
| `TIMESTAMP` (= `TIMESTAMP_LTZ`) | An **instant** in time | Stored as UTC microseconds; **displayed** in the session time zone |
| `TIMESTAMP_NTZ` | Wall-clock time with no zone ("local time as written") | Never shifts with session time zone; Databricks Runtime 13.3 LTS+ |
| `ARRAY<T>`, `STRUCT<...>`, `MAP<K,V>` | Nested/semi-structured data | Lesson 4.9 |

### 5. Decimal vs double — why money must be `DECIMAL`

`DOUBLE` stores numbers in binary; most decimal fractions (0.1, 0.2) cannot be represented exactly. Sums drift by tiny amounts, equality checks fail, and — worse — a distributed `SUM` of doubles can differ **in the last digits between runs** because partitions are added in a different order. `DECIMAL(p,s)` stores exact base-10 digits: `p` = total digits, `s` = digits after the point. `DECIMAL(10,2)` holds up to 99,999,999.99.

### 6. `TIMESTAMP` vs `TIMESTAMP_NTZ` vs `DATE`

- `TIMESTAMP` (LTZ, "local time zone") = an exact instant. Internally UTC; when you `show()` it, Spark renders it in `spark.sql.session.timeZone`. Strings without an offset are interpreted **in the session time zone** when parsed.
- `TIMESTAMP_NTZ` = just the fields (year…second). No conversion ever. Good for "the store opens at 09:00 local" data; bad for events from many zones.
- **Databricks default session time zone is UTC**; open-source Spark defaults to the JVM's local zone. Set it explicitly in jobs (`spark.conf.set("spark.sql.session.timeZone", "UTC")`).

### 7. ANSI mode (important on current runtimes)

`spark.sql.ansi.enabled` is **`true` by default in Apache Spark 4.0+ and Databricks Runtime 17.0+** (it was `false` in Spark 3.x and older runtimes). It changes runtime behaviour:

| Operation | ANSI **off** (legacy) | ANSI **on** (current default) | Safe alternative |
|---|---|---|---|
| `CAST('abc' AS INT)` | `NULL` | **error** `CAST_INVALID_INPUT` | `try_cast('abc' AS INT)` → `NULL` |
| `2147483647 + 1` (INT) | wraps to `-2147483648` | **error** `ARITHMETIC_OVERFLOW` | `try_add`, or use `BIGINT` |
| `10 / 0` | `NULL` | **error** `DIVIDE_BY_ZERO` | `try_divide(10, 0)` → `NULL` |
| `to_date('2026-02-30')` | `NULL` | **error** | `try_cast(... AS DATE)`, `try_to_timestamp` |
| `element_at(array, 10)` out of range | `NULL` | **error** | `try_element_at` |

Design rule: in Bronze → Silver casting, use `try_cast`/`try_to_timestamp` deliberately **and then validate the NULLs** (quarantine). Don't rely on a session setting to make bad data disappear.

### 8. How Spark executes it

- Creating a DataFrame from a table or file only builds a plan; the **schema** is resolved immediately (analysis) — that's why a wrong column name fails at once, while a bad value fails only when an action runs.
- `printSchema()` / `df.schema` / `df.dtypes` need **no job** (schema is metadata). Exception: reading CSV/JSON with `inferSchema` runs a job to scan the files.
- Casting is a **narrow** transformation (per row, no shuffle).

### 9. Misconceptions

1. **"A DataFrame holds the data."** It holds a plan; data lives in files and flows through executors when an action runs.
2. **"`DOUBLE` is fine for money if I round at the end."** Errors accumulate before rounding, and distributed sums aren't reproducible.
3. **"`TIMESTAMP` stores the time zone."** It stores an instant (UTC); the zone is only applied for display/parsing.
4. **"`inferSchema` is free."** It scans the data an extra time and can guess wrong.
5. **"Casting bad strings gives NULL."** Only with ANSI off or `try_cast`. On current runtimes `CAST` raises an error.

---

## B. Practical

**Runnable notebook:** [`exercises/phase-04/01_dataframes_io.py`](../../exercises/phase-04/01_dataframes_io.py) (covers 4.1 and 4.2)

### Example 1 — the same schema as a DDL string and as a `StructType`

Input (`products`, created by the setup notebook — 6 rows):

| product_id | product_name | category | unit_price | is_active |
|---|---|---|---|---|
| P1 | Wireless Mouse | Electronics | 25.00 | true |
| P2 | USB-C Cable | Electronics | 10.00 | true |
| P3 | Coffee Mug | Home | 12.50 | true |
| P4 | Desk Lamp | Home | 40.00 | true |
| P5 | Notebook | Stationery | 5.00 | true |
| P6 | Fountain Pen | Stationery | 30.00 | false |

```python
from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, BooleanType

rows = [("P1", "Wireless Mouse", "Electronics", Decimal("25.00"), True),
        ("P6", "Fountain Pen",   "Stationery",  Decimal("30.00"), False)]

ddl = "product_id STRING, product_name STRING, category STRING, unit_price DECIMAL(10,2), is_active BOOLEAN"  # 1
df_ddl = spark.createDataFrame(rows, ddl)

schema = StructType([                                                                                         # 2
    StructField("product_id",   StringType(),      nullable=False),
    StructField("product_name", StringType()),
    StructField("category",     StringType()),
    StructField("unit_price",   DecimalType(10, 2)),
    StructField("is_active",    BooleanType()),
])
df_struct = spark.createDataFrame(rows, schema)

df_struct.printSchema()                                                                                       # 3
print(df_ddl.schema.simpleString())                                                                           # 4
```

Equivalent SQL (a table schema is the same DDL):

```sql
CREATE OR REPLACE TABLE main.de_prep.products_copy (
  product_id STRING NOT NULL, product_name STRING, category STRING,
  unit_price DECIMAL(10,2), is_active BOOLEAN);
DESCRIBE TABLE main.de_prep.products_copy;
```

Expected output:

```
root
 |-- product_id: string (nullable = false)
 |-- product_name: string (nullable = true)
 |-- category: string (nullable = true)
 |-- unit_price: decimal(10,2) (nullable = true)
 |-- is_active: boolean (nullable = true)

struct<product_id:string,product_name:string,category:string,unit_price:decimal(10,2),is_active:boolean>
```

| # | Explanation |
|---|---|
| 1 | DDL string: the same syntax as a SQL column list. All columns nullable |
| 2 | `StructType` gives finer control — here `product_id` is declared non-nullable |
| 3 | `printSchema()` prints the tree; no Spark job runs (metadata only) |
| 4 | Any schema can be turned back into a compact string — handy for logging and config |

**Execution:** `createDataFrame` from a local list → a local relation, no shuffle; `printSchema` runs no job.

**Common mistakes:** passing Python `float` for a `DECIMAL` column (`createDataFrame` rejects a float for `DecimalType` — use `Decimal("25.00")`); expecting `nullable=False` on a DataFrame to be enforced like a database constraint when writing (Delta enforces `NOT NULL` only if the **table** declares it).

### Example 2 — double vs decimal

```python
spark.sql("""
  SELECT 0.1D + 0.2D                                         AS as_double,      -- 1
         CAST(0.1 AS DECIMAL(10,2)) + CAST(0.2 AS DECIMAL(10,2)) AS as_decimal, -- 2
         (0.1D + 0.2D) = 0.3D                                AS double_equal    -- 3
""").show()
```

PySpark equivalent: `spark.range(1).select((F.lit(0.1) + F.lit(0.2)).alias("as_double"), (F.lit(0.1).cast("decimal(10,2)") + F.lit(0.2).cast("decimal(10,2)")).alias("as_decimal"))` (`F.lit(0.1)` is a double).

Expected output:

| as_double | as_decimal | double_equal |
|---|---|---|
| 0.30000000000000004 | 0.30 | false |

| # | Explanation |
|---|---|
| 1 | The `D` suffix makes a `DOUBLE` literal; binary floating point can't represent 0.1 exactly |
| 2 | Exact decimal arithmetic; result type `DECIMAL(11,2)` (precision grows by 1 for addition) |
| 3 | The classic bug: equality on doubles fails. Reconciliation checks on money break this way |

### Example 3 — `TIMESTAMP` vs `TIMESTAMP_NTZ` under a different session time zone

```python
spark.conf.set("spark.sql.session.timeZone", "UTC")                                     # 1
spark.sql("""CREATE OR REPLACE TABLE main.de_prep.tz_demo AS
             SELECT TIMESTAMP'2026-03-01 10:00:00'     AS ts_ltz,
                    TIMESTAMP_NTZ'2026-03-01 10:00:00' AS ts_ntz""")                     # 2

spark.conf.set("spark.sql.session.timeZone", "Asia/Dubai")                              # 3
spark.table("main.de_prep.tz_demo").show(truncate=False)
spark.conf.set("spark.sql.session.timeZone", "UTC")                                     # 4
```

SQL: `SET TIME ZONE 'Asia/Dubai'; SELECT * FROM main.de_prep.tz_demo;`

Expected output (displayed while the session zone is Asia/Dubai, UTC+4):

| ts_ltz | ts_ntz |
|---|---|
| 2026-03-01 14:00:00 | 2026-03-01 10:00:00 |

| # | Explanation |
|---|---|
| 1 | Session zone UTC: the string `10:00:00` is interpreted as 10:00 UTC |
| 2 | Both are stored. `ts_ltz` = the instant 10:00Z; `ts_ntz` = the fields "10:00" with no zone |
| 3 | Same data, new session zone: the instant is rendered in Dubai time (14:00); NTZ is unchanged |
| 4 | Always reset — session config leaks into later cells |

**Common mistake:** storing source-local times in `TIMESTAMP` after parsing them in a UTC session → every value is off by the source's offset. Parse with an explicit offset (`to_timestamp(col, "yyyy-MM-dd HH:mm:ssXXX")`) or convert with `to_utc_timestamp(col, 'Asia/Dubai')`.

### Example 4 — ANSI casts

```python
print(spark.conf.get("spark.sql.ansi.enabled"))             # 'true' on Spark 4 / DBR 17+
spark.sql("SELECT try_cast('abc' AS INT) AS t1, try_cast('42' AS INT) AS t2, try_divide(10, 0) AS t3").show()
spark.sql("SELECT CAST('abc' AS INT)").show()               # ANSI on → error
```

Expected: `t1 = NULL, t2 = 42, t3 = NULL`; the last statement fails with `[CAST_INVALID_INPUT] The value 'abc' of the type "STRING" cannot be cast to "INT" because it is malformed...` (with ANSI off it would return `NULL`).

PySpark: `F.expr("try_cast(amount AS DECIMAL(10,2))")`; `F.try_divide(a, b)` (PySpark 3.5+); `Column.try_cast("int")` exists from PySpark 4.0.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is a DataFrame, and what does "lazy" mean for it?

<details><summary>Model answer</summary>

A distributed table with a schema (named, typed columns) represented internally as a logical plan. Transformations only extend the plan; Spark computes nothing until an action (count, show, write, collect) triggers optimisation by Catalyst and execution as jobs/stages/tasks.
</details>

**B2.** Write the schema for orders (`order_id`, `customer_id`, `order_ts`, `amount` money) as a DDL string.

<details><summary>Model answer</summary>

```python
schema = "order_id STRING, customer_id STRING, order_ts TIMESTAMP, amount DECIMAL(10,2)"
df = spark.read.schema(schema).option("header", True).csv(path)
```
IDs as STRING (leading zeros, mixed formats), money as DECIMAL, event time as TIMESTAMP.
</details>

**B3.** Why `DECIMAL` and not `DOUBLE` for money?

<details><summary>Model answer</summary>

`DOUBLE` is binary floating point: 0.1 + 0.2 = 0.30000000000000004, equality checks fail, rounding errors accumulate, and distributed sums can differ between runs because partition order changes the addition order. `DECIMAL(p,s)` is exact base-10.
</details>

### 🟡 Medium

**M1.** What is the cost of `inferSchema=true` on a 200 GB CSV, and what can go wrong besides cost?

<details><summary>Model answer</summary>

It needs an extra pass over the data (a full read unless `samplingRatio` is lowered) before the real job. It can also infer wrongly: one bad value makes a numeric column `string`; IDs with leading zeros become integers; sampling may miss rare values; types can change between daily files. Declare the schema instead.
</details>

**M2.** A `TIMESTAMP` column shows 14:00 for one user and 10:00 for another. Is the data wrong?

<details><summary>Model answer</summary>

Probably not: `TIMESTAMP` stores an instant and is rendered in each session's `spark.sql.session.timeZone`. One session is UTC, the other UTC+4 (e.g. Asia/Dubai). Standardise the session zone (UTC) for jobs; use `TIMESTAMP_NTZ` only for wall-clock values that must never shift.
</details>

**M3.** After upgrading to a newer runtime, a Silver job that used `CAST(amount AS INT)` starts failing. Why, and what's the right fix?

<details><summary>Model answer</summary>

ANSI mode is on by default in Spark 4.0 / DBR 17.0+, so invalid casts raise `CAST_INVALID_INPUT` instead of returning NULL. The right fix is not to switch ANSI off globally but to use `try_cast` where bad input is expected and then **route the resulting NULLs to quarantine** (count them, alert) — making the old silent behaviour explicit.
</details>

### 🔴 Tricky

**T1.** `SUM(amount)` over a `DOUBLE` column gives 1000000.0000000002 today and 999999.9999999998 tomorrow on the same data. Explain.

<details><summary>Model answer</summary>

Floating-point addition is not associative. Spark sums each partition, then combines partial sums; partition count/order and task scheduling can change, so the addition order changes and rounding differs. Use `DECIMAL` for exact, reproducible sums.
</details>

**T2.** `SELECT CAST(price AS DECIMAL(5,2))` on a value 123456.78 — what happens with ANSI on and off?

<details><summary>Model answer</summary>

`DECIMAL(5,2)` holds at most 999.99, so the value overflows. ANSI on: error (numeric value out of range). ANSI off: `NULL`. `try_cast` returns `NULL` in both. Choose precision from the business maximum, with headroom.
</details>

**T3.** A teammate writes `spark.createDataFrame([(1, 9.99)], "id INT, price DECIMAL(10,2)")` and it fails. Why? Fix it.

<details><summary>Model answer</summary>

Python `9.99` is a `float`; schema verification rejects a float for `DecimalType`. Pass `Decimal("9.99")` from the `decimal` module, or create the column as string/double and `.cast("decimal(10,2)")` (casting from a decimal string is exact; from a double it inherits the binary approximation, though rounding to 2 places usually hides it).
</details>

---

## E. Revision summary

- DataFrame = **lazy plan + schema**. Actions trigger execution. Schema inspection needs no job (except inference).
- Declare schemas: DDL string for most cases, `StructType` for programmatic/nullability control.
- Money → `DECIMAL(p,s)`. IDs → `STRING`. Counters/sequences → `BIGINT`.
- `TIMESTAMP` = instant (UTC inside, shown in session zone). `TIMESTAMP_NTZ` = wall clock, never shifts. Databricks session zone defaults to UTC.
- **ANSI on by default in Spark 4.0 / DBR 17.0+**: bad casts, overflow and divide-by-zero raise errors → use `try_cast`, `try_divide`, `try_to_timestamp`, then validate the NULLs.