# 1.4 Structured, Semi-Structured and Unstructured Data

**Time:** 20 min · **Prerequisites:** 1.3

**After this topic you can:**
- classify any dataset and explain why the category matters for ingestion and storage;
- compare CSV, JSON, Avro, Parquet, ORC and Delta;
- read nested JSON with an explicit schema and explain why explicit schemas matter.

---

## A. Concept deep dive

### 1. The three categories

| Category | Definition | Examples | Typical storage |
|---|---|---|---|
| **Structured** | Fixed schema: rows and columns with defined types | DB tables, CSV exports, Delta tables | Tables (Delta), Parquet |
| **Semi-structured** | Self-describing structure (field names travel with the data), but flexible: optional fields, nesting, arrays | JSON, XML, Avro messages, API responses, event logs | Raw files → parsed into structs/arrays or VARIANT |
| **Unstructured** | No inherent tabular structure | Images, PDFs, audio, video, free text, emails | Files in object storage / Unity Catalog **volumes** |

### 2. Why the category matters

| Concern | Structured | Semi-structured | Unstructured |
|---|---|---|---|
| Schema | Known in advance | Inferred or declared; **changes often** | None; metadata only |
| Parsing effort | Low | Medium–high (nesting, arrays, missing fields) | High (OCR, NLP, ML models) |
| Main risk | Type mismatches | **Schema drift**, explosion of nested arrays | Size, cost, extracting meaning |
| Spark tools | `spark.read.table/csv/parquet` | `from_json`, `explode`, `col("a.b")`, VARIANT | `binaryFile` format, ML libraries |

### 3. File formats — what a DE must know

| Format | Layout | Schema stored? | Compression | Splittable* | Typical use |
|---|---|---|---|---|---|
| **CSV** | Row, text | ❌ (header names only, no types) | Poor; gzip CSV is **not** splittable | ✅ uncompressed | Exports, partner files |
| **JSON** | Row, text | Field names per record, no types | Poor | ✅ line-delimited; ❌ multi-line | APIs, events |
| **Avro** | Row, binary | ✅ (schema in file header) | Good | ✅ | Kafka/Event Hubs messages, CDC streams |
| **Parquet** | **Columnar**, binary | ✅ + column statistics | Excellent | ✅ | Analytics storage |
| **ORC** | Columnar, binary | ✅ | Excellent | ✅ | Hive ecosystem |
| **Delta** | Parquet files + transaction log | ✅ (in log) | Excellent | ✅ | Lakehouse tables |

\* **Splittable** = Spark can split one big file into several partitions and read them in parallel. A single 10 GB gzip CSV is read by **one** task — a classic performance trap.

### 4. Semi-structured data in Spark — how it works

A JSON event:

```json
{"order_id": 101,
 "customer": {"id": "C1", "tier": "gold"},
 "items": [{"sku": "P1", "qty": 2, "price": 50.0},
           {"sku": "P7", "qty": 1, "price": 19.9}],
 "ts": "2026-10-16T09:15:00Z"}
```

Spark represents it with complex types:

| JSON | Spark type | Access |
|---|---|---|
| object `{...}` | `STRUCT<...>` | `col("customer.id")` |
| array `[...]` | `ARRAY<...>` | `explode("items")` → one row per element |
| key/value with dynamic keys | `MAP<STRING, ...>` | `col("attrs")["color"]` |

**Schema inference cost:** `spark.read.json(path)` with no schema makes Spark **read the data first just to infer the schema** — an extra job before any action, slow on large data, and the inferred schema can change from day to day (a field missing today → column disappears). In production, **declare the schema** (or let Auto Loader manage it with a schema location — Phase 7).

**VARIANT:** recent Databricks Runtime versions offer a `VARIANT` type that stores semi-structured data efficiently without a fixed schema, queried with paths like `payload:customer.id`. Useful when structure changes frequently; check the runtime docs for availability.

### 5. Misconceptions

1. **"CSV is structured, so it's safe."** CSV has no types, no enforced schema, and breaks on embedded commas/newlines/quotes and encoding issues.
2. **"Parquet is semi-structured because it supports nesting."** Parquet has a strict, declared schema — it's structured (nested structured).
3. **"JSON inference is free."** It costs an extra pass and gives unstable schemas.
4. **"Unstructured data doesn't belong in the lakehouse."** It does — in volumes, governed by Unity Catalog, with extracted metadata in tables.

---

## B. Practical — parse nested JSON with an explicit schema

```python
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               DoubleType, ArrayType, IntegerType)

raw = spark.createDataFrame([                                                    # 1
    ('{"order_id":101,"customer":{"id":"C1","tier":"gold"},"items":[{"sku":"P1","qty":2,"price":50.0},{"sku":"P7","qty":1,"price":19.9}],"ts":"2026-10-16T09:15:00Z"}',),
    ('{"order_id":102,"customer":{"id":"C2"},"items":[],"ts":"2026-10-16T10:02:00Z"}',),
    ('{"order_id":"oops","customer":null}',),
], "value STRING")

order_schema = StructType([                                                      # 2
    StructField("order_id", LongType()),
    StructField("customer", StructType([
        StructField("id", StringType()),
        StructField("tier", StringType())])),
    StructField("items", ArrayType(StructType([
        StructField("sku", StringType()),
        StructField("qty", IntegerType()),
        StructField("price", DoubleType())]))),
    StructField("ts", StringType()),
])

parsed = raw.select(F.from_json("value", order_schema).alias("o"), "value")      # 3

orders = parsed.select(                                                          # 4
    F.col("o.order_id").alias("order_id"),
    F.col("o.customer.id").alias("customer_id"),
    F.coalesce(F.col("o.customer.tier"), F.lit("standard")).alias("tier"),
    F.to_timestamp("o.ts").alias("order_ts"),
    F.size("o.items").alias("item_count"))

lines = (parsed                                                                  # 5
         .select(F.col("o.order_id").alias("order_id"),
                 F.explode_outer("o.items").alias("item"))
         .select("order_id", "item.sku", "item.qty", "item.price"))

display(orders); display(lines)
```

| Step | Explanation |
|---|---|
| 1 | Three raw JSON strings — one normal, one with missing `tier` and an empty array, one malformed |
| 2 | **Explicit schema**: no inference job, stable types, documents the contract |
| 3 | `from_json` parses each string into a struct. Malformed/incompatible fields become `null` instead of failing |
| 4 | Dot notation reaches into structs; `coalesce` supplies a default for missing `tier`; `size` counts array elements |
| 5 | `explode_outer` creates one row per item **and keeps orders with empty/null arrays** (as a null item). Plain `explode` would drop order 102 entirely |

**Expected `orders`**

| order_id | customer_id | tier | order_ts | item_count |
|---|---|---|---|---|
| 101 | C1 | gold | 2026-10-16 09:15:00 | 2 |
| 102 | C2 | standard | 2026-10-16 10:02:00 | 0 |
| null | null | standard | null | -1 or null |

The third row shows a malformed record: `order_id` is null (the whole struct, or the bad fields, become null depending on the Spark version). `size(null)` returns **-1** when ANSI mode is off (legacy `spark.sql.legacy.sizeOfNull=true`) and **null** when ANSI mode is on — another configuration-dependent behaviour. In production you'd route rows with `order_id IS NULL` to quarantine, keeping the raw `value` for debugging.

**Expected `lines`**

| order_id | sku | qty | price |
|---|---|---|---|
| 101 | P1 | 2 | 50.0 |
| 101 | P7 | 1 | 19.9 |
| 102 | null | null | null |
| null | null | null | null |

### Common mistakes

- `explode` instead of `explode_outer` → orders with no items disappear from counts.
- Exploding *two* arrays in the same select → a Cartesian product of elements.
- Relying on inference in production → schema changes break downstream silently.
- Not keeping the raw payload → impossible to debug or reprocess bad records.

---

## C. Interview questions

### 🟢 Basic

**B1.** Give two examples each of structured, semi-structured and unstructured data.

<details><summary>Model answer</summary>

Structured: a SQL `orders` table, a Delta `customers` table. Semi-structured: JSON API responses, XML invoices, Avro messages from Event Hubs. Unstructured: scanned PDF invoices, call-centre audio, product images.
</details>

**B2.** Why is Parquet preferred over CSV for analytics storage?

<details><summary>Model answer</summary>

Parquet is columnar (column pruning), compressed, stores the schema and types, stores min/max statistics for data skipping, and is splittable. CSV is text, untyped, poorly compressed, and must be fully parsed.
</details>

### 🟡 Medium

**M1.** What does "splittable" mean, and why does a single large gzip CSV file hurt performance?

<details><summary>Model answer</summary>

A splittable file can be divided into chunks that different tasks read in parallel. Gzip compression isn't splittable, so one task must decompress and read the whole file sequentially — one core does all the work while the rest of the cluster idles. Fix: ask for many smaller files, use a splittable codec/format, or repartition right after reading.
</details>

**M2.** Why should production pipelines avoid JSON schema inference?

<details><summary>Model answer</summary>

Inference triggers an extra read of the data (cost/time) and produces a schema that depends on whatever data is present that day — a missing optional field removes a column, a number appearing as string changes a type. Downstream logic then breaks. Declare a schema, or use Auto Loader with a persisted schema location and controlled evolution.
</details>

### 🔴 Tricky

**T1.** You `explode` an `items` array and your order count drops by 3 %. Why?

<details><summary>Model answer</summary>

`explode` produces zero rows for null or empty arrays, so orders with no items vanish. Use `explode_outer`, or count orders before exploding. (And counting orders after exploding without `countDistinct` would *inflate* counts — the opposite trap.)
</details>

**T2.** Is a Delta table containing a `STRUCT` column structured or semi-structured?

<details><summary>Model answer</summary>

Structured — the nested schema is declared and enforced. "Semi-structured" refers to data whose structure is carried per record and can vary (raw JSON). A VARIANT column is the semi-structured case inside a table.
</details>

---

## E. Revision summary

- Structured = fixed schema; semi-structured = self-describing, flexible, nested; unstructured = no tabular structure.
- Formats: CSV/JSON (row, text, weak), Avro (row, binary, schema — streaming), Parquet/ORC (columnar, analytics), Delta (Parquet + log).
- Gzip CSV isn't splittable → one task reads it.
- In production, **declare schemas**; use `from_json`, dot paths, `explode_outer`.
- Keep the raw payload for debugging and replay.