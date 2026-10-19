# 2.4 SparkSession vs SparkContext

**Time:** 15 min · **Prerequisites:** 2.3

**After this topic you can:**
- explain what SparkContext and SparkSession are and why SparkSession replaced the old entry points;
- explain what is session-scoped (SQL configs, temp views) vs application-scoped (executors, cluster configs);
- explain how this looks in Databricks notebooks, and what changes with Spark Connect / serverless.

---

## A. Concept deep dive

### 1. The simplest explanation

- **SparkContext** = the low-level **connection to the cluster**. It owns the link to executors and is the entry point for **RDDs** (the original Spark API).
- **SparkSession** = the modern, high-level **entry point** for everything you normally do: DataFrames, SQL, tables, reading/writing, streaming, configuration. It **contains** a SparkContext (`spark.sparkContext`) on classic Spark.

```
  Spark 1.x                                  Spark 2.0+
  ┌──────────────┐ ┌───────────┐ ┌────────────┐      ┌─────────────────────────────────┐
  │ SparkContext │ │ SQLContext│ │ HiveContext│  ──▶ │ SparkSession  (spark)            │
  │  (RDDs)      │ │(DataFrames)│ │ (Hive SQL) │      │  .read .sql .table .catalog      │
  └──────────────┘ └───────────┘ └────────────┘      │  .conf .createDataFrame .range   │
                                                       │  .readStream .udf                │
                                                       │  .sparkContext ──▶ SparkContext   │
                                                       └─────────────────────────────────┘
```

### 2. Why SparkSession exists

Before 2.0 you juggled several objects: `sc` for RDDs, `sqlContext` for DataFrames, `HiveContext` for Hive tables, a separate `StreamingContext` for DStreams. SparkSession unified them into **one entry point** with one configuration surface. `SQLContext`/`HiveContext` remain only for backward compatibility.

### 3. How they relate internally

| | SparkContext | SparkSession |
|---|---|---|
| Introduced | Spark 1.0 era | Spark 2.0 |
| Level | Low-level, cluster connection, RDD API | High-level: DataFrame, SQL, catalog, streaming |
| How many | **One active per JVM / application** | **Many** per application possible (`spark.newSession()`), all sharing the same SparkContext |
| Holds | Executors, scheduler, broadcast variables, accumulators, cluster-level config | SQL configuration (`spark.conf`), temporary views, registered temporary functions, catalog state |
| Create | Rarely directly any more | `SparkSession.builder.appName(...).getOrCreate()` |

```
  One application (one driver)
  ┌──────────────────────────────────────────────────────────┐
  │ SparkContext  (executors, scheduler, cluster configs)    │
  │    ▲                ▲                ▲                     │
  │ SparkSession A   SparkSession B   SparkSession C          │
  │ temp views,      temp views,      temp views,             │
  │ spark.conf       spark.conf       spark.conf               │
  └──────────────────────────────────────────────────────────┘
```

**`getOrCreate()`** returns the existing session if one is active (and applies any new runtime SQL options you passed); it doesn't start a second cluster.

### 4. Session-scoped vs application-scoped settings

| Setting type | Example | Can change at runtime with `spark.conf.set`? |
|---|---|---|
| **Runtime SQL configs** (session-scoped) | `spark.sql.shuffle.partitions`, `spark.sql.ansi.enabled`, `spark.sql.session.timeZone` | ✅ Yes, affects only this session |
| **Application / cluster configs** | `spark.executor.memory`, `spark.executor.cores`, `spark.driver.memory` | ❌ No — set before the application starts (on Databricks: compute configuration). Setting them at runtime has no effect or errors |

### 5. Databricks specifics

| Point | Detail |
|---|---|
| Pre-created | Notebooks and jobs get a ready `spark` (SparkSession). **Don't** build your own context, and **never** call `spark.stop()` |
| One session per notebook | Notebooks attached to the same classic compute share the cluster's driver/SparkContext but each has its own session — that's why **temp views** and `spark.conf.set(...)` in one notebook don't affect another |
| Standard access mode | Recommends using `spark`; in **Scala**, `sc`/`spark.sparkContext`/`sqlContext` are not supported. In **Python**, they're supported on Databricks Runtime 14.0+, but **RDD APIs are not supported**, and many `sc` functions (e.g. `parallelize`, `textFile`) are blocked — per current Databricks docs |
| Serverless | Only **Spark Connect** APIs; RDD APIs not supported; there is no usable SparkContext in your notebook. Only a small allow-list of Spark configs can be set (e.g. `spark.sql.shuffle.partitions`, `spark.sql.ansi.enabled`, `spark.sql.session.timeZone`, `spark.sql.files.maxPartitionBytes`) |

### 6. Spark Connect — why the SparkContext "disappears"

With Spark Connect (Spark 3.4+), your notebook holds a **client** SparkSession that sends **unresolved plans** over gRPC to a Spark server. The real SparkContext lives on the server, so the client can't hand you one. Open-source: `SparkSession.builder.remote("sc://host:15002").getOrCreate()`. Databricks Connect (for IDEs) uses the same idea via `DatabricksSession`.

Consequence: write code against **SparkSession / DataFrame / SQL APIs only**. It then runs on classic, standard and serverless compute alike.

### 7. Real data-engineering example — ShopCo

ShopCo's Silver job sets `spark.sql.shuffle.partitions` to `64` for its small daily increment and registers a temp view `v_new_orders` for a SQL MERGE. A second notebook on the same cluster still sees the default shuffle partitions and can't see `v_new_orders` — that's correct isolation, not a bug. If the team wants shared logic, it belongs in a **table/view in Unity Catalog** or a shared function, not a temp view.

### 8. When to use what

| Use | When |
|---|---|
| `spark` (SparkSession) | Always, for DataFrames, SQL, reading/writing, config |
| `spark.sparkContext` | Rarely: legacy RDD code, accumulators, `defaultParallelism` — and only on classic compute where supported |
| `spark.newSession()` | Isolated SQL config/temp views inside one application (classic Spark; uncommon in notebooks) |

### 9. Misconceptions

1. **"SparkSession replaced SparkContext."** It **wraps** it (on classic Spark). The SparkContext still exists underneath.
2. **"Each `getOrCreate()` creates a new cluster."** It returns the existing session/context.
3. **"`spark.conf.set('spark.executor.memory', '16g')` gives me bigger executors."** Executors are already running; that's an application-level setting.
4. **"Temp views are visible to everyone on the cluster."** They're session-scoped (per notebook on Databricks).
5. **"`spark.sparkContext` works everywhere."** Not on serverless; restricted on standard access mode; RDD APIs are blocked on both.

---

## B. Practical — the session in a notebook

```python
print(type(spark).__module__, type(spark).__name__)                      # 1

spark.conf.set("spark.sql.shuffle.partitions", "8")                      # 2
print(spark.conf.get("spark.sql.shuffle.partitions"))

spark.range(3).createOrReplaceTempView("v_numbers")                      # 3
spark.sql("SELECT count(*) AS n FROM v_numbers").show()

# Classic compute only (not Spark Connect / serverless):
s2 = spark.newSession()                                                  # 4
print(s2.conf.get("spark.sql.shuffle.partitions"))
try:
    s2.sql("SELECT * FROM v_numbers").show()
except Exception as e:
    print(type(e).__name__, str(e)[:60])
```

**Expected output (classic compute, Spark Classic session):**

```
pyspark.sql.session SparkSession
8
+---+
|  n|
+---+
|  3|
+---+
200                                   ← s2 has its own SQL conf: Apache default (or your cluster's setting)
AnalysisException [TABLE_OR_VIEW_NOT_FOUND] The table or view `v_numbers` cannot be found…
```

On serverless the first line shows `pyspark.sql.connect.session SparkSession`, and step 4 may not be supported. (Exact error text can differ by version.)

| # | Explanation |
|---|---|
| 1 | Reveals whether you have a classic session or a Spark Connect client |
| 2 | A runtime SQL config — session-scoped, takes effect for queries planned after this |
| 3 | Temp view = a name for a plan, stored in **this session** only |
| 4 | A second session in the same application: shares the SparkContext/executors, but not temp views or SQL conf |

### Common mistakes

- `SparkSession.builder.master("local[*]")...` copied from a laptop tutorial into a Databricks job.
- Calling `spark.stop()` at the end of a Databricks notebook/job.
- Relying on temp views to pass data between notebooks or job tasks (use tables, or task values for small parameters).
- Writing new code with `sc.parallelize` / `df.rdd` — breaks on serverless and standard access mode.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is the difference between SparkSession and SparkContext?

<details><summary>Model answer</summary>

SparkContext is the low-level connection to the cluster: it owns executors and scheduling and is the entry point for RDDs; there's one per application. SparkSession (Spark 2.0+) is the unified high-level entry point for DataFrames, SQL, catalog, reading/writing and streaming; it wraps a SparkContext, and an application can have several sessions that share it, each with its own SQL config and temp views.
</details>

**B2.** Why was SparkSession introduced?

<details><summary>Model answer</summary>

To replace the separate SQLContext, HiveContext and other entry points with one object, giving one API and one configuration surface for DataFrames, SQL, Hive tables/catalog and streaming.
</details>

**B3.** In a Databricks notebook, where does `spark` come from?

<details><summary>Model answer</summary>

Databricks creates it for you when the notebook attaches to compute; it's a SparkSession for that notebook. You shouldn't create your own SparkContext or stop the session.
</details>

### 🟡 Medium

**M1.** Two notebooks share one cluster. Notebook A sets `spark.sql.shuffle.partitions = 8` and creates a temp view. What does notebook B see?

<details><summary>Model answer</summary>

Neither. Each notebook has its own SparkSession; runtime SQL configs and temp views are session-scoped. B still has the default/cluster-level shuffle partitions and can't resolve A's temp view. Both share executors and cluster-level configuration.
</details>

**M2.** Why does `spark.conf.set("spark.executor.memory", "16g")` not help?

<details><summary>Model answer</summary>

Executor memory is an application/cluster-level setting applied when executors are launched. The executors already exist, so changing it at runtime doesn't resize them (it may be ignored or rejected). Set it in the compute configuration before start; only runtime SQL configs (`spark.sql.*`) take effect per session.
</details>

**M3.** Why is `spark.sparkContext` unavailable on Databricks serverless?

<details><summary>Model answer</summary>

Serverless uses Spark Connect: the notebook holds a thin client that sends plans to a remote Spark server. The SparkContext lives on the server, so the client can't expose it, and only Spark Connect (DataFrame/SQL) APIs are supported — RDD APIs are not.
</details>

### 🔴 Tricky

**T1.** Legacy code uses `df.rdd.getNumPartitions()` and `sc.parallelize(...)`. You're migrating to serverless / standard access mode. What breaks and what do you use instead?

<details><summary>Model answer</summary>

RDD APIs aren't supported there, so `.rdd` and `parallelize` fail. Replace `parallelize` with `spark.createDataFrame(...)` or `spark.range(...)`; replace partition counting with `spark_partition_id()` (e.g. `df.select(spark_partition_id()).distinct().count()` — counts non-empty partitions) or reason from the plan; replace RDD transformations with DataFrame functions or pandas UDFs / `mapInPandas`.
</details>

**T2.** A colleague calls `SparkSession.builder.config("spark.sql.shuffle.partitions", 50).getOrCreate()` in the middle of a notebook and says it "restarted Spark with new settings". Correct them.

<details><summary>Model answer</summary>

`getOrCreate()` returns the already-active session; nothing restarts. Runtime SQL options passed to the builder may be applied to the existing session, but application-level settings (executor memory, cores) cannot change for a running application. Using `spark.conf.set` is clearer for runtime SQL configs.
</details>

**T3.** On Spark Connect, a typo in a column name doesn't error until `show()`. On classic compute it errors immediately. Why?

<details><summary>Model answer</summary>

Spark Classic analyses (resolves) the plan eagerly when each DataFrame is created, so unknown columns raise an AnalysisException right away. Spark Connect builds an unresolved plan on the client and only sends it for analysis when needed — on an action or when schema is requested (e.g. `df.columns`, `df.schema`). Same code, different error timing.
</details>

---

## E. Revision summary

- SparkContext = cluster connection + RDD entry point, one per application.
- SparkSession = unified entry point (DataFrame, SQL, catalog, streaming); wraps SparkContext; many sessions can share one context.
- Session-scoped: runtime `spark.sql.*` configs, temp views. Application-scoped: executors, memory, cores.
- Databricks gives you `spark`; one session per notebook; never stop it.
- Spark Connect / serverless: no SparkContext, no RDDs, limited configs — write SparkSession/DataFrame/SQL code.