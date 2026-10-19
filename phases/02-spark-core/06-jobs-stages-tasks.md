# 2.6 Jobs, Stages, Tasks — How Work Is Split

**Time:** 35 min · **Prerequisites:** 2.3, 2.5

**After this topic you can:**
- define job, stage and task precisely, and say what creates each;
- predict the number of jobs, stages and tasks for a PySpark snippet;
- explain why stage boundaries are exactly the shuffles;
- explain what happens, step by step, when you call an action;
- explain why the real count in the Spark UI can differ (AQE, `show`, sorting, Delta writes, skipped stages).

---

## A. Concept deep dive

### 1. The simplest explanation

| Unit | Created by | What it is | Analogy |
|---|---|---|---|
| **Job** | An **action** (`count`, `collect`, `show`, `write`…) | All the work needed to produce that action's result | "Produce the monthly report" |
| **Stage** | The DAG scheduler, cutting the job at **shuffle boundaries** | A group of tasks that run the **same pipelined code** on different partitions, with no data exchange between them | "Phase 1: everyone summarises their own pile; Phase 2: piles are regrouped by city and totalled" |
| **Task** | One per **partition** of the stage | The smallest unit of work: one stage's code on one partition, on one core | "One person handling one pile" |

```
ACTION  ───▶  1 JOB
                 ├── Stage 0 (ShuffleMapStage) : 8 tasks  ← one per input partition
                 │        └─ writes shuffle files
                 │  ══════ shuffle boundary (Exchange) ══════
                 └── Stage 1 (ResultStage)     : 4 tasks  ← one per shuffle partition
                          └─ produces the action's result
```

### 2. Why work is split this way

- **Tasks** exist so work can run in parallel and be retried independently (one failed partition ≠ failed job).
- **Stages** exist because a shuffle is a **barrier**: no reduce-side task can start until *all* map-side tasks have written their output, since any of them may hold rows for any key. Everything between two shuffles can be **pipelined** — one task does read → filter → project → partial aggregate in one pass without materialising anything.
- **Jobs** exist because each action asks for a concrete result; Spark plans backwards from what that action needs.

### 3. How stage boundaries are found

Spark builds the job's DAG of operations and walks it backwards from the final result. Every **narrow** dependency (each output partition needs one input partition: filter, select, withColumn, union, coalesce) stays in the same stage. Every **wide/shuffle** dependency (output partition needs data from many input partitions: groupBy, join, distinct, orderBy, repartition, window) starts a new stage.

In a DataFrame physical plan, the boundaries are the **`Exchange`** nodes (Photon: names like `PhotonShuffleExchangeSink/Source`). For a straight-line plan:

```
stages = number of shuffle Exchanges + 1
```

For a plan with branches (a join of two shuffled inputs), each branch has its own map stage:

```
 orders ─ scan ─ partial agg ─▶ Exchange ─┐
                                          ├─▶ join + final ─▶ result      = 3 stages
 customers ─ scan ─ project ──▶ Exchange ─┘
```

A `BroadcastExchange` is **not** a shuffle boundary in this sense: the small side is collected and broadcast (often via its own separate job), and the big side is not shuffled.

### 4. Stage and task types

| Stage type | Task type | Does what |
|---|---|---|
| **ShuffleMapStage** | ShuffleMapTask | Computes its partition, **partitions the output by key**, writes shuffle files to local disk |
| **ResultStage** (last stage of a job) | ResultTask | Computes the final partitions and sends results to the driver (`collect`, `count`) or writes them (`write`) |

**Tasks per stage = partitions in that stage**: the first stage = input partitions (2.5); a stage after a shuffle = shuffle partitions (`spark.sql.shuffle.partitions`, possibly coalesced by AQE).

**Waves:** if a stage has 1 000 tasks and the cluster has 40 cores, tasks run in ⌈1000/40⌉ = 25 waves.

### 5. What happens when you call an action (end to end)

```
 df.groupBy("city").agg(sum("amount")).write.saveAsTable("gold")     ← action
   │
 ① DRIVER: the logical plan built by earlier transformations is analysed (names, types),
           optimised by Catalyst (pushdown, pruning), turned into a physical plan (Phase 3)
 ② DRIVER: physical plan → RDD DAG → DAG scheduler creates a JOB and cuts it into STAGES at shuffles
 ③ DRIVER: stages with no missing parents are submitted; each becomes a TaskSet (one task per partition)
 ④ DRIVER: task scheduler sends tasks to free executor cores (locality-aware)
 ⑤ EXECUTORS: run tasks: read partition → pipelined operators → write shuffle files / output
 ⑥ DRIVER: when all map tasks finish, the next stage is submitted; reduce tasks fetch shuffle blocks
 ⑦ Failures: a failed task is retried (default up to 4 attempts); lost shuffle output → parent stage re-run
 ⑧ EXECUTORS: final stage writes files / returns results; DRIVER completes the action
           (for Delta: the driver commits the transaction log entry)
```

### 6. Predicting counts — the recipe

1. **Jobs:** count the actions (start with 1 per action). Then add the known "extras" (§7).
2. **Stages:** in `explain()`, count shuffle `Exchange` nodes; stages = exchanges + 1 for each independent branch path (straight line: exchanges + 1).
3. **Tasks:** first stage(s) = input partitions; after each shuffle = shuffle partitions (AQE may coalesce); a final `Exchange SinglePartition` stage = 1 task.

Worked example (AQE **off**, `spark.sql.shuffle.partitions = 4`):

```python
orders = spark.range(0, 1_000_000, 1, 8)                 # 8 partitions
revenue = (orders.withColumn("customer_id", F.col("id") % 5000)
                 .groupBy("customer_id").agg(F.count("*").alias("n")))
revenue.write.format("noop").mode("overwrite").save()    # 1 action
```

| | Count | Why |
|---|---|---|
| Jobs | 1 | one action (`noop` sink = runs everything, writes nothing, no Delta commit jobs) |
| Stages | 2 | one Exchange (`hashpartitioning(customer_id, 4)`) |
| Tasks | 8 + 4 = 12 | stage 0 = 8 input partitions; stage 1 = 4 shuffle partitions |

`orders.count()` instead → 1 job, 2 stages, **8 + 1** tasks: partial count per partition, `Exchange SinglePartition`, final sum in 1 task.

### 7. Why the Spark UI may show different numbers (Know these — interviewers love them)

| Situation | What you see | Why |
|---|---|---|
| **AQE on** (default) | One action → **several jobs**; fewer reduce tasks than `shuffle.partitions` | AQE runs each query stage, re-optimises with real statistics, then submits the next — each submission can appear as its own job; it coalesces partitions |
| `show()` / `take(n)` / `limit` | Often **1 task**, or a few short jobs | Spark scans one partition first and only scans more (in growing batches) if it needs more rows |
| `orderBy` / `sort` | An **extra job** before the main one | Range partitioning samples the data to compute partition boundaries |
| Reading CSV/JSON with `inferSchema` | A job before your action — at `read` time | Spark scans (a sample of) the files to infer types |
| Delta `write`/`MERGE` on Databricks | Extra small jobs | Statistics collection, commit-related work, optimized writes — version-dependent |
| Broadcast join | A separate job | Collecting the small side to broadcast it |
| **Skipped stages** | Stage listed as "skipped" | Its shuffle output already exists from an earlier job, so Spark reuses it instead of recomputing |
| Serverless | No Spark UI at all | Use the **query profile** (operators, rows, time, bytes) |

### 8. Real data-engineering example — ShopCo

`gold_revenue = orders ⋈ customers → group by city, day` on 2 TB orders (~16 000 input partitions) and 5 GB customers, shuffle partitions `auto` / AQE:

| Stage | Tasks | Work |
|---|---|---|
| Scan orders + filter + project + shuffle write | ~16 000 | narrow work, pipelined |
| Scan customers + project + shuffle write | ~40 | narrow work |
| Join + partial aggregate + shuffle write | N (AQE-chosen) | sort-merge join |
| Final aggregate + write Delta files | M (coalesced) | result stage |

If one city (Dubai) holds 60 % of rows, the stage containing the join/aggregation by city shows one task running 50× longer than the median — skew (Phase 3).

### 9. Misconceptions

1. **"One action = exactly one job."** Usually, but AQE, sorting, `show`, schema inference, broadcasts and Delta writes can add jobs.
2. **"One stage per transformation."** Narrow transformations are pipelined together into one stage; only shuffles create new stages.
3. **"Tasks = cores."** Tasks = partitions. Cores decide how many tasks run *at the same time*.
4. **"The `*(2)` in `explain()` is stage 2."** That's a **whole-stage code generation** id, not a Spark scheduler stage.
5. **"Stages run one at a time."** Independent stages (two sides of a join) can run concurrently.

---

## B. Practical — predict, then verify

**Runnable notebook:** [`exercises/phase-02/06_predict_jobs_stages_tasks.py`](../../exercises/phase-02/06_predict_jobs_stages_tasks.py)

The notebook turns AQE off (where allowed) and sets `spark.sql.shuffle.partitions = 4` so predictions are exact, then turns AQE back on so you can see the difference. **Write your prediction in the table before running each cell.**

| # | Snippet (orders = `spark.range(0, 1_000_000, 1, 8)` + columns) | Jobs | Stages | Tasks |
|---|---|---|---|---|
| E1 | `orders.filter("amount > 50").select("order_id")` (no action) | 0 | 0 | 0 |
| E2 | E1 + `.write.format("noop")…save()` | 1 | 1 | 8 |
| E3 | `groupBy("customer_id").agg(sum)` + noop write | 1 | 2 | 8 + 4 |
| E4 | `orders.filter("amount > 50").count()` → **500000** | 1 | 2 | 8 + 1 |
| E5 | E3 + `.orderBy("revenue")` + noop write | typically 2 (sampling + main) | 2 + 3 (one **skipped**) | see notebook |
| E6 | join of two aggregates on `k` (broadcast disabled) + noop write | 1 | 3 | 8 + 4 + 4 |
| E7 | `orders.show(5)` | typically 1 | 1 | typically 1 |
| E8 | `revenue.count()` twice | 2 | 2 + 2 (no reuse expected on plain DataFrame actions — check for "skipped") | — |

**Expected data result (E3):** customer `c` has 200 orders, each with amount `(c % 100) + 1`, so revenue = `200 × ((c % 100) + 1)` — e.g. customer 0 → 200, customer 42 → 8 600, customer 4999 → 20 000.

| Notebook line | Explanation |
|---|---|
| `set_conf("spark.sql.adaptive.enabled", "false")` | Makes the static plan the executed plan, so counts match the recipe (classic compute only; serverless won't allow it) |
| `write.format("noop")` | A built-in sink that executes the whole plan but writes nothing — clean measurement, no Delta extra jobs |
| `spark.sparkContext.setJobDescription(...)` | Labels jobs in the Spark UI (classic only; wrapped in try/except) |
| `df.explain()` before running | Count `Exchange` nodes → stages |

### Common mistakes

- Measuring with `show()` (partial scan) or `display()` and concluding the whole job is fast.
- Counting transformations instead of shuffles.
- Forgetting that each action re-runs the whole lineage from the source (no automatic caching).
- Reading `*(n)` codegen ids as stage numbers.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is a task?

<details><summary>Model answer</summary>

A task is the smallest unit of execution in Spark: the code of one stage applied to one partition, sent by the driver to one executor core. A stage has as many tasks as it has partitions. Tasks are retried independently if they fail.
</details>

**B2.** What is the difference between a job, a stage and a task?

<details><summary>Model answer</summary>

A job is all the work triggered by one action. The DAG scheduler splits a job into stages at shuffle boundaries; each stage is a set of tasks running the same pipelined code on different partitions with no data exchange between them. A task is one stage on one partition, executed on one core.
</details>

**B3.** What creates a new stage?

<details><summary>Model answer</summary>

A shuffle (wide dependency) — e.g. groupBy/aggregation, join (non-broadcast), distinct, orderBy, repartition, window with partitionBy. Narrow transformations are pipelined into the current stage.
</details>

### 🟡 Medium

**M1.** Predict jobs, stages and tasks: data read in 8 partitions → `filter` → `groupBy(city).count()` → `write`, with shuffle partitions 4 and AQE off.

<details><summary>Model answer</summary>

1 job (one action), 2 stages (one shuffle for the groupBy), 8 + 4 = 12 tasks: the first stage reads, filters, partially aggregates and writes shuffle files for 8 partitions; the second stage reads shuffle data for 4 partitions, finishes the aggregation and writes. (On Delta there may be small extra jobs; with AQE on, the second stage likely runs fewer tasks.)
</details>

**M2.** Why must all map tasks finish before the reduce stage starts?

<details><summary>Model answer</summary>

Each reduce partition needs rows for its keys from every map task; any map task might still produce rows for that key. Until all map outputs are written and their locations registered with the driver, a reducer can't be sure it has all its input. Hence a shuffle is a stage barrier.
</details>

**M3.** What happens when you call an action?

<details><summary>Model answer</summary>

The driver takes the logical plan built by the transformations, analyses and optimises it (Catalyst), produces a physical plan, and the DAG scheduler creates a job split into stages at shuffle boundaries. Stages are submitted in dependency order; each becomes a set of tasks, one per partition, which the task scheduler assigns to executor cores. Executors run the tasks, writing shuffle files between stages, retries handle failures, and the last stage returns results to the driver or writes them to storage.
</details>

### 🔴 Tricky

**T1.** A single `df.write.saveAsTable(...)` shows 4 jobs in the Spark UI. You expected 1. Give plausible reasons.

<details><summary>Model answer</summary>

AQE submits query stages separately and they can appear as separate jobs; a sort in the plan adds a sampling job; a broadcast join adds a job to build the broadcast; schema inference may have run earlier; Delta/Databricks writes add jobs (stats collection, optimized writes, commit work). Check each job's description and the SQL tab to map jobs to the query.
</details>

**T2.** The second time you run the same aggregation, one stage shows as "skipped". What does that mean, and is it caching?

<details><summary>Model answer</summary>

Spark found the shuffle output of that stage still available on executors from a previous job using the same shuffle, so it didn't recompute it. It's shuffle-file reuse, not `cache()`: it only works while the shuffle files exist and the same shuffle dependency is reused (common with RDDs or reused DataFrame plans); it's not a guarantee you should design around.
</details>

**T3.** A stage has 200 tasks: 199 finish in 5 seconds, one takes 20 minutes. Adding nodes doesn't help. Why, and what's your next step?

<details><summary>Model answer</summary>

The stage can't finish until its slowest task does, and one task runs on one core — that's data skew: one partition (one hot key) holds far more data. More nodes only speed up the 199 short tasks. Check task metrics (input/shuffle read size, max vs median), find the hot key, then use AQE skew join handling, salting, broadcasting the small side, or handling the hot key separately (Phase 3).
</details>

---

## E. Revision summary

- **Action → job; shuffle → new stage; partition → task.** Tasks run one per core at a time; extra tasks run in waves.
- Stages = shuffle Exchanges + 1 (straight line); joins have one map stage per shuffled side.
- Tasks: first stage = input partitions; after shuffle = shuffle partitions (AQE may coalesce); `count` final stage = 1.
- Extra jobs: AQE, `show`/`take`, sort sampling, inferSchema, broadcast, Delta writes; "skipped" = reused shuffle output.
- Verify with `explain()` + Spark UI (classic) or query profile (serverless).