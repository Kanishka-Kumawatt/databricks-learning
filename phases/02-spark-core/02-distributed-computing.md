# 2.2 Distributed Computing & Parallel Processing — Why Multiple Machines

**Time:** 25 min · **Prerequisites:** 2.1

**After this topic you can:**
- explain parallel vs distributed processing, and vertical vs horizontal scaling;
- explain why one big machine eventually stops being enough;
- explain the costs of distribution (coordination, network, failures, skew) and the law that limits speed-up;
- explain why "moving computation to the data" changed in the cloud lakehouse.

---

## A. Concept deep dive

### 1. The simplest explanation

- **Parallel processing:** doing many pieces of work **at the same time** (on many CPU cores).
- **Distributed processing:** those cores are spread across **many machines** connected by a network, coordinated as one system.

Spark does both: it splits data into pieces (**partitions**, 2.5) and runs one piece per CPU core across many machines.

```
2 TB orders ──split──▶ 16 000 pieces of ~128 MB
                         │
     ┌───────────────────┼───────────────────┐
  machine 1           machine 2   …      machine 50
  16 cores            16 cores            16 cores
  16 pieces at once   16 pieces at once   16 pieces at once   → 800 pieces in parallel
```

### 2. Why multiple machines? (the problem being solved)

| Limit of one machine | Why it matters |
|---|---|
| **I/O bandwidth** | Reading 2 TB through one machine's network/disk takes a long time no matter how fast the CPU is. 50 machines read 50 slices simultaneously |
| **CPU cores** | One server has tens of cores; a cluster has hundreds or thousands |
| **Memory** | Joins and aggregations need working memory; one machine's RAM is a hard ceiling |
| **Cost curve** | Very large single machines cost disproportionately more than many commodity ones |
| **Availability** | One machine = single point of failure. A cluster can lose a node and continue |
| **Elasticity** | In the cloud you add machines for the nightly peak and remove them after (autoscaling) |

Illustrative arithmetic (numbers are made up but realistic in shape): if one machine reads at ~0.5 GB/s, 2 TB takes ~67 minutes just to read. With 50 machines reading in parallel, the ideal is ~1.5 minutes — plus coordination overhead.

### 3. Vertical vs horizontal scaling

| | Vertical (scale **up**) | Horizontal (scale **out**) |
|---|---|---|
| How | Bigger machine (more cores/RAM) | More machines |
| Limit | Largest machine available; price | Coordination and network costs |
| Code changes | None | Need a distributed engine (Spark) |
| Failure | Whole job dies with the machine | Lost work re-runs elsewhere |
| In Databricks | Larger worker/driver instance type | More workers, autoscaling |

Real clusters use **both**: right-sized nodes, and enough of them.

### 4. How distributed processing works (the generic recipe)

1. **Split** the data into independent chunks (partitions).
2. **Ship code to the data**: every worker runs the same function on its chunks.
3. **Combine** partial results — this often requires moving data between machines by key (**shuffle**, 2.10).
4. **Coordinate** with a central process (Spark's **driver**, 2.3) that plans, assigns work, tracks progress and retries failures.

### 5. The costs of distribution — nothing is free

| Cost | What happens |
|---|---|
| **Coordination overhead** | Planning, scheduling, launching tasks: tens of milliseconds to seconds. Dominates on tiny data |
| **Network** | Combining results by key moves data between machines; network is far slower than RAM |
| **Stragglers / skew** | The job finishes when the **slowest** piece finishes. One huge partition (e.g. one customer with 40 % of orders) keeps everyone waiting |
| **Partial failure** | With 100 machines, something fails regularly. The engine must detect and recompute lost work |
| **Serial parts** | Some steps can't be parallelised (driver-side planning, writing one final file, `collect()` to the driver) |

**Amdahl's law** — the speed-up is limited by the part that stays serial:

```
speed-up = 1 / ( s + (1 − s) / N )        s = serial fraction, N = workers

s = 5 %, N = 100  →  1 / (0.05 + 0.0095) ≈ 16.8×   (not 100×)
```

### 6. Data locality — then and now

| Hadoop on-premise | Cloud lakehouse (Databricks) |
|---|---|
| Data stored on the **same machines** that compute (HDFS) | Data in **object storage** (S3 / ADLS / GCS), compute separate |
| "Move computation to the data" — schedule tasks where the blocks are | Every task reads over the network from storage; locality preference matters less |
| Storage and compute scale together | Storage and compute scale **independently**; clusters can be shut down without losing data |

Spark still schedules for locality when it can (e.g. data cached on an executor, `spark.locality.wait` default 3s), and Databricks adds local SSD **disk caching** of remote files on supported instance types — but the dominant cloud model is "separated storage, elastic compute".

### 7. Real data-engineering example — ShopCo

ShopCo's `orders` table is 2 TB, growing 5 GB/day. The nightly Gold job (filter → join customers → aggregate by city/day):

| Setup | What happens |
|---|---|
| One 16-core VM with pandas | Doesn't fit in RAM → fails; even if chunked, hours of single-threaded work |
| Spark, 1 driver + 4 workers × 16 cores | 64 tasks at a time; ~16 000 input pieces ≈ 250 "waves" of tasks |
| Spark, 1 driver + 20 workers × 16 cores | 320 tasks at a time; ~50 waves; faster until shuffle/network or skew becomes the limit |
| Customer "C-MARKETPLACE" owns 40 % of orders | The aggregation/join task for that key is enormous → one straggler; adding machines doesn't help (skew, Phase 3) |

### 8. When to scale out / when not

| ✅ Scale out | ❌ Scaling out won't help |
|---|---|
| Data volume or I/O is the bottleneck | Data is skewed into one partition |
| Many independent partitions waiting to run (tasks queued) | Work is serial (driver-side Python loop, `collect()`, single output file) |
| Memory pressure from too much data per core | Data is tiny — overhead dominates |

### 9. Misconceptions

1. **"Twice the machines = twice as fast."** Only for perfectly parallel work; shuffles, skew and serial steps cap the speed-up (Amdahl).
2. **"Distributed means each machine gets a full copy."** Each gets **different pieces** (except broadcast variables/joins, which deliberately copy small data to every machine).
3. **"Parallel and distributed are the same."** A laptop with 8 cores is parallel but not distributed; Spark local mode is exactly that.
4. **"Locality is still the main Spark optimisation."** In cloud lakehouses, minimising data read (pruning, data skipping) and shuffles matters far more.

---

## B. Practical — see parallel pieces

Run in any notebook (works on classic and serverless compute).

```python
from pyspark.sql import functions as F

nums = spark.range(0, 12, 1, numPartitions=3)                         # 1

(nums.withColumn("pid", F.spark_partition_id())                       # 2
     .groupBy("pid")
     .agg(F.count("*").alias("rows"), F.min("id").alias("first_id"),
          F.max("id").alias("last_id"))
     .orderBy("pid")
     .show())                                                          # 3
```

**Expected output:**

```
+---+----+--------+-------+
|pid|rows|first_id|last_id|
+---+----+--------+-------+
|  0|   4|       0|      3|
|  1|   4|       4|      7|
|  2|   4|       8|     11|
+---+----+--------+-------+
```

| # | Explanation |
|---|---|
| 1 | Creates ids 0–11 split into exactly 3 partitions. `spark.range` slices the range into contiguous, evenly sized chunks |
| 2 | `spark_partition_id()` returns the id of the partition each row lives in — a way to *see* the split |
| 3 | Each partition was processed by its own task, potentially on a different core or machine |

**Try:** change `numPartitions` to 4 and 12 and predict the output first (3 rows each; 1 row each).

**Thinking exercise (no code):** with 1 000 partitions and a cluster of 4 workers × 8 cores (32 slots), how many "waves" of tasks? → ⌈1000 / 32⌉ = 32 waves (31 full waves + a final wave of 8 tasks). If one partition is 50× bigger than the rest, the last wave is dominated by that one task.

### Common mistakes

- Adding workers to fix a job that is slow because of one skewed key or a driver-side loop.
- Forgetting that a small dataset with 200 partitions creates 200 tiny tasks — overhead without benefit.
- Assuming "more cores per machine" and "more machines" behave identically (network, memory per core and failure domains differ).

---

## C. Interview questions

### 🟢 Basic

**B1.** Why do we need multiple machines to process data?

<details><summary>Model answer</summary>

Because a single machine has hard limits on I/O bandwidth, CPU cores and memory, gets disproportionately expensive as it grows, and is a single point of failure. Splitting data across many machines lets each read and process a slice in parallel, so total throughput grows with the cluster, and the failure of one machine only loses a small part of the work, which can be recomputed.
</details>

**B2.** What is the difference between parallel and distributed processing?

<details><summary>Model answer</summary>

Parallel processing runs multiple pieces of work simultaneously, e.g. on many cores. Distributed processing spreads that work across multiple networked machines coordinated as one system. Distributed implies dealing with network transfer and partial failures; a multi-core laptop is parallel but not distributed.
</details>

**B3.** What is the difference between vertical and horizontal scaling?

<details><summary>Model answer</summary>

Vertical scaling uses a bigger machine (more CPU/RAM); it's simple but capped by the largest machine and cost. Horizontal scaling adds more machines; it scales much further but needs a distributed engine and brings coordination, network and failure-handling costs.
</details>

### 🟡 Medium

**M1.** Doubling the number of workers only made a job 20 % faster. Give three possible reasons.

<details><summary>Model answer</summary>

(1) Data skew — one or a few huge partitions dominate runtime. (2) Serial parts — driver-side code, `collect()`, a single output file, or planning time. (3) Too few partitions to use the extra cores. Also: shuffle/network bottlenecks, source throughput limits (e.g. a JDBC source), or the job being small enough that overhead dominates.
</details>

**M2.** Explain Amdahl's law with a data-pipeline example.

<details><summary>Model answer</summary>

Speed-up = 1 / (s + (1−s)/N), where s is the serial fraction. If 10 % of a pipeline is serial — e.g. a driver-side loop calling an API per file — then even infinite workers give at most 10× speed-up. To go faster you must shrink the serial part, not just add machines.
</details>

**M3.** What does "move computation to the data" mean, and is it still true on Databricks?

<details><summary>Model answer</summary>

In Hadoop, data blocks lived on the compute nodes, so the scheduler ran tasks on the node holding the block to avoid network transfer. On Databricks, data lives in cloud object storage separate from compute, so tasks read over the network anyway; Spark still prefers local data when it exists (cached data, Databricks disk cache), but the bigger wins are reading less data (pruning, data skipping) and avoiding shuffles. Separation lets storage and compute scale independently.
</details>

### 🔴 Tricky

**T1.** A job processes 10 GB in 1 000 partitions on a 4-node cluster. You scale to 40 nodes and it's no faster. What do you check first?

<details><summary>Model answer</summary>

Whether the extra cores are actually used: look at the stages in the Spark UI — task counts, task duration distribution (max vs median for skew), and whether a few long tasks dominate. Also check for serial driver work between Spark jobs (timeline gaps), source throttling, and whether autoscaling actually added nodes. With 1 000 tasks of 10 MB, tasks may be so short that scheduling overhead dominates — fewer, larger partitions could even be faster.
</details>

**T2.** "Distributed systems are more reliable because there are more machines." True?

<details><summary>Model answer</summary>

More machines mean more failures in absolute terms; reliability comes only from the engine's ability to tolerate them. Spark tolerates executor/task failures by retrying tasks and recomputing lost partitions from lineage, but the driver is a single point of failure for the application: if it dies, the application fails and must be restarted (e.g. by job retries), so pipelines must be idempotent.
</details>

**T3.** Why can a single-node pandas job beat a 10-node Spark cluster?

<details><summary>Model answer</summary>

If the data fits comfortably in one machine's memory, pandas avoids Spark's planning, task scheduling, serialization, and network shuffle costs, plus cluster start-up time. Distribution only pays off when the work is large enough that parallel speed-up outweighs those fixed costs.
</details>

---

## E. Revision summary

- Parallel = many cores at once; distributed = many machines coordinated as one.
- Multiple machines because one machine caps I/O, cores, memory, cost-efficiency and availability.
- Recipe: split → ship code to data → combine (shuffle) → coordinate (driver).
- Costs: coordination, network, skew/stragglers, failures, serial parts (Amdahl).
- Cloud lakehouse separates storage and compute; reading less and shuffling less beat data locality.