# Phase 2 — Apache Spark Core

**Day 1 PM · 5 h · Level: ⭐ Master**

**Objective:** explain exactly what happens between writing a DataFrame line and results appearing — which machine does what, and why.

| # | Topic | Time |
|---|---|---|
| 2.1 | Why Spark was created (MapReduce limits, in-memory processing) | 20 min |
| 2.2 | Distributed computing & parallel processing — why multiple machines | 25 min |
| 2.3 | Cluster architecture: driver, executors, cluster manager | 35 min |
| 2.4 | SparkSession vs SparkContext | 15 min |
| 2.5 | Partitions — the unit of parallelism | 25 min |
| 2.6 | Jobs, stages, tasks — how work is split | 35 min |
| 2.7 | DAG & DAG scheduler | 20 min |
| 2.8 | Lazy evaluation; transformations vs actions | 30 min |
| 2.9 | Narrow vs wide transformations | 25 min |
| 2.10 | Shuffle — what it is, why it is expensive | 35 min |
| — | Practical: predict jobs/stages/tasks, verify in Spark UI | 30 min |
| — | Quiz + revision | 25 min |

**Gate:** given a short PySpark snippet, predict the number of jobs and stages, point to the shuffle, and explain the driver's vs executors' roles.