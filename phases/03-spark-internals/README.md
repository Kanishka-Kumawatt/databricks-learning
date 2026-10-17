# Phase 3 — Spark Internals & Performance Concepts

**Day 2 AM · 4 h · Level: ⭐ core, conceptual for the rest**

| # | Topic | Time |
|---|---|---|
| 3.1 | Logical vs physical plans; reading `explain()` | 30 min |
| 3.2 | Catalyst optimizer (predicate pushdown, column pruning) | 25 min |
| 3.3 | Adaptive Query Execution (AQE) | 20 min |
| 3.4 | Join strategies: broadcast hash, sort-merge, shuffle hash | 35 min |
| 3.5 | Data skew, spill, memory pressure, GC (conceptual) | 30 min |
| 3.6 | Driver memory vs executor memory; common OOMs | 20 min |
| 3.7 | Caching & persistence — when it helps, when it hurts | 20 min |
| 3.8 | Checkpointing (RDD/DataFrame vs streaming) | 15 min |
| 3.9 | Spark UI tour: jobs, stages, SQL tab, executors | 25 min |
| 3.10 | RDD vs DataFrame vs Dataset; language support | 15 min |
| — | Quiz + revision | 25 min |

**Gate:** read a physical plan, name the join strategy and the exchange (shuffle) nodes, and propose a fix for a skewed join.