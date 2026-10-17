# Phase 9 — CDC & Slowly Changing Dimensions

**Day 4 PM · 3 h · Level: ⭐ Master**

| # | Topic | Time |
|---|---|---|
| 9.1 | What CDC is; log-based vs snapshot comparison | 15 min |
| 9.2 | Inserts, updates, deletes; sequence columns & ordering | 20 min |
| 9.3 | Duplicates, late & out-of-order events, idempotency | 20 min |
| 9.4 | Initial load vs incremental loads | 10 min |
| 9.5 | SCD1 vs SCD2; current flag, effective/end dates, surrogate keys | 25 min |
| 9.6 | MERGE-based CDC and SCD2 (single atomic MERGE) | 35 min |
| 9.7 | AUTO CDC (`create_auto_cdc_flow`) in declarative pipelines | 20 min |
| 9.8 | Deletes & tombstones; multiple updates per key; replay & reconciliation | 20 min |
| — | Practical: customer CDC through Bronze → Silver → Gold with before/after tables | 15 min |

**Gate:** build SCD2 by hand for a customer dataset and explain every column and every MERGE clause.