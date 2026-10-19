# Databricks notebook source
# Phase 8.2 — runs only on the FALSE branch of the condition task `has_data`.
# Choose deliberately: "no file today" is either normal (log and succeed) or an incident (raise -> task fails -> alert).

# COMMAND ----------

dbutils.widgets.text("run_date", "2026-10-16")
dbutils.widgets.text("fail_when_empty", "false")       # job parameter; set "true" in prod if a missing file is an incident

run_date = dbutils.widgets.get("run_date")
row_count = dbutils.jobs.taskValues.get(taskKey="check_landing", key="row_count",
                                        default=0, debugValue=0)   # debugValue is used outside a job run

msg = f"No ShopCo orders for {run_date} (row_count={row_count}). Pipeline refresh was skipped."
print(msg)

if dbutils.widgets.get("fail_when_empty").lower() == "true":
    raise RuntimeError(msg)        # a failed task -> on_failure notification fires

dbutils.notebook.exit(msg)         # succeeds; downstream ALL_DONE task still runs