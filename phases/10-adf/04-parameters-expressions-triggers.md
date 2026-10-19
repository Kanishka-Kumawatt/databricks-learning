# 10.4 Parameters, Variables, Expressions and Triggers

**Time:** 12 min · **Prerequisites:** 10.2, 10.3

**After this topic you can:**
- distinguish pipeline, dataset, linked service and global parameters from variables;
- read and write ADF expressions, including string interpolation and system variables;
- choose between schedule, tumbling window, storage event and custom event triggers;
- explain tumbling window semantics: windows, backfill, retry, `maxConcurrency`, dependencies.

---

## A. Concept deep dive

### 1. The simplest explanation

- **Parameters** are **inputs** fixed for the whole run (like function arguments).
- **Variables** are **mutable state** inside one run (like local variables), changed with Set Variable / Append Variable.
- **Expressions** (`@...`) compute values at run time from parameters, variables, activity outputs and system variables.
- **Triggers** start runs and pass values into pipeline parameters.

### 2. Parameter scopes

| Scope | Defined on | Read with | Typical use |
|---|---|---|---|
| **Pipeline parameter** | Pipeline | `@pipeline().parameters.source_table` | Run inputs: table, run date, environment flags |
| **Dataset parameter** | Dataset | `@dataset().folderPath` | One generic dataset for many folders/tables |
| **Linked service parameter** | Linked service | `@{linkedService().databaseName}` | One connection definition for many databases |
| **Global parameter** | Factory (Manage hub) | `@pipeline().globalParameters.env` | Factory-wide constants (environment name, alert URL key name); overridden per environment in CI/CD |
| **Variable** | Pipeline | `@variables('fileFound')` | State that changes during the run (flags, counters, collected arrays) |

Parameters can't be changed during a run; variables can't be passed in from a trigger. Variables are **pipeline-scoped** (shared by all parallel ForEach iterations — 10.3).

### 3. Expressions

| Form | Meaning | Example |
|---|---|---|
| `"@expr"` (whole value) | The value *is* the result of the expression (keeps type: number, bool, array, object) | `"@activity('LookupOld').output.firstRow"` |
| `"text @{expr} text"` | **String interpolation** — result converted to string inside text | `"orders/run_id=@{pipeline().RunId}"` |
| `"@@text"` | Literal string starting with `@` | `"@@notAnExpression"` |

Common functions: `concat`, `formatDateTime`, `addDays`, `addHours`, `utcnow`, `coalesce`, `equals`, `greater`, `and`, `or`, `not`, `if`, `string`, `int`, `json`, `createArray`, `length`, `contains`.

### 4. System variables you'll use

| Expression | Value |
|---|---|
| `@pipeline().RunId` | ID of this pipeline run (unique per run) |
| `@pipeline().Pipeline` / `@pipeline().DataFactory` | Pipeline name / factory name |
| `@pipeline().TriggerType` / `TriggerName` / `TriggerTime` | How and when the run was started (`Manual`, `ScheduleTrigger`, …) |
| `@pipeline()?.TriggeredByPipelineRunId` | Parent run ID when called by Execute Pipeline (null-safe `?.`) |
| `@trigger().scheduledTime` / `@trigger().startTime` | Schedule trigger: planned time / actual fire time |
| `@trigger().outputs.windowStartTime` / `windowEndTime` | Tumbling window boundaries |
| `@triggerBody().folderPath` / `@triggerBody().fileName` | Storage event trigger: the blob that fired it |
| `@triggerBody().event.data.<key>` | Custom event trigger: field from the event payload |
| `@activity('X').output...` / `@activity('X').Status` | Output / status of an earlier activity |
| `@item()` | Current ForEach element |

Trigger values must be **mapped to pipeline parameters** in the trigger definition; inside the pipeline you then read `@pipeline().parameters.windowStart`.

### 5. Trigger types

| | **Schedule** | **Tumbling window** | **Storage event** | **Custom event** |
|---|---|---|---|---|
| Fires | Wall-clock recurrence (cron-like) | Fixed, contiguous, non-overlapping windows from a `startTime` | Blob created/deleted in ADLS Gen2 / GPv2 (via Event Grid) | Event published to an Event Grid custom topic |
| Pipelines | many-to-many | **exactly one** pipeline | many | many |
| State | none | **each window has state** (waiting, running, succeeded, failed) | none | none |
| Past runs (backfill) | ❌ | ✅ `startTime` in the past → windows run oldest → newest | ❌ | ❌ |
| Retry | via activity retries | ✅ `retryPolicy` (count, intervalInSeconds ≥ 30) on the pipeline run | activity retries | activity retries |
| Concurrency | none (runs may overlap unless pipeline `concurrency` is set) | `maxConcurrency` 1–50 (required) | — | — |
| Dependencies | ❌ | ✅ on other tumbling triggers or **self-dependency** (previous window must succeed) | ❌ | ❌ |
| Values | `scheduledTime`, `startTime` | `windowStartTime`, `windowEndTime` | `folderPath`, `fileName` | `event.data.*` |

Tumbling window details worth knowing:
- `frequency` is `Minute`, `Hour` or `Month`; minimum window 5 minutes; **`frequency`/`interval` can't be changed after publishing**.
- `delay` waits after the window end before starting (e.g. let late records arrive) without changing the window boundaries.
- Backfill example: hourly trigger, `startTime` 24 h ago, `maxConcurrency` 10 → first 10 windows run in parallel, then the next 10…
- A **self-dependency** with negative `offset` makes windows strictly sequential: window *t* waits for *t-1* to succeed (useful when each window depends on the previous state).
- Failed or cancelled windows can be **rerun** from monitoring; reruns use the latest published trigger definition and re-evaluate dependencies.

Storage event trigger notes: needs the Event Grid resource provider registered; filters only by `blobPathBeginsWith` / `blobPathEndsWith` (no wildcards); `ignoreEmptyBlobs` defaults to true; one event per blob → many small files = many runs.

### 6. Which trigger when — ShopCo

| ShopCo need | Trigger | Why |
|---|---|---|
| Nightly orders load at 02:00 UTC | **Schedule** | Simple; the watermark (10.6) defines what to load, not the clock |
| Hourly clickstream partitions must be processed for each hour, including a backfill of last week | **Tumbling window** | Window = data slice; backfill; retry; self-dependency for ordering |
| Partner drops `returns_*.csv` whenever ready | **Storage event** | Run as soon as the file lands, no polling |
| ERP team publishes "export completed" event | **Custom event** | Decoupled signal with payload (export path) |

### 7. Misconceptions

1. **"Schedule trigger = tumbling window."** Schedule is stateless wall-clock; tumbling windows are stateful slices with backfill, retry and dependencies.
2. **"The trigger time is the data window."** For a schedule trigger it's just when it fired; define the extract with a watermark or explicit parameters.
3. **"Variables can be passed from the trigger."** Only parameters can.
4. **"`@{}` and `@` are the same."** `@{}` always produces a string; `@` keeps the type — `"@{activity('Lookup').output.firstRow}"` gives a JSON string, not an object.
5. **"Storage events fire once per folder."** They fire per blob — a Copy writing 200 part-files can fire 200 runs unless you filter on a marker file (e.g. `_SUCCESS`).

---

## B. Practical — triggers and parameters

**File:** [`exercises/phase-10/04_triggers.json`](../../exercises/phase-10/04_triggers.json) (study bundle, abbreviated): schedule, tumbling window (with retry + self-dependency), storage event and custom event triggers.

Tumbling window trigger, explained:

```json
{
  "name": "TR_Clickstream_Hourly",
  "properties": {
    "type": "TumblingWindowTrigger",
    "typeProperties": {
      "frequency": "Hour",
      "interval": 1,
      "startTime": "2026-10-11T00:00:00Z",
      "delay": "00:15:00",
      "maxConcurrency": 4,
      "retryPolicy": { "count": 2, "intervalInSeconds": 300 },
      "dependsOn": [
        { "type": "SelfDependencyTumblingWindowTriggerReference", "offset": "-01:00:00", "size": "01:00:00" }
      ]
    },
    "pipeline": {
      "pipelineReference": { "referenceName": "PL_Clickstream_Hour", "type": "PipelineReference" },
      "parameters": {
        "windowStart": "@trigger().outputs.windowStartTime",
        "windowEnd": "@trigger().outputs.windowEndTime"
      }
    }
  }
}
```

| Line | Explanation |
|---|---|
| `frequency`/`interval` | 1-hour windows; fixed forever once published |
| `startTime` in the past | Backfills every hour since 11 Oct, oldest first |
| `delay` 15 min | Each window starts 15 min after it closes (late events) |
| `maxConcurrency` 4 | At most 4 windows run at once during backfill |
| `retryPolicy` | Failed pipeline run for a window retried twice, 5 min apart, same parameters |
| self-dependency `offset -01:00:00` | Window *t* waits for window *t-1* to succeed. **Note:** with self-dependency, windows effectively run one after another, so concurrency gains during backfill are limited |
| `pipeline` (singular) | Tumbling window → exactly one pipeline |
| `windowStart`/`windowEnd` | Mapped to pipeline parameters; the pipeline filters `event_time >= windowStart AND event_time < windowEnd` (half-open interval, no overlap) |

### Common mistakes

- Forgetting to **start** a trigger after creating it (new triggers are Stopped) — or forgetting to stop triggers before a CI/CD deployment.
- Time zones: schedule trigger `timeZone` defaults to UTC; DST shifts surprise teams that expect local time.
- Using a schedule trigger for hourly slices and recomputing "previous hour" with `utcnow()` → a delayed run processes the wrong hour. Tumbling windows pass exact boundaries.
- Storage event trigger on the whole container → events from unrelated files and runaway runs/cost.

---

## C. Interview questions

### 🟢 Basic

**B1.** What's the difference between a parameter and a variable in ADF?

<details><summary>Model answer</summary>

Parameters are inputs supplied when a run starts (trigger, Execute Pipeline, manual) and are read-only during the run. Variables are pipeline-scoped mutable values changed by Set Variable/Append Variable during the run; they can't be passed in from outside.
</details>

**B2.** Name the four trigger types.

<details><summary>Model answer</summary>

Schedule (wall-clock recurrence), tumbling window (fixed contiguous time windows with state), storage event (blob created/deleted via Event Grid), custom event (Event Grid custom topic events). Plus manual/on-demand runs (debug, REST API, SDK).
</details>

**B3.** How do you use the file name of the blob that fired a storage event trigger?

<details><summary>Model answer</summary>

Map `@triggerBody().fileName` (and `@triggerBody().folderPath`) to pipeline parameters in the trigger definition, then use `@pipeline().parameters.<name>` inside the pipeline.
</details>

### 🟡 Medium

**M1.** When would you choose a tumbling window trigger over a schedule trigger?

<details><summary>Model answer</summary>

When each run must process an exact time slice and you need backfill of past windows, per-window state and rerun, automatic retry of the pipeline run, controlled concurrency, or dependencies between windows/triggers. Schedule triggers are stateless and simpler — fine when a watermark defines the extract.
</details>

**M2.** Explain `@` vs `@{}` with an example where it matters.

<details><summary>Model answer</summary>

`"@activity('L').output.firstRow.qty"` returns the typed value (e.g. integer 5). `"@{activity('L').output.firstRow.qty}"` returns the string "5"; interpolation is meant for building strings like paths or SQL. Passing `@{...}` where an object or number is expected (e.g. ForEach items, an int parameter) causes type errors or a JSON string instead of an array.
</details>

**M3.** What does `maxConcurrency` do on a tumbling window trigger, and what happens if `startTime` is a month ago?

<details><summary>Model answer</summary>

It caps how many windows run simultaneously (1–50). With `startTime` a month ago, the trigger creates a window for every interval since then and runs them oldest-first, up to `maxConcurrency` at a time, before running future windows. For long histories Microsoft recommends a separate initial historical load instead.
</details>

### 🔴 Tricky

**T1.** A storage event trigger fires 300 pipeline runs every night and the Databricks bill explodes. Why, and how do you fix it?

<details><summary>Model answer</summary>

The trigger fires once per blob created; the upstream writer produces many part-files (or the filter is too broad). Fix: filter on a marker file (`blobPathEndsWith: "_SUCCESS"` or a manifest), narrow `blobPathBeginsWith`, set pipeline `concurrency`, or let Databricks Auto Loader / a Databricks file-arrival trigger process all new files incrementally in one run.
</details>

**T2.** Your hourly tumbling window pipeline must not process hour 10 before hour 9 has succeeded (it updates running totals). How do you configure it, and what's the trade-off?

<details><summary>Model answer</summary>

Add a self-dependency (`SelfDependencyTumblingWindowTriggerReference` with `offset: -01:00:00`). Window *t* waits until *t-1* succeeds; if *t-1* fails, later windows stay "waiting on dependency" until it's rerun successfully. Trade-off: no parallel backfill and one failure blocks the chain — monitoring and alerting on waiting windows matter.
</details>

**T3.** A schedule trigger runs every 15 minutes; sometimes a run takes 25 minutes. What goes wrong, and what are two fixes?

<details><summary>Model answer</summary>

Runs overlap — two runs read the same watermark, copy overlapping data and race to update the watermark (duplicates or skipped windows if non-idempotent). Fixes: set pipeline `concurrency: 1` (extra runs queue), use a tumbling window trigger with `maxConcurrency: 1` (+ self-dependency), make writes idempotent (MERGE, run-specific folders), and speed up the pipeline.
</details>

---

## E. Revision summary

- **Parameters** = run inputs (pipeline/dataset/linked service/**global**); **variables** = mutable, pipeline-scoped state.
- `@expr` keeps type; `@{expr}` interpolates into a string; `@@` escapes.
- System variables: `pipeline().RunId`, `TriggerTime`, `trigger().scheduledTime`, `trigger().outputs.windowStartTime/windowEndTime`, `triggerBody().fileName/folderPath`, `triggerBody().event.data.*`.
- **Schedule**: stateless wall clock, n:m. **Tumbling window**: 1 pipeline, stateful windows, backfill oldest-first, `retryPolicy`, `maxConcurrency` 1–50, dependencies/self-dependency, `delay`; frequency/interval immutable. **Storage event**: per blob, Event Grid, begins/ends-with filters. **Custom event**: Event Grid topic + payload.