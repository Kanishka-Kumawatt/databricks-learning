# 12.13 Project Stories — STAR Templates and the 2-Minute Pitch

**Time:** 30 min (write) + 15 min (say out loud, timed)

**After this topic you can:**
- deliver a 2-minute architecture pitch without notes;
- tell four deep-dive stories in STAR form (Situation, Task, Action, Result) in 2–3 minutes each;
- anticipate and answer the follow-ups an interviewer will ask after each story.

**Rules for every story**
- **Confidential:** patterns and decisions only — no client names beyond "an airline", no internal system,
  catalog, path, or person names, no security findings.
- **"I" vs "our team":** use "I" only for what you personally did. Interviewers probe; over-claiming collapses fast.
- **Numbers:** use numbers you can defend (approximate is fine: "roughly 40 % faster", "about a dozen notebooks").
  If you don't know, say what you would measure.
- **Action is 60 % of the story.** Situation and task in 2–3 sentences.

---

## 1. The 2-minute architecture pitch (script)

> "I worked on an airline's enterprise data platform on **Azure Databricks and Azure Data Factory**.
>
> Data comes from many source systems — partner files over SFTP, relational databases, ERP extracts, SharePoint
> files, REST APIs and event streams. **ADF** copies it into an **ADLS Gen2 landing zone**, orchestrated by
> master, wrapper and child pipelines that are **metadata-driven** — a config table says what to load.
>
> **Databricks** then loads it through **raw, staging and curated** layers — essentially bronze, silver and gold —
> as Delta tables in **Unity Catalog**, with one catalog per business domain. Staging handles cleaning, dedup and
> **SCD1/SCD2** history; curated holds business models used by **Power BI** and downstream systems.
>
> What makes it production-grade is the frameworks: an **audit framework** that records every run and activity
> with source and target counts and supports restarts; **DQ checks** with alerts; **GDPR retention** rules that
> purge or anonymise personal data; secrets in **Key Vault**; and **CI/CD** for both ADF and Databricks.
>
> The biggest change during my time was the **Hive metastore to Unity Catalog migration** — moving to
> three-part names, external locations and volumes, and making the code work on UC access modes — and the shift of
> new pipelines to **declarative pipelines and Databricks Jobs deployed with bundles**.
>
> My own focus was **[fill in: e.g. migrating X domain's notebooks / building Y / fixing Z]**."

Practise: record yourself; target 1:45–2:00. Then the 30-second version (first and last paragraphs only).

---

## 2. Story A — HMS → Unity Catalog migration (lead with this one)

| STAR | Prompt — fill in with your specifics | Example phrasing (generic) |
|---|---|---|
| **Situation** | What was the platform state? Why migrate? | "Our notebooks used the Hive metastore, two-part table names and storage mounts; the organisation was standardising on Unity Catalog for governance and lineage." |
| **Task** | What was *your* scope? Which domain/how many notebooks/what deadline? | "I was responsible for migrating ___ notebooks / pipelines for ___ subject area(s) and validating them before cut-over." |
| **Action** | 3–5 concrete actions, in order. Pick from: mapping old → new names; env parameter → catalog; mounts → external locations/volumes; RDD → DataFrame; `/dbfs` → volumes/`dbutils.fs`; passing params to child notebooks; fixing imports; runtime upgrade; parallel-run comparison | "I first built a mapping of every table to its new catalog/schema … then I … The trickiest bug was a child notebook silently falling back to its default catalog because `dbutils.notebook.run` doesn't inherit widgets — I fixed it by …" |
| **Result** | What changed? Numbers? What did the team adopt from you? | "All ___ notebooks ran on UC compute; parallel runs matched on counts and content for ___ days; we cut over with no rollback. The change-tagging convention / checklist I used was reused for ___." |

**Your notes:**

- Situation: ______________________________________________
- Task: ___________________________________________________
- Action 1: ________________________________________________
- Action 2: ________________________________________________
- Action 3: ________________________________________________
- Hardest bug and how I found it: ____________________________
- Result (number): __________________________________________

**Likely follow-ups**
1. Mount vs external location — what's the security difference?
2. Why did RDD code fail? Why not just use a dedicated cluster?
3. How exactly did you compare old vs new outputs? What if counts matched but values didn't?
4. How did you handle tables written by both old and new code during the transition?
5. What would you do differently on the next migration?
6. How did the catalog get resolved per environment? What stops prod code writing to dev?
7. Did the runtime upgrade change any results?

---

## 3. Story B — SCD2 / CDC correctness

| STAR | Prompt | Example phrasing (generic) |
|---|---|---|
| **Situation** | Which dimension? How was SCD2 implemented? | "A passenger-style dimension was maintained with SCD2 written as a MERGE to expire rows and a separate INSERT for new versions." |
| **Task** | What did you notice or own? | "I was asked to investigate a discrepancy / I reviewed the pattern while migrating it." |
| **Action** | Diagnosis steps (history, time travel, checks for keys without current rows / overlapping windows); the fix (single MERGE with staged union, timestamp guard, dedup by latest) | "I queried `DESCRIBE HISTORY` and found … I wrote the 'one current row per key' check … I proposed a single MERGE …" |
| **Result** | Outcome — or, if not implemented, what you proposed and its expected impact | "The check now runs as a DQ rule; the new pattern is used for ___." |

**Your notes:** S ____ · T ____ · A ____ · R ____

**Likely follow-ups**
1. Walk me through the MERGE clause by clause. Why `merge_key = NULL`?
2. What if two changes for the same key arrive in one batch?
3. What if an older change arrives after a newer one?
4. How are surrogate keys generated? Why not `row_number()`?
5. How do deletes work in SCD2?
6. How would AUTO CDC do this? What are `__START_AT`/`__END_AT`?
7. How would you repair a table that already has broken history?

---

## 4. Story C — Framework / restartability

| STAR | Prompt | Example phrasing (generic) |
|---|---|---|
| **Situation** | Many pipelines, failures at night, manual reruns | "Loads ran overnight across many objects; when one failed, someone had to work out what had loaded and what hadn't." |
| **Task** | Your role in the audit / restart / DQ framework | "I extended / used / onboarded pipelines to the audit framework …" |
| **Action** | Run/activity tables, counts, status, error log; restart logic; making it safe (atomic slice overwrite); alerts; making a failure really fail | "I added source/target counts for ___ … changed the restart from delete-then-insert to `replaceWhere` for ___ … ensured failure branches end in a Fail activity …" |
| **Result** | Fewer manual reruns? Faster diagnosis? Caught a data loss? | "Reruns became a single click; reconciliation caught ___." |

**Your notes:** S ____ · T ____ · A ____ · R ____

**Likely follow-ups**
1. Is delete-then-insert idempotent? Is it atomic? What's the difference?
2. What happens if two runs start at once?
3. Counts match but data is wrong — how would you catch that?
4. Where do the audit tables live and who reads them?
5. How would this look with Databricks Jobs instead of ADF?

---

## 5. Story D — Performance or cost

| STAR | Prompt | Example phrasing (generic) |
|---|---|---|
| **Situation** | A slow or expensive job — which symptom? | "A daily DQ step took ___ minutes on a wide table" / "Jobs ran on an always-on all-purpose cluster." |
| **Task** | Your goal | "Reduce runtime / cost without changing results." |
| **Action** | How you diagnosed (Spark UI: number of jobs, stages, skew, spill; driver memory; billing tags) and what you changed (single-pass aggregation; remove `collect()`; broadcast join; job clusters; clustering) | "In the Spark UI I saw one job per column — N full scans. I rewrote it as one aggregation …" |
| **Result** | Before/after numbers | "From ___ min to ___ min; ___ % fewer DBUs." |

**Your notes:** S ____ · T ____ · A ____ · R ____

**Likely follow-ups**
1. What did you see in the Spark UI exactly?
2. How did you make sure results didn't change?
3. Why is `collect()` dangerous? When is it fine?
4. What would you look at if the job were slow because of a join?
5. Job cluster vs all-purpose: how much cheaper and why?

---

## 6. Generic follow-ups for any story

- "What was the hardest part?" — have one technical and one people/process answer.
- "What did you learn?" — one sentence you'd apply next time.
- "What would you do differently?" — link to [12.8](08-what-i-would-improve.md).
- "How did you test it?" — unit tests on functions, parallel runs, DQ checks, UAT.
- "Who else was involved?" — roles, not names.

## 7. Self-check before the interview

- [ ] Pitch under 2 minutes, out loud, twice.
- [ ] Each story under 3 minutes with at least one number.
- [ ] I can draw the architecture in 90 seconds.
- [ ] Every "I" is something I personally did (see *Personal contributions* in the [README](README.md)).
- [ ] Nothing confidential: no internal names, paths, hostnames, people, or security findings.