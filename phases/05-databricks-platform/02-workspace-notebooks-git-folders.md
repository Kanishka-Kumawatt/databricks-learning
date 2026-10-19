# 5.2 Workspace, Notebooks, Folders, Git Folders, Permissions

**Time:** 15 min · **Prerequisites:** 5.1

**After this topic you can:**
- explain account vs workspace, and which things are managed at each level;
- describe the workspace file tree, notebooks, workspace files and Git folders;
- set permissions on workspace objects correctly (and know they are *not* data permissions);
- explain why production code should come from Git, not from someone's home folder.

---

## A. Concept deep dive

### 1. The simplest explanation

- An **account** is your organisation's top-level Databricks container: identities, workspaces, Unity Catalog metastores, billing.
- A **workspace** is one collaboration environment (one URL, e.g. `adb-1234567890123456.7.azuredatabricks.net`) where people write notebooks, run compute, define jobs and query data.
- Inside a workspace you organise **notebooks, files, queries, dashboards** in **folders**, and connect folders to Git with **Git folders**.

### 2. Account vs workspace — who manages what

| Level | Managed by | Examples |
|---|---|---|
| **Account** (Azure: account console at `accounts.azuredatabricks.net`) | Account admins | Users, groups, service principals (typically synced from Microsoft Entra ID), creating workspaces, **Unity Catalog metastores** and assigning them to workspaces, usage/billing, policies |
| **Workspace** | Workspace admins | Which identities can use this workspace, entitlements, compute policies, workspace settings, pools, SQL warehouses |
| **Object level** | Object owners / users with CAN MANAGE | Permissions on a notebook, folder, job, compute, warehouse, secret scope |
| **Data level (Unity Catalog)** | Metastore/catalog/schema owners, users with MANAGE | `GRANT SELECT ON TABLE …` — **separate** from workspace object permissions (5.8, 5.10) |

Why it matters: one Unity Catalog metastore can serve **several workspaces in the same region**, so dev/test/prod workspaces can share one governed data view (with different grants and catalog-to-workspace bindings).

### 3. What lives in the workspace tree

```
Workspace
├── Users/
│   └── asha@shopco.com/          ← your Home folder (private by default)
│       ├── scratch_notebook
│       └── .bundle/…             ← where `bundle deploy -t dev` puts files by default (5.11)
├── Shared/                       ← visible to all workspace users (avoid for prod code)
└── (Git folders can be created in Home or other folders, linked to Azure DevOps, GitHub, GitLab, …)
```

| Object | What it is |
|---|---|
| **Notebook** | Cells of Python, SQL, Scala or R + Markdown; magic commands `%sql`, `%python`, `%md`, `%run`, `%pip`. New notebooks are saved in `.ipynb` format by default; the "source" format (`# Databricks notebook source`, `# COMMAND ----------`) used by this repo's exercises is still importable |
| **Workspace files** | Plain `.py`, `.yml`, `.json`, `requirements.txt`, etc. — `.py` files are importable as Python modules |
| **Folder** | Groups objects; permissions set on a folder are inherited by its contents |
| **Git folder** (formerly **Repos**) | A folder linked to a remote Git repo: clone, branch, pull, commit, push, resolve conflicts from the UI |
| Queries, dashboards, alerts, jobs, pipelines | Other workspace objects with their own permissions |

Workspace objects are stored by the platform (control plane databases + workspace storage), **not** in your data lake. Tables are not files in the workspace tree.

### 4. Notebooks — what actually runs where

```
 Browser (notebook UI) ──▶ control plane ──▶ attached compute (driver)
                                               │ Python/SQL REPL, SparkSession `spark`
                                               └──▶ executors on workers (for Spark work)
```

- A notebook is just *code + output*. To run it, you **attach** it to compute (classic compute, serverless, or a SQL warehouse for SQL-only notebooks — 5.5).
- Variables live in the **REPL on the driver** of the attached compute. Detach/restart = state is gone.
- `%run ./other_notebook` executes another notebook **inline in the same session** (shares variables). `dbutils.notebook.run()` runs it as a separate ephemeral run (own scope, returns a string). Modern practice for shared code: put functions in `.py` workspace files and `import` them.

### 5. Permissions on workspace objects

| Object | Levels (lowest → highest) |
|---|---|
| Notebooks / folders / files | CAN READ → CAN RUN → CAN EDIT → CAN MANAGE |
| Compute | CAN ATTACH TO → CAN RESTART → CAN MANAGE (plus CAN VIEW in Beta) |
| Jobs | CAN VIEW → CAN MANAGE RUN → CAN MANAGE; IS OWNER |
| SQL warehouses | CAN VIEW → CAN USE → CAN MONITOR → CAN MANAGE; IS OWNER for creator |
| Secret scopes | READ → WRITE → MANAGE (5.10) |

**Entitlements** (workspace-level abilities granted to users/groups): *Workspace access*, *Databricks SQL access*, *Allow unrestricted cluster creation*, *Allow pool creation*.

**The crucial distinction:** CAN RUN on a notebook does **not** let you read the tables it queries. Data access is checked by Unity Catalog against *the identity running the code*. A user who can run a notebook but lacks `SELECT` on `shopco_prod.gold.revenue` gets a permission error.

### 6. Git folders — why and how

| Without Git | With Git folders |
|---|---|
| Code in `/Users/asha/…` — at risk when she leaves; no review | Code in a repo; branches, pull requests, history |
| Prod job points at a notebook someone can edit live | Prod job runs a version deployed by CI (bundle) |
| "Which version ran last night?" — unknown | Commit recorded |

Typical flow: create a Git folder in your Home folder → branch `feature/orders-dedup` → develop on dev compute → commit/push → pull request → CI validates and deploys a **bundle** to test/prod with a **service principal** (5.11).

### 7. Real example — ShopCo

- Account admins sync Entra ID groups `shopco-data-engineers`, `shopco-analysts` and create service principal `shopco-prod-sp`.
- Three workspaces: `shopco-dev`, `shopco-test`, `shopco-prod`, all attached to one regional UC metastore.
- In `shopco-prod`, engineers have **CAN VIEW** on jobs and **no edit** rights on deployed code (the bundle deploys to a folder only the service principal can edit). Analysts don't need notebooks at all — they use a SQL warehouse with `SELECT` on Gold.

### 8. Legacy vs current terminology

| Legacy | Current | Note |
|---|---|---|
| Repos | **Git folders** | Same idea; Git folders can be created in different places in the workspace tree |
| "Workspace" = only the notebook tree | Workspace = whole environment (compute, jobs, catalog UI, SQL) | |
| Users managed per workspace | **Account-level identities**, assigned to workspaces (identity federation) | |
| DBFS for code/libraries | Workspace files, Git folders, UC volumes | DBFS root/mounts are legacy |

### 9. Misconceptions

1. **"Notebook permissions control data access."** No — Unity Catalog grants do.
2. **"The workspace is where data is stored."** Tables live in cloud storage registered in Unity Catalog.
3. **"`%run` and `dbutils.notebook.run` are the same."** `%run` = inline, same session; `notebook.run` = separate run, separate state.
4. **"Shared folder is a fine place for production code."** Usually many people can edit it; use Git + deployment.

---

## B. Practical — workspace hygiene walkthrough

1. **Create a Git folder:** *Workspace → Home → Create → Git folder* → paste repo URL (Azure DevOps or GitHub) → **Create**. (Set a Git credential once under *Settings → Linked accounts*.)
2. **Branch:** click the branch-name button → *Create branch* `feature/phase5`.
3. **Add a notebook** in the Git folder, edit, then *Commit & push* with a message.
4. **Share a folder correctly:** folder ⋮ → *Share* → add **group** `shopco-data-engineers` with CAN EDIT, `shopco-analysts` with CAN READ. Use **groups**, not individuals.
5. **Test the permission boundary:** give a colleague CAN RUN on a notebook that reads a table they have no `SELECT` on → their run fails with a permission error. This proves object permissions ≠ data permissions.

Run in a notebook to see who you are and what the session uses:

```python
print(spark.sql("SELECT current_user() AS me, current_catalog() AS cat, current_schema() AS sch").first())
```

| Part | Explanation |
|---|---|
| `current_user()` | The identity Unity Catalog checks for data access (you interactively; the job's `run_as` identity in jobs) |
| `current_catalog()` / `current_schema()` | Defaults used when you write a one- or two-part table name |

**Expected:** something like `Row(me='asha@shopco.com', cat='…', sch='default')` — values depend on your workspace defaults.

### Common mistakes
- Granting permissions to individuals → unmaintainable when people move teams.
- Production jobs pointing to notebooks in personal folders.
- Using `%run` chains five levels deep instead of importable modules.
- Assuming a Git folder auto-syncs: you must **pull** to get others' changes.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is the difference between a Databricks account and a workspace?

<details><summary>Model answer</summary>

The account is the top-level organisational container: identities (users, groups, service principals), workspaces, Unity Catalog metastores and billing are managed there by account admins. A workspace is one collaboration environment with its own URL, where users create notebooks, compute, jobs and SQL warehouses; workspace admins manage its settings, entitlements and policies. One account has many workspaces; a metastore can be shared by workspaces in the same region.
</details>

**B2.** What are Git folders?

<details><summary>Model answer</summary>

Folders in the workspace linked to a remote Git repository (formerly called Repos). You can clone, create branches, pull, commit and push from the UI or API, so notebooks and files are version-controlled and can flow through pull requests and CI/CD.
</details>

**B3.** Name the permission levels on a notebook.

<details><summary>Model answer</summary>

CAN READ, CAN RUN, CAN EDIT, CAN MANAGE — usually granted to groups, often at folder level so they inherit.
</details>

### 🟡 Medium

**M1.** A user has CAN RUN on a notebook that reads `shopco_prod.gold.revenue`, but the run fails with a permission error. Why?

<details><summary>Model answer</summary>

Notebook permissions only control the workspace object. Data access is enforced by Unity Catalog against the identity executing the code. The user needs `USE CATALOG` on `shopco_prod`, `USE SCHEMA` on `gold` and `SELECT` on the table (or on a parent, inherited).
</details>

**M2.** `%run` vs `dbutils.notebook.run()` — when would you use each?

<details><summary>Model answer</summary>

`%run` includes another notebook inline in the current session — its functions and variables become available; good for small shared setup. `dbutils.notebook.run(path, timeout, args)` starts the target as a separate ephemeral run with its own state and returns a string; useful for simple dynamic orchestration, though Lakeflow Jobs tasks are preferred for production. For reusable code, prefer `.py` workspace files and imports.
</details>

**M3.** Why should production jobs not run notebooks from a user's Home folder?

<details><summary>Model answer</summary>

No version control or review, anyone with edit rights can change prod behaviour live, the code is tied to one person's account, and you can't tell which version ran. Production code should be deployed from Git (bundles via CI/CD) into a location editable only by a service principal.
</details>

### 🔴 Tricky

**T1.** Your company has dev and prod workspaces in the same region. Can a prod job read a table created in dev? What decides it?

<details><summary>Model answer</summary>

If both workspaces are attached to the same Unity Catalog metastore, the table can be visible from both — subject to (1) UC grants for the identity running the prod job and (2) **workspace-catalog bindings**, which can restrict a catalog (e.g. `shopco_dev`) to specific workspaces. Good practice: separate catalogs per environment and bind them, so prod can't accidentally read dev data.
</details>

**T2.** Someone left the company and their notebooks are no longer reachable. How do you avoid this situation?

<details><summary>Model answer</summary>

Content in a removed user's Home folder can usually be recovered by a workspace admin (exact behaviour: verify in your workspace), but relying on that is the real problem. Keep shared and production code in Git (Git folders for development, bundles deployed by CI for prod), make jobs owned/run by service principals, and use Home folders only for scratch work.
</details>

**T3.** Why do "groups, not individuals" matter more on Databricks than in a single database?

<details><summary>Model answer</summary>

Permissions exist at several layers — workspace objects, compute, jobs, warehouses, secret scopes and Unity Catalog data — across multiple workspaces. Granting to individuals multiplies entries across all layers; group-based grants synced from Entra ID mean joining/leaving a team updates every layer at once, reduces drift and makes audits possible.
</details>

---

## E. Revision summary

- **Account** = identities, workspaces, metastores, billing. **Workspace** = one environment (URL) with notebooks, compute, jobs, warehouses.
- Workspace tree: Home (`/Users/<email>`), Shared, Git folders (formerly Repos). Workspace objects ≠ data.
- Notebooks run on **attached compute**; state lives on the driver. `%run` inline vs `dbutils.notebook.run` separate.
- Object permissions (CAN READ/RUN/EDIT/MANAGE) are **separate from** Unity Catalog data grants.
- Production code = Git → CI/CD → deployed by a service principal; grant to **groups**.