# 10.2 Pipelines, Activities, Datasets, Linked Services, Integration Runtime

**Time:** 12 min · **Prerequisites:** 10.1

**After this topic you can:**
- explain the ADF building blocks and how they reference each other;
- choose the right Integration Runtime (Azure, Self-hosted, Azure-SSIS);
- read and write linked service and dataset JSON, including Key Vault secret references and managed identity;
- explain how ADF resources are stored in Git and deployed (CI/CD at concept level).

---

## A. Concept deep dive

### 1. The simplest explanation

| Building block | One-line meaning | Analogy |
|---|---|---|
| **Linked service** | *How to connect* to a store or compute (endpoint + authentication + which IR) | A saved connection string |
| **Dataset** | *Which data* inside that store (table, folder, file, format) | A named pointer to a table/folder |
| **Activity** | *One step* of work (copy, run notebook, look up, loop, wait…) | A task |
| **Pipeline** | A group of activities with dependencies, parameters and variables | A job / DAG |
| **Integration Runtime (IR)** | *Where* the step physically executes (compute + network location) | The worker that does the step |
| **Trigger** (10.4) | *When* a pipeline runs | The scheduler |

### 2. How they reference each other

```
 Trigger ──runs──▶ Pipeline ──contains──▶ Activity (Copy)
                                            │ inputs / outputs
                                            ▼
                                         Dataset  (e.g. DS_LandingParquet, folderPath param)
                                            │ linkedServiceName
                                            ▼
                                       Linked service (LS_ADLS_ShopCo: url + managed identity)
                                            │ connectVia
                                            ▼
                                   Integration Runtime (AutoResolve Azure IR / Self-hosted IR)

 Activity (DatabricksNotebook) ──linkedServiceName──▶ LS_Databricks (compute linked service) ─▶ IR dispatches
```

Not every activity needs a dataset: Databricks, Web and Stored Procedure activities point straight at a linked service (or URL); control activities (ForEach, If, Set Variable) need neither.

### 3. Activity categories

| Category | Examples | Who does the work |
|---|---|---|
| **Data movement** | Copy | The IR reads source and writes sink |
| **Data transformation** | Databricks Notebook / Jar / Python / Job, Stored Procedure, Data Flow, Azure Function, HDInsight | External compute; IR only dispatches (Data Flow runs on ADF-managed Spark) |
| **Control flow** | Lookup, Get Metadata, ForEach, If Condition, Switch, Until, Wait, Set/Append Variable, Execute Pipeline, Web, Webhook, Filter, Validation, Fail | The ADF service itself (Lookup/Get Metadata run on the IR of the data store) |

Soft limit: **120 activities per pipeline** (including inner activities of containers). Hitting it is a sign to split into child pipelines.

### 4. Integration Runtime types

| IR | What it is | Use it for | Notes |
|---|---|---|---|
| **Azure IR** | Fully managed serverless compute in Azure | Copy between cloud stores, Data Flows, dispatching activities to public endpoints | Default `AutoResolveIntegrationRuntime`. Copy power measured in **DIUs**. With **Managed Virtual Network** + managed private endpoints it can reach Private-Link-only stores |
| **Self-hosted IR (SHIR)** | Software you install on Windows machines inside your network (on-prem or VNet) | Sources/sinks with no public access (on-prem SQL Server, file shares, private SAP), bring-your-own drivers | Makes **only outbound** HTTP(S) connections; scale out to multiple nodes (active-active) for HA; you patch and monitor it; needs a JRE for some formats (e.g. Parquet) |
| **Azure-SSIS IR** | Managed cluster of VMs that runs SSIS packages | Lift-and-shift of existing SSIS packages | Not supported in Synapse; you pay while it's running — start/stop it |

**Copy IR selection rule:** if either the source or the sink linked service uses a self-hosted IR, the copy runs on the self-hosted IR. Lookup and Get Metadata run on the IR of the data store's linked service.

**Auto-resolve location:** for Copy, the Azure IR tries to run in the sink's region; for Lookup/Get Metadata and activity dispatch it uses the factory's region. If data must not leave a geography, create an Azure IR in a fixed region and reference it via `connectVia`.

### 5. Authentication options for linked services

| Option | How | Verdict |
|---|---|---|
| **System-assigned managed identity** | Created with the factory; grant it RBAC on the target (e.g. *Storage Blob Data Contributor* on ADLS, a contained DB user in Azure SQL) | ✅ Preferred for Azure services — no secrets |
| **User-assigned managed identity** | Separate Azure identity attached to the factory and referenced via a **credential** object | ✅ Good when identity should be managed as code, shared, or survive factory re-creation |
| **Service principal** | App registration; secret or certificate stored in Key Vault | ⚠️ When MI isn't supported for that connector, or cross-tenant |
| **Secret in Key Vault** (password, key, token) | Linked service field of type `AzureKeyVaultSecret` pointing to a Key Vault linked service | ✅ The pattern when a secret is unavoidable (on-prem SQL password, Databricks PAT) |
| **Inline secret in linked service** | `SecureString` typed into the linked service | ❌ Avoid — environment-specific, hard to rotate, and Git-integrated factories won't store some secrets in Git anyway |

Key Vault pattern: the factory's managed identity needs **Get/List** on secrets (access policy) or the **Key Vault Secrets User** role (RBAC). Omit `secretVersion` to always use the latest version — so rotation needs no redeploy.

### 6. Datasets: typed and parameterized

A **parameterized dataset** avoids one dataset per table/folder: `DS_LandingParquet` takes `folderPath` as a parameter, and the Copy activity passes `orders/run_id=<RunId>`. Linked services can be parameterized too (`@{linkedService().databaseName}`), useful for one SQL linked service across many databases.

### 7. Where resources live — Git and CI/CD (concept level)

```
 dev factory (Git-integrated)                       test / prod factories (no direct editing)
 ┌────────────────────────────┐   PR + merge    ┌──────────────┐   deploy ARM template   ┌─────────┐
 │ feature branch: pipeline/, │ ─────────────▶  │ main         │ ──────────────────────▶ │ test    │ ─▶ prod
 │ dataset/, linkedService/,  │                 │ (collab      │  (stop triggers before, │         │
 │ trigger/ JSON files        │                 │  branch)     │   start after)          └─────────┘
 └────────────────────────────┘                 └──────┬───────┘
                                                       │ ARM template generated by:
                     (a) "Publish" button in ADF Studio → writes to the adf_publish branch, or
                     (b) build pipeline with npm package @microsoft/azure-data-factory-utilities
                         npm run build validate <rootFolder> <factoryId>
                         npm run build export <rootFolder> <factoryId> [outputFolder]
```

- Each resource is a JSON file in the repo (`pipeline/PL_x.json`, `linkedService/LS_y.json`, …).
- Environment-specific values (URLs, Key Vault names, cluster IDs) are overridden with **ARM template parameters** during deployment; secrets stay in each environment's Key Vault.
- Triggers should be **stopped before** deployment and restarted after (the npm-generated artifact includes pre/post-deployment scripts for this).
- Integration runtimes must have the **same name and type** in every environment.
- Both the `adf_publish` flow and the npm "automated publish" flow are supported; the npm flow removes the manual Publish click.

### 8. ShopCo example

| Resource | Name | Key settings |
|---|---|---|
| Key Vault LS | `LS_KeyVault` | `baseUrl` of `kv-shopco-dev` |
| Azure SQL LS | `LS_AzureSql_ShopCo` | managed identity, Azure IR |
| On-prem SQL Server LS | `LS_SqlServer_Inventory` | `connectVia` → `SHIR-HeadOffice`, password from Key Vault |
| ADLS Gen2 LS | `LS_ADLS_ShopCo` | `AzureBlobFS`, url only → system-assigned managed identity |
| Datasets | `DS_AzureSql_Query`, `DS_LandingParquet(folderPath)` | parameterized |
| Databricks LS | `LS_Databricks_JobCluster` | MSI auth, new job cluster (10.7) |

### 9. Misconceptions

1. **"A dataset stores data."** It stores only a *pointer* (location, format, schema hints).
2. **"The self-hosted IR needs inbound firewall rules."** No — it makes outbound connections to ADF.
3. **"The Azure IR is a VM I size."** It's serverless; you set DIUs per Copy; activity dispatch needs no sizing.
4. **"Put the password in the linked service, it's encrypted."** It's still an environment-specific secret in the factory definition — use Key Vault or managed identity.
5. **"Publish = deploy to prod."** Publish updates the *dev* factory's live mode and generates ARM templates; promotion to test/prod is a separate CI/CD release.

---

## B. Practical — linked services and datasets JSON

**File:** [`exercises/phase-10/02_linked_services_and_datasets.json`](../../exercises/phase-10/02_linked_services_and_datasets.json) — a *study bundle* (one JSON file containing several resources, abbreviated). In a Git-integrated factory each resource is its own file. Later lessons reference these names.

Key parts, explained:

```json
{
  "name": "LS_SqlServer_Inventory",
  "properties": {
    "type": "SqlServer",
    "typeProperties": {
      "connectionString": "Data Source=sql-inv-01;Initial Catalog=Inventory;Integrated Security=False;User ID=adf_reader;",
      "password": {
        "type": "AzureKeyVaultSecret",
        "store": { "referenceName": "LS_KeyVault", "type": "LinkedServiceReference" },
        "secretName": "sql-inventory-adf-reader-password"
      }
    },
    "connectVia": { "referenceName": "SHIR-HeadOffice", "type": "IntegrationRuntimeReference" }
  }
}
```

| Line | Meaning |
|---|---|
| `"type": "SqlServer"` | Connector type (on-prem/VM SQL Server). Azure SQL is `AzureSqlDatabase` |
| `connectionString` | No password inside — only server, database, user. (Newer connector versions use separate `server`/`database`/`authenticationType` properties — verify in your factory) |
| `password` → `AzureKeyVaultSecret` | Fetched from Key Vault at run time with the factory's managed identity |
| `secretName`, no `secretVersion` | Always the latest secret version → rotation without redeploy |
| `connectVia` → `SHIR-HeadOffice` | Runs on the self-hosted IR inside the head-office network; any Copy using it runs on the SHIR |

```json
{
  "name": "DS_LandingParquet",
  "properties": {
    "type": "Parquet",
    "linkedServiceName": { "referenceName": "LS_ADLS_ShopCo", "type": "LinkedServiceReference" },
    "parameters": { "folderPath": { "type": "string" } },
    "typeProperties": {
      "location": {
        "type": "AzureBlobFSLocation",
        "fileSystem": "landing",
        "folderPath": { "value": "@dataset().folderPath", "type": "Expression" }
      },
      "compressionCodec": "snappy"
    }
  }
}
```

| Line | Meaning |
|---|---|
| `"type": "Parquet"` | Format dataset; the store-specific part is in `location` |
| `parameters.folderPath` | Caller supplies the folder → one dataset for every run/table |
| `AzureBlobFSLocation` | ADLS Gen2 location (`fileSystem` = container) |
| `@dataset().folderPath` | Expression reading the dataset's own parameter |

### Common mistakes

- ADLS managed identity has *Reader* (control plane) instead of **Storage Blob Data Contributor** (data plane) → `403 AuthorizationPermissionMismatch`.
- Key Vault behind a firewall without allowing trusted Microsoft services / private endpoint → "failed to get secret".
- SHIR installed on one VM with no second node → single point of failure for all on-prem loads.
- Different IR names in dev and prod → ARM deployment fails or references break.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is the difference between a linked service and a dataset?

<details><summary>Model answer</summary>

A linked service defines how to connect to a store or compute — endpoint, authentication, and which IR to use. A dataset defines which data inside that store — a table, folder or file and its format — and references the linked service. Many datasets can share one linked service.
</details>

**B2.** Name the three Integration Runtime types and when each is used.

<details><summary>Model answer</summary>

Azure IR — managed serverless compute for cloud-to-cloud copy, data flows and dispatching activities. Self-hosted IR — installed inside a private network to reach on-prem or private stores; outbound-only connectivity. Azure-SSIS IR — managed VMs to run existing SSIS packages.
</details>

**B3.** What are the three categories of ADF activities?

<details><summary>Model answer</summary>

Data movement (Copy), data transformation (Databricks, Stored Procedure, Data Flow, Azure Function, HDInsight…), and control flow (Lookup, ForEach, If, Until, Wait, Set Variable, Execute Pipeline, Web, Fail…).
</details>

### 🟡 Medium

**M1.** How should a linked service get the password for an on-prem SQL Server, and what permission is needed?

<details><summary>Model answer</summary>

Store the password in Azure Key Vault, create a Key Vault linked service, and set the password field as an `AzureKeyVaultSecret` reference (store + secretName, no version for automatic latest). Grant the factory's managed identity Get/List secret permissions (access policy) or the Key Vault Secrets User role. The SQL linked service runs via the self-hosted IR.
</details>

**M2.** A Copy activity reads from on-prem SQL Server (via SHIR) and writes to ADLS (Azure IR linked service). Which IR runs the copy, and what network path is needed?

<details><summary>Model answer</summary>

The self-hosted IR — if either side uses a SHIR, the copy runs there. The SHIR machine needs local network access to SQL Server and outbound HTTPS (443) to ADF and to the ADLS endpoint (or a staging store). No inbound ports are opened.
</details>

**M3.** Explain at a high level how ADF changes reach production.

<details><summary>Model answer</summary>

The dev factory is Git-integrated; developers work in feature branches and merge via PR to the collaboration branch. ARM templates are generated either by Publish (to `adf_publish`) or by a build pipeline using the `@microsoft/azure-data-factory-utilities` npm package (`validate` and `export`). A release pipeline deploys the ARM template to test/prod with environment-specific parameter overrides, stopping triggers before and starting them after. Secrets come from each environment's Key Vault.
</details>

### 🔴 Tricky

**T1.** Copy to ADLS works in dev but fails in prod with `403 AuthorizationPermissionMismatch`. The linked service JSON is identical. Why?

<details><summary>Model answer</summary>

The prod factory has a different system-assigned managed identity (each factory has its own), and that identity lacks the data-plane role (Storage Blob Data Contributor) on the prod storage account, or ACLs on the container/folder. Identical JSON doesn't mean identical permissions. Fix the RBAC/ACL (ideally in infrastructure-as-code); consider a user-assigned MI; also check storage firewall/private endpoint settings.
</details>

**T2.** Your single SHIR node is patched and rebooted every Tuesday at 03:00 and nightly loads fail. Fix the design.

<details><summary>Model answer</summary>

Add a second (or more) SHIR node — nodes run active-active, so one can reboot while the other serves jobs; stagger patching; add activity retries for transient failures; alert on SHIR node status with Azure Monitor. Make copies idempotent (run-specific landing folders + MERGE downstream) so retries are safe.
</details>

**T3.** Someone says "let's create one dataset per table — it's clearer." What's the downside, and what do you propose?

<details><summary>Model answer</summary>

Hundreds of near-identical JSON files to maintain and deploy, and every new table needs a code change. Propose parameterized datasets (table/folder as parameters) driven by a metadata/control table: a Lookup reads the table list, a ForEach passes each item to one generic Copy. New tables become a row in the control table, not a deployment.
</details>

---

## E. Revision summary

- **Linked service** = how to connect (endpoint + auth + IR). **Dataset** = which data. **Activity** = one step. **Pipeline** = group with dependencies. **IR** = where it runs. **Trigger** = when.
- IRs: **Azure** (serverless, DIUs, managed VNet option), **Self-hosted** (inside private network, outbound only, scale-out nodes), **Azure-SSIS** (SSIS packages).
- Copy runs on the SHIR if either side uses one.
- Auth: **managed identity** first; unavoidable secrets in **Key Vault** (`AzureKeyVaultSecret`, no version = latest); never inline.
- CI/CD: Git in dev → PR → ARM template (Publish/`adf_publish` or npm `azure-data-factory-utilities`) → deploy with parameters, stop/start triggers.