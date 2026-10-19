# 12.6 Security & CI/CD

**Time:** 15 min · **Prerequisites:** Phase 5 (secrets, UC), Phase 11 (CI/CD)

**After this topic you can:**
- explain Key Vault-backed secret scopes and why secrets are redacted in notebook output;
- explain moving ADF → Databricks authentication from a PAT to a **managed identity**, and what permission it needs;
- describe PGP encryption of outbound files at a concept level;
- describe the CI/CD flow: bundles with dev/prod targets, service-principal `run_as`, PR validation, code-quality
  gates, and ADF ARM-template export.

---

## A. Concept / story

### 1. Secrets

```
 Azure Key Vault  ◄── secret scope (Key Vault-backed) ◄── dbutils.secrets.get(scope, key)  in notebooks
       ▲                                                   (value shown as [REDACTED] in output)
 ADF linked services reference Key Vault secrets directly
```

- A **Key Vault-backed secret scope** is a read-only pointer to a vault; secrets are managed in Key Vault, rotated
  there, and Databricks ACLs on the scope control who can read.
- Redaction is a convenience, not a security boundary — anyone who can run code with the secret can exfiltrate it,
  so scope ACLs matter.
- The UC migration *removed* many secret reads: storage keys/SAS used for mounts were replaced by storage
  credentials (managed identity via an access connector) on external locations — no secret in the notebook at all.

### 2. PAT → managed identity for ADF → Databricks

| | Personal access token | Managed identity |
|---|---|---|
| Identity | A user (or SP) the token belongs to | The data factory's own Entra identity |
| Lifecycle | Expires; must be rotated and stored (Key Vault) | No secret to store or rotate |
| Failure mode | Pipeline breaks when the token expires or the user leaves | — |
| Setup | Token in linked service / Key Vault | Linked service auth = managed identity (system-assigned; user-assigned for the Job activity); grant the identity access to the workspace (commonly **Contributor** on the workspace resource) and the job/cluster permissions it needs |

### 3. PGP for outbound files (concept)

Partners received extracts encrypted with **their public key**; only their private key decrypts. Our private key
(for signing or for inbound files) lives in Key Vault. Flow: build file in a volume → encrypt (a Python PGP library
in the job, or a function) → move to outbound → transfer by ADF/SFTP → audit row. Encryption is a side effect —
make it idempotent (don't send twice on retry; record what was sent).

### 4. CI/CD

```
 feature branch ──PR──► validation pipeline:  lint / unit tests / code-quality gate (static analysis)
                                               databricks bundle validate -t dev
        │ merge
        ▼
 main ──► deploy pipeline:  bundle deploy -t dev  →  (approval) →  bundle deploy -t prod
                            jobs run_as a service principal in prod
 ADF:   author in dev factory (Git-integrated) → publish → ARM template → deploy to test/prod with parameter overrides
```

| Element | Purpose |
|---|---|
| **Bundles** (`databricks.yml`, targets `dev`/`prod`) | Jobs, pipelines and their compute as YAML in Git; `mode: development` prefixes resources per user, `mode: production` enforces checks |
| **`run_as` service principal** | Production runs don't depend on a person; permissions decoupled from the deployer |
| **PR validation** | Bundle validate, tests, a code-quality/static-analysis gate before merge |
| **Template for new projects** | A bundle template (`databricks bundle init <template>`) so new pipelines start with the same layout, tests and conventions |
| **ADF ARM export** | Publish generates ARM templates; environment-specific values (URLs, IR names) become parameters |

### 5. Misconceptions

1. **"Secrets are safe because output says REDACTED."** Code can still leak them.
2. **"Managed identity = no permissions needed."** It still needs explicit grants.
3. **"CI/CD = copying notebooks."** It's versioned, reviewed, tested, environment-parameterised deployment.

---

## B. Practical — a minimal bundle with dev/prod targets

```yaml
# databricks.yml (generic)
bundle:
  name: bookings_pipeline
include:
  - resources/*.yml
variables:
  catalog:
    default: dev_catalog
targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://<dev-workspace-url>
  prod:
    mode: production
    workspace:
      host: https://<prod-workspace-url>
    variables:
      catalog: prod_catalog
    run_as:
      service_principal_name: <prod-sp-application-id>
```

```python
# reading a secret (never print it)
api_key = dbutils.secrets.get(scope="<kv-backed-scope>", key="partner-api-key")
```

Commands: `databricks bundle validate -t dev` · `databricks bundle deploy -t dev` · `databricks bundle run -t dev bookings_daily`.

---

## C. Interview questions

### 🟢 Basic

**B1.** What is a Key Vault-backed secret scope?

<details><summary>Model answer</summary>

A Databricks secret scope that reads secrets from an Azure Key Vault instead of storing them in Databricks. Secrets
are managed and rotated in Key Vault; notebooks read them with `dbutils.secrets.get`, and output is redacted.
</details>

**B2.** Why run production jobs as a service principal?

<details><summary>Model answer</summary>

So production doesn't depend on a person's account (leaving, password changes, expired tokens), permissions can
be least-privilege for that workload, and the identity that deploys is decoupled from the one that runs.
</details>

**B3.** What's in a PR validation pipeline?

<details><summary>Model answer</summary>

Linting and formatting, unit tests for transformation functions, a static-analysis/code-quality gate, and
`databricks bundle validate` for the target, so broken YAML or code is caught before merge.
</details>

### 🟡 Medium

**M1.** Why move ADF → Databricks from a PAT to managed identity?

<details><summary>Model answer</summary>

PATs are long-lived secrets tied to an identity, must be rotated and stored, and break pipelines when they expire.
With the factory's managed identity there's no secret: ADF gets an Entra token for itself, and we grant that
identity access to the workspace and the jobs/clusters it uses.
</details>

**M2.** How do ADF changes reach production?

<details><summary>Model answer</summary>

Dev factory is Git-integrated; publishing generates ARM templates; a release pipeline deploys them to test/prod
with environment-specific parameter files (linked service URLs, IR names, Key Vault references). Triggers are
stopped before deployment and restarted after.
</details>

**M3.** How did UC reduce the number of secrets?

<details><summary>Model answer</summary>

Mounts needed storage keys/SAS or SP secrets. With UC, a storage credential backed by an access connector's
managed identity is attached to external locations, and users get grants on those — no storage secret appears in
notebooks or secret scopes.
</details>

### 🔴 Tricky

**T1.** A developer prints `dbutils.secrets.get(...)` and sees `[REDACTED]`. Is the secret safe?

<details><summary>Model answer</summary>

No. Redaction only matches the literal value in output; transforming it (reversing, splitting characters, writing
it to a table or file) exposes it. Control who can read the scope, and prefer identity-based auth so there's no
secret to leak.
</details>

**T2.** A bundle deploys to prod with `run_as` an SP, but the job fails with permission denied on a table. Why?

<details><summary>Model answer</summary>

The job now runs as the SP, not the developer — the SP needs `USE CATALOG`, `USE SCHEMA`, `SELECT`/`MODIFY` on the
tables, permissions on external locations/volumes, and secret scope read. Grant those (ideally via a group) as part
of environment setup.
</details>

**T3.** PGP-encrypted files are sent to a partner from a job that retries on failure. Risk?

<details><summary>Model answer</summary>

Duplicate delivery: the transfer is a side effect that isn't rolled back. Record each sent file (name, checksum,
run id) and check before sending; make transfer a separate task after the data write succeeds.
</details>

---

## E. Revision summary

- Secrets: **Key Vault-backed scopes**, `dbutils.secrets.get`, redaction ≠ security, scope ACLs.
- ADF → Databricks: **PAT → managed identity** (no secret; grant the identity workspace access).
- PGP: encrypt with partner's public key; keys in Key Vault; idempotent sending.
- CI/CD: PR validation (tests, quality gate, `bundle validate`) → `bundle deploy -t dev/prod`, prod `run_as` SP; ADF via ARM export + parameters.