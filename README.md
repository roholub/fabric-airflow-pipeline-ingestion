# Microsoft Fabric + Azure SQL Hyperscale
## Airflow + Pipeline Ingestion | Nightly Partition Rebuild Pattern

Hi there! 👋🏽 Have you ever had data that won't stop changing after it lands? This is a fully working reference implementation for ingesting large-scale,
time-partitioned data from Azure SQL Hyperscale into Microsoft Fabric
Lakehouse using Apache Airflow and Fabric Pipelines, built specifically
for data that keeps changing after it is first written
(data drift / data settling). 

This is all built around the idea of a hands-on POC, fast to run, but production notes are included.

> **This is Part 3 of a three-part repo series.**
> Each repo solves the same core problem in a different way.
> Read the full story below to help pick the right approach.

---

## The Three-Repo Story – Start Here

Before diving in, it helps to understand why this repo exists and how
it fits with the other two. Each one is a different tool for the same job.

---

### 📦 [fabric-hyperscale-copy-job](https://github.com/roholub/fabric-hyperscale-copy-job)
**The simplest option. Start here if you are new.**

This repo shows you how to move data from Azure SQL Hyperscale into
Microsoft Fabric Lakehouse using a **Fabric Copy Job**, no code,
no orchestration, just a few clicks.

What it is great for:
- One-time full historical loads (all your data in one shot)
- Simple incremental loads where new rows are only ever added
- Teams who want a click-based solution with zero infrastructure

What it cannot do:
- Handle **data drift**. If a row from 30 days ago gets updated in
	SQL Hyperscale, the Copy Job does not pick it up because watermark logic
	only looks forward, not backward.

> I love using analogies! Think of data drift as a **moving truck**. Great for the big move,
> but it does not come back every night to rearrange the furniture.

---

### 📓 [fabric-airflow-notebook-ingestion](https://github.com/roholub/fabric-airflow-notebook-ingestion)
**The orchestrated option. Great if you need full control.**

This repo shows you how to use **Apache Airflow + PySpark Notebooks**
to rebuild the last 60 days of data every night. Airflow loops over
60 partition dates and triggers one notebook per date. Each notebook
reads that date from SQL Hyperscale and atomically replaces it in the
Fabric Lakehouse using Delta replaceWhere.

What it is great for:
- Full control over the rebuild process
- Teams comfortable with Python and Spark
- Use cases where you need data transformation during ingestion
	(clean, enrich, reshape while moving)

What to keep in mind:
- Each notebook triggers a Spark session
- 60 dates = 60 Spark sessions per nightly rebuild
- More powerful than needed for pure data copy use cases

> Here is another analogy💡: Think of this airflow solution as **hiring 60 specialized workers**. Each is excellent, but you are paying for 60 crews when one conveyor belt may be enough.

---

### ⚡ [fabric-airflow-pipeline-ingestion](https://github.com/roholub/fabric-airflow-pipeline-ingestion) – YOU ARE HERE
**The efficient option. Airflow orchestration without Spark overhead.**

This repo shows you how to use **Apache Airflow + Fabric Pipelines**
to do the same nightly rebuild, but using a serverless Copy Data
activity instead of Spark notebooks. Airflow loops over 60 dates and
triggers one Fabric Pipeline per date. The pipeline reads that date
from SQL Hyperscale and overwrites it in the Lakehouse. No Spark.
No notebooks. Fully serverless.

What makes this one different:
- Same Airflow orchestration power as fabric-airflow-notebook-ingestion
- Zero Spark sessions. Microsoft manages compute automatically
- SQL-native. SQL teams can open the pipeline UI and understand it fast
- Handles data drift cleanly, same outcome as notebook approach
- Parallelism friendly, multiple partitions can run simultaneously

> Ready for yet another analogy? 😁Think of this solution as **a smart conveyor belt with traffic control**.
> Airflow controls what runs and when, pipeline moves data automatically,
> and no heavy machinery is needed.

---

## The Core Problem – Data Drift and Settling

Most pipelines assume this:

> A record written today will not change tomorrow.

In many real systems, that is wrong.

A shipment recorded today may not become final for 60 days as downstream
events complete. Carrier confirmations, weight adjustments, invoice
settlements, returns, and corrections modify the same row long after
initial insert.

This is **data drift** or **data settling**.

```
THE DATA SETTLING LIFECYCLE
-------------------------------------------------------------

Day 0   Shipment recorded
Status: Pending  | Version: 1 | Still changing

Day 7   Carrier confirms pickup
Status: Settling | Version: 2 | Still changing

Day 30  Delivery confirmed, weight re-measured
Status: Settling | Version: 3 | Still changing

Day 60+ Invoice finalized, all adjustments complete
Status: Settled  | Version: 4 | FROZEN FOREVER

-------------------------------------------------------------
Rule: Records 0-59 days old  -> still changing -> rebuild tonight
      Records 60+ days old   -> frozen forever -> never touch again
-------------------------------------------------------------
```

### Why Simple Strategies Fail

| Strategy | What happens |
|---|---|
| **Append only** | Old rows stay beside new rows, duplicates everywhere |
| **Watermark** | Only catches new rows, misses updates to existing rows |
| **Full reload** | Too slow at 200M+ rows for nightly use |
| **Delete + append** | Two operations, risk if one fails mid-process |
| **Atomic partition replace** | ✅ Delete and rewrite are one atomic operation |

This repo uses **atomic partition replace**, the cleanest strategy for
this pattern.

---

## How This Repo Works

```
EVERY NIGHT - THE TWO-PHASE STORY
==============================================================

PHASE 1 - One Time Only
(fabric-hyperscale-copy-job)

Azure SQL Hyperscale                    Fabric Lakehouse
+----------------------+                +---------------------+
| dbo.fact_shipments   |                | fact_shipments      |
| ~200M rows           | --- Copy Job ->| Delta table         |
| All history          |  (full load)   | Bronze layer        |
+----------------------+                +---------------------+

Done once. Gets all history into Fabric.

PHASE 2 - Every Night
(THIS REPO - fabric-airflow-pipeline-ingestion)

Apache Airflow (Fabric)
+-----------------------------------------------------------+
| DAG: fabric_shipment_pipeline_rebuild                    |
|                                                          |
| Loops over last 60 dates                                 |
| For each date calls Fabric REST API with rebuild_date    |
+----------------------+-----------------------------------+
                       |
                       v
Fabric Pipeline (one run per date)
+-----------------------------------------------------------+
| pipeline_shipment_partition_rebuild                      |
| Parameter: rebuild_date = '2026-04-11'                   |
| Copy Data Activity                                       |
| SELECT * FROM dbo.fact_shipments WHERE shipment_date = x |
| Source: SQL Hyperscale  ->  Sink: Fabric Lakehouse       |
| Write mode: overwrite partition                          |
+-----------------------------------------------------------+

No Spark. No notebooks.
Microsoft manages all compute.

==============================================================
```

---

## Who Does What

Here is how every component works:

- **Fabric Copy Job (Standalone) = The Bootstrap Loader**
  Runs the one-time historical load from SQL Hyperscale into the Lakehouse.
  It is great for initial bulk ingest, but it does not orchestrate nightly partition rebuilds.

- **Airflow DAG = The Manager / Scheduler**
  Handles orchestration and timing only. It decides which dates run and when,
  then triggers the pipeline. It never reads, writes, or deletes business data.

- **Fabric Pipeline = The Worker**
  Executes the Copy Data task for one partition date at a time using overwrite.
  Overwrite here is atomic: delete + write happen as one operation, never partial.

- **notebook_validate.py = The Auditor**
  Read-only verification step after ingestion. It compares source and sink counts
  and reports pass/fail. It never writes or deletes anything.

Atomic overwrite:
If a write fails midway, the old partition data is still there.
You never end up with a half-written table or zero rows for that partition.

---

## How the Three Repos Work Together

```
+---------------------------------------------------------------+
| AZURE SQL HYPERSCALE (SOURCE)                                |
| dbo.fact_shipments (~200M rows)                              |
| Last 60 days changing, 60+ days frozen                       |
+-------------------------------+-------------------------------+
                                |
              +-----------------+-----------------+
              |                 |                 |
              v                 v                 v
        Option A           Option B          Option C
        Copy Job           Airflow+Notebook  Airflow+Pipeline
        simplest           max control       serverless orchestration

                                v
+---------------------------------------------------------------+
| FABRIC LAKEHOUSE (SINK)                                       |
| fact_shipments Delta table, Bronze layer, always current      |
+---------------------------------------------------------------+
```

## Repository Contents

| File | Purpose | Where it runs |
|---|---|---|
| synthetic_data_generation.sql | Builds/populates dbo.fact_shipments with drift simulation | Azure SQL Hyperscale |
| airflow_dag.py | Nightly DAG triggering one Fabric Pipeline per date | Microsoft Fabric Airflow |
| fabric_pipeline_definition.json | Clean pipeline definition (strict JSON) | Microsoft Fabric Pipeline |
| fabric_pipeline_definition.jsonc | Same definition with extensive inline comments | Reference / learning |
| notebook_validate.py | Validation notebook comparing source vs sink counts | Microsoft Fabric Notebook |
| grant_spn_access.sql | Grants SPN read access to SQL Hyperscale | Azure SQL Hyperscale |
| README.md | This document | Anywhere |

---

## Prerequisites

### Azure
- Azure subscription with SQL Hyperscale access
- Microsoft Entra ID admin access (to create SPN)
- Azure SQL Hyperscale database

### Microsoft Fabric
- Fabric workspace
- Fabric capacity (any SKU)
- Fabric Lakehouse in workspace
- Apache Airflow Job item

### Local Tools
- https://code.visualstudio.com
- https://marketplace.visualstudio.com/items?itemName=ms-mssql.mssql
- Azure Active Directory - Universal with MFA (for SQL connections)

### If You Already Ran the Other Repos
You can reuse:
- Existing SQL Hyperscale database
- Existing dbo.fact_shipments table
- Existing Fabric workspace/Lakehouse
- Existing SPN

No need to regenerate data or rebuild infra.

---

## Quick Start – (Order Matters)

Run setup in this exact sequence:
1. SQL data generation
2. SPN access grant
3. Build Pipeline
4. Test Pipeline with Debug (standalone)
5. Deploy Airflow DAG
6. Trigger DAG
7. Run validation notebook

Do not skip steps. The flow is dependency-based, and skipping earlier steps
causes downstream failures (auth errors, missing parameter errors, or empty loads).

---

## Step-by-Step Setup Guide

### Step 1 – Set Up SQL Hyperscale (skip if already done)
If starting fresh:
1. Connect VS Code to SQL Hyperscale
2. Open synthetic_data_generation.sql
3. Run sections in order
4. Wait for completion (about 2-3 hours full run)
5. Validate row counts

---

### Step 2 – Grant Service Principal Access
1. Open grant_spn_access.sql
2. Update @spn_name
3. Run sections in order
4. Confirm user_type = EXTERNAL_USER and role_name = db_datareader

---

### Step 3 – Create the Fabric Pipeline

In Fabric UI:
1. Open workspace and create Data Pipeline
2. Name it pipeline_shipment_partition_rebuild
3. Add Copy data activity
4. Add pipeline parameter rebuild_date (String)
5. Configure source: Azure SQL Database + SPN auth
6. Use query filtered by @{pipeline().parameters.rebuild_date}
7. Configure sink: Lakehouse table fact_shipments
8. Set write behavior: Overwrite
9. Save

⚠️ Important:
The expression @{pipeline().parameters.rebuild_date} cannot be validated
until the rebuild_date parameter already exists in the pipeline.

See fabric_pipeline_definition.json for strict JSON.
See fabric_pipeline_definition.jsonc for commented learning version.

---

### Step 4 – Test Pipeline Manually
1. Open pipeline in Fabric UI
2. Click Debug
3. Pass a recent rebuild_date
4. Run and confirm activity succeeds
5. Verify table rows in Lakehouse

---

### Step 5 – Deploy the Airflow DAG
In Fabric Airflow:
1. Create new DAG file airflow_dag.py
2. Update FABRIC_WORKSPACE_ID and FABRIC_PIPELINE_ID
3. Paste DAG content and save immediately
4. Confirm DAG appears in Airflow UI

How to find IDs:
- Workspace ID from URL groups/{id}
- Pipeline ID from URL pipelines/{id}

Starter Pool note:
Fabric Airflow Starter Pool is always on and managed by Microsoft.
No manual cluster wake-up is needed before runs.

---

### Step 6 - Trigger DAG Manually (POC)
1. Open Monitor Airflow
2. Find fabric_shipment_pipeline_rebuild
3. Trigger DAG
4. Watch graph view
5. Confirm partition tasks go green

⚠️ Stop-run warning:
To stop a running DAG, use **Mark as Failed**.
Do **not** use the Delete button.
Delete removes DAG registration and you must re-save airflow_dag.py to restore it.

---

### Step 7 - Validate Results
1. Open notebook_validate.py in Fabric Notebook
2. Attach Lakehouse
3. Fill config values in cell 1
4. Run all
5. Review summary

Expected:
- All dates PASS
- row_count_hyperscale equals row_count_lakehouse for each date

---

### Step 8 - Enable Nightly Automation
In airflow_dag.py:

```python
SCHEDULE = None
```

Change to:

```python
SCHEDULE = '0 3 * * *'
```

Then toggle the DAG ON in Airflow UI.
With schedule enabled, Airflow runs nightly with zero human intervention.

---

## Architecture Decision Guide

```
START
  |
  +-- First load into Fabric?
  |   +-- Yes -> use fabric-hyperscale-copy-job
  |   +-- No
  |
  +-- Do rows change after initial write?
      +-- No  -> fabric-hyperscale-copy-job
      +-- Yes
          +-- Team needs Spark transforms?
              +-- Yes -> fabric-airflow-notebook-ingestion
              +-- No  -> fabric-airflow-pipeline-ingestion (this repo)
```

---

## How the DAG Handles Failures

If one partition fails, Airflow retries that partition only.
Other partitions continue independently.

This reduces blast radius and keeps nightly rebuild resilient.

---

## Setting Up Alerts

Monitoring is split across two UIs. Understanding the difference prevents missed alerts.

**Important:** Fabric Monitor shows pipeline activity runs only.
It does NOT track Airflow DAG health. The DAG is monitored separately in the Airflow UI.

### Alert Options for Fabric Pipeline

**Option A – Activity-Level Alert (Recommended for POC)**

Add an Outlook or Teams activity directly to the pipeline canvas.
1. Open pipeline in Fabric UI
2. Add a new activity: Outlook or Teams notification
3. Connect it to CopyShipmentPartition with an **On Failure** dependency
4. Configure recipient(s) and failure message template

This fires instantly when the Copy Data activity fails, regardless
of whether Airflow triggered it, you ran it manually, or it ran on schedule.

**Option B – Schedule-Based Notifications**

Available in pipeline Schedule settings, but has a constraint:
1. Open pipeline, go to Schedule tab
2. Configure notifications under notification settings
3. Set email/Teams recipients

⚠️ Note: This only works for scheduled pipeline runs.
Airflow-triggered runs bypass the schedule, so these notifications will not fire.

**Option C – Fabric Activator (Production Scale)**

For enterprise monitoring across many pipelines:
1. Create a new Activator item in Fabric workspace
2. Configure Job Events trigger
3. Select pipeline + Failed run event
4. Configure action: send email or Teams notification

This centralizes monitoring for all pipelines in your workspace.

### Alert Options for Airflow DAG

Airflow DAG failures are monitored separately via the Apache Airflow callback mechanism.

The DAG sends nothing to Fabric Monitor (Fabric only sees pipeline activity).
Instead, use Airflow's **on_failure_callback** in airflow_dag.py:

```python
def alert_on_failure(context):
    # Custom notification logic here
    # Can post to Teams, email, or custom webhook
    pass

default_args = {
    'on_failure_callback': alert_on_failure,
    ...
}
```

This is optional for POC but recommended for production.
See [Apache Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#error-handling)
for full callback configuration and examples.

---

## Enabling Production Scheduling

| Setting | POC Value | Production Value |
|---|---|---|
| SCHEDULE | None | '0 3 * * *' |
| MAX_PARALLEL_TASKS | 5 | 10-20 |
| SPN credentials | Inline in notebook | Azure Key Vault |
| Airflow IDs | Hardcoded | Airflow Variables |
| DAG toggle | OFF | ON |

---

## Related Repositories

| Repository | What it does | When to use |
|---|---|---|
| [fabric-hyperscale-copy-job](https://github.com/roholub/fabric-hyperscale-copy-job) | Full load + simple incremental via Copy Job | First step, no code |
| [fabric-airflow-notebook-ingestion](https://github.com/roholub/fabric-airflow-notebook-ingestion) | Nightly rebuild via Airflow + PySpark notebooks | When you need Spark transforms |
| fabric-airflow-pipeline-ingestion | Nightly rebuild via Airflow + Fabric pipelines | Airflow orchestration without Spark |

---

## Helper Scripts

### scripts/export-jsonc.ps1

PowerShell utility script that strips `//` comment lines from
fabric_pipeline_definition.jsonc and generates a clean
fabric_pipeline_definition.json file.

Use it when:
- You update fabric_pipeline_definition.jsonc (the commented version)
- You want to regenerate fabric_pipeline_definition.json automatically
- You want to avoid manually editing both files

How to run it:
From the repo root in a PowerShell terminal:

```powershell
.\scripts\export-jsonc.ps1
```

The script validates output as strict JSON before writing the final file,
so you get immediate feedback if a comment change breaks JSON structure.

---

## Disclaimer

This repository is a personal project and is not affiliated with,
endorsed by, or associated with Microsoft Corporation or any of its
subsidiaries. All views and code expressed here are my own.

---

*Author: Rocio Holub - [github.com/roholub](https://github.com/roholub/)*
