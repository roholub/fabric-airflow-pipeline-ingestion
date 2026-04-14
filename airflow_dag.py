# =============================================================================
# FABRIC + SQL HYPERSCALE | AIRFLOW + PIPELINE INGESTION
# File: airflow_dag.py
# =============================================================================
#
# WHAT THIS FILE IS:
#   This is an Apache Airflow DAG (workflow definition file).
#   It is the ORCHESTRATOR in the nightly rebuild pipeline.
#   Think of it as the manager - it decides WHEN and WHAT to run,
#   while a Fabric Pipeline (Copy Data activity) does the actual
#   data movement (the worker). No Spark. No notebooks. Fully serverless.
#
#
# WHAT IS APACHE AIRFLOW?
#   Apache Airflow is an open-source platform for scheduling and
#   monitoring workflows. You define your workflow as a Python file
#   (called a DAG), and Airflow runs it on a schedule, handles retries
#   if something fails, and gives you a visual dashboard to monitor it.
#   In this repo we use Airflow hosted inside Microsoft Fabric.
#
# WHAT IS A DAG?
#   DAG stands for Directed Acyclic Graph.
#   Think of it as a flowchart:
#     - Each box is a TASK
#     - Arrows between boxes define the ORDER tasks run in
#     - Acyclic means no loops - it always flows forward
#
#   This DAG has 3 stages:
#     Stage 1: get_dates_to_rebuild -> figures out which 60 dates to rebuild
#     Stage 2: rebuild_partition_*  -> triggers a Fabric Pipeline per date
#     Stage 3: validate_rebuild     -> logs a final completion summary
#
# WHAT DOES THE FABRIC PIPELINE DO?
#   The Fabric Pipeline uses a Copy Data activity to:
#     1. Connect to Azure SQL Hyperscale (source) - no Spark, no JDBC driver
#     2. Run a parameterized SQL query: WHERE shipment_date = @rebuild_date
#     3. Write the results directly into Fabric Lakehouse Bronze layer
#        using Delta Lake replaceWhere (atomic partition replacement)
#   
#   The pipeline is fully serverless - Microsoft manages all compute.
#   A SQL team can open the pipeline in the Fabric UI and read every step.
#
# WHY PIPELINES INSTEAD OF NOTEBOOKS?
#   Notebooks:           60 dates x 1 Spark session each = 60 Spark startups
#                        Each startup: 2-5 min warmup + Fabric capacity cost
#                        Total: potentially 5-8 hours
#
#   Pipelines (THIS):    60 dates x 1 serverless pipeline each = 0 Spark
#                        No warmup, SQL-native connectors
#                        Total: ~1-3 hours
#
# HOW IT TRIGGERS THE PIPELINE:
#   Airflow calls the Fabric REST API once per date:
#     POST https://api.fabric.microsoft.com/v1
#          /workspaces/{workspace_id}
#          /items/{pipeline_id}/jobs/instances?jobType=Pipeline
#   Fabric runs the Copy Data pipeline and passes the date as a parameter.
#   The pipeline reads: WHERE shipment_date = @rebuild_date
#
# HOW TO DEPLOY TO FABRIC AIRFLOW:
#   1. Go to your Fabric workspace
#   2. Open or create an Apache Airflow Job item
#   3. In the left panel click the dags folder
#   4. Click New DAG file and name it airflow_dag.py
#   5. Paste this entire file into the editor
#   6. Click Save immediately (do not switch tabs before saving)
#      WARNING: switching tabs before saving causes the editor to go
#      blank and lose all content. Save first, explore later.
#   7. The DAG appears in the Airflow UI within 30 seconds
#
# HOW TO TRIGGER MANUALLY (recommended for POC):
#   1. In Fabric Airflow editor click Monitor Airflow (top toolbar)
#   2. Find fabric_shipment_pipeline_rebuild in the DAG list
#   3. Click the Play button on the right side of the row
#   4. Watch tasks run in real time in the Graph view
#
# HOW TO ENABLE NIGHTLY AUTOMATION (production):
#   1. Change SCHEDULE = None to SCHEDULE = '0 3 * * *' below
#      (3am UTC recommended - after nightly Hyperscale rebuild)
#   2. Save the file
#   3. In the Airflow UI toggle the DAG to ON (blue toggle)
#   4. The DAG will now run automatically every night at 3am UTC
#
#
# PREREQUISITES:
#   - This file uploaded to Fabric Airflow dags folder
#   - Fabric Pipeline created in your workspace
#     (see fabric_pipeline_definition.json for the pipeline definition)
#   - FABRIC_WORKSPACE_ID and FABRIC_PIPELINE_ID updated below
#   - Fabric Airflow cluster started
#   - SPN has db_datareader access on SQL Hyperscale
#     (run grant_spn_access.sql first)
#   - SPN has Contributor access on Fabric workspace
#
# DISCLAIMER:
#   This script is a personal project and is not affiliated with,
#   endorsed by, or associated with Microsoft Corporation or any of
#   its subsidiaries. All views and code expressed here are my own.
#
# AUTHOR: Rocio Holub (https://github.com/roholub)
# =============================================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import requests


# =============================================================================
# CONFIGURATION
# =============================================================================
# Update FABRIC_WORKSPACE_ID and FABRIC_PIPELINE_ID before deploying.
#
# HOW TO FIND YOUR WORKSPACE ID:
#   Go to app.fabric.microsoft.com and open your workspace.
#   Look at the browser URL:
#   .../groups/YOUR-WORKSPACE-ID-HERE/...
#   Copy the long ID between /groups/ and the next /
#
# HOW TO FIND YOUR PIPELINE ID:
#   Open your Fabric Pipeline in Fabric.
#   Look at the browser URL:
#   .../pipelines/YOUR-PIPELINE-ID-HERE/...
#   Copy the long ID between /pipelines/ and the next /
#
# WHAT IS SCHEDULE?
#   None          = manual trigger only (recommended for POC)
#   '0 3 * * *'   = run at 3am UTC every night (production)
#
#   Why 3am? Run AFTER nightly Hyperscale rebuild completes
#   so Airflow picks up fully settled data, not mid-rebuild snapshots.
#
#   Cron syntax: '0 3 * * *'
#     0 = minute 0
#     3 = hour 3 (3am UTC)
#     * = every day of month
#     * = every month
#     * = every day of week
#   Read as: run at 3am UTC every single day
#
# WHAT IS MAX_PARALLEL_TASKS?
#   How many pipeline runs to trigger simultaneously.
#   5 means up to 5 Fabric Pipeline runs in parallel at any moment.
#   Unlike notebooks, pipelines are serverless so parallelism is cheap.
#   Safe range for POC: 5-10. Production: up to 20.
#   Higher = faster total runtime, minimal extra cost.
#
# OPTIONAL UPGRADE (production): Use Airflow Variables instead of
# hardcoded IDs for better security and easier config management:
#
#   from airflow.models import Variable
#   FABRIC_WORKSPACE_ID = Variable.get('fabric_workspace_id')
#   FABRIC_PIPELINE_ID  = Variable.get('fabric_pipeline_id')
#
#   To set Variables: Airflow UI -> Admin -> Variables -> Add
#   This keeps sensitive IDs out of your code entirely.
# =============================================================================

DAG_ID              = 'fabric_shipment_pipeline_rebuild'

# !! SCHEDULE IS INTENTIONALLY None FOR POC - CHANGE FOR PRODUCTION !!
# To enable nightly runs change None to '0 3 * * *'
SCHEDULE            = None

LOOKBACK_DAYS       = 60
MAX_PARALLEL_TASKS  = 5

# !! UPDATE THESE WITH YOUR REAL VALUES !!
FABRIC_WORKSPACE_ID = 'enter-your-fabric-workspace-id-here'
FABRIC_PIPELINE_ID  = 'enter-your-fabric-pipeline-id-here'
FABRIC_API_BASE     = 'https://api.fabric.microsoft.com/v1'


# =============================================================================
# DEFAULT ARGUMENTS
# =============================================================================
# These settings apply to every task in the DAG unless overridden.
#
# WHAT IS retries?
#   If a task fails Airflow will try again this many times.
#   2 retries means 3 total attempts (1 original + 2 retries).
#
# WHAT IS retry_delay?
#   How long to wait between retry attempts.
#   timedelta(minutes=5) means wait 5 minutes before retrying.
#   This gives Fabric time to recover from transient errors.
#
# WHAT IS email_on_failure?
#   Whether to send an email when a task fails.
#   Set to False here - configure email alerts in Airflow settings
#   if you want failure notifications.
# =============================================================================

default_args = {
    'owner'           : 'fabric-ingestion',
    'retries'         : 2,
    'retry_delay'     : timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry'  : False,
}


# =============================================================================
# TASK 1: get_dates_to_rebuild
# =============================================================================
# Generates the list of dates to rebuild and pushes to XCom for logging.
#
# WHAT IS LOGICAL DATE vs datetime.today()?
#   datetime.today() returns the current clock time on the worker.
#   This works for normal runs but breaks for backfills.
#
#   data_interval_end is Airflow's logical date for this run.
#   It respects the scheduled date so backfills work correctly.
#   We use data_interval_end with a fallback to datetime.today()
#   for manual or ad-hoc triggers where logical date may not be set.
#
# WHAT IS XCom?
#   XCom (Cross-Communication) is Airflow's way of passing small
#   amounts of data between tasks. xcom_push() saves a value that
#   other tasks can read. ti = Task Instance.
#
# NOTE ON STATIC vs DYNAMIC TASK CREATION:
#   The 60 individual rebuild tasks are created STATICALLY at DAG
#   parse time from dates_for_tasks (not from this function output).
#   This is intentional - static tasks show all 60 tasks in the
#   Airflow graph view immediately, which is beginner-friendly.
#
#   Advanced users: Airflow 2.3+ supports dynamic task mapping via
#   .expand() which creates tasks from Task 1 output at runtime.
#   See Airflow docs: authoring-and-scheduling/dynamic-task-mapping
# =============================================================================

def get_dates_to_rebuild(**context):
    try:
        from airflow.operators.python import get_current_context
        ctx          = get_current_context()
        logical_date = ctx['data_interval_end'].date()
        yesterday    = logical_date - timedelta(days=1)
        logging.info(f'Using Airflow logical date: {logical_date}')
    except Exception:
        yesterday = datetime.today().date() - timedelta(days=1)
        logging.info(f'Falling back to datetime.today(): {yesterday}')

    dates = [
        (yesterday - timedelta(days=i)).strftime('%Y-%m-%d')
        for i in range(LOOKBACK_DAYS)
    ]

    logging.info('=' * 60)
    logging.info(f'Nightly pipeline rebuild starting for {len(dates)} dates')
    logging.info(f'Date range: {dates[-1]} to {dates[0]}')
    logging.info(f'Engine: Fabric Pipeline (serverless, no Spark)')
    logging.info('=' * 60)

    context['ti'].xcom_push(key='dates_to_rebuild', value=dates)
    return dates


# =============================================================================
# TASK 2: rebuild_partition
# =============================================================================
# Triggers a Fabric Pipeline for one specific date via the Fabric REST API.
# The pipeline uses a Copy Data activity to re-copy that date's partition
# from SQL Hyperscale into Fabric Lakehouse Bronze. No Spark. Serverless.
#
# This function runs once per date - 60 times per DAG run,
# up to MAX_PARALLEL_TASKS running simultaneously.
#
# HOW IS THIS DIFFERENT FROM REPO 2 (NOTEBOOKS)?
#   Repo 2: POST .../items/{notebook_id}/jobs/instances?jobType=RunNotebook
#            -> Fabric spins up a Spark session, runs PySpark code
#            
#
#   This repo: POST .../items/{pipeline_id}/jobs/instances?jobType=Pipeline
#            -> Fabric runs a Copy Data activity (serverless, no Spark)
#            
#
# WHAT DOES HTTP 202 MEAN?
#   202 Accepted means Fabric received the request and queued the
#   pipeline. It does NOT mean the pipeline finished successfully.
#   The pipeline runs asynchronously in Fabric after this returns.
#   notebook_validate.py (run separately) catches pipeline failures.
#
# OPTIONAL UPGRADE (production): Poll for pipeline completion
#   Currently fire-and-forget (trigger + 202 = task success).
#   For production poll until pipeline completes:
#   1. Extract job instance ID from 202 response Location header
#   2. Loop: GET /workspaces/{id}/items/{id}/jobs/instances/{job_id}
#   3. Check status: Completed / Failed / Cancelled / Running
#   4. Sleep 30 seconds between polls
#   5. Raise exception if Failed or Cancelled
#   6. Return success when Completed
# =============================================================================

def rebuild_partition(date_str, **context):
    logging.info(f'Triggering Fabric Pipeline for partition date: {date_str}')

    # Key difference from Repo 2:
    # Repo 2: .../items/{notebook_id}/jobs/instances?jobType=RunNotebook
    # This:   .../items/{pipeline_id}/jobs/instances?jobType=Pipeline
    api_url = (
        f'{FABRIC_API_BASE}'
        f'/workspaces/{FABRIC_WORKSPACE_ID}'
        f'/items/{FABRIC_PIPELINE_ID}'
        f'/jobs/instances?jobType=Pipeline'
    )

    # Pass partition date to the pipeline as a parameter.
    # The Fabric Pipeline reads this as:
    #   @pipeline().parameters.rebuild_date
    # Used in the Copy Data source query:
    #   SELECT * FROM dbo.fact_shipments
    #   WHERE shipment_date = '@{pipeline().parameters.rebuild_date}'
    payload = {
        'executionData': {
            'parameters': {
                'rebuild_date': {
                    'value': date_str,
                    'type' : 'string'
                }
            }
        }
    }

    try:
        logging.info(f'Calling Fabric Pipeline API for date: {date_str}')

        response = requests.post(
            url=api_url,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=300
        )

        if response.status_code in [200, 202]:
            logging.info(f'SUCCESS: Pipeline triggered for {date_str}')
            logging.info(f'API response status: {response.status_code}')
            location = response.headers.get('Location', 'not provided')
            logging.info(f'Job Location header: {location}')
        else:
            raise Exception(
                f'Fabric API returned unexpected status {response.status_code} '
                f'for date {date_str}. Response: {response.text}'
            )

    except requests.exceptions.Timeout:
        raise Exception(
            f'Timeout calling Fabric API for {date_str}. '
            'Check Fabric capacity status and try again.'
        )

    except requests.exceptions.ConnectionError:
        raise Exception(
            f'Cannot connect to Fabric API for {date_str}. '
            'Check network connectivity and Fabric service status.'
        )

    logging.info(f'Pipeline partition rebuild triggered: {date_str}')


# =============================================================================
# TASK 3: validate_rebuild
# =============================================================================
# Runs after ALL 60 pipeline tasks complete successfully.
# Logs a completion summary for this DAG run.
#
# WHY IS THIS A SEPARATE TASK?
#   If validate_rebuild is green ALL 60 tasks succeeded.
#   If any rebuild task failed validate_rebuild never runs -
#   giving you a single clear signal that something needs attention.
#
# NOTE ON DEEPER VALIDATION:
#   This task only logs a summary - it does not compare row counts.
#   For full data validation run notebook_validate.py manually in Fabric.
# =============================================================================

def validate_rebuild(**context):
    try:
        from airflow.operators.python import get_current_context
        ctx          = get_current_context()
        logical_date = ctx['data_interval_end'].date()
        yesterday    = logical_date - timedelta(days=1)
    except Exception:
        yesterday = datetime.today().date() - timedelta(days=1)

    oldest = yesterday - timedelta(days=LOOKBACK_DAYS - 1)

    logging.info('=' * 60)
    logging.info('NIGHTLY PIPELINE REBUILD COMPLETE')
    logging.info('=' * 60)
    logging.info(f'Dates rebuilt:   {LOOKBACK_DAYS}')
    logging.info(f'Date range:      {oldest} to {yesterday}')
    logging.info(f'Parallel tasks:  {MAX_PARALLEL_TASKS}')
    logging.info(f'Engine:          Fabric Pipeline (serverless, no Spark)')
    logging.info(f'Cost estimate:   ~$0.01-0.05 for this run')
    logging.info('Next step:       Run notebook_validate.py in Fabric.')
    logging.info('=' * 60)


# =============================================================================
# DAG DEFINITION
# =============================================================================
# Assembles the tasks into a DAG and defines how they connect.
#
# WHAT IS catchup=False?
#   Disables Airflow running all missed historical runs on first deploy.
#   Always use False unless you specifically need historical backfills.
#
# WHAT IS max_active_tasks?
#   Maximum tasks running simultaneously.
#   Unlike notebooks, pipelines are serverless so this can safely be
#   set higher (10-20) in production without worrying about Spark cost.
#
# WHAT DOES >> MEAN?
#   The >> operator defines task dependencies.
#   task_a >> task_b        = run b only AFTER a succeeds
#   task_a >> [b, c]        = run b and c after a succeeds
#   [task_a, task_b] >> c   = run c only after BOTH a and b succeed
#
# NOTE ON schedule= vs schedule_interval=:
#   schedule= is the modern Airflow 2.4+ syntax.
#   If you are on an older Airflow version use schedule_interval= instead.
# =============================================================================

with DAG(
    dag_id=DAG_ID,
    description=(
        'Nightly rebuild of last 60 days of shipment partitions '
        'from Azure SQL Hyperscale into Microsoft Fabric Lakehouse. '
        'Uses Airflow to trigger one Fabric Pipeline (Copy Data activity) '
        'per partition date. Serverless - no Spark sessions required. '
        'Phase 2 of 2: see fabric-hyperscale-copy-job for Phase 1.'
    ),
    schedule=SCHEDULE,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_tasks=MAX_PARALLEL_TASKS,
    tags=['fabric', 'hyperscale', 'partitions', 'pipeline', 'serverless'],
    default_args=default_args,
) as dag:

    task_get_dates = PythonOperator(
        task_id='get_dates_to_rebuild',
        python_callable=get_dates_to_rebuild,
    )

    # -------------------------------------------------------------------------
    # STATIC TASK CREATION (parse time)
    # -------------------------------------------------------------------------
    # Date list generated here at parse time so Airflow displays all 60
    # tasks in the graph view immediately before any run starts.
    # -------------------------------------------------------------------------
    yesterday_date  = datetime.today().date() - timedelta(days=1)
    dates_for_tasks = [
        (yesterday_date - timedelta(days=i)).strftime('%Y-%m-%d')
        for i in range(LOOKBACK_DAYS)
    ]

    # Create one pipeline trigger task per date (60 tasks total)
    # Each task calls the Fabric REST API with a different partition date
    rebuild_tasks = []
    for date_str in dates_for_tasks:
        task = PythonOperator(
            task_id=f'rebuild_partition_{date_str}',
            python_callable=rebuild_partition,
            op_kwargs={'date_str': date_str},
        )
        rebuild_tasks.append(task)

    task_validate = PythonOperator(
        task_id='validate_rebuild',
        python_callable=validate_rebuild,
    )

    # -------------------------------------------------------------------------
    # TASK DEPENDENCIES
    # -------------------------------------------------------------------------
    # Visual flow:
    #   get_dates_to_rebuild
    #          |
    #          v
    #   rebuild_partition_2026-04-11 ... rebuild_partition_2026-02-10
    #   (up to MAX_PARALLEL_TASKS running simultaneously)
    #          |
    #          v
    #   validate_rebuild
    # -------------------------------------------------------------------------
    task_get_dates >> rebuild_tasks >> task_validate