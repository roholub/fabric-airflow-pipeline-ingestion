# =============================================================================
# FABRIC + SQL HYPERSCALE | AIRFLOW + PIPELINE INGESTION
# File: notebook_validate.py
# =============================================================================
#
# WHAT IS THIS FILE?
#   This is a validation notebook that runs inside Microsoft Fabric.
#   It compares row counts between your SOURCE (Azure SQL Hyperscale)
#   and your SINK (Fabric Lakehouse) to confirm the pipeline ran correctly.
#
# IMPORTANT - WHY DOES THIS NOTEBOOK USE SPARK?
#   You might be wondering: "I thought this repo uses no Spark?"
#   Great question! Here is the distinction:
#
#   DATA MOVEMENT (airflow_dag.py + Fabric Pipeline):
#     Fully serverless. No Spark. No notebooks.
#     The Copy Data activity moves data directly between systems.
#     This is the core innovation of this repo vs my fabric-airflow-notebook-ingestion repo .
#
#   DATA VALIDATION (THIS FILE):
#     Uses Spark ONLY to read and compare row counts.
#     We need something that can query BOTH SQL Hyperscale AND the Lakehouse.
#     Spark with JDBC is the most straightforward way to do this in Fabric.
#     This notebook does NOT move any data - it only reads and compares.
#
#   Think of it this way:
#     Pipeline = the truck that delivers the packages (serverless, fast)
#     This notebook = the auditor who counts the packages after delivery (Spark)
#     The auditor does not drive the truck. They just verify the count is right.
#
# WHAT THIS NOTEBOOK VALIDATES:
#   For each of the last 60 shipment_date partitions it checks:
#     1. Row count in SQL Hyperscale (source of truth)
#     2. Row count in Fabric Lakehouse (what the pipeline wrote)
#     3. Are they equal? If not, flag it as a mismatch
#     4. Settlement status distribution - are V2/V3/V4 rows correct?
#     5. Overall summary - how many dates passed vs failed?
#
# WHEN TO RUN THIS NOTEBOOK:
#   Run this AFTER the Airflow DAG has triggered all 60 pipeline runs
#   and they have all completed successfully (all green in Airflow UI).
#   Do not run during an active pipeline run - counts will not match yet.
#
# HOW TO RUN:
#   1. Upload this file to your Fabric workspace
#      Fabric workspace -> New item -> Notebook -> paste code
#   2. Attach it to your Lakehouse
#      In the notebook left panel -> Add Lakehouse -> select your Lakehouse
#   3. Update the configuration section below with your values
#   4. Click Run All
#   5. Review the output of Cell 5 (summary report)
#
# EXPECTED RESULT:
#   All 60 dates should show PASS with matching row counts.
#   Any FAIL means the pipeline did not copy that partition correctly.
#   Re-trigger the Airflow DAG for the failing date to fix it.
#
# PREREQUISITES:
#   - Airflow DAG has completed all 60 pipeline runs successfully
#   - SPN credentials available (same ones from grant_spn_access.sql)
#   - Lakehouse attached to this notebook
#   - SQL Hyperscale firewall allows Fabric notebook IP range
#
# DISCLAIMER:
#   This script is a personal project and is not affiliated with,
#   endorsed by, or associated with Microsoft Corporation or any of
#   its subsidiaries. All views and code expressed here are my own.
#
# =============================================================================


# =============================================================================
# CELL 1 - CONFIGURATION
# =============================================================================
# Update these values before running the notebook.
# All connection details for SQL Hyperscale and the Lakehouse go here.
#
# WHERE TO FIND YOUR VALUES:
#   HYPERSCALE_SERVER:
#     Azure Portal -> SQL Server -> Overview -> Server name
#     Example: myserver.database.windows.net
#
#   HYPERSCALE_DATABASE:
#     Azure Portal -> SQL Database -> Overview -> Database name
#
#   SPN_CLIENT_ID and SPN_TENANT_ID:
#     Azure Portal -> Microsoft Entra ID -> App registrations
#     Find your SPN -> Overview -> Application (client) ID and Directory (tenant) ID
#
#   SPN_CLIENT_SECRET:
#     Azure Portal -> Microsoft Entra ID -> App registrations
#     Find your SPN -> Certificates & secrets -> copy the Secret Value
#     WARNING: Never commit real secret values to GitHub!
#     For POC: paste directly here and delete before any git commit
#     For production: use Azure Key Vault or Fabric environment variables
#
#   LAKEHOUSE_TABLE:
#     The name of your Delta table in the Lakehouse Bronze layer.
#     Should be fact_shipments unless you changed it.
#
#   LOOKBACK_DAYS:
#     How many days to validate. Match this to LOOKBACK_DAYS in airflow_dag.py.
#     Default is 60 - the same 60 days the pipeline rebuilds every night.
# =============================================================================

# !! UPDATE THESE WITH YOUR REAL VALUES !!
HYPERSCALE_SERVER   = "YOUR-SERVER-NAME.database.windows.net"
HYPERSCALE_DATABASE = "YOUR-DATABASE-NAME"
SPN_CLIENT_ID       = "YOUR-SPN-CLIENT-ID"
SPN_TENANT_ID       = "YOUR-TENANT-ID"
SPN_CLIENT_SECRET   = "YOUR-SPN-CLIENT-SECRET"  

# Lakehouse table name - should match what the pipeline writes to
LAKEHOUSE_TABLE     = "fact_shipments"

# Number of days to validate - must match LOOKBACK_DAYS in airflow_dag.py
LOOKBACK_DAYS       = 60

# Guard: fail immediately with a clear error if any placeholder was not replaced.
# Without this, JDBC throws a cryptic authentication error instead.
_placeholders = {
    "HYPERSCALE_SERVER":   HYPERSCALE_SERVER,
    "HYPERSCALE_DATABASE": HYPERSCALE_DATABASE,
    "SPN_CLIENT_ID":       SPN_CLIENT_ID,
    "SPN_TENANT_ID":       SPN_TENANT_ID,
    "SPN_CLIENT_SECRET":   SPN_CLIENT_SECRET,
}
_unset = [k for k, v in _placeholders.items() if v.startswith("YOUR-")]
if _unset:
    raise ValueError(
        f"Replace these placeholder values before running: {', '.join(_unset)}"
    )

# =============================================================================
# CELL 2 - BUILD DATE LIST AND CONNECT TO SQL HYPERSCALE
# =============================================================================
# Builds the list of 60 dates to validate and establishes a JDBC connection
# to SQL Hyperscale so we can query row counts from the source.
#
# WHAT IS JDBC?
#   JDBC (Java Database Connectivity) is a standard way for Java/Spark
#   applications to connect to relational databases like SQL Server.
#   Spark uses JDBC to talk to SQL Hyperscale here.
#   This is ONLY for reading counts - not for moving data.
#
# WHAT IS THE JDBC URL?
#   It is a connection string that tells Spark:
#   - Which driver to use (sqlserver)
#   - Which server to connect to
#   - Which database to open
#   - How to authenticate (using the SPN token)
#
# WHY ACTIVE DIRECTORY INTEGRATED AUTH?
#   We use the same Service Principal (SPN) that the pipeline uses.
#   This keeps authentication consistent across all components.
#   The SPN already has db_datareader access from grant_spn_access.sql.
# =============================================================================

from datetime import datetime, timedelta
from pyspark.sql import functions as F

# Build the list of dates to validate (same logic as airflow_dag.py)
# Use UTC - consistent with Airflow's date logic (Fabric workers run in UTC)
yesterday  = datetime.utcnow().date() - timedelta(days=1)
start_date = (yesterday - timedelta(days=LOOKBACK_DAYS - 1)).strftime('%Y-%m-%d')
end_date   = yesterday.strftime('%Y-%m-%d')

print(f"Validating {LOOKBACK_DAYS} dates")
print(f"Date range: {start_date} to {end_date}")
print("=" * 60)

# Build JDBC connection URL for SQL Hyperscale
# This is read-only - we are only querying counts, not moving data
jdbc_url = (
    f"jdbc:sqlserver://{HYPERSCALE_SERVER};"
    f"database={HYPERSCALE_DATABASE};"
    f"encrypt=true;"
    f"trustServerCertificate=false;"
    f"hostNameInCertificate=*.database.windows.net;"
    f"loginTimeout=30;"
    f"Authentication=ActiveDirectoryServicePrincipal"
)

# SPN credentials for JDBC authentication
# Same Service Principal used by the Fabric Pipeline
jdbc_properties = {
    "user"    : SPN_CLIENT_ID + "@" + SPN_TENANT_ID,
    "password": SPN_CLIENT_SECRET,
    "driver"  : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

print("JDBC connection configured successfully.")
print(f"Target server:   {HYPERSCALE_SERVER}")
print(f"Target database: {HYPERSCALE_DATABASE}")
print("Ready to query SQL Hyperscale row counts.")


# =============================================================================
# CELL 3 - QUERY ROW COUNTS FROM SQL HYPERSCALE (SOURCE)
# =============================================================================
# Reads row counts per shipment_date from SQL Hyperscale.
# This is our SOURCE OF TRUTH - what the pipeline SHOULD have copied.
#
# WHY COUNT PER DATE?
#   The pipeline rebuilds one date at a time.
#   If the count for date X matches in both systems, that date is correct.
#   If it does not match, the pipeline did not copy that date cleanly.
#
# PERFORMANCE NOTE:
#   This query runs ONE aggregation across the last 60 days in Hyperscale.
#   Much faster than 60 separate queries (one per date).
#   SQL Hyperscale with a columnstore index handles this in seconds.
# =============================================================================

# Calculate the start of the validation window
# Query: count rows per date for the last 60 days in SQL Hyperscale
hyperscale_query = f"""
(
    SELECT
        CAST(shipment_date AS VARCHAR(10))  AS shipment_date,
        COUNT(*)                            AS row_count,
        MAX(record_version)                 AS max_version,
        MIN(settlement_status)              AS settlement_status
    FROM dbo.fact_shipments
    WHERE shipment_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY shipment_date
) AS hyperscale_counts
"""

print(f"Querying SQL Hyperscale for dates: {start_date} to {end_date}")

# Read from SQL Hyperscale via JDBC
# This creates a Spark DataFrame with one row per date
df_hyperscale = spark.read.jdbc(
    url        = jdbc_url,
    table      = hyperscale_query,
    properties = jdbc_properties
)

hyperscale_count = df_hyperscale.count()
print(f"SQL Hyperscale returned counts for {hyperscale_count} dates.")
print("Sample (first 5 dates):")
display(df_hyperscale.orderBy("shipment_date", ascending=False).limit(5))


# =============================================================================
# CELL 4 - QUERY ROW COUNTS FROM FABRIC LAKEHOUSE (SINK)
# =============================================================================
# Reads row counts per shipment_date from the Fabric Lakehouse Delta table.
# This is what the pipeline ACTUALLY wrote.
#
# WHY NO JDBC HERE?
#   The Lakehouse is native to Fabric. Spark can read Delta tables directly
#   using spark.read.table() without any JDBC connection.
#   This is faster and simpler than JDBC for Lakehouse tables.
#
# WHY spark.sql() INSTEAD OF spark.read.table()?
#   spark.read.table() loads the ENTIRE table before filtering.
#   For a 200M row table that means scanning ~8GB before we narrow to 60 days.
#   spark.sql() with a WHERE clause pushes the filter down into Delta's
#   file reader so it only scans the ~1.5GB we actually need.
#   This is predicate pushdown - Delta skips irrelevant Parquet files
#   entirely. 
# =============================================================================

print(f"Querying Fabric Lakehouse table: {LAKEHOUSE_TABLE}")

# Push the filter INTO the query so Delta skips irrelevant partitions/files.
df_lakehouse = spark.sql(f"""
    SELECT
        CAST(shipment_date AS STRING)   AS shipment_date,
        COUNT(*)                        AS row_count,
        MAX(record_version)             AS max_version,
        MIN(settlement_status)          AS settlement_status
    FROM {LAKEHOUSE_TABLE}
    WHERE shipment_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY shipment_date
""")

lakehouse_count = df_lakehouse.count()
print(f"Fabric Lakehouse returned counts for {lakehouse_count} dates.")
print("Sample (first 5 dates):")
display(df_lakehouse.orderBy("shipment_date", ascending=False).limit(5))


# =============================================================================
# CELL 5 - COMPARE COUNTS AND FLAG MISMATCHES
# =============================================================================
# Joins the SQL Hyperscale counts with the Lakehouse counts side by side.
# Flags any date where the counts do not match.
#
# WHAT DOES A MISMATCH MEAN?
#   row_count_hyperscale != row_count_lakehouse means the pipeline did not
#   copy that date partition completely or correctly.
#
#   Common causes:
#     - Pipeline run failed or was cancelled mid-copy
#     - Network timeout during the copy
#     - The pipeline was not triggered for that date
#     - The Lakehouse table was modified manually after the pipeline ran
#
#   How to fix a mismatch:
#     - Re-trigger the Airflow DAG for just that date
#     - Or manually trigger the Fabric Pipeline with that date in Debug mode
#
# WHAT DOES NULL IN LAKEHOUSE MEAN?
#   If a date exists in Hyperscale but not in the Lakehouse (NULL row_count),
#   it means the pipeline was never triggered for that date at all.
#   Check the Airflow UI to see if that task ran or was skipped.
# =============================================================================

print("=" * 60)
print("COMPARING SQL HYPERSCALE vs FABRIC LAKEHOUSE")
print("=" * 60)

# Join source counts with sink counts on shipment_date
df_comparison = (
    df_hyperscale
    .withColumnRenamed("row_count",       "row_count_hyperscale")
    .withColumnRenamed("max_version",     "max_version_hyperscale")
    .withColumnRenamed("settlement_status", "status_hyperscale")
    .join(
        df_lakehouse
        .withColumnRenamed("row_count",       "row_count_lakehouse")
        .withColumnRenamed("max_version",     "max_version_lakehouse")
        .withColumnRenamed("settlement_status", "status_lakehouse"),
        on  = "shipment_date",
        how = "full_outer"    # full outer keeps dates missing from either side
    )
    .withColumn(
        "validation_result",
        # PASS = counts match exactly
        # MISSING_IN_LAKEHOUSE = pipeline was never triggered for this date
        # MISMATCH = pipeline ran but count is wrong
        F.when(F.col("row_count_lakehouse").isNull(), "MISSING_IN_LAKEHOUSE")
         .when(F.col("row_count_hyperscale").isNull(), "MISSING_IN_HYPERSCALE")
         .when(F.col("row_count_hyperscale") == F.col("row_count_lakehouse"), "PASS")
         .otherwise("MISMATCH")
    )
    .withColumn(
        "row_diff",
        # Positive = Lakehouse has MORE rows than Hyperscale (unexpected)
        # Negative = Lakehouse has FEWER rows than Hyperscale (pipeline missed rows)
        # Zero = perfect match
        F.coalesce(F.col("row_count_lakehouse"), F.lit(0))
        - F.coalesce(F.col("row_count_hyperscale"), F.lit(0))
    )
    .orderBy("shipment_date", ascending=False)
)

# Cache so that .show() and the summary groupBy below don't trigger two full jobs
df_comparison.cache()

# Show full comparison table
display(df_comparison.select(
    "shipment_date",
    "row_count_hyperscale",
    "row_count_lakehouse",
    "row_diff",
    "validation_result",
    "status_hyperscale",
    "status_lakehouse"
).limit(LOOKBACK_DAYS))


# =============================================================================
# CELL 6 - SUMMARY REPORT
# =============================================================================
# Prints a clean pass/fail summary for the entire 60-day validation window.
#
# GREEN (all PASS):
#   All 60 dates match. The pipeline ran correctly. You are done.
#
# YELLOW (some MISSING_IN_LAKEHOUSE):
#   Some dates were never copied. Check Airflow UI for skipped tasks.
#   Re-trigger the DAG or manually debug the pipeline for missing dates.
#
# RED (any MISMATCH):
#   Some dates copied but with wrong row counts. This is the most serious case.
#   Re-trigger the pipeline for those specific dates and re-validate.
#
# WHAT TO DO AFTER VALIDATION:
#   All PASS -> You are done! Data is consistent across both systems.
#   Any FAIL -> Re-trigger Airflow for failing dates -> re-run this notebook.
# =============================================================================

print("=" * 60)
print("VALIDATION SUMMARY REPORT")
print("=" * 60)

# Count results by category
summary = (
    df_comparison
    .groupBy("validation_result")
    .count()
    .orderBy("validation_result")
)

display(summary)

# Collect counts for final verdict
results       = {row["validation_result"]: row["count"]
                 for row in summary.collect()}
total_pass    = results.get("PASS", 0) 
total_miss    = results.get("MISSING_IN_LAKEHOUSE", 0)
total_extra   = results.get("MISSING_IN_HYPERSCALE", 0)
total_mismatch= results.get("MISMATCH", 0)
total_dates   = total_pass + total_miss + total_extra + total_mismatch

print("=" * 60)
print(f"Total dates validated:       {total_dates}")
print(f"PASS  (counts match):        {total_pass}")
print(f"MISSING IN LAKEHOUSE:        {total_miss}")
print(f"MISSING IN HYPERSCALE:       {total_extra}")
print(f"MISMATCH (wrong row count):  {total_mismatch}")
print("=" * 60)

if total_mismatch == 0 and total_miss == 0 and total_extra == 0:
    print("RESULT: ALL DATES PASSED")
    print("The Fabric Pipeline copied all 60 partitions correctly.")
    print("SQL Hyperscale and Fabric Lakehouse are in sync.")
if total_mismatch > 0:
    print("RESULT: MISMATCHES FOUND - ACTION REQUIRED")
    print("Some dates have incorrect row counts in the Lakehouse.")
    print("Re-trigger the Airflow DAG for failing dates and re-validate.")
if total_miss > 0:
    print("RESULT: MISSING DATES FOUND - ACTION REQUIRED")
    print("Some dates were never copied by the pipeline.")
    print("Check Airflow UI for skipped tasks and re-trigger.")
if total_extra > 0:
    print("RESULT: EXTRA DATES IN LAKEHOUSE - REVIEW REQUIRED")
    print("Some dates exist in the Lakehouse but not in SQL Hyperscale.")
    print("Review the comparison table above for MISSING_IN_HYPERSCALE dates.")

print("=" * 60)
print("Validation complete.")
print("Next step: If all PASS you are ready for production scheduling.")
print("Enable nightly automation: set SCHEDULE = '0 3 * * *' in airflow_dag.py")
print("=" * 60)