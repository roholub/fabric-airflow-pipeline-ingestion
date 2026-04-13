-- =============================================================================
-- FABRIC + SQL HYPERSCALE | AIRFLOW + PIPELINE INGESTION          
-- File: synthetic_data_generation.sql
-- =============================================================================
-- PURPOSE:
--     Generates a realistic logistics dataset in Azure SQL Hyperscale that
--     simulates a common and challenging data engineering pattern:
--
--     DATA SETTLING / DATA DRIFT
--     ─────────────────────────
--     In many real-world systems, a record written today does not become
--     final immediately. It keeps getting updated for days or weeks after
--     the original transaction as downstream processes complete:
--
--       Day 0:  Shipment recorded  → Status: Pending,  Version: 1
--       Day 7:  Carrier confirmed  → Status: Settling, Version: 2
--       Day 30: Delivery confirmed → Status: Settling, Version: 3
--       Day 60: Invoice settled    → Status: Settled,  Version: 4 (frozen)
--
--     This means:
--       - Any record from the LAST 60 DAYS may still change tonight
--       - Any record OLDER than 60 days is frozen and will never change
--       - A simple append or watermark strategy will MISS these updates
--       - The robust pattern is: rebuild the last 60 days nightly
--         using atomic partition replacement in the Lakehouse
--
-- WHY SECTIONS ARE BATCHED:
--     Long-running queries can cause TCP connection timeouts in VS Code.
--     To prevent this, Sections 4 and 5 are broken into smaller batches.
--     Run one batch at a time - if a batch fails, just re-run that batch.
--     Each batch completes in ~20-30 minutes safely.
--
-- RESUME GUIDE (if you already have partial data):
--     0 rows:   Start from Section 3
--     ~2M rows: Start from Section 4A
--     ~50M rows: Start from Section 4B
--     ~100M rows: Start from Section 4C
--     ~150M rows: Start from Section 4D
--     ~200M rows: Start from Section 5A (data settling)
--     Settled:  Start from Section 6A (validation)
--
-- HOW TO RUN:
--     Run each section individually in VS Code (highlight + Ctrl+Shift+E)
--     Do NOT run all sections at once - they must run in order
--     Wait for each section to complete before running the next one
--
-- ADJUSTING ROW COUNT:
--     To generate fewer rows reduce @maxIterations in each 4x batch
--     4A + 4B only (~100M rows) is sufficient for most POC scenarios
--
-- COST ESTIMATE (Azure SQL Hyperscale):
--     - Full data generation: ~2-3 hours total across all batches
--     - Storage:              ~8-12 GB
--     - Recommended SKU:      General Purpose 2 vCores or higher
--
-- DISCLAIMER:
--     This script is a personal project and is not affiliated with,
--     endorsed by, or associated with Microsoft Corporation or any of its
--     subsidiaries. All views and code expressed here are my own.
--
-- AUTHOR: Rocio Holub (https://github.com/roholub)
-- =============================================================================


-- =============================================================================
-- SECTION 1: OPTIONAL CLEANUP
-- Run ONLY if you want to start completely fresh
-- WARNING: This permanently deletes ALL data in fact_shipments
-- ESTIMATED TIME: < 1 minute
-- =============================================================================

/*
IF OBJECT_ID('dbo.fact_shipments', 'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.fact_shipments;
    PRINT 'fact_shipments dropped successfully.';
END
*/


-- =============================================================================
-- SECTION 2: CREATE TABLE
-- Creates fact_shipments with a clustered columnstore index
-- ESTIMATED TIME: < 1 minute
-- =============================================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'dbo.fact_shipments')
    AND type IN (N'U')
)
BEGIN
    CREATE TABLE dbo.fact_shipments
    (
        shipment_id         BIGINT          NOT NULL,
        shipment_date       DATE            NOT NULL,
        record_version      INT             NOT NULL DEFAULT 1,
        settlement_status   VARCHAR(20)     NOT NULL DEFAULT 'Pending',
        carrier_code        VARCHAR(10)     NOT NULL,
        origin_region       VARCHAR(20)     NOT NULL,
        destination_region  VARCHAR(20)     NOT NULL,
        estimated_cost      DECIMAL(18,2)   NOT NULL,
        actual_cost         DECIMAL(18,2)   NULL,
        weight_lbs          DECIMAL(10,2)   NOT NULL,
        payload             VARCHAR(100)    NOT NULL
    );

    CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_shipments
    ON dbo.fact_shipments
    WITH (DATA_COMPRESSION = COLUMNSTORE);

    PRINT 'fact_shipments created successfully.';
END
ELSE
BEGIN
    PRINT 'fact_shipments already exists - skipping creation.';
END
GO


-- =============================================================================
-- SECTION 3: BASE DATA GENERATION (~5M rows)
-- Inserts initial rows spread across the last 365 days
-- All records start as Version 1 / Pending
-- ESTIMATED TIME: ~1-2 minutes
-- ROWS AFTER: ~5 million
-- =============================================================================

DECLARE @today     DATE = CAST(GETDATE() AS DATE);
DECLARE @startDate DATE = DATEADD(DAY, -365, @today);

INSERT INTO dbo.fact_shipments
(
    shipment_id, shipment_date, record_version, settlement_status,
    carrier_code, origin_region, destination_region,
    estimated_cost, actual_cost, weight_lbs, payload
)
SELECT TOP 5000000
    ROW_NUMBER() OVER (ORDER BY a.object_id, b.object_id),
    CAST(DATEADD(DAY, ABS(CHECKSUM(NEWID())) % 365, @startDate) AS DATE),
    1,
    'Pending',
    'CAR-' + CHAR(65 + (ABS(CHECKSUM(NEWID())) % 5)),
    CASE ABS(CHECKSUM(NEWID())) % 5
        WHEN 0 THEN 'North' WHEN 1 THEN 'South'
        WHEN 2 THEN 'East'  WHEN 3 THEN 'West' ELSE 'Central'
    END,
    CASE ABS(CHECKSUM(NEWID())) % 5
        WHEN 0 THEN 'North' WHEN 1 THEN 'South'
        WHEN 2 THEN 'East'  WHEN 3 THEN 'West' ELSE 'Central'
    END,
    CAST(500  + (ABS(CHECKSUM(NEWID())) % 2000) AS DECIMAL(18,2)),
    NULL,
    CAST(100  + (ABS(CHECKSUM(NEWID())) % 900)  AS DECIMAL(10,2)),
    'SHIPMENT-BATCH-INITIAL-' + CAST(ABS(CHECKSUM(NEWID())) % 10000 AS VARCHAR(10))
FROM sys.objects a
CROSS JOIN sys.objects b
CROSS JOIN sys.objects c;

DECLARE @s3count BIGINT;
SELECT @s3count = COUNT_BIG(*) FROM dbo.fact_shipments;
DECLARE @s3msg VARCHAR(100);
SET @s3msg = 'Section 3 complete. Total rows: ' + CAST(@s3count AS VARCHAR(20));
PRINT @s3msg;
GO


-- =============================================================================
-- SECTION 4A: DATA EXPANSION - BATCH 1 of 4 (iterations 1-10)
-- ESTIMATED TIME: ~20-25 minutes
-- ROWS AFTER: ~50 million
-- NOTE: If you already have ~50M+ rows skip to Section 4B
-- =============================================================================

DECLARE @iter4A    INT = 1;
DECLARE @max4A     INT = 10;
DECLARE @offset4A  INT = 0;

WHILE @iter4A <= @max4A
BEGIN
    SET @offset4A = @iter4A * 2;

    INSERT INTO dbo.fact_shipments
    (
        shipment_id, shipment_date, record_version, settlement_status,
        carrier_code, origin_region, destination_region,
        estimated_cost, actual_cost, weight_lbs, payload
    )
    SELECT
        shipment_id + (@iter4A * 5000000),
        DATEADD(DAY, -(@offset4A % 365), shipment_date),
        1, 'Pending',
        carrier_code, origin_region, destination_region,
        estimated_cost, NULL, weight_lbs,
        'SHIPMENT-BATCH-' + CAST(@iter4A AS VARCHAR(10))
    FROM dbo.fact_shipments
    WHERE shipment_id <= 5000000;

    IF @iter4A % 3 = 0
    BEGIN
        DECLARE @p4A VARCHAR(100);
        SET @p4A = '4A Progress: iteration ' + CAST(@iter4A AS VARCHAR(5))
                 + ' of ' + CAST(@max4A AS VARCHAR(5));
        PRINT @p4A;
    END

    SET @iter4A = @iter4A + 1;
END

DECLARE @c4A BIGINT;
SELECT @c4A = COUNT_BIG(*) FROM dbo.fact_shipments;
DECLARE @m4A VARCHAR(100);
SET @m4A = 'Section 4A complete. Total rows: ' + CAST(@c4A AS VARCHAR(20));
PRINT @m4A;
GO


-- =============================================================================
-- SECTION 4B: DATA EXPANSION - BATCH 2 of 4 (iterations 11-20)
-- ESTIMATED TIME: ~20-25 minutes
-- ROWS AFTER: ~100 million
-- NOTE: If you already have ~100M+ rows skip to Section 4C
-- =============================================================================

DECLARE @iter4B    INT = 1;
DECLARE @max4B     INT = 10;
DECLARE @offset4B  INT = 0;

WHILE @iter4B <= @max4B
BEGIN
    SET @offset4B = (@iter4B + 10) * 2;

    INSERT INTO dbo.fact_shipments
    (
        shipment_id, shipment_date, record_version, settlement_status,
        carrier_code, origin_region, destination_region,
        estimated_cost, actual_cost, weight_lbs, payload
    )
    SELECT
        shipment_id + ((@iter4B + 10) * 5000000),
        DATEADD(DAY, -(@offset4B % 365), shipment_date),
        1, 'Pending',
        carrier_code, origin_region, destination_region,
        estimated_cost, NULL, weight_lbs,
        'SHIPMENT-BATCH-' + CAST(@iter4B + 10 AS VARCHAR(10))
    FROM dbo.fact_shipments
    WHERE shipment_id <= 5000000;

    IF @iter4B % 3 = 0
    BEGIN
        DECLARE @p4B VARCHAR(100);
        SET @p4B = '4B Progress: iteration ' + CAST(@iter4B + 10 AS VARCHAR(5))
                 + ' of 40';
        PRINT @p4B;
    END

    SET @iter4B = @iter4B + 1;
END

DECLARE @c4B BIGINT;
SELECT @c4B = COUNT_BIG(*) FROM dbo.fact_shipments;
DECLARE @m4B VARCHAR(100);
SET @m4B = 'Section 4B complete. Total rows: ' + CAST(@c4B AS VARCHAR(20));
PRINT @m4B;
GO


-- =============================================================================
-- SECTION 4C: DATA EXPANSION - BATCH 3 of 4 (iterations 21-30)
-- ESTIMATED TIME: ~20-25 minutes
-- ROWS AFTER: ~150 million
-- NOTE: If you already have ~150M+ rows skip to Section 4D
-- =============================================================================

DECLARE @iter4C    INT = 1;
DECLARE @max4C     INT = 10;
DECLARE @offset4C  INT = 0;

WHILE @iter4C <= @max4C
BEGIN
    SET @offset4C = (@iter4C + 20) * 2;

    INSERT INTO dbo.fact_shipments
    (
        shipment_id, shipment_date, record_version, settlement_status,
        carrier_code, origin_region, destination_region,
        estimated_cost, actual_cost, weight_lbs, payload
    )
    SELECT
        shipment_id + ((@iter4C + 20) * 5000000),
        DATEADD(DAY, -(@offset4C % 365), shipment_date),
        1, 'Pending',
        carrier_code, origin_region, destination_region,
        estimated_cost, NULL, weight_lbs,
        'SHIPMENT-BATCH-' + CAST(@iter4C + 20 AS VARCHAR(10))
    FROM dbo.fact_shipments
    WHERE shipment_id <= 5000000;

    IF @iter4C % 3 = 0
    BEGIN
        DECLARE @p4C VARCHAR(100);
        SET @p4C = '4C Progress: iteration ' + CAST(@iter4C + 20 AS VARCHAR(5))
                 + ' of 40';
        PRINT @p4C;
    END

    SET @iter4C = @iter4C + 1;
END

DECLARE @c4C BIGINT;
SELECT @c4C = COUNT_BIG(*) FROM dbo.fact_shipments;
DECLARE @m4C VARCHAR(100);
SET @m4C = 'Section 4C complete. Total rows: ' + CAST(@c4C AS VARCHAR(20));
PRINT @m4C;
GO


-- =============================================================================
-- SECTION 4D: DATA EXPANSION - BATCH 4 of 4 (iterations 31-40)
-- ESTIMATED TIME: ~20-25 minutes
-- ROWS AFTER: ~200 million
-- =============================================================================

DECLARE @iter4D    INT = 1;
DECLARE @max4D     INT = 10;
DECLARE @offset4D  INT = 0;

WHILE @iter4D <= @max4D
BEGIN
    SET @offset4D = (@iter4D + 30) * 2;

    INSERT INTO dbo.fact_shipments
    (
        shipment_id, shipment_date, record_version, settlement_status,
        carrier_code, origin_region, destination_region,
        estimated_cost, actual_cost, weight_lbs, payload
    )
    SELECT
        shipment_id + ((@iter4D + 30) * 5000000),
        DATEADD(DAY, -(@offset4D % 365), shipment_date),
        1, 'Pending',
        carrier_code, origin_region, destination_region,
        estimated_cost, NULL, weight_lbs,
        'SHIPMENT-BATCH-' + CAST(@iter4D + 30 AS VARCHAR(10))
    FROM dbo.fact_shipments
    WHERE shipment_id <= 5000000;

    IF @iter4D % 3 = 0
    BEGIN
        DECLARE @p4D VARCHAR(100);
        SET @p4D = '4D Progress: iteration ' + CAST(@iter4D + 30 AS VARCHAR(5))
                 + ' of 40';
        PRINT @p4D;
    END

    SET @iter4D = @iter4D + 1;
END

DECLARE @c4D BIGINT;
SELECT @c4D = COUNT_BIG(*) FROM dbo.fact_shipments;
DECLARE @m4D VARCHAR(100);
SET @m4D = 'Section 4D complete. Total rows: ' + CAST(@c4D AS VARCHAR(20));
PRINT @m4D;
GO


-- OPTIONAL GUARDRAIL (run before major mutation sections):
-- Quick snapshot to confirm version/status distribution before updates
/*
SELECT
    shipment_date,
    record_version,
    settlement_status,
    COUNT_BIG(*) AS row_count
FROM dbo.fact_shipments
WHERE shipment_date >= DATEADD(DAY, -70, CAST(GETDATE() AS DATE))
GROUP BY shipment_date, record_version, settlement_status
ORDER BY shipment_date DESC, record_version;
*/


-- =============================================================================
-- SECTION 5A: SIMULATE DATA SETTLING - Version 2 (0-30 days old)
-- Updates records from the last 30 days to Settling / Version 2
-- ESTIMATED TIME: ~3-5 minutes
-- ROWS AFFECTED: ~10-15 million (records from last 30 days)
-- =============================================================================

DECLARE @today5A DATE = CAST(GETDATE() AS DATE);

UPDATE dbo.fact_shipments
SET
    record_version    = 2,
    settlement_status = 'Settling',
    actual_cost       = CAST(estimated_cost * 0.97 AS DECIMAL(18,2))
WHERE
    shipment_date >= DATEADD(DAY, -30, @today5A)
    AND shipment_date <= @today5A;

DECLARE @c5A BIGINT;
SELECT @c5A = COUNT_BIG(*) FROM dbo.fact_shipments WHERE record_version = 2;
DECLARE @m5A VARCHAR(100);
SET @m5A = 'Section 5A complete. Version 2 rows: ' + CAST(@c5A AS VARCHAR(20));
PRINT @m5A;
GO


-- =============================================================================
-- SECTION 5B: SIMULATE DATA SETTLING - Version 3 (31-59 days old)
-- Updates records from 31-59 days ago to Settling / Version 3
-- ESTIMATED TIME: ~3-5 minutes
-- ROWS AFFECTED: ~10-15 million (records from days 31-59)
-- =============================================================================

DECLARE @today5B DATE = CAST(GETDATE() AS DATE);

UPDATE dbo.fact_shipments
SET
    record_version    = 3,
    settlement_status = 'Settling',
    actual_cost       = CAST(estimated_cost * 0.95 AS DECIMAL(18,2))
WHERE
    shipment_date >= DATEADD(DAY, -59, @today5B)
    AND shipment_date <  DATEADD(DAY, -30, @today5B);

DECLARE @c5B BIGINT;
SELECT @c5B = COUNT_BIG(*) FROM dbo.fact_shipments WHERE record_version = 3;
DECLARE @m5B VARCHAR(100);
SET @m5B = 'Section 5B complete. Version 3 rows: ' + CAST(@c5B AS VARCHAR(20));
PRINT @m5B;
GO


-- =============================================================================
-- SECTION 5C: SIMULATE DATA SETTLING - Version 4 (60+ days old - FROZEN)
-- Updates all records older than 60 days to Settled / Version 4
-- These records will NEVER change again - they are frozen forever
-- ESTIMATED TIME: ~10-15 minutes (largest update - most rows)
-- ROWS AFFECTED: ~150-170 million (all records older than 60 days)
-- =============================================================================

DECLARE @today5C DATE = CAST(GETDATE() AS DATE);

UPDATE dbo.fact_shipments
SET
    record_version    = 4,
    settlement_status = 'Settled',
    actual_cost       = CAST(estimated_cost * 0.94 AS DECIMAL(18,2))
WHERE
    shipment_date < DATEADD(DAY, -59, @today5C);

DECLARE @c5C BIGINT;
SELECT @c5C = COUNT_BIG(*) FROM dbo.fact_shipments WHERE record_version = 4;
DECLARE @m5C VARCHAR(100);
SET @m5C = 'Section 5C complete. Version 4 (frozen) rows: ' + CAST(@c5C AS VARCHAR(20));
PRINT @m5C;
PRINT 'Data settling simulation complete. All versions updated.';
GO


-- =============================================================================
-- SECTION 6A: DISTRIBUTION ANALYSIS - Overall Summary
-- Run after Section 5C to verify totals look correct
-- ESTIMATED TIME: ~1-2 minutes
-- =============================================================================

DECLARE @totalRows BIGINT;
SELECT @totalRows = COUNT_BIG(*) FROM dbo.fact_shipments;
DECLARE @summaryMsg VARCHAR(100);
SET @summaryMsg = 'Total rows in fact_shipments: ' + CAST(@totalRows AS VARCHAR(20));
PRINT @summaryMsg;

SELECT
    COUNT_BIG(*)                    AS total_rows,
    MIN(shipment_date)              AS earliest_date,
    MAX(shipment_date)              AS latest_date,
    COUNT(DISTINCT shipment_date)   AS distinct_dates,
    COUNT(DISTINCT carrier_code)    AS distinct_carriers
FROM dbo.fact_shipments;
GO


-- =============================================================================
-- SECTION 6B: DISTRIBUTION ANALYSIS - Settlement Status Breakdown
-- Verify the settling pattern looks correct across all 4 versions
-- ESTIMATED TIME: ~1-2 minutes
-- EXPECTED: Version 2 (~last 30 days), Version 3 (~days 31-59),
--           Version 4 (~everything older than 60 days)
-- =============================================================================

SELECT
    settlement_status,
    record_version,
    COUNT_BIG(*)        AS row_count,
    MIN(actual_cost)    AS min_actual_cost,
    MAX(actual_cost)    AS max_actual_cost
FROM dbo.fact_shipments
GROUP BY settlement_status, record_version
ORDER BY record_version;
GO


-- =============================================================================
-- SECTION 6C: DISTRIBUTION ANALYSIS - By Carrier
-- ESTIMATED TIME: < 1 minute
-- =============================================================================

SELECT
    carrier_code,
    COUNT_BIG(*)        AS row_count,
    MIN(shipment_date)  AS earliest_date,
    MAX(shipment_date)  AS latest_date
FROM dbo.fact_shipments
GROUP BY carrier_code
ORDER BY carrier_code;
GO


-- =============================================================================
-- SECTION 6D: DISTRIBUTION ANALYSIS - Most Recent 10 Dates
-- These are the dates that will be rebuilt every night by Airflow
-- ESTIMATED TIME: < 1 minute
-- =============================================================================

SELECT TOP 10
    shipment_date,
    COUNT(*)                AS row_count,
    MIN(record_version)     AS min_version,
    MAX(record_version)     AS max_version,
    MIN(settlement_status)  AS settlement_status
FROM dbo.fact_shipments
GROUP BY shipment_date
ORDER BY shipment_date DESC;
GO


-- =============================================================================
-- SECTION 7A: NIGHTLY REBUILD SIMULATION - Insert new shipments
-- Simulates new data arriving for tomorrow
-- ESTIMATED TIME: < 1 minute
-- =============================================================================

DECLARE @today7A    DATE   = CAST(GETDATE() AS DATE);
DECLARE @tomorrow   DATE   = DATEADD(DAY, 1, @today7A);
DECLARE @seed_date  DATE;
DECLARE @before7A   BIGINT;

SELECT @before7A = COUNT_BIG(*) FROM dbo.fact_shipments;

-- Critical fix: use latest available date as seed source
SELECT @seed_date = MAX(shipment_date)
FROM dbo.fact_shipments
WHERE shipment_date <= @today7A;

INSERT INTO dbo.fact_shipments
(
    shipment_id, shipment_date, record_version, settlement_status,
    carrier_code, origin_region, destination_region,
    estimated_cost, actual_cost, weight_lbs, payload
)
SELECT TOP 50000
    shipment_id + 900000000,
    @tomorrow,
    1, 'Pending',
    carrier_code, origin_region, destination_region,
    estimated_cost, NULL, weight_lbs,
    'SHIPMENT-NIGHTLY-NEW-'
        + CAST(ABS(CHECKSUM(NEWID())) % 10000 AS VARCHAR(10))
FROM dbo.fact_shipments
WHERE shipment_date = @seed_date;

DECLARE @after7A  BIGINT;
DECLARE @new7A    BIGINT;
SELECT @after7A = COUNT_BIG(*) FROM dbo.fact_shipments;
SET @new7A = @after7A - @before7A;

DECLARE @msg7A VARCHAR(160);
SET @msg7A = 'Section 7A complete. Seed date: '
          + CONVERT(VARCHAR(10), @seed_date, 120)
          + '. New rows inserted: ' + CAST(@new7A AS VARCHAR(20));
PRINT @msg7A;
GO


-- =============================================================================
-- SECTION 7B: NIGHTLY REBUILD SIMULATION - Advance settlement versions
-- Simulates records crossing the 30-day boundary from Version 2 to Version 3
-- ESTIMATED TIME: ~2-3 minutes
-- =============================================================================

DECLARE @today7B DATE = CAST(GETDATE() AS DATE);

-- Critical fix: target boundary date only (31 days old)
UPDATE dbo.fact_shipments
SET
    record_version    = 3,
    settlement_status = 'Settling',
    actual_cost       = CAST(estimated_cost * 0.95 AS DECIMAL(18,2))
WHERE
    shipment_date = DATEADD(DAY, -31, @today7B)
    AND record_version = 2;

DECLARE @rows7B BIGINT = @@ROWCOUNT;
DECLARE @m7B VARCHAR(140);
SET @m7B = 'Section 7B complete. Rows moved V2 -> V3: ' + CAST(@rows7B AS VARCHAR(20));
PRINT @m7B;
PRINT 'Nightly rebuild simulation complete.';
GO


-- =============================================================================
-- SECTION 8: FABRIC SQL ANALYTICS ENDPOINT VALIDATION
-- Run these queries in Fabric AFTER Airflow + pipelines have run
-- WHERE: Fabric Lakehouse -> SQL Analytics Endpoint -> New Query
-- =============================================================================

/*
-- Validation 1: Row count should match SQL Hyperscale
SELECT
    COUNT(*)                        AS total_rows,
    MIN(shipment_date)              AS earliest_date,
    MAX(shipment_date)              AS latest_date,
    COUNT(DISTINCT shipment_date)   AS distinct_dates,
    COUNT(DISTINCT carrier_code)    AS distinct_carriers
FROM dbo.fact_shipments;

-- Validation 2: Confirm last 60 days were rebuilt correctly
SELECT
    settlement_status,
    record_version,
    COUNT(*) AS row_count
FROM dbo.fact_shipments
WHERE shipment_date >= DATEADD(DAY, -60, CAST(GETDATE() AS DATE))
GROUP BY settlement_status, record_version
ORDER BY record_version;

-- Validation 3: Confirm frozen records (60+ days) were NOT changed
SELECT
    COUNT(*)            AS frozen_rows,
    MIN(shipment_date)  AS earliest_frozen,
    MAX(shipment_date)  AS latest_frozen
FROM dbo.fact_shipments
WHERE shipment_date < DATEADD(DAY, -60, CAST(GETDATE() AS DATE))
AND settlement_status = 'Settled';
*/
