/*
================================================================================
FABRIC + SQL HYPERSCALE | AIRFLOW + PIPELINE INGESTION
File: grant_spn_access.sql
================================================================================
PURPOSE:
    Grant a Microsoft Entra Service Principal (SPN) read access to your
    Azure SQL Hyperscale database so that the Fabric Pipelines
    can authenticate and read data from SQL Hyperscale.

WHEN TO RUN:
    Run this script in your Azure SQL Hyperscale database AFTER:
    1. Creating the Service Principal in Microsoft Entra ID
    2. Saving the Client ID, Tenant ID, and Secret Value

PREREQUISITES:
    - Azure SQL Hyperscale with Microsoft Entra authentication enabled
    - A Service Principal already created in Microsoft Entra ID
    - Connect using an account with db_owner or equivalent rights
    - Run via VS Code mssql extension or Azure Data Studio
    - Connect using Azure Active Directory - Universal with MFA

HOW THIS FITS IN THE PIPELINE:
    The Airflow DAG triggers one Fabric Pipeline per partition date nightly.
    The Pipeline uses a Copy Data activity to connect to SQL Hyperscale
    using this Service Principal to read each date partition.
    Without this access, the Fabric Pipelines cannot read from SQL Hyperscale.
    No Spark or JDBC required - the Pipeline uses native SQL connectors.

CROSS-TENANT NOTES:
    This can work in same-tenant and cross-tenant scenarios, but the SPN
    identity must be resolvable by the SQL tenant (for example as a guest/
    service principal object in Microsoft Entra).
    Coordinate with identity admins if CREATE USER fails with principal lookup errors.

DISCLAIMER:
    This script is a personal project and is not affiliated with,
    endorsed by, or associated with Microsoft Corporation or any of its
    subsidiaries. All views and code expressed here are my own.
================================================================================
*/

--------------------------------------------------------------------------------
-- SECTION 0: SET SPN NAME ONCE
-- Update this value to your SPN display name in Microsoft Entra ID
--------------------------------------------------------------------------------
DECLARE @spn_name SYSNAME = N'fabric-copyjob-spn';
PRINT 'Target SPN display name: ' + @spn_name;
GO

--------------------------------------------------------------------------------
-- SECTION 1: PRE-FLIGHT CHECK
-- Verify you are connected to the correct database before proceeding
--------------------------------------------------------------------------------
PRINT '================================================================';
PRINT 'PRE-FLIGHT CHECK';
PRINT '================================================================';
PRINT 'Current database:  ' + DB_NAME();
PRINT 'Current server:    ' + @@SERVERNAME;
PRINT 'Current user:      ' + SYSTEM_USER;
PRINT 'Run time:          ' + CONVERT(VARCHAR, GETDATE(), 121);
PRINT '================================================================';
PRINT 'Confirm the above matches your target database before proceeding.';
PRINT '================================================================';
GO

--------------------------------------------------------------------------------
-- SECTION 2: CREATE EXTERNAL USER FOR SERVICE PRINCIPAL (idempotent)
--------------------------------------------------------------------------------
PRINT 'SECTION 2: Creating external user for Service Principal...';

DECLARE @spn_name SYSNAME = N'fabric-copyjob-spn';
DECLARE @sql NVARCHAR(MAX);

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_principals
    WHERE name = @spn_name
)
BEGIN
    SET @sql = N'CREATE USER ' + QUOTENAME(@spn_name) + N' FROM EXTERNAL PROVIDER;';
    EXEC sp_executesql @sql;
    PRINT 'External user created successfully.';
END
ELSE
BEGIN
    PRINT 'External user already exists. Skipping CREATE USER.';
END
GO

--------------------------------------------------------------------------------
-- SECTION 3: GRANT READ ACCESS (idempotent)
-- Grants db_datareader role to the SPN
-- This allows the Fabric Pipelines to read ALL tables in the database
-- Principle of least privilege - read only, no write access
--------------------------------------------------------------------------------
PRINT 'SECTION 3: Granting db_datareader role...';

DECLARE @spn_name SYSNAME = N'fabric-copyjob-spn';
DECLARE @sql NVARCHAR(MAX);

IF EXISTS (
    SELECT 1
    FROM sys.database_principals
    WHERE name = @spn_name
)
AND NOT EXISTS (
    SELECT 1
    FROM sys.database_role_members drm
    JOIN sys.database_principals r
      ON drm.role_principal_id = r.principal_id
    JOIN sys.database_principals m
      ON drm.member_principal_id = m.principal_id
    WHERE r.name = N'db_datareader'
      AND m.name = @spn_name
)
BEGIN
    SET @sql = N'ALTER ROLE db_datareader ADD MEMBER ' + QUOTENAME(@spn_name) + N';';
    EXEC sp_executesql @sql;
    PRINT 'db_datareader role granted successfully.';
END
ELSE
BEGIN
    PRINT 'db_datareader already granted (or user missing). Skipping.';
END
GO

--------------------------------------------------------------------------------
-- SECTION 4: GRANT SPECIFIC TABLE ACCESS (optional - more restrictive)
-- Use this INSTEAD of Section 3 if you want to limit access
-- to only the fact_shipments table
-- Uncomment and run instead of Section 3 if preferred
--------------------------------------------------------------------------------
/*
DECLARE @spn_name SYSNAME = N'fabric-copyjob-spn';
DECLARE @sql NVARCHAR(MAX) =
    N'GRANT SELECT ON dbo.fact_shipments TO ' + QUOTENAME(@spn_name) + N';';
EXEC sp_executesql @sql;
PRINT 'SELECT granted on dbo.fact_shipments only.';
*/
GO

--------------------------------------------------------------------------------
-- SECTION 5: VERIFY ACCESS
-- Confirms the SPN was created correctly with the right permissions
--------------------------------------------------------------------------------
PRINT '================================================================';
PRINT 'SECTION 5: Verifying SPN access...';
PRINT '================================================================';

DECLARE @spn_name SYSNAME = N'fabric-copyjob-spn';

SELECT
    dp.name        AS user_name,
    dp.type_desc   AS user_type,
    r.name         AS role_name,
    dp.create_date AS created_on,
    dp.modify_date AS last_modified
FROM sys.database_principals dp
LEFT JOIN sys.database_role_members drm
    ON dp.principal_id = drm.member_principal_id
LEFT JOIN sys.database_principals r
    ON drm.role_principal_id = r.principal_id
WHERE dp.name = @spn_name;

PRINT '================================================================';
PRINT 'Expected result:';
PRINT '  user_type = EXTERNAL_USER';
PRINT '  role_name = db_datareader (if Section 3 used)';
PRINT '================================================================';
GO

--------------------------------------------------------------------------------
-- SECTION 6: OPTIONAL - REVOKE ACCESS (safe cleanup)
-- Uncomment and run when testing is complete
--------------------------------------------------------------------------------
/*
DECLARE @spn_name SYSNAME = N'fabric-copyjob-spn';
DECLARE @sql NVARCHAR(MAX);

IF EXISTS (
    SELECT 1
    FROM sys.database_role_members drm
    JOIN sys.database_principals r
      ON drm.role_principal_id = r.principal_id
    JOIN sys.database_principals m
      ON drm.member_principal_id = m.principal_id
    WHERE r.name = N'db_datareader'
      AND m.name = @spn_name
)
BEGIN
    SET @sql = N'ALTER ROLE db_datareader DROP MEMBER ' + QUOTENAME(@spn_name) + N';';
    EXEC sp_executesql @sql;
END

IF EXISTS (
    SELECT 1
    FROM sys.database_principals
    WHERE name = @spn_name
)
BEGIN
    SET @sql = N'DROP USER ' + QUOTENAME(@spn_name) + N';';
    EXEC sp_executesql @sql;
END

PRINT 'SPN access revoked and user dropped (if present).';
*/
GO