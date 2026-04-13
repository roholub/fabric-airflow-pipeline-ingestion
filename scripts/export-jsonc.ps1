param(
    [string]$Source = "fabric_pipeline_definition.jsonc",
    [string]$Output = "fabric_pipeline_definition.json"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -Path $Source)) {
    throw "Source file not found: $Source"
}

# Remove comment-only lines that start with // to produce strict JSON.
# This preserves // inside JSON string values (for example, URLs).
$strictJson = Get-Content -Path $Source | Where-Object { $_ -notmatch '^\s*//' }

# Validate JSON before writing the output file.
$null = ($strictJson -join "`n") | ConvertFrom-Json

Set-Content -Path $Output -Value $strictJson

Write-Output "Exported strict JSON: $Output"
