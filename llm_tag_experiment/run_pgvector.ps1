
param(
    [string]$XlsxPath = ""
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

$outDir = Join-Path $scriptDir "output"
if ($XlsxPath) {
    $xlsxFull = if ([System.IO.Path]::IsPathRooted($XlsxPath)) { $XlsxPath } else { Join-Path $scriptDir $XlsxPath }
} else {
    $latest = Get-ChildItem $outDir -Filter "classification_from_files_*.xlsx" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if (-not $latest) { Write-Error "No classification_from_files_*.xlsx in output folder"; exit 1 }
    $xlsxFull = $latest.FullName
}

if (-not (Test-Path $xlsxFull)) { Write-Error "File not found: $xlsxFull"; exit 1 }
$xlsxName = [System.IO.Path]::GetFileName($xlsxFull)

$env:EXPERIMENT_SOURCE_FILE = $xlsxName
$env:EXPERIMENT_OUT_DIR = $outDir
$env:EXPERIMENT_SCHEMA = if ($env:EXPERIMENT_SCHEMA) { $env:EXPERIMENT_SCHEMA } else { "experiments" }

Write-Host "XLSX: $xlsxFull"
Write-Host "source_file (for pgvector): $xlsxName"
Write-Host ""

Write-Host "=== 1. Load to Postgres ==="
python load_results_to_postgres.py $xlsxFull
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
Write-Host "=== 2. pgvector: NN + LOOCV + export ==="
python pgvector_experiment.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Done. Enriched XLSX: output\classification_partial_pgvector.xlsx"
