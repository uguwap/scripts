# Запуск LLM-классификации назначений платежей.
# Выполни в PowerShell из папки scripts\llm_tag_experiment:
#   .\run_classify.ps1

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

$csvName = "extracted_transactions_20260310_114243.csv"
$outDir = Join-Path $scriptDir "output"
$csvPath = Join-Path $outDir $csvName
if (-not (Test-Path $csvPath)) {
    $latest = Get-ChildItem $outDir -Filter "extracted_transactions_*.csv" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($latest) { $csvPath = $latest.FullName } else { Write-Error "No extracted CSV in output folder"; exit 1 }
}

$env:EXTRACTED_CSV_PATH = $csvPath
$env:LLM_API_KEY = "Fp6BKzEAzCxLJUTgfV6T4BNyrOV6V9eM0nkSxTd9+rY="
$env:LLM_BASE_URL = "https://neuro.sspb.ru/v1"
$env:LLM_MODEL = "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B"
$env:LLM_PROVIDER = "claude"
$env:LLM_MAX_TOKENS = "4096"
$env:LLM_TEMPERATURE = "0.0"

Write-Host "Input CSV: $env:EXTRACTED_CSV_PATH"
Write-Host "Running classifier..."
python .\classify_payment_purposes_llm.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "Done. Check output folder."
