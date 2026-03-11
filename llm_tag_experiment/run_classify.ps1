# Запуск LLM-классификации назначений платежей.
# Выполни в PowerShell из папки scripts\llm_tag_experiment:
#   .\run_classify.ps1
#
# Креды и параметры LLM (LLM_API_KEY, LLM_BASE_URL, LLM_MODEL и т.д.) не заданы в скрипте.
# Задай их в окружении или положи .env в эту папку / в корень репозитория (см. .env.example).

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

$outDir = Join-Path $scriptDir "output"
$latest = Get-ChildItem $outDir -Filter "extracted_transactions_*.csv" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $latest) { Write-Error "No extracted_transactions_*.csv in output folder"; exit 1 }
$env:EXTRACTED_CSV_PATH = $latest.FullName
$env:PYTHONUNBUFFERED = "1"

Write-Host "Input CSV: $env:EXTRACTED_CSV_PATH"
Write-Host "Running classifier..."
python -u .\classify_payment_purposes_llm.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "Done. Check output folder."
