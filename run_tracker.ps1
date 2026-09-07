# run_tracker.ps1 -- Full canonical OSINT pipeline.
# Runs, in order: multi-source ingestion -> FIRMS thermal ingest -> AI analysis
# -> artifact export. Each stage logs to logs/ and a failed stage does not
# abort the remaining stages (exit code is non-zero if any stage fails).
#
# Usage:
#   .\run_tracker.ps1                  # full pipeline
#   .\run_tracker.ps1 -SkipAnalysis    # ingest + export only (no LLM calls)
#   .\run_tracker.ps1 -SkipIngest      # analysis + export only

param(
    [switch]$SkipIngest,
    [switch]$SkipAnalysis
)

$ErrorActionPreference = "Continue"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { $Python = "python" }

$LogDir = Join-Path $ProjectRoot "logs"
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }
$RunLog = Join-Path $LogDir "run_tracker.log"

function Write-Log([string]$Message) {
    $Line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $Message"
    Add-Content -Path $RunLog -Value $Line -Encoding UTF8
    Write-Host $Line
}

function Invoke-Stage([string]$Name, [string]$ScriptPath, [string[]]$Arguments = @()) {
    Write-Log "START $Name"
    & $Python $ScriptPath @Arguments 2>&1 | ForEach-Object {
        Add-Content -Path $RunLog -Value $_ -Encoding UTF8
        Write-Host $_
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Log "FAIL  $Name (exit $LASTEXITCODE)"
        return $false
    }
    Write-Log "OK    $Name"
    return $true
}

$failures = 0
Write-Log "=== Pipeline run started ==="

if (-not $SkipIngest) {
    if (-not (Invoke-Stage "master_ingestor" "scripts\master_ingestor.py")) { $failures++ }
    if (-not (Invoke-Stage "firms_thermal"   "map_loader.py")) { $failures++ }
}

if (-not $SkipAnalysis) {
    if (-not (Invoke-Stage "ai_agent"        "scripts\ai_agent.py")) { $failures++ }
}

if (-not (Invoke-Stage "generate_output" "scripts\generate_output.py")) { $failures++ }

if ($failures -gt 0) {
    Write-Log "=== Pipeline finished with $failures failed stage(s) ==="
    exit 1
}
Write-Log "=== Pipeline finished successfully ==="
exit 0
