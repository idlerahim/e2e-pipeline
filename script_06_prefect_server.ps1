# ============================================================
#  RocoMart Pipeline - Prefect Setup Script
# ============================================================

$ErrorActionPreference = "Stop"

# ── Helpers ──────────────────────────────────────────────────
function Write-Header($msg) {
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  $msg" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════════" -ForegroundColor Cyan
}

function Write-Step($msg) {
    Write-Host "  ► $msg" -ForegroundColor Yellow
}

function Write-OK($msg) {
    Write-Host "  ✔ $msg" -ForegroundColor Green
}

# ════════════════════════════════════════════════════════════
# STEP 1 — Clean stale Prefect DB files
# ════════════════════════════════════════════════════════════
Write-Header "STEP 1 — Cleaning stale Prefect DB files"

$dbFiles = @("prefect.db", "prefect.db-shm", "prefect.db-wal")

foreach ($file in $dbFiles) {
    $fullPath = Join-Path $PSScriptRoot $file
    if (Test-Path $fullPath) {
        Remove-Item $fullPath -Force
        Write-OK "Removed: $file"
    } else {
        Write-Step "Not found (skipping): $file"
    }
}

# ════════════════════════════════════════════════════════════
# STEP 2 — Launch Prefect Server in a new window
# ════════════════════════════════════════════════════════════
Write-Header "STEP 2 — Starting Prefect Server"

$serverScript = @'
$host.UI.RawUI.WindowTitle = "RocoMart — Prefect Server"
Write-Host "Configuring Prefect messaging broker..." -ForegroundColor Cyan
prefect config set PREFECT_MESSAGING_BROKER='prefect.server.utilities.messaging.memory'
Write-Host ""
Write-Host "Starting Prefect server..." -ForegroundColor Cyan
prefect server start
'@

$encodedServer = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($serverScript))

Start-Process powershell -ArgumentList "-NoExit", "-EncodedCommand", $encodedServer

Write-OK "Prefect Server window launched."

# ════════════════════════════════════════════════════════════
# STEP 3 — Wait for server to be ready
# ════════════════════════════════════════════════════════════
Write-Header "STEP 3 — Waiting for Prefect Server to be ready"

$maxWait   = 90   # seconds
$interval  = 3    # seconds between checks
$elapsed   = 0
$serverUp  = $false

Write-Step "Polling http://127.0.0.1:4200/api/health ..."

while ($elapsed -lt $maxWait) {
    Start-Sleep -Seconds $interval
    $elapsed += $interval

    try {
        $resp = Invoke-WebRequest -Uri "http://127.0.0.1:4200/api/health" `
                                  -UseBasicParsing -TimeoutSec 3 -ErrorAction Stop
        if ($resp.StatusCode -eq 200) {
            $serverUp = $true
            break
        }
    } catch {
        Write-Host "  … waiting ($elapsed s)" -ForegroundColor DarkGray
    }
}

if (-not $serverUp) {
    Write-Host ""
    Write-Host "  ✖ Prefect server did not respond within $maxWait seconds." -ForegroundColor Red
    Write-Host "    Check the Server window for errors, then re-run this script." -ForegroundColor Red
    exit 1
}

Write-OK "Prefect Server is up!"

# ════════════════════════════════════════════════════════════
# STEP 4 — Launch Work-Pool + Deploy + Worker in a new window
# ════════════════════════════════════════════════════════════
Write-Header "STEP 4 — Creating work-pool, deploying pipeline & starting worker"

# NOTE: `prefect work-pool create` asks two interactive prompts:
#   "Do you want to use remote storage?" → answer: n
#   "Would you like to schedule a run?"  → answer: n
# We pipe "n`nn`n" to send two newline-separated 'n' answers automatically.

$deployScript = @'
$host.UI.RawUI.WindowTitle = "RocoMart — Deploy & Worker"

Write-Host ""
Write-Host "Creating default-work-pool (answering NO to remote storage & schedule)..." -ForegroundColor Cyan
"n`nn`n" | prefect work-pool create -t process "default-work-pool"

Write-Host ""
Write-Host "Deploying RocoMart pipeline..." -ForegroundColor Cyan
prefect deploy orchestration/pipeline_flow.py:rocomart_data_pipeline `
    -n "RocoMart Pipeline" `
    -p default-work-pool

Write-Host ""
Write-Host "Starting worker on default-work-pool..." -ForegroundColor Cyan
python -m prefect worker start -p "default-work-pool"
'@

$encodedDeploy = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($deployScript))

Start-Process powershell -ArgumentList "-NoExit", "-EncodedCommand", $encodedDeploy

Write-OK "Deploy & Worker window launched."

# ════════════════════════════════════════════════════════════
# STEP 5 — Open Prefect UI
# ════════════════════════════════════════════════════════════
Write-Header "STEP 5 — Opening Prefect UI"

Start-Sleep -Seconds 3   # small delay so the browser doesn't race ahead
Start-Process "http://127.0.0.1:4200"

Write-OK "Browser opened → http://127.0.0.1:4200"

# ════════════════════════════════════════════════════════════
# Done!
# ════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "╔═══════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║   ✔  RocoMart Prefect Setup Complete!                 ║" -ForegroundColor Green
Write-Host "╠═══════════════════════════════════════════════════════╣" -ForegroundColor Green
Write-Host "║                                                       ║" -ForegroundColor Green
Write-Host "║  Prefect Server   → running in Window 1              ║" -ForegroundColor Green
Write-Host "║  Pipeline Deploy  → running in Window 2              ║" -ForegroundColor Green
Write-Host "║  Worker           → running in Window 2              ║" -ForegroundColor Green
Write-Host "║                                                       ║" -ForegroundColor Green
Write-Host "║  UI  →  http://127.0.0.1:4200                        ║" -ForegroundColor Green
Write-Host "║                                                       ║" -ForegroundColor Green
Write-Host "║  Navigate to  Flow Runs  to trigger &                ║" -ForegroundColor Green
Write-Host "║  monitor your RocoMart pipeline!                     ║" -ForegroundColor Green
Write-Host "║                                                       ║" -ForegroundColor Green
Write-Host "╚═══════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""

# Script exits; the two spawned windows stay alive independently.
