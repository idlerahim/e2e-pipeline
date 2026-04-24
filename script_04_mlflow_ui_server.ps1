# ============================================================
#  RocoMart Pipeline - MLflow UI Setup Script
# ============================================================

$ErrorActionPreference = "Stop"

# ── Helpers ──────────────────────────────────────────────────
function Write-Header($msg) {
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  $msg" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════════" -ForegroundColor Cyan
}

function Write-OK($msg) {
    Write-Host "  ✔ $msg" -ForegroundColor Green
}

# ════════════════════════════════════════════════════════════
# STEP 1 — Launch MLflow UI in a new window
# ════════════════════════════════════════════════════════════
Write-Header "Launching MLflow Tracking UI"

$mlflowScript = @'
$host.UI.RawUI.WindowTitle = "RocoMart — MLflow UI"
Write-Host "Starting MLflow UI..." -ForegroundColor Cyan
Write-Host "Default Address: http://127.0.0.1:5000" -ForegroundColor Gray
Write-Host ""

mlflow ui
'@

$encodedMlflow = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($mlflowScript))

Start-Process powershell -ArgumentList "-NoExit", "-EncodedCommand", $encodedMlflow

Write-OK "MLflow UI window launched."

# ════════════════════════════════════════════════════════════
# STEP 2 — Open MLflow in Browser
# ════════════════════════════════════════════════════════════
Write-Header "Opening MLflow Interface"

Start-Sleep -Seconds 2
Start-Process "http://127.0.0.1:5000"

Write-OK "Browser opened → http://127.0.0.1:5000"

# ════════════════════════════════════════════════════════════
# Done!
# ════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "╔═══════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║   ✔  MLflow Setup Complete!                           ║" -ForegroundColor Green
Write-Host "╠═══════════════════════════════════════════════════════╣" -ForegroundColor Green
Write-Host "║                                                       ║" -ForegroundColor Green
Write-Host "║  MLflow Server → running in New Window                ║" -ForegroundColor Green
Write-Host "║  UI Address    → http://127.0.0.1:5000                ║" -ForegroundColor Green
Write-Host "║                                                       ║" -ForegroundColor Green
Write-Host "╚═══════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""