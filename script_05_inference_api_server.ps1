# ============================================================
#  RocoMart Pipeline - Inference API Setup Script
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
# STEP 1 — Launch Inference API in a new window
# ════════════════════════════════════════════════════════════
Write-Header "Launching RocoMart Inference API"

# We wrap the command in a script block for the new window
$apiScript = @'
$host.UI.RawUI.WindowTitle = "RocoMart — Inference API (Port 8000)"
Write-Host "Starting Inference API..." -ForegroundColor Cyan
Write-Host "Host: 0.0.0.0 | Port: 8000" -ForegroundColor Gray
Write-Host ""

python -m inference.inference_api --port 8000 --host 0.0.0.0
'@

# Encode the command to handle complex arguments safely
$encodedApi = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($apiScript))

# Start the process in a new window (-NoExit keeps it open if the process crashes/finishes)
Start-Process powershell -ArgumentList "-NoExit", "-EncodedCommand", $encodedApi

Write-OK "Inference API window launched."

# ════════════════════════════════════════════════════════════
# Done!
# ════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "╔═══════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║   ✔  Inference API Setup Initiated!                   ║" -ForegroundColor Green
Write-Host "╠═══════════════════════════════════════════════════════╣" -ForegroundColor Green
Write-Host "║                                                       ║" -ForegroundColor Green
Write-Host "║  API Status   → running in New Window                 ║" -ForegroundColor Green
Write-Host "║  Endpoint     → http://localhost:8000                 ║" -ForegroundColor Green
Write-Host "║                                                       ║" -ForegroundColor Green
Write-Host "╚═══════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""