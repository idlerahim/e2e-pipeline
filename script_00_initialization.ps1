# =========================
# Setup Script (setup.ps1)
# =========================

# -------- Colors --------
function Info($msg)    { Write-Host "[INFO]  $msg" -ForegroundColor Cyan }
function Success($msg) { Write-Host "[OK]    $msg" -ForegroundColor Green }
function Warn($msg)    { Write-Host "[WARN]  $msg" -ForegroundColor Yellow }
function Skip($msg)    { Write-Host "[SKIP]  $msg" -ForegroundColor DarkYellow }
function ErrorMsg($msg){ Write-Host "[ERROR] $msg" -ForegroundColor Red }

function Ask-YesNo($message) {
    while ($true) {
        $response = Read-Host "$message (y/n)"
        if ($response -match '^[Yy]$') { return $true }
        elseif ($response -match '^[Nn]$') { return $false }
    }
}

function Is-VenvActive {
    return $env:VIRTUAL_ENV -ne $null
}

$summary = @()

# -------------------------
# 1. Virtual Environment
# -------------------------
Info "Step 1: Virtual Environment"

if (Test-Path ".venv") {
    if (Is-VenvActive) {
        Skip ".venv already active"
        $summary += "venv: already active"
    } else {
        if (Ask-YesNo ".venv exists but not active. Activate it?") {
            Info "Activating .venv"
            .\.venv\Scripts\Activate.ps1
            Success "venv activated"
            $summary += "venv: activated"
        } else {
            Skip "Skipped venv activation"
            $summary += "venv: skipped activation"
        }
    }
} else {
    if (Ask-YesNo ".venv not found. Create and activate it?") {
        Info "Creating .venv"
        python -m venv .venv
        .\.venv\Scripts\Activate.ps1
        Success "venv created and activated"
        $summary += "venv: created"
    } else {
        Skip "Skipped venv creation"
        $summary += "venv: not created"
    }
}

# -------------------------
# 2. Install Requirements
# -------------------------
Info "Step 2: Install Dependencies"

if (Is-VenvActive) {
    Skip "venv is active -> skipping pip install (as requested)"
    $summary += "requirements: skipped (venv active)"
} else {
    if (Ask-YesNo "venv not active. Install requirements?") {
        if (Test-Path "requirements.txt") {
            Info "Installing dependencies"
            pip install -r requirements.txt
            Success "dependencies installed"
            $summary += "requirements: installed"
        } else {
            Warn "requirements.txt not found"
            $summary += "requirements: missing file"
        }
    } else {
        Skip "Skipped installing dependencies"
        $summary += "requirements: skipped"
    }
}

# -------------------------
# 3. Git + DVC Setup
# -------------------------
Info "Step 3: Git & DVC"

if (Test-Path ".git") {
    Skip "Git already initialized"
    $summary += "git/dvc: already initialized"
} else {
    if (Ask-YesNo "Initialize Git and DVC?") {

        # --- Git ---
        Info "Initializing Git"
        git init
        git add .
        if (Test-Path ".gitignore") { git add .gitignore }
        if (Test-Path ".dvcignore") { git add .dvcignore }
        git commit -m "initial commit"
        Success "Git initialized"

        # --- DVC ---
        Info "Initializing DVC"
        dvc init
        git add .dvc .dvcignore
        git commit -m "initialize dvc"

        $paths = @("data_lake", "dataset", "models", "mlruns")
        foreach ($p in $paths) {
            if (Test-Path $p) {
                Info "Tracking $p"
                dvc add $p
            } else {
                Warn "$p not found"
            }
        }

        Success "DVC initialized"
        $summary += "git/dvc: initialized"

        # -------------------------
        # 4. DVC Remote
        # -------------------------
        Info "Step 4: DVC Remote"

        if (Ask-YesNo "Setup local DVC remote (/tmp/dvc-storage)?") {
            Info "Adding remote"
            dvc remote add -d local_storage /tmp/dvc-storage
            dvc push
            dvc pull
            Success "DVC remote configured"
            $summary += "dvc remote: configured"
        } else {
            Skip "Skipped DVC remote setup"
            $summary += "dvc remote: skipped"
        }

    } else {
        Skip "Skipped Git & DVC setup"
        $summary += "git/dvc: skipped"
    }
}

# -------------------------
# Summary
# -------------------------
Write-Host "`n========== SUMMARY ==========" -ForegroundColor Magenta
foreach ($item in $summary) {
    Write-Host "- $item" -ForegroundColor White
}
Write-Host "=============================" -ForegroundColor Magenta