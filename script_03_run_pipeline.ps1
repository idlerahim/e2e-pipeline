<#
.SYNOPSIS
    RecoMart End-to-End Pipeline Runner
.DESCRIPTION
    Executes all 9 pipeline tasks in sequence.
    Confirms once with the user, then runs without interruption.
    Prints PASS / FAIL after every task and a summary table at the end.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"          # keep going even on non-terminating errors

# ─────────────────────────────────────────────────────────────────────────────
#  Result Tracker
# ─────────────────────────────────────────────────────────────────────────────
$Results = [System.Collections.Generic.List[PSCustomObject]]::new()

function Add-Result {
    param([string]$Task, [string]$Input, [string]$Output, [bool]$Pass)
    $Results.Add([PSCustomObject]@{
            Task   = $Task
            Input  = $Input
            Output = $Output
            Status = if ($Pass) { "✔  PASS" } else { "✘  FAIL" }
        })
}

# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────
function Write-TaskHeader {
    param([int]$Num, [string]$Desc)
    Write-Host ""
    Write-Host ("  " + ("─" * 68)) -ForegroundColor DarkGray
    Write-Host ("  [Task $Num / 9]  $Desc") -ForegroundColor Yellow
    Write-Host ("  " + ("─" * 68)) -ForegroundColor DarkGray
    Write-Host ""
}

function Write-Status {
    param([bool]$Pass, [string]$Label)
    Write-Host ""
    if ($Pass) {
        Write-Host "  ✔  $Label  →  PASS" -ForegroundColor Green
    }
    else {
        Write-Host "  ✘  $Label  →  FAIL" -ForegroundColor Red
    }
}

function Invoke-Step {
    <#  Run a command string, echo it first, return bool success.
        Output is piped to Out-Host so stdout/stderr stream to the console
        but never pollute the function's return value (which must be [bool]).  #>
    param([string]$Cmd)
    Write-Host "    » $Cmd" -ForegroundColor DarkCyan
    Invoke-Expression $Cmd | Out-Host
    return [bool]($LASTEXITCODE -eq 0)
}

function Invoke-StepCapture {
    <#  Run a command string, capture + stream output, return @{Ok; Lines}
        $LASTEXITCODE is read before any other PS command can clobber it.  #>
    param([string]$Cmd)
    Write-Host "    » $Cmd" -ForegroundColor DarkCyan
    $lines = Invoke-Expression "$Cmd 2>&1"
    $ec = $LASTEXITCODE          # capture immediately
    $lines | ForEach-Object { Write-Host "    $_" }
    return @{ Ok = [bool]($ec -eq 0); Lines = $lines }
}

function Get-LatestFSDir {
    <#  Return the FullName of the highest v_* directory in the feature store #>
    $dir = Get-ChildItem "data_lake/serving/feature_store" `
        -Directory -Filter 'v_*' -ErrorAction SilentlyContinue |
    Sort-Object Name |
    Select-Object -Last 1
    if ($dir) { return $dir.FullName }
    return $null
}

function Query-SQLite {
    <#  Run a sqlite3 query and return the result rows as an array of strings.
        The leading comma (,) forces PowerShell to preserve the array wrapper
        even when the result contains only one item.                          #>
    param([string]$DbPath, [string]$Query)
    try {
        $rows = sqlite3 $DbPath $Query 2>$null
        return , @($rows | Where-Object { $_ -ne "" })
    }
    catch {
        return , @()
    }
}

# ─────────────────────────────────────────────────────────────────────────────
#  Pre-flight Banner & One-Time Confirmation
# ─────────────────────────────────────────────────────────────────────────────
Clear-Host
Write-Host ""
Write-Host "  ╔══════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "  ║              RecoMart  •  End-to-End Pipeline Runner             ║" -ForegroundColor Cyan
Write-Host "  ╚══════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""
Write-Host "  9 tasks will run sequentially — no further prompts after you confirm." -ForegroundColor White
Write-Host ""

$summary = @(
    [PSCustomObject]@{ N = 1; Summary = "Problem Formulation    Print project goal & scope to console" },
    [PSCustomObject]@{ N = 2; Summary = "Data Ingestion         Run CSV ingestion  (venv / git / dvc setup skipped)" },
    [PSCustomObject]@{ N = 3; Summary = "Raw Data Storage       Generate catalog & stats; verify checksums" },
    [PSCustomObject]@{ N = 4; Summary = "Data Validation        Schema, type & quality checks on raw data" },
    [PSCustomObject]@{ N = 5; Summary = "Data Preparation       Clean, merge & produce train/test splits" },
    [PSCustomObject]@{ N = 6; Summary = "Feature Engineering    Build user, item & interaction feature sets" },
    [PSCustomObject]@{ N = 7; Summary = "Feature Store          Register snapshot, query features, build training sets" },
    [PSCustomObject]@{ N = 8; Summary = "Data Versioning        Informational note  (model versioning skipped)" },
    [PSCustomObject]@{ N = 9; Summary = "Model Training & Pred  Train KNN" }
)

foreach ($t in $summary) {
    Write-Host ("  Task {0,-2}  │  {1}" -f $t.N, $t.Summary) -ForegroundColor Gray
}

Write-Host ""
$confirm = Read-Host "  ► Proceed with all 9 tasks? (press Enter or Y to continue / N to abort)"
if ($confirm -match '^[Nn]') {
    Write-Host ""
    Write-Host "  Pipeline aborted by user." -ForegroundColor Red
    exit 0
}
Write-Host ""
Write-Host "  Starting pipeline …" -ForegroundColor Cyan

# Ensure Python subprocesses can print Unicode (✓, ╫, etc.) on Windows consoles
$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# ═════════════════════════════════════════════════════════════════════════════
#  TASK 1 — Problem Formulation
# ═════════════════════════════════════════════════════════════════════════════
Write-TaskHeader 1 "Problem Formulation"

Write-Host "  RecoMart Pipeline : Building a personalized product recommendation system" -ForegroundColor Cyan
Write-Host "  Goal              : Ingest, validate & serve data to boost conversions and reduce churn." -ForegroundColor Gray
Write-Host ""

$t1Pass = $true     # purely informational — no executable to fail
Write-Status $t1Pass "Task 1 – Problem Formulation"
Add-Result "1 – Problem Formulation" "N/A" "Console output" $t1Pass

# ═════════════════════════════════════════════════════════════════════════════
#  TASK 2 — Data Ingestion
# ═════════════════════════════════════════════════════════════════════════════
Write-TaskHeader 2 "Data Ingestion  (venv / git / dvc setup skipped)"

Write-Host "  ℹ  Environment setup, Git initialisation & DVC configuration are skipped." -ForegroundColor DarkYellow
Write-Host "  ℹ  Running CSV ingestion only." -ForegroundColor DarkYellow
Write-Host ""

$t2Pass = Invoke-Step "python run_ingestion.py --mode csv"

Write-Status $t2Pass "Task 2 – Data Ingestion"
Add-Result "2 – Data Ingestion" "Raw CSV source files" "data_lake/raw/*" $t2Pass

# ═════════════════════════════════════════════════════════════════════════════
#  TASK 3 — Raw Data Storage
# ═════════════════════════════════════════════════════════════════════════════
Write-TaskHeader 3 "Raw Data Storage  (catalog, stats & checksum verification)"

Write-Host "  [3.1] Generating data catalog and statistics …" -ForegroundColor White
$t3a = Invoke-Step "python -m storage.storage_manager"

Write-Host ""
Write-Host "  [3.2] Verifying checksums …" -ForegroundColor White
$t3b = Invoke-Step "python -m storage.storage_manager --verify"

$t3Pass = $t3a -and $t3b
Write-Status $t3Pass "Task 3 – Raw Data Storage"
Add-Result "3 – Raw Data Storage" "data_lake/raw/*" "data_catalog.json, checksums.json, stats" $t3Pass

# ═════════════════════════════════════════════════════════════════════════════
#  TASK 4 — Data Validation
# ═════════════════════════════════════════════════════════════════════════════
Write-TaskHeader 4 "Data Validation  (schema, types & quality checks)"

$t4Pass = Invoke-Step "python -m validation.validate_data"

Write-Status $t4Pass "Task 4 – Data Validation"
Add-Result "4 – Data Validation" "data_lake/raw/*" "validation_report.json" $t4Pass

# ═════════════════════════════════════════════════════════════════════════════
#  TASK 5 — Data Preparation
# ═════════════════════════════════════════════════════════════════════════════
Write-TaskHeader 5 "Data Preparation  (clean, merge & train/test split)"

$t5Pass = Invoke-Step "python -m preparation.prepare_data"

Write-Status $t5Pass "Task 5 – Data Preparation"
Add-Result "5 – Data Preparation" "data_lake/raw/*" "data_lake/processed/* (train / test splits)" $t5Pass

# ═════════════════════════════════════════════════════════════════════════════
#  TASK 6 — Feature Engineering
# ═════════════════════════════════════════════════════════════════════════════
Write-TaskHeader 6 "Feature Engineering  (user / item / interaction features)"

$t6Pass = Invoke-Step "python -m transformation.feature_engineering"

Write-Status $t6Pass "Task 6 – Feature Engineering"
Add-Result "6 – Feature Engineering" "data_lake/processed/*" "data_lake/features/* (user_features, item_features)" $t6Pass

# ═════════════════════════════════════════════════════════════════════════════
#  TASK 7 — Feature Store
# ═════════════════════════════════════════════════════════════════════════════
Write-TaskHeader 7 "Feature Store  (register → status → training sets → PIT → user/item queries)"

$t7Pass = $true

# 7.1 — Register snapshot
Write-Host "  [7.1] Registering feature snapshot from Task 6 output …" -ForegroundColor White
$ok = Invoke-Step "python -m feature_store.feature_store_manager --register"
$t7Pass = $t7Pass -and $ok

# 7.2 — Store status
Write-Host ""
Write-Host "  [7.2] Checking feature store status …" -ForegroundColor White
$ok = Invoke-Step "python -m feature_store.feature_store_manager --status"
$t7Pass = $t7Pass -and $ok

# 7.3 — Training set (full)
Write-Host ""
Write-Host "  [7.3] Generating full training set …" -ForegroundColor White
$ok = Invoke-Step "python -m feature_store.feature_store_manager --training-set"
$t7Pass = $t7Pass -and $ok

# # 7.3b — Training set (sampled)
# Write-Host ""
# Write-Host "  [7.3b] Generating sampled training set (n=10 000) …" -ForegroundColor White
# $ok = Invoke-Step "python -m feature_store.feature_store_manager --training-set --sample 10000"
# $t7Pass = $t7Pass -and $ok

# 7.4 — Point-in-time retrieval
# Use current datetime (not just date) so it is always >= any snapshot created
# earlier today. The feature store parses this as a full timestamp.
# $pitDate = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
# Write-Host ""
# Write-Host "  [7.4] Point-in-time retrieval  (PIT = $pitDate — now, covers latest snapshot) …" -ForegroundColor White
# $ok = Invoke-Step "python -m feature_store.feature_store_manager --training-set --pit `"$pitDate`""
# $t7Pass = $t7Pass -and $ok

# ── Resolve latest feature store DB ─────────────────────────────────────────
Write-Host ""
Write-Host "  [7.5] Resolving latest feature store version directory …" -ForegroundColor White
$latestFSDir = Get-LatestFSDir

# 7.5 — Dynamic user query
$fallbackUsers = "0006fdc98a402fceb4eb0ee528f6a8d4, 00c04df1c94e385d57d4a33a2965217c"
$queryUsersArg = $fallbackUsers

if ($latestFSDir) {
    $fsDb = Join-Path $latestFSDir "features.db"
    Write-Host "    » sqlite3 `"$fsDb`" `"SELECT DISTINCT customer_unique_id FROM user_features LIMIT 20;`"" -ForegroundColor DarkCyan
    $userRows = Query-SQLite $fsDb "SELECT DISTINCT customer_unique_id FROM user_features LIMIT 20;"
    if ($userRows.Count -ge 2) {
        $queryUsersArg = "$($userRows[0].Trim()), $($userRows[1].Trim())"
        Write-Host "    ✔ Resolved users  → $queryUsersArg" -ForegroundColor DarkGreen
    }
    elseif ($userRows.Count -eq 1) {
        $queryUsersArg = $userRows[0].Trim()
        Write-Host "    ✔ Resolved user   → $queryUsersArg" -ForegroundColor DarkGreen
    }
    else {
        Write-Host "    ⚠  No user rows returned — falling back to hardcoded IDs." -ForegroundColor DarkYellow
    }
}
else {
    Write-Host "    ⚠  Feature store directory not found — using fallback user IDs." -ForegroundColor DarkYellow
}

Write-Host ""
Write-Host "  [7.5] Querying user features for: $queryUsersArg" -ForegroundColor White
$ok = Invoke-Step "python -m feature_store.feature_store_manager --query-users `"$queryUsersArg`""
$t7Pass = $t7Pass -and $ok

# 7.6 — Dynamic item query
$fallbackItems = "0030e635639c898b323826589761cf23, 00ab8a8b9fe219511dc3f178c6d79698"
$queryItemsArg = $fallbackItems

if ($latestFSDir) {
    $fsDb = Join-Path $latestFSDir "features.db"
    Write-Host ""
    Write-Host "    » sqlite3 `"$fsDb`" `"SELECT DISTINCT product_id FROM item_features LIMIT 20;`"" -ForegroundColor DarkCyan
    $itemRows = Query-SQLite $fsDb "SELECT DISTINCT product_id FROM item_features LIMIT 20;"
    if ($itemRows.Count -ge 2) {
        $queryItemsArg = "$($itemRows[0].Trim()), $($itemRows[1].Trim())"
        Write-Host "    ✔ Resolved items  → $queryItemsArg" -ForegroundColor DarkGreen
    }
    elseif ($itemRows.Count -eq 1) {
        $queryItemsArg = $itemRows[0].Trim()
        Write-Host "    ✔ Resolved item   → $queryItemsArg" -ForegroundColor DarkGreen
    }
    else {
        Write-Host "    ⚠  No item rows returned — falling back to hardcoded IDs." -ForegroundColor DarkYellow
    }
}
else {
    Write-Host "    ⚠  Feature store directory not found — using fallback item IDs." -ForegroundColor DarkYellow
}

Write-Host ""
Write-Host "  [7.6] Querying item features for: $queryItemsArg" -ForegroundColor White
$ok = Invoke-Step "python -m feature_store.feature_store_manager --query-items `"$queryItemsArg`""
$t7Pass = $t7Pass -and $ok

Write-Status $t7Pass "Task 7 – Feature Store"
Add-Result "7 – Feature Store" "data_lake/features/*" "feature_store snapshot, training sets, PIT dataset, user/item queries" $t7Pass

# ═════════════════════════════════════════════════════════════════════════════
#  TASK 8 — Data Versioning  (informational)
# ═════════════════════════════════════════════════════════════════════════════
Write-TaskHeader 8 "Data Versioning  (model versioning skipped)"

Write-Host "  ℹ  Model versioning via DVC / MLflow is skipped in this automated run." -ForegroundColor DarkYellow
Write-Host "  ℹ  Data artefacts from Tasks 2–7 are tracked via storage checksums (Task 3)." -ForegroundColor DarkYellow
Write-Host "  ℹ  To enable full versioning: run  dvc add  +  git commit  after each stage." -ForegroundColor DarkYellow
Write-Host ""

$t8Pass = $true
Write-Status $t8Pass "Task 8 – Data Versioning"
Add-Result "8 – Data Versioning" "N/A (skipped)" "Informational — no artefacts generated" $t8Pass

# ═════════════════════════════════════════════════════════════════════════════
#  TASK 9 — Model Training & Prediction
# ═════════════════════════════════════════════════════════════════════════════
Write-TaskHeader 9 "Model Training & Prediction  (KNN)"

$t9Pass = $true

# 9.1 — Train models; capture output to extract a real user ID
Write-Host ""
Write-Host "  [9.1] Training models (KNN) …" -ForegroundColor White
Write-Host "    » python -m models.model_training" -ForegroundColor DarkCyan

$trainLines = python -m models.model_training 2>&1
$trainEc = $LASTEXITCODE          # capture before anything else runs
$trainLines | ForEach-Object { Write-Host "    $_" }
$trainOk = [bool]($trainEc -eq 0)
$t9Pass = $t9Pass -and $trainOk

Write-Status $t9Pass "Task 9 – Model Training & Prediction"
Add-Result "9 – Model Training & Pred" "feature_store training set" "Trained models (KNN), recommendation lists, rating predictions" $t9Pass

# ═════════════════════════════════════════════════════════════════════════════
#  FINAL SUMMARY TABLE
# ═════════════════════════════════════════════════════════════════════════════
Write-Host ""
Write-Host ""
Write-Host "  ╔══════════════════════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "  ║                        Pipeline Execution Summary                               ║" -ForegroundColor Cyan
Write-Host "  ╚══════════════════════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Column widths
$w1 = 28   # Task
$w2 = 36   # Input
$w3 = 52   # Output
$w4 = 9    # Status
$sep = "  " + ("─" * ($w1 + $w2 + $w3 + $w4 + 6))

# Header
$hdr = "  {0,-$w1}  {1,-$w2}  {2,-$w3}  {3,-$w4}" -f "Task", "Input (files)", "Output (generated)", "Status"
Write-Host $hdr -ForegroundColor White
Write-Host $sep -ForegroundColor DarkGray

foreach ($r in $Results) {
    $color = if ($r.Status -match "PASS") { "Green" } else { "Red" }
    $row = "  {0,-$w1}  {1,-$w2}  {2,-$w3}  {3,-$w4}" -f $r.Task, $r.Input, $r.Output, $r.Status
    Write-Host $row -ForegroundColor $color
}

Write-Host $sep -ForegroundColor DarkGray

$passCount = @($Results | Where-Object { $_.Status -match "PASS" }).Count
$failCount = @($Results | Where-Object { $_.Status -match "FAIL" }).Count

Write-Host ""
Write-Host ("  Completed  │  {0} / {1} tasks PASSED   │   {2} FAILED" -f $passCount, $Results.Count, $failCount) -ForegroundColor White
Write-Host ""

if ($failCount -eq 0) {
    Write-Host "  ✔  All tasks completed successfully. RecoMart pipeline is ready." -ForegroundColor Green
}
else {
    Write-Host "  ✘  $failCount task(s) failed. Review the output above for details." -ForegroundColor Red
}

Write-Host ""