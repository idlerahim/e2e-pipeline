#Requires -Version 5.1
<#
.SYNOPSIS
    RecoMart Super Script — unified launcher for all project scripts.

.DESCRIPTION
    Displays a numbered activity menu and accepts:
      • A single number  (e.g.  3 )       → run that one activity
      • A range          (e.g.  1-5 )     → run activities 1 through 5 in order
      • "all"                             → run every activity (0-14) in order

    Inputs 3-11 all map to script_03_run_pipeline.ps1 (the 9-task pipeline);
    when a range covers several of those IDs the pipeline script runs only once.

    All console output is mirrored to:   logs/super_script.log

.NOTES
    Place this file in the same folder as the individual script_0x_*.ps1 files.
    Run with:   .\super_script.ps1
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"
$ProgressPreference = "SilentlyContinue"

# ─────────────────────────────────────────────────────────────────────────────
#  Paths & Logging
# ─────────────────────────────────────────────────────────────────────────────
$ScriptDir = $PSScriptRoot
$LogDir = Join-Path $ScriptDir "logs"
$LogFile = Join-Path $LogDir "super_script.log"

if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

# Start-Transcript mirrors everything (stdout + stderr) to the log file.
# Append so previous runs are preserved.
Start-Transcript -Path $LogFile -Append -Force | Out-Null

# ─────────────────────────────────────────────────────────────────────────────
#  Colour Helpers
# ─────────────────────────────────────────────────────────────────────────────
function Write-Header {
    param([string]$Text)
    $line = "═" * 72
    Write-Host ""
    Write-Host "  ╔$line╗" -ForegroundColor Cyan
    Write-Host ("  ║  {0,-70}║" -f $Text) -ForegroundColor Cyan
    Write-Host "  ╚$line╝" -ForegroundColor Cyan
    Write-Host ""
}

function Write-Section {
    param([string]$Text)
    Write-Host ""
    Write-Host "  ┌─ $Text" -ForegroundColor Yellow
    Write-Host "  └$("─" * 68)" -ForegroundColor DarkGray
    Write-Host ""
}

function Write-LogInfo { param([string]$m) Write-Host "  [INFO]  $m" -ForegroundColor Cyan }
function Write-LogOk { param([string]$m) Write-Host "  [OK]    $m" -ForegroundColor Green }
function Write-LogWarn { param([string]$m) Write-Host "  [WARN]  $m" -ForegroundColor Yellow }
function Write-LogError { param([string]$m) Write-Host "  [ERROR] $m" -ForegroundColor Red }
function Write-LogSkip { param([string]$m) Write-Host "  [SKIP]  $m" -ForegroundColor DarkYellow }

# ─────────────────────────────────────────────────────────────────────────────
#  Activity / Menu Table
#  Each entry: Id, Label, Script (filename relative to $ScriptDir)
# ─────────────────────────────────────────────────────────────────────────────
$Activities = @(
    [PSCustomObject]@{
        Id     = 0
        Label  = "Initialization  (venv · git · dvc · remote setup)"
        Script = "script_00_initialization.ps1"
        Group  = "Setup"
    }
    [PSCustomObject]@{
        Id     = 1
        Label  = "Clean Project  (purge data_lake, logs, mlruns, etc.)"
        Script = "script_01_clean_project.ps1"
        Group  = "Setup"
    }
    [PSCustomObject]@{
        Id     = 2
        Label  = "Download & Extract Data"
        Script = "script_02_download_extract_data.ps1"
        Group  = "Data"
    }
    # ── Pipeline tasks 1-9 (all handled by script_03_run_pipeline.ps1) ──────
    [PSCustomObject]@{
        Id     = 3
        Label  = "Pipeline – Task 1 : Problem Formulation"
        Script = "script_03_run_pipeline.ps1"
        Group  = "Pipeline"
    }
    [PSCustomObject]@{
        Id     = 4
        Label  = "Pipeline – Task 2 : Data Ingestion"
        Script = "script_03_run_pipeline.ps1"
        Group  = "Pipeline"
    }
    [PSCustomObject]@{
        Id     = 5
        Label  = "Pipeline – Task 3 : Raw Data Storage"
        Script = "script_03_run_pipeline.ps1"
        Group  = "Pipeline"
    }
    [PSCustomObject]@{
        Id     = 6
        Label  = "Pipeline – Task 4 : Data Validation"
        Script = "script_03_run_pipeline.ps1"
        Group  = "Pipeline"
    }
    [PSCustomObject]@{
        Id     = 7
        Label  = "Pipeline – Task 5 : Data Preparation"
        Script = "script_03_run_pipeline.ps1"
        Group  = "Pipeline"
    }
    [PSCustomObject]@{
        Id     = 8
        Label  = "Pipeline – Task 6 : Feature Engineering"
        Script = "script_03_run_pipeline.ps1"
        Group  = "Pipeline"
    }
    [PSCustomObject]@{
        Id     = 9
        Label  = "Pipeline – Task 7 : Feature Store"
        Script = "script_03_run_pipeline.ps1"
        Group  = "Pipeline"
    }
    [PSCustomObject]@{
        Id     = 10
        Label  = "Pipeline – Task 8 : Data Versioning"
        Script = "script_03_run_pipeline.ps1"
        Group  = "Pipeline"
    }
    [PSCustomObject]@{
        Id     = 11
        Label  = "Pipeline – Task 9 : Model Training & Prediction"
        Script = "script_03_run_pipeline.ps1"
        Group  = "Pipeline"
    }
    # ── Servers ──────────────────────────────────────────────────────────────
    [PSCustomObject]@{
        Id     = 12
        Label  = "MLflow UI Server"
        Script = "script_04_mlflow_ui_server.ps1"
        Group  = "Servers"
    }
    [PSCustomObject]@{
        Id     = 13
        Label  = "Inference API Server"
        Script = "script_05_inference_api_server.ps1"
        Group  = "Servers"
    }
    [PSCustomObject]@{
        Id     = 14
        Label  = "Perfect Server"
        Script = "script_06_perfect_server.ps1"
        Group  = "Servers"
    }
)

# ─────────────────────────────────────────────────────────────────────────────
#  Show Menu
# ─────────────────────────────────────────────────────────────────────────────
function Show-Menu {
    Clear-Host
    Write-Header "RecoMart  •  Super Script  —  Activity Launcher"

    $currentGroup = ""
    foreach ($act in $Activities) {
        if ($act.Group -ne $currentGroup) {
            $currentGroup = $act.Group
            Write-Host ("  ── {0} {1}" -f $currentGroup, ("─" * (60 - $currentGroup.Length))) -ForegroundColor DarkGray
        }
        Write-Host ("  [{0,2}]  {1}" -f $act.Id, $act.Label) -ForegroundColor White
    }

    Write-Host ""
    Write-Host "  ── Input Examples " + ("─" * 52) -ForegroundColor DarkGray
    Write-Host "  Single   :  3          → run activity 3 only" -ForegroundColor Gray
    Write-Host "  Range    :  1-5        → run activities 1, 2, 3 (pipeline), 4, 5 in order" -ForegroundColor Gray
    Write-Host "  All      :  all        → run every activity (0 → 14) in order" -ForegroundColor Gray
    Write-Host "  Quit     :  q / exit   → exit without running anything" -ForegroundColor Gray
    Write-Host ""
    Write-Host ("  Log file : {0}" -f $LogFile) -ForegroundColor DarkGray
    Write-Host ""
}

# ─────────────────────────────────────────────────────────────────────────────
#  Parse User Input  →  returns an ordered list of activity IDs
# ─────────────────────────────────────────────────────────────────────────────
function Resolve-InputToIds {
    param([string]$Raw)

    $raw = $Raw.Trim()

    # "all"
    if ($raw -ieq "all") {
        return (0..14)
    }

    # Range  "N-M"
    if ($raw -match '^(\d+)-(\d+)$') {
        [int]$from = [int]$Matches[1]
        [int]$to = [int]$Matches[2]
        if ($from -gt $to) { $from, $to = $to, $from }   # swap if backwards
        if ($from -lt 0 -or $to -gt 14) {
            Write-LogWarn "Range must be within 0-14. Got: $raw"
            return $null
        }
        return ($from..$to)
    }

    # Single number
    if ($raw -match '^\d+$') {
        [int]$id = [int]$raw
        if ($id -lt 0 -or $id -gt 14) {
            Write-LogWarn "ID must be 0-14. Got: $id"
            return $null
        }
        return @($id)
    }

    Write-LogWarn "Unrecognised input: '$raw'"
    return $null
}

# ─────────────────────────────────────────────────────────────────────────────
#  Build Execution Plan
#  Deduplicate the same script file while preserving order; this ensures
#  script_03_run_pipeline.ps1 runs only once even when several pipeline IDs
#  (3-11) appear in the selection.
# ─────────────────────────────────────────────────────────────────────────────
function Build-ExecutionPlan {
    param([int[]]$Ids)

    $seen = [System.Collections.Generic.HashSet[string]]::new()
    $plan = [System.Collections.Generic.List[PSCustomObject]]::new()

    foreach ($id in $Ids) {
        $act = $Activities | Where-Object { $_.Id -eq $id }
        if (-not $act) { continue }

        $scriptKey = $act.Script.ToLower()
        if ($seen.Add($scriptKey)) {
            # First time we see this script → add it
            $plan.Add([PSCustomObject]@{
                    Id     = $id
                    Label  = $act.Label
                    Script = $act.Script
                    Group  = $act.Group
                })
        }
        # else: already queued (e.g. another pipeline ID) → skip duplicate
    }

    return $plan
}

# ─────────────────────────────────────────────────────────────────────────────
#  Execute a single script and return success / failure
# ─────────────────────────────────────────────────────────────────────────────
function Invoke-Activity {
    param(
        [PSCustomObject]$Activity,
        [int]$StepNum,
        [int]$TotalSteps
    )

    $scriptPath = Join-Path $ScriptDir $Activity.Script

    Write-Section ("Step $StepNum / $TotalSteps  →  [{0,2}] {1}" -f $Activity.Id, $Activity.Label)

    # Verify the script file exists
    if (-not (Test-Path $scriptPath)) {
        Write-LogError "Script not found: $scriptPath"
        return $false
    }

    Write-LogInfo "Script  : $($Activity.Script)"
    Write-LogInfo "Started : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    Write-Host ""

    # Execute in a child PowerShell process so each script gets its own scope
    # and any Set-StrictMode / $ErrorActionPreference inside it doesn't bleed.
    $proc = Start-Process -FilePath "powershell.exe" `
        -ArgumentList "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "`"$scriptPath`"" `
        -NoNewWindow `
        -PassThru `
        -Wait

    $exitCode = $proc.ExitCode
    $ok = ($exitCode -eq 0)

    Write-Host ""
    Write-LogInfo "Finished : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"

    if ($ok) {
        Write-LogOk  "Exit code $exitCode  →  SUCCESS"
    }
    else {
        Write-LogError "Exit code $exitCode  →  FAILED"
    }

    return $ok
}

# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────

Show-Menu

$userInput = Read-Host "  ► Enter your choice"
Write-Host ""

# Handle quit
if ($userInput -imatch '^(q|quit|exit)$') {
    Write-LogInfo "User chose to exit. Bye!"
    Stop-Transcript | Out-Null
    exit 0
}

# Resolve to IDs
$selectedIds = Resolve-InputToIds -Raw $userInput

if ($null -eq $selectedIds -or $selectedIds.Count -eq 0) {
    Write-LogError "No valid activities resolved from input: '$userInput'"
    Stop-Transcript | Out-Null
    exit 1
}

# Build de-duplicated execution plan
$plan = Build-ExecutionPlan -Ids $selectedIds

if ($plan.Count -eq 0) {
    Write-LogError "Execution plan is empty."
    Stop-Transcript | Out-Null
    exit 1
}

# ── Confirm before running ───────────────────────────────────────────────────
Write-Header "Execution Plan"

$i = 1
foreach ($step in $plan) {
    Write-Host ("  {0,2}.  [{1,2}]  {2}" -f $i, $step.Id, $step.Label) -ForegroundColor White
    Write-Host ("        Script : {0}" -f $step.Script) -ForegroundColor DarkGray
    $i++
}

Write-Host ""
$confirm = Read-Host "  ► Run $($plan.Count) script(s) listed above? (Y to continue / N to abort)"
if ($confirm -notmatch '^[Yy]') {
    Write-LogInfo "Aborted by user."
    Stop-Transcript | Out-Null
    exit 0
}

# ── Execute ───────────────────────────────────────────────────────────────────
Write-Header "Execution Started  —  $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"

$results = [System.Collections.Generic.List[PSCustomObject]]::new()
$stepNum = 1
$allPassed = $true

foreach ($step in $plan) {
    $ok = Invoke-Activity -Activity $step -StepNum $stepNum -TotalSteps $plan.Count

    $results.Add([PSCustomObject]@{
            Step   = $stepNum
            Id     = $step.Id
            Label  = $step.Label
            Script = $step.Script
            Status = if ($ok) { "✔  PASS" } else { "✘  FAIL" }
        })

    if (-not $ok) { $allPassed = $false }
    $stepNum++
}

# ── Final Summary ─────────────────────────────────────────────────────────────
Write-Header "Execution Summary  —  $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"

$w1 = 4    # Step
$w2 = 4    # Id
$w3 = 54   # Label
$w4 = 40   # Script
$w5 = 9    # Status
$sep = "  " + ("─" * ($w1 + $w2 + $w3 + $w4 + $w5 + 10))

$hdr = "  {0,-$w1}  {1,-$w2}  {2,-$w3}  {3,-$w4}  {4,-$w5}" -f "Step", "ID", "Activity", "Script", "Status"
Write-Host $hdr -ForegroundColor White
Write-Host $sep -ForegroundColor DarkGray

foreach ($r in $results) {
    $color = if ($r.Status -match "PASS") { "Green" } else { "Red" }
    $row = "  {0,-$w1}  {1,-$w2}  {2,-$w3}  {3,-$w4}  {4,-$w5}" -f $r.Step, $r.Id, $r.Label, $r.Script, $r.Status
    Write-Host $row -ForegroundColor $color
}

Write-Host $sep -ForegroundColor DarkGray

$passCount = @($results | Where-Object { $_.Status -match "PASS" }).Count
$failCount = @($results | Where-Object { $_.Status -match "FAIL" }).Count

Write-Host ""
Write-Host ("  Completed : {0} / {1} PASSED   |   {2} FAILED" -f $passCount, $results.Count, $failCount) -ForegroundColor White
Write-Host ("  Log saved : {0}" -f $LogFile) -ForegroundColor DarkGray
Write-Host ""

if ($allPassed) {
    Write-Host "  ✔  All selected activities completed successfully." -ForegroundColor Green
}
else {
    Write-Host "  ✘  One or more activities failed. Check the log for details." -ForegroundColor Red
}

Write-Host ""

Stop-Transcript | Out-Null

exit $(if ($allPassed) { 0 } else { 1 })