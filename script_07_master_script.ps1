#Requires -Version 5.1
<#
.SYNOPSIS
    RecoMart Super Script — unified launcher for all project scripts.

.DESCRIPTION
    Displays a numbered activity menu and accepts:
      * A single number  (e.g.  3 )    - run that one activity
      * A range          (e.g.  1-5 )  - run activities 1 through 5 in order
      * "all"                          - run every activity (0-14) in order

    Inputs 3-11 all map to script_03_run_pipeline.ps1 (the full 9-task pipeline).
    When a range covers several of those IDs the pipeline script runs only once.

.NOTES
    Place this file in the same folder as the individual script_0x_*.ps1 files.
    Run with:   .\super_script.ps1
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"
$ProgressPreference = "SilentlyContinue"

$ScriptDir = $PSScriptRoot

# ─────────────────────────────────────────────────────────────────────────────
#  Colour helpers
# ─────────────────────────────────────────────────────────────────────────────
function Write-Header {
    param([string]$Text)
    $line = "=" * 72
    Write-Host ""
    Write-Host "  +$line+" -ForegroundColor Cyan
    Write-Host ("  |  {0,-70}|" -f $Text) -ForegroundColor Cyan
    Write-Host "  +$line+" -ForegroundColor Cyan
    Write-Host ""
}

function Write-Section {
    param([string]$Text)
    Write-Host ""
    Write-Host "  --- $Text" -ForegroundColor Yellow
    Write-Host ("  " + ("-" * 68)) -ForegroundColor DarkGray
    Write-Host ""
}

function Write-LogInfo { param([string]$m) Write-Host "  [INFO]  $m" -ForegroundColor Cyan }
function Write-LogOk { param([string]$m) Write-Host "  [OK]    $m" -ForegroundColor Green }
function Write-LogWarn { param([string]$m) Write-Host "  [WARN]  $m" -ForegroundColor Yellow }
function Write-LogError { param([string]$m) Write-Host "  [ERROR] $m" -ForegroundColor Red }

# ─────────────────────────────────────────────────────────────────────────────
#  Activity table
# ─────────────────────────────────────────────────────────────────────────────
$Activities = @(
    [PSCustomObject]@{ Id = 0; Group = "Setup"; Label = "Initialization  (venv, git, dvc, remote)"; Script = "script_00_initialization.ps1" }
    [PSCustomObject]@{ Id = 1; Group = "Setup"; Label = "Clean Project  (purge data_lake, logs, mlruns)"; Script = "script_01_clean_project.ps1" }
    [PSCustomObject]@{ Id = 2; Group = "Data"; Label = "Download & Extract Data"; Script = "script_02_download_extract_data.ps1" }
    [PSCustomObject]@{ Id = 3; Group = "Pipeline"; Label = "Pipeline - Task 1 : Problem Formulation"; Script = "script_03_run_pipeline.ps1" }
    [PSCustomObject]@{ Id = 4; Group = "Pipeline"; Label = "Pipeline - Task 2 : Data Ingestion"; Script = "script_03_run_pipeline.ps1" }
    [PSCustomObject]@{ Id = 5; Group = "Pipeline"; Label = "Pipeline - Task 3 : Raw Data Storage"; Script = "script_03_run_pipeline.ps1" }
    [PSCustomObject]@{ Id = 6; Group = "Pipeline"; Label = "Pipeline - Task 4 : Data Validation"; Script = "script_03_run_pipeline.ps1" }
    [PSCustomObject]@{ Id = 7; Group = "Pipeline"; Label = "Pipeline - Task 5 : Data Preparation"; Script = "script_03_run_pipeline.ps1" }
    [PSCustomObject]@{ Id = 8; Group = "Pipeline"; Label = "Pipeline - Task 6 : Feature Engineering"; Script = "script_03_run_pipeline.ps1" }
    [PSCustomObject]@{ Id = 9; Group = "Pipeline"; Label = "Pipeline - Task 7 : Feature Store"; Script = "script_03_run_pipeline.ps1" }
    [PSCustomObject]@{ Id = 10; Group = "Pipeline"; Label = "Pipeline - Task 8 : Data Versioning"; Script = "script_03_run_pipeline.ps1" }
    [PSCustomObject]@{ Id = 11; Group = "Pipeline"; Label = "Pipeline - Task 9 : Model Training & Prediction"; Script = "script_03_run_pipeline.ps1" }
    [PSCustomObject]@{ Id = 12; Group = "Servers"; Label = "MLflow UI Server"; Script = "script_04_mlflow_ui_server.ps1" }
    [PSCustomObject]@{ Id = 13; Group = "Servers"; Label = "Inference API Server"; Script = "script_05_inference_api_server.ps1" }
    [PSCustomObject]@{ Id = 14; Group = "Servers"; Label = "Perfect Server"; Script = "script_06_perfect_server.ps1" }
)

# ─────────────────────────────────────────────────────────────────────────────
#  Show menu
# ─────────────────────────────────────────────────────────────────────────────
function Show-Menu {
    Clear-Host
    Write-Header "RecoMart  -  Super Script  -  Activity Launcher"

    $currentGroup = ""
    foreach ($act in $Activities) {
        if ($act.Group -ne $currentGroup) {
            $currentGroup = $act.Group
            Write-Host ("  -- {0} {1}" -f $currentGroup, ("-" * (62 - $currentGroup.Length))) -ForegroundColor DarkGray
        }
        Write-Host ("  [{0,2}]  {1}" -f $act.Id, $act.Label) -ForegroundColor White
    }

    Write-Host ""
    Write-Host ("  -- Usage " + ("-" * 62)) -ForegroundColor DarkGray
    Write-Host "  Single  :  3      - run activity 3 only"                             -ForegroundColor Gray
    Write-Host "  Range   :  1-5    - run activities 1, 2, pipeline (once), etc."      -ForegroundColor Gray
    Write-Host "  All     :  all    - run every activity 0 to 14 in order"             -ForegroundColor Gray
    Write-Host "  Quit    :  q      - exit"                                            -ForegroundColor Gray
    Write-Host ""
}

# ─────────────────────────────────────────────────────────────────────────────
#  Parse input -> typed [int[]] so .Count is always reliable
# ─────────────────────────────────────────────────────────────────────────────
function Resolve-InputToIds {
    param([string]$Raw)

    $trimmed = $Raw.Trim()

    # "all"
    if ($trimmed -ieq "all") {
        [int[]]$ids = 0..14
        return $ids
    }

    # Range: digits HYPHEN digits
    if ($trimmed -match '^(\d+)-(\d+)$') {
        [int]$from = [int]$Matches[1]
        [int]$to = [int]$Matches[2]
        if ($from -gt $to) { [int]$tmp = $from; $from = $to; $to = $tmp }
        if ($from -lt 0 -or $to -gt 14) {
            Write-LogWarn "Range must be 0-14. Got: $trimmed"
            [int[]]$empty = @()
            return $empty
        }
        [int[]]$ids = $from..$to
        return $ids
    }

    # Single integer
    if ($trimmed -match '^\d+$') {
        [int]$id = [int]$trimmed
        if ($id -lt 0 -or $id -gt 14) {
            Write-LogWarn "ID must be 0-14. Got: $id"
            [int[]]$empty = @()
            return $empty
        }
        [int[]]$ids = @($id)
        return $ids
    }

    Write-LogWarn "Unrecognised input: '$trimmed'"
    [int[]]$empty = @()
    return $empty
}

# ─────────────────────────────────────────────────────────────────────────────
#  Build execution plan — deduplicate by script filename, preserve order
# ─────────────────────────────────────────────────────────────────────────────
function Build-ExecutionPlan {
    param([int[]]$Ids)

    $seen = [System.Collections.Generic.HashSet[string]]::new()
    $list = [System.Collections.Generic.List[PSCustomObject]]::new()

    foreach ($id in $Ids) {
        $act = $Activities | Where-Object { $_.Id -eq $id }
        if (-not $act) { continue }

        $key = $act.Script.ToLower()
        if ($seen.Add($key)) {
            $list.Add([PSCustomObject]@{
                    Id     = $id
                    Label  = $act.Label
                    Script = $act.Script
                })
        }
    }

    # Return as typed array so .Count is always available on the caller side
    [PSCustomObject[]]$result = $list.ToArray()
    return $result
}

# ─────────────────────────────────────────────────────────────────────────────
#  Execute one script in a child powershell.exe process
#
#  Windows PowerShell 5.1 reads .ps1 files as ANSI by default.
#  Scripts that contain Unicode (✓ ✗ etc.) get mangled, breaking the parser.
#  Fix: read the file as UTF-8, write a BOM-prefixed temp copy in the same
#  directory (so $PSScriptRoot stays correct), run that, then delete it.
# ─────────────────────────────────────────────────────────────────────────────
function Invoke-Activity {
    param(
        [PSCustomObject]$Activity,
        [int]$StepNum,
        [int]$TotalSteps
    )

    $scriptPath = Join-Path $ScriptDir $Activity.Script
    Write-Section ("Step $StepNum / $TotalSteps  ->  [{0,2}] {1}" -f $Activity.Id, $Activity.Label)

    if (-not (Test-Path $scriptPath)) {
        Write-LogError "Script not found: $scriptPath"
        return $false
    }

    Write-LogInfo "Script  : $($Activity.Script)"
    Write-LogInfo "Started : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    Write-Host ""

    # --- UTF-8 BOM temp-file workaround ---
    # [System.Text.Encoding]::UTF8 emits the BOM (EF BB BF) on WriteAllText,
    # which tells PS5.1 to parse the file as UTF-8 instead of ANSI.
    # The temp file lives beside the original so $PSScriptRoot is unchanged.
    $scriptDir = Split-Path $scriptPath -Parent
    $tempScript = Join-Path $scriptDir ("_run_" + [System.IO.Path]::GetRandomFileName() + ".ps1")
    $fileContent = [System.IO.File]::ReadAllText($scriptPath, [System.Text.Encoding]::UTF8)
    [System.IO.File]::WriteAllText($tempScript, $fileContent, [System.Text.Encoding]::UTF8)

    try {
        $proc = Start-Process `
            -FilePath    "powershell.exe" `
            -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $tempScript) `
            -NoNewWindow -PassThru -Wait
        [bool]$ok = ($proc.ExitCode -eq 0)
    }
    finally {
        Remove-Item $tempScript -Force -ErrorAction SilentlyContinue
    }

    Write-Host ""
    Write-LogInfo "Finished : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    if ($ok) { Write-LogOk    "Exit $($proc.ExitCode)  ->  SUCCESS" }
    else { Write-LogError "Exit $($proc.ExitCode)  ->  FAILED" }

    return $ok
}

# =============================================================================
#  MAIN
# =============================================================================

Show-Menu

$userInput = Read-Host "  > Enter your choice"
Write-Host ""

# Quit?
if ($userInput -imatch '^(q|quit|exit)$') {
    Write-LogInfo "Exiting."
    exit 0
}

# ── Resolve to typed [int[]] ─────────────────────────────────────────────────
[int[]]$selectedIds = Resolve-InputToIds -Raw $userInput

if ($selectedIds.Count -eq 0) {
    Write-LogError "No valid activities resolved from: '$userInput'"
    exit 1
}

# ── Build de-duplicated plan ──────────────────────────────────────────────────
[PSCustomObject[]]$plan = Build-ExecutionPlan -Ids $selectedIds

if ($plan.Count -eq 0) {
    Write-LogError "Execution plan is empty."
    exit 1
}

# ── Show plan and confirm ─────────────────────────────────────────────────────
Write-Header "Execution Plan"

for ($i = 0; $i -lt $plan.Count; $i++) {
    Write-Host ("  {0,2}.  [{1,2}]  {2}" -f ($i + 1), $plan[$i].Id, $plan[$i].Label) -ForegroundColor White
    Write-Host ("        Script : {0}" -f $plan[$i].Script) -ForegroundColor DarkGray
}

Write-Host ""
$confirm = Read-Host ("  > Run {0} script(s) above? [Y] to continue / N to abort (default Y)" -f $plan.Count)
if ($confirm -ne "" -and $confirm -notmatch '^[Yy]') {
    Write-LogInfo "Aborted."
    exit 0
}

# ── Execute ───────────────────────────────────────────────────────────────────
Write-Header ("Execution Started  -  {0}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))

$results = [System.Collections.Generic.List[PSCustomObject]]::new()
$allPassed = $true

for ($i = 0; $i -lt $plan.Count; $i++) {
    [bool]$ok = Invoke-Activity -Activity $plan[$i] -StepNum ($i + 1) -TotalSteps $plan.Count
    $results.Add([PSCustomObject]@{
            Step   = $i + 1
            Id     = $plan[$i].Id
            Label  = $plan[$i].Label
            Script = $plan[$i].Script
            Status = if ($ok) { "PASS" } else { "FAIL" }
        })
    if (-not $ok) { $allPassed = $false }
}

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Header ("Execution Summary  -  {0}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))

$w1 = 4; $w2 = 4; $w3 = 50; $w4 = 38; $w5 = 6
$sep = "  " + ("-" * ($w1 + $w2 + $w3 + $w4 + $w5 + 10))
Write-Host ("  {0,-$w1}  {1,-$w2}  {2,-$w3}  {3,-$w4}  {4,-$w5}" -f "Step", "ID", "Activity", "Script", "Status") -ForegroundColor White
Write-Host $sep -ForegroundColor DarkGray

foreach ($r in $results) {
    $color = if ($r.Status -eq "PASS") { "Green" } else { "Red" }
    Write-Host ("  {0,-$w1}  {1,-$w2}  {2,-$w3}  {3,-$w4}  {4,-$w5}" -f $r.Step, $r.Id, $r.Label, $r.Script, $r.Status) -ForegroundColor $color
}

Write-Host $sep -ForegroundColor DarkGray

$pass = @($results | Where-Object { $_.Status -eq "PASS" }).Count
$fail = @($results | Where-Object { $_.Status -eq "FAIL" }).Count
Write-Host ""
Write-Host ("  {0} / {1} PASSED   |   {2} FAILED" -f $pass, $results.Count, $fail) -ForegroundColor White
Write-Host ""
if ($allPassed) { Write-Host "  [OK]  All selected activities completed successfully." -ForegroundColor Green }
else { Write-Host "  [!!]  One or more activities failed."                  -ForegroundColor Red }
Write-Host ""

exit $(if ($allPassed) { 0 } else { 1 })