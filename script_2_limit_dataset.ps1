# ============================================================
#  sample_csvs.ps1  -  Interactively sample rows in CSV files
# ============================================================

function Format-Size($bytes) {
    if ($bytes -ge 1MB) { return "{0:N2} MB" -f ($bytes / 1MB) }
    elseif ($bytes -ge 1KB) { return "{0:N2} KB" -f ($bytes / 1KB) }
    else { return "$bytes B" }
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   CSV Row Sampler" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# --- Hardcoded Logic ---
$rawDir = "D:\Data\Portfolio\Projects\Python\e2e-pipeline\dataset"
$pattern = "dataset"

if (-not (Test-Path $rawDir)) {
    Write-Host "  ERROR: Path not found: $rawDir" -ForegroundColor Red
    exit 1
}

$allCsvFiles = Get-ChildItem -Path $rawDir -Filter "*.csv"
$csvFiles = $allCsvFiles | Where-Object { $_.Name -like "*$pattern*" }

Write-Host "  Found $($allCsvFiles.Count) CSV file(s) in: $rawDir" -ForegroundColor DarkGray
Write-Host "  $($csvFiles.Count) of $($allCsvFiles.Count) CSV(s) match pattern " -NoNewline -ForegroundColor DarkGray
Write-Host "'$pattern'" -ForegroundColor Yellow
Write-Host ""

# --- Single Confirmation Step ---
Write-Host "  Confirm sampling at " -NoNewline
Write-Host "100%" -ForegroundColor Yellow -NoNewline
Write-Host "? (Enter 'Y' to proceed, or enter a new % 1-100)" -ForegroundColor White
Write-Host "  [Y/1-100]" -ForegroundColor DarkGray -NoNewline
Write-Host ": " -NoNewline
$userInput = Read-Host

$percentage = 100
if ($userInput -match '^\d+$') {
    $percentage = [int]$userInput
}
elseif ($userInput -notin @("", "Y", "y", "yes", "YES")) {
    Write-Host "`n  Aborted." -ForegroundColor Red
    exit 0
}

$ratio = $percentage / 100.0
Write-Host "`n  Processing $percentage% sampling..." -ForegroundColor Cyan
Write-Host ""

# --- Process Each CSV ---
$summary = @()
foreach ($file in $allCsvFiles) {
    if ($file.Name -like "*$pattern*") {
        $origSize = $file.Length
        
        if ($percentage -eq 100) {
            # Fast-track 100% (No write operation)
            $reader = [System.IO.StreamReader]::new($file.FullName)
            $header = $reader.ReadLine()
            $reader.Close()
            $origCols = if ($header) { ($header -split ',').Count } else { 0 }
            $lineCount = 0
            foreach ($line in [System.IO.File]::ReadLines($file.FullName)) { $lineCount++ }
            $origRows = [math]::Max(0, $lineCount - 1)

            Write-Host "  ✓ $($file.Name)" -ForegroundColor Green
            $summary += [PSCustomObject]@{
                "CSV File" = $file.Name; "Orig Cols" = $origCols; "Orig Rows" = "{0:N0}" -f $origRows; 
                "Orig Size" = Format-Size $origSize; "New Rows" = "{0:N0}" -f $origRows; "New Size" = Format-Size $origSize 
            }
        }
        else {
            # Perform Sampling
            $rows = Import-Csv -Path $file.FullName
            $origRows = $rows.Count
            $origCols = if ($origRows -gt 0) { ($rows[0].PSObject.Properties | Measure-Object).Count } else { 0 }
            $sampleCount = [math]::Max(1, [math]::Round($origRows * $ratio))
            $sampled = $rows | Get-Random -Count $sampleCount
            $sampled | Export-Csv -Path $file.FullName -NoTypeInformation -Encoding UTF8

            Write-Host "  ✓ $($file.Name)" -ForegroundColor Green
            $summary += [PSCustomObject]@{
                "CSV File" = $file.Name; "Orig Cols" = $origCols; "Orig Rows" = "{0:N0}" -f $origRows; 
                "Orig Size" = Format-Size $origSize; "New Rows" = "{0:N0}" -f $sampled.Count; "New Size" = Format-Size (Get-Item $file.FullName).Length
            }
        }
    }
    else {
        Write-Host "  - $($file.Name) (skipped)" -ForegroundColor DarkGray
    }
}

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "   Summary" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
$summary | Format-Table -AutoSize