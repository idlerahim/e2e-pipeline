# ============================================================
#  sample_csvs.ps1  -  Interactively sample rows in CSV files
# ============================================================

function Read-Input($prompt, $default) {
    Write-Host "$prompt " -ForegroundColor Yellow -NoNewline
    Write-Host "[$default]" -ForegroundColor DarkGray -NoNewline
    Write-Host ": " -NoNewline
    $input = Read-Host
    if ([string]::IsNullOrWhiteSpace($input)) { return $default }
    return $input.Trim()
}

function Format-Size($bytes) {
    if     ($bytes -ge 1MB) { return "{0:N2} MB" -f ($bytes / 1MB) }
    elseif ($bytes -ge 1KB) { return "{0:N2} KB" -f ($bytes / 1KB) }
    else                    { return "$bytes B" }
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   CSV Row Sampler" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# --- Step 1: Location ---
$defaultDir = Join-Path $PSScriptRoot "dataset"
$rawDir     = Read-Input "CSV folder path" $defaultDir

if (-not (Test-Path $rawDir)) {
    Write-Host ""
    Write-Host "  ERROR: Path not found: $rawDir" -ForegroundColor Red
    exit 1
}

$allCsvFiles = Get-ChildItem -Path $rawDir -Filter "*.csv"
if ($allCsvFiles.Count -eq 0) {
    Write-Host ""
    Write-Host "  ERROR: No CSV files found in: $rawDir" -ForegroundColor Red
    exit 1
}

Write-Host "  Found $($allCsvFiles.Count) CSV file(s) in: $rawDir" -ForegroundColor DarkGray
Write-Host ""

# --- Step 2: CSV identifier filter ---
$pattern  = Read-Input "CSV filename identifier (only filenames containing this will be processed)" "dataset"
$csvFiles = $allCsvFiles | Where-Object { $_.Name -like "*$pattern*" }

Write-Host ""
if ($csvFiles.Count -eq 0) {
    Write-Host "  ERROR: No CSV files matched pattern '$pattern'" -ForegroundColor Red
    exit 1
}
Write-Host "  $($csvFiles.Count) of $($allCsvFiles.Count) CSV(s) match pattern " -NoNewline -ForegroundColor DarkGray
Write-Host "'$pattern'" -ForegroundColor Yellow
Write-Host ""

# --- Step 3: Sample percentage ---
$percentage = $null
while ($null -eq $percentage) {
    $raw = Read-Input "Sample percentage (1-100)" "25"
    if ($raw -match '^\d+$' -and [int]$raw -ge 1 -and [int]$raw -le 100) {
        $percentage = [int]$raw
    } else {
        Write-Host "  Please enter a whole number between 1 and 100." -ForegroundColor Red
    }
}

$ratio = $percentage / 100.0
Write-Host ""

# --- Step 4: Confirm ---
Write-Host "  Ready to sample $($csvFiles.Count) matched CSV(s) at " -NoNewline
Write-Host "$percentage%" -ForegroundColor Yellow -NoNewline
Write-Host " in: " -NoNewline
Write-Host $rawDir -ForegroundColor Yellow
Write-Host ""
Write-Host "  Columns will NOT be affected - only rows will be sampled." -ForegroundColor DarkGray
Write-Host ""
Write-Host "  Confirm? " -ForegroundColor Yellow -NoNewline
Write-Host "[Y/n]" -ForegroundColor DarkGray -NoNewline
Write-Host ": " -NoNewline
$confirm = Read-Host

if ($confirm -notin @("", "Y", "y", "yes", "YES")) {
    Write-Host ""
    Write-Host "  Aborted." -ForegroundColor Red
    exit 0
}

Write-Host ""
Write-Host "  Processing..." -ForegroundColor Cyan
Write-Host ""

# --- Step 5: Sample each CSV ---
$summary = @()

foreach ($file in $allCsvFiles) {
    if ($file.Name -like "*$pattern*") {
        $origSize = $file.Length

        if ($percentage -eq 100) {
            # --- 100%: skip processing, just report original stats ---
            # Read only the header + first data line to get column count
            $reader   = [System.IO.StreamReader]::new($file.FullName)
            $header   = $reader.ReadLine()
            $reader.Close()

            $origCols = if ($header) { ($header -split ',').Count } else { 0 }
            # Line count minus header = row count
            $lineCount = 0
            foreach ($line in [System.IO.File]::ReadLines($file.FullName)) { $lineCount++ }
            $origRows = [math]::Max(0, $lineCount - 1)

            Write-Host "  ✓ " -ForegroundColor Green -NoNewline
            Write-Host $file.Name -NoNewline
            Write-Host "  " -NoNewline
            Write-Host "[100% - kept as-is]" -ForegroundColor DarkGreen

            $summary += [PSCustomObject]@{
                "CSV File"  = $file.Name
                "Orig Cols" = $origCols
                "Orig Rows" = "{0:N0}" -f $origRows
                "Orig Size" = Format-Size $origSize
                "New Cols"  = $origCols
                "New Rows"  = "{0:N0}" -f $origRows
                "New Size"  = Format-Size $origSize
            }
        } else {
            # --- < 100%: sample rows ---
            $rows     = Import-Csv -Path $file.FullName
            $origRows = $rows.Count
            $origCols = if ($origRows -gt 0) { ($rows[0].PSObject.Properties | Measure-Object).Count } else { 0 }

            $sampleCount = [math]::Max(1, [math]::Round($origRows * $ratio))
            $sampled     = $rows | Get-Random -Count $sampleCount

            $sampled | Export-Csv -Path $file.FullName -NoTypeInformation -Encoding UTF8

            $newSize = (Get-Item $file.FullName).Length
            $newRows = $sampled.Count
            $newCols = $origCols

            Write-Host "  ✓ " -ForegroundColor Green -NoNewline
            Write-Host $file.Name -NoNewline
            Write-Host "  " -NoNewline
            Write-Host "[matched: '$pattern']" -ForegroundColor DarkGreen

            $summary += [PSCustomObject]@{
                "CSV File"  = $file.Name
                "Orig Cols" = $origCols
                "Orig Rows" = "{0:N0}" -f $origRows
                "Orig Size" = Format-Size $origSize
                "New Cols"  = $newCols
                "New Rows"  = "{0:N0}" -f $newRows
                "New Size"  = Format-Size $newSize
            }
        }
    } else {
        Write-Host "  - " -ForegroundColor DarkGray -NoNewline
        Write-Host $file.Name -NoNewline
        Write-Host "  " -NoNewline
        Write-Host "[skipped: no match for '$pattern']" -ForegroundColor DarkGray
    }
}

# --- Step 6: Summary table ---
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   Summary" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

$summary | Format-Table -AutoSize

Write-Host ""