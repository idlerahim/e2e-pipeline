$folders = @("data_lake", "reports", "logs", "mlruns", "dataset")
$files = @("mlflow.db", "prefect.db", "cosine_sim.npy")

Write-Host "`nCleaning project directory..." -ForegroundColor Cyan

foreach ($folder in $folders) {
    $path = Join-Path $PSScriptRoot $folder

    if (Test-Path $path) {
        Remove-Item -Path "$path\*" -Recurse -Force
        Write-Host "  ✓ Emptied $folder\" -ForegroundColor Green
    }
    else {
        Write-Host "  - Skipped $folder\ (not found)" -ForegroundColor DarkGray
    }
}

foreach ($file in $files) {
    $path = Join-Path $PSScriptRoot $file

    if (Test-Path $path) {
        Remove-Item -Path $path -Force
        Write-Host "  ✓ Removed $file" -ForegroundColor Green
    }
    else {
        Write-Host "  - Skipped $file (not found)" -ForegroundColor DarkGray
    }
}

# Extract all zips from dataset_archived into dataset
Write-Host "`nExtracting archives..." -ForegroundColor Cyan

$archiveDir = Join-Path $PSScriptRoot "dataset_archived"
$destDir = Join-Path $PSScriptRoot "dataset"

if (Test-Path $archiveDir) {
    $zips = Get-ChildItem -Path $archiveDir -Filter "*.zip"

    if ($zips.Count -eq 0) {
        Write-Host "  - No .zip files found in dataset_archived\" -ForegroundColor DarkGray
    }
    else {
        foreach ($zip in $zips) {
            try {
                Expand-Archive -Path $zip.FullName -DestinationPath $destDir -Force
                Write-Host "  ✓ Extracted $($zip.Name)" -ForegroundColor Green
            }
            catch {
                Write-Host "  ✗ Failed to extract $($zip.Name): $_" -ForegroundColor Red
            }
        }
    }
}
else {
    Write-Host "  - Skipped extraction (dataset_archived\ not found)" -ForegroundColor DarkGray
}

Write-Host "`nDone.`n" -ForegroundColor Cyan