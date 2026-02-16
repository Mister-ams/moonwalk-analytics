# =====================================================================
# MOONWALK DATA REFRESH - PARALLEL ONEDRIVE SYNC v2.5.5
# =====================================================================
# OPTIMIZED WORKFLOW:
# 1. Python saves CSVs to LOCAL staging (fast, no OneDrive sync)
# 2. PowerShell starts Excel load from LOCAL
# 3. PARALLEL: Copy LOCAL CSVs to OneDrive (background job)
# 4. Both operations finish around same time
#
# Expected savings: 6-7s (OneDrive copy removed from critical path)
#
# Use -NoPause to skip the final "Press Enter" prompt (for chaining
# with Start-Dashboard.ps1 or other scripts).
# =====================================================================

param([switch]$NoPause)

# =====================================================================
# LOCK FILE MECHANISM (prevents concurrent runs)
# =====================================================================

. "$PSScriptRoot\helpers.ps1"

$LOCK_FILE = Join-Path $env:TEMP "moonwalk_refresh.lock"
$LOCK_STALE_HOURS = 2

function Test-LockFile {
    if (Test-Path $LOCK_FILE) {
        try {
            $lockContent = Get-Content $LOCK_FILE -Raw | ConvertFrom-Json
            $lockPid = $lockContent.PID
            $lockTime = [DateTime]::Parse($lockContent.Timestamp)
            $elapsedHours = ((Get-Date) - $lockTime).TotalHours

            # Check if process still running
            $processExists = Get-Process -Id $lockPid -ErrorAction SilentlyContinue

            if ($processExists) {
                Write-Host ""
                Write-Host "======================================================================"
                Write-Host "ERROR: Another refresh is already running (PID $lockPid)"
                Write-Host "======================================================================"
                Write-Host ""
                Write-Host "Started: $($lockContent.Timestamp)"
                Write-Host "Elapsed: $([math]::Round($elapsedHours, 1)) hours"
                Write-Host ""
                Write-Host "If you believe this is stale, delete:"
                Write-Host "  $LOCK_FILE"
                Write-Host ""
                return $true
            }
            elseif ($elapsedHours -lt $LOCK_STALE_HOURS) {
                Write-Host ""
                Write-Host "======================================================================"
                Write-Host "ERROR: Stale lock file found (process no longer running)"
                Write-Host "======================================================================"
                Write-Host ""
                Write-Host "Lock created: $($lockContent.Timestamp)"
                Write-Host "Lock PID: $lockPid (no longer running)"
                Write-Host ""
                Write-Host "Lock is recent (< $LOCK_STALE_HOURS hours), NOT auto-clearing."
                Write-Host "If you believe this is safe to clear, delete:"
                Write-Host "  $LOCK_FILE"
                Write-Host ""
                return $true
            }
            else {
                # Auto-clear stale lock
                Write-Host ""
                Write-Host "  [WARN] Clearing stale lock file from PID $lockPid" -ForegroundColor Yellow
                Write-Host "         (process no longer running, $([math]::Round($elapsedHours, 1)) hours old)"
                Remove-Item $LOCK_FILE -Force
                return $false
            }
        }
        catch {
            Write-Host "  [WARN] Invalid lock file, clearing..." -ForegroundColor Yellow
            Remove-Item $LOCK_FILE -Force
            return $false
        }
    }
    return $false
}

function New-LockFile {
    $lockData = @{
        PID = $PID
        Timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
        User = $env:USERNAME
        Computer = $env:COMPUTERNAME
    }
    $lockData | ConvertTo-Json | Out-File $LOCK_FILE -Encoding UTF8
    Write-Host "  [OK] Lock file created (PID $PID)" -ForegroundColor Green
}

function Remove-LockFile {
    if (Test-Path $LOCK_FILE) {
        Remove-Item $LOCK_FILE -Force -ErrorAction SilentlyContinue
    }
}

function Exit-WithRefreshError ([string]$Message) {
    Exit-WithError -Message $Message -LockFile $LOCK_FILE -NoPause:$NoPause
}

# Check for existing lock
if (Test-LockFile) {
    if (-not $NoPause) {
        Read-Host "Press Enter to exit"
    }
    exit 1
}

# Create lock file
New-LockFile

# Ensure cleanup on exit
trap {
    Remove-LockFile
    break
}

$null = Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action {
    Remove-LockFile
}

Write-Host ""
Write-Host "======================================================================"
Write-Host "MOONWALK DATA REFRESH - PARALLEL SYNC v2.5.5"
Write-Host "======================================================================"
Write-Host ""

# =====================================================================
# CONFIGURATION (centralized in config.ps1)
# =====================================================================

. "$PSScriptRoot\config.ps1"
. "$PSScriptRoot\logger_config.ps1"

$PYTHON_SCRIPT_FOLDER = $MoonwalkConfig.PythonScriptFolder
$DATA_FOLDER          = $MoonwalkConfig.DataFolder
$LOCAL_STAGING_FOLDER = $MoonwalkConfig.LocalStagingFolder
$ONEDRIVE_DATA_FOLDER = $MoonwalkConfig.OneDriveDataFolder

# CSV files to sync (centralized in config.ps1)
$CSV_FILES = $MoonwalkConfig.RequiredCsvs

$PYTHON_SCRIPT = "cleancloud_to_excel_MASTER.py"

# Persistent logging
$LogFile = Initialize-MoonwalkLog -DataFolder $DATA_FOLDER
Write-MoonwalkLog -Level INFO -Message "Refresh started (v2.5.5)" -LogFile $LogFile

# =====================================================================
# PRE-FLIGHT CHECKS
# =====================================================================

Write-Host "[PRE-FLIGHT] Checking setup..."
Write-Host ""

# Create local staging folder if needed
if (-not (Test-Path $LOCAL_STAGING_FOLDER)) {
    New-Item -ItemType Directory -Path $LOCAL_STAGING_FOLDER | Out-Null
    Write-Host "  OK Created local staging folder: $LOCAL_STAGING_FOLDER" -ForegroundColor Green
} else {
    Write-Host "  OK Local staging folder exists" -ForegroundColor Green
}

# Check Python
try {
    $pythonVersion = python --version 2>&1
    Write-Host "  OK Python found: $pythonVersion" -ForegroundColor Green
}
catch {
    Exit-WithRefreshError "Python not found in PATH"
}

# Check Python script folder
if (-not (Test-Path $PYTHON_SCRIPT_FOLDER)) {
    Exit-WithRefreshError "Python script folder not found: $PYTHON_SCRIPT_FOLDER"
}
Write-Host "  OK Python script folder found" -ForegroundColor Green

# Check master script exists
$scriptPath = Join-Path $PYTHON_SCRIPT_FOLDER $PYTHON_SCRIPT
if (-not (Test-Path $scriptPath)) {
    Exit-WithRefreshError "Python script not found: $scriptPath"
}
Write-Host "  OK Python master script found" -ForegroundColor Green

# AUTO-DETECT: Find the most recent Lime_Reporting*.xlsx file
Write-Host ""
Write-Host "  Searching for latest Lime_Reporting file..." -ForegroundColor Yellow
$excelFiles = Get-ChildItem -Path $DATA_FOLDER -Filter "Lime_Reporting*.xlsx" | 
    Where-Object { $_.Name -notlike "*~*" } |  # Exclude temp files
    Sort-Object LastWriteTime -Descending

if ($excelFiles.Count -eq 0) {
    Exit-WithRefreshError "No Lime_Reporting*.xlsx files found in $DATA_FOLDER"
}

$EXCEL_FILE = $excelFiles[0].FullName
$EXCEL_FILENAME = $excelFiles[0].Name

Write-Host "  OK Found: $EXCEL_FILENAME" -ForegroundColor Green
Write-Host "     Last modified: $($excelFiles[0].LastWriteTime)" -ForegroundColor Gray

Write-Host ""
Write-Host "[OK] All checks passed. Starting refresh..." -ForegroundColor Green
Write-Host ""

# =====================================================================
# STEP 1: RUN PYTHON TRANSFORMATIONS (outputs to LOCAL)
# =====================================================================

Write-Host "[STEP 1/3] Running Python transformations..." -ForegroundColor Cyan
Write-Host "            (Output: LOCAL staging folder)" -ForegroundColor Gray
Write-Host ""

$startTime = Get-Date

try {
    Push-Location $PYTHON_SCRIPT_FOLDER
    cmd /c "echo. | python $PYTHON_SCRIPT"
    $exitCode = $LASTEXITCODE
    Pop-Location
    
    if ($exitCode -ne 0) {
        throw "Python script failed with exit code $exitCode"
    }
    
    $pythonTime = (Get-Date) - $startTime
    Write-Host ""
    Write-Host "[OK] Python transformations completed in $([math]::Round($pythonTime.TotalSeconds, 1)) seconds" -ForegroundColor Green
    Write-Host ""
}
catch {
    Pop-Location
    Exit-WithRefreshError "Python transformation failed: $($_.Exception.Message)"
}

# Verify CSV files exist in LOCAL staging
Write-Host "  Verifying CSV files in local staging..."
$missingFiles = @()
foreach ($csvFile in $CSV_FILES) {
    $localPath = Join-Path $LOCAL_STAGING_FOLDER $csvFile
    if (-not (Test-Path $localPath)) {
        $missingFiles += $csvFile
    }
}

if ($missingFiles.Count -gt 0) {
    Exit-WithRefreshError "Missing CSV files in local staging: $($missingFiles -join ', ')"
}
Write-Host "  OK All 5 CSV files present in local staging" -ForegroundColor Green
Write-Host ""

# =====================================================================
# STEP 2: PARALLEL OPERATIONS
# =====================================================================
# A. Excel refresh (reads from LOCAL) - FOREGROUND
# B. Copy LOCAL CSVs to OneDrive - BACKGROUND JOB (parallel)
# =====================================================================

Write-Host "[STEP 2/3] Starting parallel operations..." -ForegroundColor Cyan
Write-Host ""

# ─────────────────────────────────────────────────────────────────────
# 2A. START BACKGROUND JOB: Copy LOCAL to OneDrive
# ─────────────────────────────────────────────────────────────────────

Write-Host "  [BACKGROUND] Starting OneDrive sync job..." -ForegroundColor Yellow

$syncJob = Start-Job -ScriptBlock {
    param($LocalFolder, $OneDriveFolder, $Files)
    
    $results = @{
        StartTime = Get-Date
        Files = @()
        Success = $true
        Error = $null
    }
    
    try {
        # Create OneDrive folder if needed
        if (-not (Test-Path $OneDriveFolder)) {
            New-Item -ItemType Directory -Path $OneDriveFolder -Force | Out-Null
        }
        
        # Copy each CSV file
        foreach ($file in $Files) {
            $sourcePath = Join-Path $LocalFolder $file
            $destPath = Join-Path $OneDriveFolder $file
            
            $fileStart = Get-Date
            Copy-Item -Path $sourcePath -Destination $destPath -Force
            $fileTime = ((Get-Date) - $fileStart).TotalSeconds
            
            $results.Files += @{
                Name = $file
                Time = $fileTime
                Size = (Get-Item $sourcePath).Length
            }
        }
        
        $results.EndTime = Get-Date
        $results.TotalTime = ($results.EndTime - $results.StartTime).TotalSeconds
    }
    catch {
        $results.Success = $false
        $results.Error = $_.Exception.Message
    }
    
    return $results
} -ArgumentList $LOCAL_STAGING_FOLDER, $ONEDRIVE_DATA_FOLDER, $CSV_FILES

Write-Host "  [BACKGROUND] OneDrive sync job started (Job ID: $($syncJob.Id))" -ForegroundColor Green
Write-Host ""

# ─────────────────────────────────────────────────────────────────────
# 2A-2. START BACKGROUND JOB: Rebuild DuckDB for dashboard
# ─────────────────────────────────────────────────────────────────────

Write-Host "  [BACKGROUND] Starting DuckDB rebuild job..." -ForegroundColor Yellow

$duckdbJob = Start-Job -ScriptBlock {
    param($ScriptFolder)
    $startTime = Get-Date
    try {
        $result = & python "$ScriptFolder\cleancloud_to_duckdb.py" 2>&1
        $elapsed = ((Get-Date) - $startTime).TotalSeconds
        return @{ Success = ($LASTEXITCODE -eq 0); Time = $elapsed; Output = ($result -join "`n") }
    }
    catch {
        $elapsed = ((Get-Date) - $startTime).TotalSeconds
        return @{ Success = $false; Time = $elapsed; Output = $_.Exception.Message }
    }
} -ArgumentList $PYTHON_SCRIPT_FOLDER

Write-Host "  [BACKGROUND] DuckDB rebuild job started (Job ID: $($duckdbJob.Id))" -ForegroundColor Green
Write-Host ""

# ─────────────────────────────────────────────────────────────────────
# 2B. FOREGROUND: Excel Refresh (reads from LOCAL)
# ─────────────────────────────────────────────────────────────────────

Write-Host "  [FOREGROUND] Opening Excel and refreshing data..." -ForegroundColor Cyan
Write-Host "               (Reading from LOCAL staging folder)" -ForegroundColor Gray
Write-Host ""

$excelStartTime = Get-Date
$phase1Time = 0
$phase2Time = 0
$phase3Time = 0
$phase4Time = 0
$phase5Time = 0
$phase6Time = 0

# Watchdog: kills EXCEL.EXE if refresh exceeds timeout
$excelTimeout = $MoonwalkConfig.ExcelTimeoutSeconds
Write-MoonwalkLog -Level INFO -Message "Excel refresh starting (timeout: ${excelTimeout}s)" -LogFile $LogFile

$watchdog = Start-Job -ScriptBlock {
    param($Seconds)
    Start-Sleep -Seconds $Seconds
    # If we reach here, Excel has hung
    Get-Process -Name 'EXCEL' -ErrorAction SilentlyContinue | Stop-Process -Force
    return "TIMEOUT"
} -ArgumentList $excelTimeout

try {
    # PHASE 1: Open Excel
    Write-Host "    [PHASE 1] Opening Excel application..." -ForegroundColor Yellow
    $phase1Start = Get-Date

    $excel = New-Object -ComObject Excel.Application
    $excel.Visible = $false
    $excel.DisplayAlerts = $false
    
    $phase1Time = ((Get-Date) - $phase1Start).TotalSeconds
    Write-Host "    [OK] Excel opened ($([math]::Round($phase1Time, 2))s)" -ForegroundColor Green
    
    # PHASE 2: Open Workbook
    Write-Host "    [PHASE 2] Opening workbook..." -ForegroundColor Yellow
    $phase2Start = Get-Date
    
    $workbook = $excel.Workbooks.Open($EXCEL_FILE)
    
    $phase2Time = ((Get-Date) - $phase2Start).TotalSeconds
    Write-Host "    [OK] Workbook opened ($([math]::Round($phase2Time, 2))s)" -ForegroundColor Green
    
    # PHASE 3: Configure Settings
    Write-Host "    [PHASE 3] Applying optimization settings..." -ForegroundColor Yellow
    $phase3Start = Get-Date
    
    $originalCalcMode = $excel.Calculation
    $excel.Calculation = $MoonwalkConfig.XlCalculationManual
    $excel.EnableEvents = $false
    $excel.ScreenUpdating = $false
    
    $phase3Time = ((Get-Date) - $phase3Start).TotalSeconds
    Write-Host "    [OK] Optimizations applied ($([math]::Round($phase3Time, 2))s)" -ForegroundColor Green
    
    # PHASE 4: PowerQuery Refresh (reads from LOCAL - should be FAST!)
    Write-Host "    [PHASE 4] Refreshing PowerQuery (from LOCAL CSVs)..." -ForegroundColor Yellow
    $phase4Start = Get-Date
    
    $workbook.RefreshAll()
    $excel.CalculateUntilAsyncQueriesDone()
    
    $phase4Time = ((Get-Date) - $phase4Start).TotalSeconds
    Write-Host "    [OK] PowerQuery refresh completed ($([math]::Round($phase4Time, 2))s)" -ForegroundColor Green
    
    # PHASE 5: PowerPivot Calculation
    Write-Host "    [PHASE 5] PowerPivot calculation..." -ForegroundColor Yellow
    $phase5Start = Get-Date
    
    $workbook.Application.Calculate()
    
    $phase5Time = ((Get-Date) - $phase5Start).TotalSeconds
    Write-Host "    [OK] PowerPivot calculation completed ($([math]::Round($phase5Time, 2))s)" -ForegroundColor Green
    
    # PHASE 6: Save (in place - updates same file)
    Write-Host "    [PHASE 6] Saving workbook..." -ForegroundColor Yellow
    $phase6Start = Get-Date
    
    # Restore settings
    $excel.Calculation = $originalCalcMode
    $excel.EnableEvents = $true
    $excel.ScreenUpdating = $true
    
    # Save in place (updates the same file)
    $workbook.Save()
    
    # Close workbook
    $workbook.Close($false)
    
    # Quit Excel
    $excel.Quit()
    
    # Clean up COM objects
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($workbook) | Out-Null
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null
    [System.GC]::Collect()
    [System.GC]::WaitForPendingFinalizers()
    
    $phase6Time = ((Get-Date) - $phase6Start).TotalSeconds
    Write-Host "    [OK] Saved and closed ($([math]::Round($phase6Time, 2))s)" -ForegroundColor Green
    
    # Cancel watchdog (Excel completed normally)
    Stop-Job -Job $watchdog -ErrorAction SilentlyContinue
    Remove-Job -Job $watchdog -Force -ErrorAction SilentlyContinue

    $excelTime = (Get-Date) - $excelStartTime
    Write-Host ""
    Write-Host "  [OK] Excel refresh completed in $([math]::Round($excelTime.TotalSeconds, 1)) seconds" -ForegroundColor Green
    Write-MoonwalkLog -Level INFO -Message "Excel refresh completed in $([math]::Round($excelTime.TotalSeconds, 1))s" -LogFile $LogFile
    Write-Host ""
}
catch {
    # Cancel watchdog
    Stop-Job -Job $watchdog -ErrorAction SilentlyContinue
    $watchdogOutput = Receive-Job -Job $watchdog -ErrorAction SilentlyContinue
    Remove-Job -Job $watchdog -Force -ErrorAction SilentlyContinue

    if ($watchdogOutput -eq "TIMEOUT") {
        Write-MoonwalkLog -Level ERROR -Message "Excel refresh timed out after ${excelTimeout}s - EXCEL.EXE killed by watchdog" -LogFile $LogFile
    }

    # COM cleanup
    if ($excel) {
        try {
            $excel.Calculation = $MoonwalkConfig.XlCalculationAutomatic
            $excel.EnableEvents = $true
            $excel.ScreenUpdating = $true
        } catch {}
    }
    if ($workbook) { try { $workbook.Close($false) } catch {} }
    if ($excel) { try { $excel.Quit() } catch {} }

    # Stop background jobs
    if ($syncJob) { Stop-Job -Job $syncJob; Remove-Job -Job $syncJob }
    if ($duckdbJob) { Stop-Job -Job $duckdbJob; Remove-Job -Job $duckdbJob }

    Exit-WithRefreshError "Excel refresh failed: $($_.Exception.Message)"
}

# ─────────────────────────────────────────────────────────────────────
# WAIT FOR BACKGROUND JOB TO COMPLETE
# ─────────────────────────────────────────────────────────────────────

Write-Host "  [BACKGROUND] Waiting for OneDrive sync to complete..." -ForegroundColor Yellow

$syncResult = Wait-Job -Job $syncJob | Receive-Job
Remove-Job -Job $syncJob

if ($syncResult.Success) {
    Write-Host "  [OK] OneDrive sync completed in $([math]::Round($syncResult.TotalTime, 2))s" -ForegroundColor Green
    Write-Host ""
    Write-Host "    Files synced to OneDrive:" -ForegroundColor Gray
    foreach ($file in $syncResult.Files) {
        $sizeMB = $file.Size / 1MB
        Write-Host "      - $($file.Name) ($([math]::Round($sizeMB, 2)) MB, $([math]::Round($file.Time, 2))s)" -ForegroundColor Gray
    }
    Write-Host ""
} else {
    Write-Host "  [WARNING] OneDrive sync had issues: $($syncResult.Error)" -ForegroundColor Yellow
    Write-Host "            Files remain in local staging: $LOCAL_STAGING_FOLDER" -ForegroundColor Yellow
    Write-Host ""
}

# Wait for DuckDB rebuild
Write-Host "  [BACKGROUND] Waiting for DuckDB rebuild to complete..." -ForegroundColor Yellow

$duckdbResult = Wait-Job -Job $duckdbJob | Receive-Job
Remove-Job -Job $duckdbJob

if ($duckdbResult.Success) {
    Write-Host "  [OK] DuckDB rebuild completed in $([math]::Round($duckdbResult.Time, 2))s" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "  [WARNING] DuckDB rebuild failed ($([math]::Round($duckdbResult.Time, 2))s) - dashboard will fall back to CSV" -ForegroundColor Yellow
    Write-Host ""
}

# =====================================================================
# SUMMARY
# =====================================================================

$totalTime = (Get-Date) - $startTime
$excelTotalTime = $phase1Time + $phase2Time + $phase3Time + $phase4Time + $phase5Time + $phase6Time

Write-Host "======================================================================"
Write-Host "COMPLETE! (PARALLEL SYNC v2.5.5)" -ForegroundColor Green
Write-Host "======================================================================"
Write-Host ""

Write-Host "Overall Timing:" -ForegroundColor Cyan
Write-Host "  Python Transformations:  $([math]::Round($pythonTime.TotalSeconds, 1))s"
Write-Host "  Excel Refresh + Save:    $([math]::Round($excelTotalTime, 1))s"
if ($syncResult.Success) {
    Write-Host "  OneDrive Sync (parallel): $([math]::Round($syncResult.TotalTime, 2))s" -ForegroundColor Gray
}
if ($duckdbResult.Success) {
    Write-Host "  DuckDB Rebuild (parallel): $([math]::Round($duckdbResult.Time, 2))s" -ForegroundColor Gray
}
Write-Host "  --------------------------------------------------------"
Write-Host "  TOTAL TIME:              $([math]::Round($totalTime.TotalSeconds, 1))s"
Write-Host ""

Write-Host "Excel Breakdown (6 Phases):" -ForegroundColor Cyan
Write-Host "  Phase 1: Open Excel           $([math]::Round($phase1Time, 2))s  ($([math]::Round(($phase1Time/$excelTotalTime)*100, 1))%)"
Write-Host "  Phase 2: Open Workbook        $([math]::Round($phase2Time, 2))s  ($([math]::Round(($phase2Time/$excelTotalTime)*100, 1))%)"
Write-Host "  Phase 3: Apply Settings       $([math]::Round($phase3Time, 2))s  ($([math]::Round(($phase3Time/$excelTotalTime)*100, 1))%)"
Write-Host "  Phase 4: PowerQuery Load      $([math]::Round($phase4Time, 2))s  ($([math]::Round(($phase4Time/$excelTotalTime)*100, 1))%)"
Write-Host "  Phase 5: PowerPivot Calc      $([math]::Round($phase5Time, 2))s  ($([math]::Round(($phase5Time/$excelTotalTime)*100, 1))%)"
Write-Host "  Phase 6: Save & Close         $([math]::Round($phase6Time, 2))s  ($([math]::Round(($phase6Time/$excelTotalTime)*100, 1))%)"
Write-Host ""

Write-Host "Workflow Strategy:" -ForegroundColor Cyan
Write-Host "  âœ" Python outputs to LOCAL (fast, no OneDrive sync)" -ForegroundColor Green
Write-Host "  âœ" Excel reads from LOCAL (no network overhead)" -ForegroundColor Green
Write-Host "  âœ" OneDrive sync runs in PARALLEL (not blocking)" -ForegroundColor Green
Write-Host "  âœ" DuckDB rebuild runs in PARALLEL (not blocking)" -ForegroundColor Green
Write-Host ""

Write-Host "Files Updated:" -ForegroundColor Cyan
Write-Host "  âœ" Local: $LOCAL_STAGING_FOLDER" -ForegroundColor Green
Write-Host "     - All_Customers_Python.csv"
Write-Host "     - All_Sales_Python.csv"
Write-Host "     - All_Items_Python.csv"
Write-Host "     - Customer_Quality_Monthly_Python.csv"
Write-Host "     - DimPeriod_Python.csv"
Write-Host ""
if ($syncResult.Success) {
    Write-Host "  âœ" OneDrive: $ONEDRIVE_DATA_FOLDER" -ForegroundColor Green
    Write-Host "     - (All 5 CSV files synced)" -ForegroundColor Gray
    Write-Host ""
}
if ($duckdbResult.Success) {
    Write-Host "  âœ" DuckDB: $DATA_FOLDER\analytics.duckdb" -ForegroundColor Green
    Write-Host "     - (Dashboard database rebuilt with indexes)" -ForegroundColor Gray
    Write-Host ""
}

Write-Host "Excel Workbook:" -ForegroundColor Cyan
Write-Host "  âœ" Updated: $EXCEL_FILENAME" -ForegroundColor Green
Write-Host "  âœ" Location: $DATA_FOLDER" -ForegroundColor Green
Write-Host ""

Write-Host "READY TO USE!" -ForegroundColor Green
Write-Host ""
Write-Host "======================================================================"
Write-Host ""

# Clean up lock file
Remove-LockFile
Write-Host "  [OK] Lock file removed" -ForegroundColor Green
Write-MoonwalkLog -Level INFO -Message "Refresh completed in $([math]::Round($totalTime.TotalSeconds, 1))s" -LogFile $LogFile
Write-Host ""

if (-not $NoPause) {
    Read-Host "Press Enter to exit"
}
