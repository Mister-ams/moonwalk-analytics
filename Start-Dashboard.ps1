<#
.SYNOPSIS
    Moonwalk Dashboard Orchestrator
.DESCRIPTION
    Single script to prepare the backend and launch the Streamlit dashboard.
    1. Pre-flight  — verifies required CSVs exist
    2. DuckDB      — rebuilds analytics.duckdb if CSVs are newer (or DB is missing)
    3. Port        — kills any stale Streamlit process on 8504
    4. Launch      — starts Streamlit and opens the browser
#>

$ErrorActionPreference = 'Stop'

# ── Configuration ───────────────────────────────────────────────────
$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Definition
. "$ScriptDir\config.ps1"
. "$ScriptDir\helpers.ps1"
. "$ScriptDir\logger_config.ps1"

$CsvFolder  = $MoonwalkConfig.LocalStagingFolder
$DataFolder = Split-Path $ScriptDir -Parent
$DbPath     = Join-Path $DataFolder 'analytics.duckdb'
$Port       = $MoonwalkConfig.DashboardPort

$RequiredCsvs = $MoonwalkConfig.RequiredCsvs

$Divider = '-' * 60

# ── Logging ───────────────────────────────────────────────────────
$LogFile = Initialize-MoonwalkLog -DataFolder $DataFolder

# ── 1. Pre-flight: check CSVs ──────────────────────────────────────
Write-Host ""
Write-Host "  $Divider"
Write-Host "  MOONWALK DASHBOARD - Starting up"
Write-Host "  $Divider"
Write-Host ""

Write-Step "Checking required CSV files ..."

$missing = @()
foreach ($csv in $RequiredCsvs) {
    $path = Join-Path $CsvFolder $csv
    if (-not (Test-Path $path)) {
        $missing += $csv
    }
}

if ($missing.Count -gt 0) {
    Write-Fail "Missing CSV files in ${CsvFolder}:"
    foreach ($m in $missing) { Write-Host "        - $m" -ForegroundColor Red }
    Write-Host ""
    Write-Fail "Run the ETL pipeline first:  python cleancloud_to_excel_MASTER.py"
    Write-Host ""
    Read-Host "  Press Enter to exit"
    exit 1
}

Write-Ok "All $($RequiredCsvs.Count) required CSVs found."

# ── 2. DuckDB: rebuild if stale or missing ─────────────────────────
Write-Step "Checking DuckDB freshness ..."

$needsRebuild = $false
$dbTmpPath    = "$DbPath.tmp"

# Promote a .tmp left by a Prefect run that couldn't rename the locked file.
# By the time Start-Dashboard runs the lock is usually cleared.
if (Test-Path $dbTmpPath) {
    $tmpTime = (Get-Item $dbTmpPath).LastWriteTime
    $dbTime  = if (Test-Path $DbPath) { (Get-Item $DbPath).LastWriteTime } else { [datetime]::MinValue }
    if ($tmpTime -gt $dbTime) {
        try {
            Move-Item -Path $dbTmpPath -Destination $DbPath -Force -ErrorAction Stop
            Write-Ok "Promoted analytics.duckdb.tmp (left by Prefect run)."
        }
        catch {
            Write-Warn "Could not promote .tmp file: $_"
        }
    }
}

if (-not (Test-Path $DbPath)) {
    Write-Warn "analytics.duckdb not found - will rebuild."
    $needsRebuild = $true
}
else {
    # Compare DB modified time against the newest CSV
    $dbTime = (Get-Item $DbPath).LastWriteTime
    $newestCsv = $RequiredCsvs |
        ForEach-Object { Get-Item (Join-Path $CsvFolder $_) } |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if ($newestCsv.LastWriteTime -gt $dbTime) {
        Write-Warn "CSVs are newer than DuckDB ($($newestCsv.Name) modified $($newestCsv.LastWriteTime.ToString('yyyy-MM-dd HH:mm'))) - will rebuild."
        $needsRebuild = $true
    }
    else {
        Write-Ok "DuckDB is up to date."
    }
}

if ($needsRebuild) {
    Write-Step "Rebuilding analytics.duckdb (timeout: $($MoonwalkConfig.DuckDbTimeoutSeconds)s) ..."
    $duckScript = Join-Path $ScriptDir 'cleancloud_to_duckdb.py'

    if (Test-Path $duckScript) {
        $duckResult = Invoke-WithTimeout -ScriptBlock {
            param($ScriptFolder)
            Set-Location $ScriptFolder
            $output = & python "$ScriptFolder\cleancloud_to_duckdb.py" 2>&1
            return @{ ExitCode = $LASTEXITCODE; Output = ($output -join "`n") }
        } -ArgumentList @($ScriptDir) -TimeoutSeconds $MoonwalkConfig.DuckDbTimeoutSeconds -Label 'DuckDB rebuild'

        if ($duckResult.TimedOut) {
            Write-MoonwalkLog -Level WARN -Message "DuckDB rebuild timed out after $($MoonwalkConfig.DuckDbTimeoutSeconds)s - dashboard will fall back to CSV." -LogFile $LogFile
        }
        elseif ($duckResult.Success -and $duckResult.Output.ExitCode -eq 0) {
            Write-Ok "DuckDB rebuild complete ($([math]::Round($duckResult.Seconds, 1))s)."
            Write-MoonwalkLog -Level INFO -Message "DuckDB rebuild complete ($([math]::Round($duckResult.Seconds, 1))s)" -LogFile $LogFile
        }
        else {
            Write-Warn "DuckDB rebuild failed - dashboard will fall back to CSV."
            Write-MoonwalkLog -Level WARN -Message "DuckDB rebuild failed after $([math]::Round($duckResult.Seconds, 1))s" -LogFile $LogFile
        }
    }
    else {
        Write-Warn "cleancloud_to_duckdb.py not found - dashboard will use CSV fallback."
    }
}

# ── 3. Port: clear stale Streamlit (retry loop) ──────────────────
Write-Step "Checking port $Port ..."

$portConn = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue

if ($portConn) {
    $procIds = $portConn | Select-Object -ExpandProperty OwningProcess -Unique
    Write-Warn "Port $Port is in use (PID: $($procIds -join ', ')) - stopping old process ..."

    foreach ($id in $procIds) {
        try {
            Stop-Process -Id $id -Force -ErrorAction Stop
            Write-Ok "Stopped PID $id."
        }
        catch {
            Write-Fail "Could not stop PID ${id}: $_"
        }
    }

    # Retry loop: check port every 2s, up to 5 attempts (10s total)
    $maxRetries = 5
    $portFreed = $false
    for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
        Start-Sleep -Seconds 2
        $stillBusy = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue
        if (-not $stillBusy) {
            $portFreed = $true
            break
        }
        Write-Warn "Port $Port still busy (attempt $attempt/$maxRetries) ..."
    }

    if (-not $portFreed) {
        Write-Fail "Port $Port is still occupied after $($maxRetries * 2)s. Cannot launch dashboard."
        Write-MoonwalkLog -Level ERROR -Message "Port $Port not freed after $($maxRetries * 2)s" -LogFile $LogFile
        Read-Host "  Press Enter to exit"
        exit 1
    }
}

Write-Ok "Port $Port is available."

# ── 4. Launch Streamlit ─────────────────────────────────────────────
Write-Step "Launching Streamlit dashboard on port $Port ..."

Start-Process cmd -ArgumentList "/k cd /d `"$ScriptDir`" && python -m streamlit run moonwalk_dashboard.py --server.port $Port --server.headless true"

Start-Sleep -Seconds 2
Start-Process "http://localhost:$Port"

Write-Host ""
Write-Host "  $Divider"
Write-Ok "Dashboard running at http://localhost:$Port"
Write-Host "  $Divider"
Write-Host ""
