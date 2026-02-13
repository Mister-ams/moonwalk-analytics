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
$CsvFolder  = $MoonwalkConfig.LocalStagingFolder
$DbPath     = Join-Path (Split-Path $ScriptDir -Parent) 'analytics.duckdb'
$Port       = 8504

$RequiredCsvs = @(
    'All_Sales_Python.csv'
    'All_Items_Python.csv'
    'All_Customers_Python.csv'
    'DimPeriod_Python.csv'
)

$Divider = '-' * 60

# ── Helpers ─────────────────────────────────────────────────────────
function Write-Step ([string]$Message) {
    Write-Host "  [*] $Message" -ForegroundColor Cyan
}

function Write-Ok ([string]$Message) {
    Write-Host "  [+] $Message" -ForegroundColor Green
}

function Write-Warn ([string]$Message) {
    Write-Host "  [!] $Message" -ForegroundColor Yellow
}

function Write-Fail ([string]$Message) {
    Write-Host "  [-] $Message" -ForegroundColor Red
}

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
    Write-Step "Rebuilding analytics.duckdb ..."
    $duckScript = Join-Path $ScriptDir 'cleancloud_to_duckdb.py'

    if (Test-Path $duckScript) {
        Push-Location $ScriptDir
        try {
            python $duckScript 2>&1 | ForEach-Object { Write-Host "        $_" }
            if ($LASTEXITCODE -eq 0) {
                Write-Ok "DuckDB rebuild complete."
            }
            else {
                Write-Warn "DuckDB rebuild returned exit code $LASTEXITCODE - dashboard will fall back to CSV."
            }
        }
        catch {
            Write-Warn "DuckDB rebuild failed: $_ - dashboard will fall back to CSV."
        }
        finally { Pop-Location }
    }
    else {
        Write-Warn "cleancloud_to_duckdb.py not found - dashboard will use CSV fallback."
    }
}

# ── 3. Port: clear stale Streamlit ─────────────────────────────────
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

    # Brief pause to let the port release
    Start-Sleep -Seconds 2

    # Verify port is free
    $stillBusy = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue
    if ($stillBusy) {
        Write-Fail "Port $Port is still occupied. Cannot launch dashboard."
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
