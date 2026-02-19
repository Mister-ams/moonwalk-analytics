<#
.SYNOPSIS
    Moonwalk Operational API local dev launcher
.DESCRIPTION
    Starts the FastAPI server on 127.0.0.1:8000 with auto-reload enabled.
    Requires a .env file with MOONWALK_API_KEY set.
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
. "$ScriptDir\config.ps1"
. "$ScriptDir\helpers.ps1"
. "$ScriptDir\logger_config.ps1"

$DataFolder = Split-Path $ScriptDir -Parent
$Port       = 8000
$Divider    = '-' * 60

# ── Logging ───────────────────────────────────────────────────────
$LogFile = Initialize-MoonwalkLog -DataFolder $DataFolder

# ── Header ────────────────────────────────────────────────────────
Write-Host ""
Write-Host "  $Divider"
Write-Host "  MOONWALK OPERATIONAL API - Starting up"
Write-Host "  $Divider"
Write-Host ""

# ── Check for .env file ───────────────────────────────────────────
$EnvFile = Join-Path $ScriptDir '.env'
if (-not (Test-Path $EnvFile)) {
    Write-Warn ".env file not found at: $EnvFile"
    Write-Warn "Copy .env.example to .env and set MOONWALK_API_KEY before starting."
    Write-Host ""
    Write-Warn "The API will start but ALL requests will be rejected (no key configured)."
    Write-Host ""
}
else {
    Write-Ok ".env file found."
}

# ── Check uvicorn is available ────────────────────────────────────
Write-Step "Checking uvicorn ..."
$uvicorn = Get-Command uvicorn -ErrorAction SilentlyContinue
if (-not $uvicorn) {
    Write-Fail "uvicorn not found. Install with: pip install 'uvicorn[standard]'"
    Write-MoonwalkLog -Level ERROR -Message "uvicorn not found — cannot start API" -LogFile $LogFile
    Read-Host "  Press Enter to exit"
    exit 1
}
Write-Ok "uvicorn found."

# ── Launch ────────────────────────────────────────────────────────
Write-Step "Launching Moonwalk Operational API on port $Port ..."
Write-Host ""
Write-Host "  $Divider"
Write-Ok "API running at http://127.0.0.1:$Port"
Write-Ok "Docs at      http://127.0.0.1:$Port/docs"
Write-Host "  $Divider"
Write-Host ""
Write-MoonwalkLog -Level INFO -Message "Starting API on port $Port" -LogFile $LogFile

Set-Location $ScriptDir
python -m uvicorn api.main:app --host 127.0.0.1 --port $Port --reload
