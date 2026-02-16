# =====================================================================
# Persistent PowerShell logging — dot-source from other PS scripts
# Mirrors Python's logger_config.py: daily log file + console output.
# Log directory: Moonwalk Data/logs/  (same as Python ETL logs)
# =====================================================================

function Initialize-MoonwalkLog {
    <#
    .SYNOPSIS
        Create (or reuse) a daily log file and return its path.
    #>
    param([string]$DataFolder)

    $logsDir = Join-Path $DataFolder 'logs'
    if (-not (Test-Path $logsDir)) {
        New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
    }

    $dateStamp = (Get-Date).ToString('yyyy-MM-dd')
    $logFile   = Join-Path $logsDir "moonwalk_ps_$dateStamp.log"

    # Create if missing
    if (-not (Test-Path $logFile)) {
        New-Item -ItemType File -Path $logFile -Force | Out-Null
    }

    return $logFile
}

function Write-MoonwalkLog {
    <#
    .SYNOPSIS
        Write a message to console (colored) and append to the daily log file.
    .PARAMETER Level
        INFO, WARN, ERROR, or DEBUG.
    .PARAMETER Message
        The log message.
    .PARAMETER LogFile
        Path to the daily log file (from Initialize-MoonwalkLog).
    #>
    param(
        [ValidateSet('INFO','WARN','ERROR','DEBUG')]
        [string]$Level = 'INFO',
        [Parameter(Mandatory)][string]$Message,
        [string]$LogFile
    )

    $timestamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
    $logLine   = "$timestamp | $($Level.PadRight(5)) | $Message"

    # Console color by level
    $color = switch ($Level) {
        'INFO'  { 'Cyan'   }
        'WARN'  { 'Yellow' }
        'ERROR' { 'Red'    }
        'DEBUG' { 'Gray'   }
    }
    Write-Host "  $logLine" -ForegroundColor $color

    # File append (skip if no log file provided)
    if ($LogFile) {
        Add-Content -Path $LogFile -Value $logLine -Encoding UTF8
    }
}
