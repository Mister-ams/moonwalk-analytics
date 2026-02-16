# =====================================================================
# Shared PowerShell helpers — dot-source from other PS scripts
# =====================================================================

# ── Console output helpers ───────────────────────────────────────────

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

# ── Error exit ───────────────────────────────────────────────────────

function Exit-WithError {
    param(
        [Parameter(Mandatory)][string]$Message,
        [string]$LockFile,
        [switch]$NoPause
    )
    Write-Host "  ERROR: $Message" -ForegroundColor Red
    if ($LockFile -and (Test-Path $LockFile)) {
        Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
    }
    if (-not $NoPause) { Read-Host "Press Enter to exit" }
    exit 1
}

# ── Command existence check ──────────────────────────────────────────

function Test-Command ([string]$Name) {
    <#
    .SYNOPSIS
        Returns $true if a command (exe, alias, function) exists in the current session/PATH.
    #>
    $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

# ── Timeout wrapper ──────────────────────────────────────────────────

function Invoke-WithTimeout {
    <#
    .SYNOPSIS
        Run a script block as a background job with a timeout.
    .PARAMETER ScriptBlock
        The code to execute.
    .PARAMETER ArgumentList
        Arguments passed to the script block.
    .PARAMETER TimeoutSeconds
        Maximum seconds to wait. Default 120.
    .PARAMETER Label
        Human-readable label for log messages.
    .OUTPUTS
        Hashtable with keys: Success, TimedOut, Output, Seconds
    #>
    param(
        [Parameter(Mandatory)][ScriptBlock]$ScriptBlock,
        [object[]]$ArgumentList = @(),
        [int]$TimeoutSeconds = 120,
        [string]$Label = 'Job'
    )

    $job = Start-Job -ScriptBlock $ScriptBlock -ArgumentList $ArgumentList
    $startTime = Get-Date

    $finished = $job | Wait-Job -Timeout $TimeoutSeconds

    $elapsed = ((Get-Date) - $startTime).TotalSeconds

    if (-not $finished) {
        # Timed out
        Stop-Job -Job $job -ErrorAction SilentlyContinue
        $output = Receive-Job -Job $job -ErrorAction SilentlyContinue
        Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
        return @{
            Success    = $false
            TimedOut   = $true
            Output     = $output
            Seconds    = $elapsed
        }
    }

    $output = Receive-Job -Job $job
    $jobState = $job.State
    Remove-Job -Job $job -Force -ErrorAction SilentlyContinue

    return @{
        Success    = ($jobState -eq 'Completed')
        TimedOut   = $false
        Output     = $output
        Seconds    = $elapsed
    }
}
