<#
.SYNOPSIS
    Moonwalk Analytics Launcher
.DESCRIPTION
    Interactive menu to launch workflows, ETL scripts, dashboard, and Claude Code.
    PowerShell replacement for the old Moonwalk Launcher.bat + moonwalk_launcher.py.
#>

$ErrorActionPreference = 'Continue'
$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Definition
. "$ScriptDir\config.ps1"
$WorkingDir  = $MoonwalkConfig.LocalStagingFolder

# ── Script catalogue ────────────────────────────────────────────────
$Scripts = @(
    # Workflows (combined operations)
    @{ File = '_workflow_refresh_dashboard';     Desc = 'Full Refresh then Launch Dashboard';                Section = 'Workflows';             IsWorkflow = $true }
    @{ File = 'refresh_moonwalk_data.ps1';       Desc = 'Full Refresh: ETL + Excel + OneDrive + DuckDB';    Section = $null }
    @{ File = 'Start-Dashboard.ps1';             Desc = 'Launch Dashboard (rebuilds DuckDB if stale)';      Section = $null }

    # ETL Pipeline
    @{ File = 'cleancloud_to_excel_MASTER.py';   Desc = 'ETL orchestrator - runs all transforms';           Section = 'ETL Pipeline' }
    @{ File = 'cleancloud_to_duckdb.py';         Desc = 'Rebuild analytics.duckdb from staging CSVs';       Section = $null }

    # Individual Transforms
    @{ File = 'generate_dimperiod.py';                  Desc = 'Date dimension table (auto 3-month lookahead)';  Section = 'Individual Transforms' }
    @{ File = 'transform_all_customers.py';             Desc = 'Customer master data';                           Section = $null }
    @{ File = 'transform_all_sales.py';                 Desc = 'Orders + Subscriptions + Invoices';              Section = $null }
    @{ File = 'transform_all_items.py';                 Desc = 'Item-level data with categories';                Section = $null }
    @{ File = 'transform_customer_quality_monthly.py';  Desc = 'Monthly customer quality scoring';               Section = $null }

    # Utilities
    @{ File = 'extract_powerquery_mcode.ps1';    Desc = 'Extract Power Query M-code from Excel';             Section = 'Utilities' }
)

$Divider = '-' * 70

# ── Menu ────────────────────────────────────────────────────────────
function Show-Menu {
    Clear-Host
    Write-Host "  $Divider"
    Write-Host '  MOONWALK ANALYTICS - Script Launcher'
    Write-Host "  $Divider"
    Write-Host ''

    for ($i = 0; $i -lt $Scripts.Count; $i++) {
        $entry = $Scripts[$i]

        if ($entry.Section) {
            if ($i -gt 0) { Write-Host '' }
            Write-Host "  $($entry.Section)"
            Write-Host "  $('-' * 40)"
        }

        $num  = '{0,2}' -f ($i + 1)
        $file = if ($entry.IsWorkflow) { '>> COMBINED <<'.PadRight(45) } else { $entry.File.PadRight(45) }
        Write-Host "  [$num]  $file $($entry.Desc)"
    }

    Write-Host ''
    Write-Host "  $Divider"
    Write-Host '  [C]  Open Claude Code in project directory'
    Write-Host '  [Q]  Quit'
    Write-Host "  $Divider"
    Write-Host ''
}

# ── Launch helpers ──────────────────────────────────────────────────
function Start-ClaudeCode {
    Write-Host "`n  Launching Claude Code ...`n"
    try {
        Start-Process cmd -ArgumentList "/k cd /d `"$WorkingDir`" && claude"
        Write-Host '  Claude Code window opened.'
    }
    catch {
        Write-Host "  ERROR: Could not launch Claude Code. Is it installed and in PATH?"
    }
}

function Start-Script ([int]$Index) {
    $entry    = $Scripts[$Index]
    $filename = $entry.File

    # Combined workflow: Refresh (no pause) then Dashboard
    if ($entry.IsWorkflow) {
        Write-Host "`n  Launching: Full Refresh + Dashboard"
        Write-Host "  Refresh data, then open dashboard automatically."
        Start-Process powershell -ArgumentList (
            "-ExecutionPolicy Bypass -NoExit -Command " +
            "cd '$ScriptDir'; " +
            "& '.\refresh_moonwalk_data.ps1' -NoPause; " +
            "if (`$LASTEXITCODE -eq 0 -or `$LASTEXITCODE -eq `$null) { & '.\Start-Dashboard.ps1' } " +
            "else { Write-Host '  Refresh failed - skipping dashboard.' -ForegroundColor Red; Read-Host 'Press Enter to exit' }"
        )
        Write-Host '  Opened in new window.'
        return
    }

    $filepath = Join-Path $ScriptDir $filename

    if (-not (Test-Path $filepath)) {
        Write-Host "`n  File not found: $filepath"
        return
    }

    Write-Host "`n  Launching: $filename"
    Write-Host "  $($entry.Desc)"

    if ($filename -match '\.ps1$') {
        Start-Process powershell -ArgumentList "-ExecutionPolicy Bypass -NoExit -Command cd '$ScriptDir'; & '.\$filename'"
    }
    else {
        Start-Process powershell -ArgumentList "-ExecutionPolicy Bypass -NoExit -Command cd '$ScriptDir'; python '.\$filename'"
    }

    Write-Host '  Opened in new window.'
}

# ── Main loop ───────────────────────────────────────────────────────
while ($true) {
    Show-Menu
    $choice = (Read-Host '  Select an option').Trim().ToUpper()

    if ($choice -eq 'Q') {
        Write-Host "`n  Goodbye.`n"
        break
    }
    elseif ($choice -eq 'C') {
        Start-ClaudeCode
        Read-Host "`n  Press Enter to continue..."
    }
    else {
        $idx = $null
        if ([int]::TryParse($choice, [ref]$idx)) {
            $idx--   # 0-based
            if ($idx -ge 0 -and $idx -lt $Scripts.Count) {
                Start-Script $idx
                Read-Host "`n  Press Enter to continue..."
            }
            else {
                Write-Host '  Invalid selection.'
                Read-Host "`n  Press Enter to continue..."
            }
        }
        else {
            Write-Host '  Invalid input.'
            Read-Host "`n  Press Enter to continue..."
        }
    }
}
