<#
.SYNOPSIS
    Moonwalk Analytics Launcher
.DESCRIPTION
    Interactive menu to launch workflows, ETL scripts, dashboard, Claude Code, and ChatGPT CLI.
    PowerShell replacement for the old Moonwalk Launcher.bat + moonwalk_launcher.py.
#>

$ErrorActionPreference = 'Continue'
$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Definition
. "$ScriptDir\config.ps1"
$WorkingDir  = $MoonwalkConfig.LocalStagingFolder
$ChatGPTLauncherScript = Join-Path $ScriptDir 'launch_chatgpt_cli.py'
$InvoiceAutomationDir = Join-Path (Split-Path $env:USERPROFILE) "$($env:USERNAME)\Downloads\InvoiceAutomation"
$PythonExe  = $null

$PythonCandidates = @()
if ($env:MOONWALK_PYTHON_EXE) { $PythonCandidates += $env:MOONWALK_PYTHON_EXE }
$PythonCandidates += (Join-Path $env:LOCALAPPDATA 'Python\bin\python.exe')

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if ($pythonCmd) {
    $pythonCmdPath = if ($pythonCmd.Path) { $pythonCmd.Path } else { $pythonCmd.Source }
    if ($pythonCmdPath) { $PythonCandidates += $pythonCmdPath }
}

foreach ($candidate in $PythonCandidates) {
    if (-not $candidate) { continue }
    if ((Test-Path $candidate) -and ($candidate -notmatch '\\Microsoft\\WindowsApps\\python\.exe$')) {
        $PythonExe = $candidate
        break
    }
}

# ── Script catalogue ────────────────────────────────────────────────
# WorkflowCmd: inline PS command string used when IsWorkflow = $true.
# For regular scripts, File drives the launch (ps1 or py auto-detected).
$Scripts = @(
    # ── Workflows (combined, launch in new window) ───────────────────
    @{
        IsWorkflow  = $true
        Label       = 'Prefect: ETL + DuckDB + Notion + Dashboard'
        Desc        = '[recommended]'
        Section     = 'Workflows'
        WorkflowCmd = "cd '$ScriptDir'; python '.\moonwalk_flow.py'; if (`$LASTEXITCODE -eq 0 -or `$LASTEXITCODE -eq `$null) { & '.\Start-Dashboard.ps1' } else { Write-Host '  Prefect flow failed - skipping dashboard.' -ForegroundColor Red; Read-Host 'Press Enter to exit' }"
    }
    @{
        IsWorkflow  = $true
        Label       = 'Legacy: Excel COM + OneDrive + Dashboard'
        Desc        = ''
        Section     = $null
        WorkflowCmd = "cd '$ScriptDir'; & '.\refresh_moonwalk_data.ps1' -NoPause; if (`$LASTEXITCODE -eq 0 -or `$LASTEXITCODE -eq `$null) { & '.\Start-Dashboard.ps1' } else { Write-Host '  Refresh failed - skipping dashboard.' -ForegroundColor Red; Read-Host 'Press Enter to exit' }"
    }

    # ── Refresh ──────────────────────────────────────────────────────
    @{ File = 'moonwalk_flow.py';            Desc = 'Prefect Flow: ETL + DuckDB + Notion  [recommended]';  Section = 'Refresh' }
    @{ File = 'Start-Dashboard.ps1';         Desc = 'Launch Dashboard (rebuilds DuckDB if stale)';          Section = $null }
    @{ File = 'refresh_cli.py';              Desc = 'Lightweight fallback: ETL + DuckDB, no Prefect / Notion'; Section = $null }

    # ── Utilities ────────────────────────────────────────────────────
    @{ IsInvoice = $true; Label = 'Invoice Automation'; Desc = 'Prefect: PDF -> Parse -> Stripe -> Postgres'; Section = 'Utilities' }
    @{ File = 'notion_kpi_push.py';          Desc = 'Push KPI rows to Notion database (standalone)';        Section = $null }
    @{ File = 'Start-API.ps1';               Desc = 'Start Operational API (local dev, port 8000)';         Section = $null }
    @{ File = 'verify_migration.py';         Desc = 'Verify ETL output against golden baselines';           Section = $null }
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
        $file = if ($entry.Label) { $entry.Label.PadRight(45) } elseif ($entry.IsWorkflow) { '>> COMBINED <<'.PadRight(45) } else { $entry.File.PadRight(45) }
        Write-Host "  [$num]  $file $($entry.Desc)"
    }

    Write-Host ''
    Write-Host "  $Divider"
    Write-Host '  [C]  Open Claude Code in project directory'
    Write-Host '  [G]  Open ChatGPT CLI in project directory'
    Write-Host '  [R]  Restart this launcher'
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

function Start-InvoiceAutomation {
    Write-Host "`n  Launching Invoice Automation (Prefect) ...`n"
    if (-not (Test-Path $InvoiceAutomationDir)) {
        Write-Host "  ERROR: InvoiceAutomation directory not found: $InvoiceAutomationDir" -ForegroundColor Red
        return
    }

    # Check Docker + Postgres
    $dockerOk = $false
    try {
        $null = docker ps 2>&1
        if ($LASTEXITCODE -eq 0) { $dockerOk = $true }
    } catch {}

    if (-not $dockerOk) {
        Write-Host '  ERROR: Docker is not running. Start Docker Desktop first.' -ForegroundColor Red
        return
    }

    $pgRunning = docker ps --filter "name=postgres" --format "{{.Names}}" 2>&1
    if ($pgRunning -notmatch 'postgres') {
        Write-Host '  ERROR: Postgres container is not running.' -ForegroundColor Red
        Write-Host '  Start it with:  docker start postgres' -ForegroundColor Yellow
        return
    }

    $flowScript = Join-Path $InvoiceAutomationDir 'flows\invoice_flow.py'
    if (-not (Test-Path $flowScript)) {
        Write-Host "  ERROR: invoice_flow.py not found: $flowScript" -ForegroundColor Red
        return
    }

    # Launch file picker + Prefect flow in a new window
    $invoiceCmd = @(
        "-ExecutionPolicy Bypass -NoExit -Command",
        "cd '$InvoiceAutomationDir';",
        "Write-Host '';",
        "Write-Host '  ============================================================' -ForegroundColor Cyan;",
        "Write-Host '  Invoice Automation - Prefect Flow' -ForegroundColor Cyan;",
        "Write-Host '  ============================================================' -ForegroundColor Cyan;",
        "Write-Host '  PDF -> Parse -> Phone Lookup -> Stripe Link -> Postgres' -ForegroundColor Gray;",
        "Write-Host '';",
        "Write-Host '  [1] Pick PDF file(s)' -ForegroundColor White;",
        "Write-Host '  [2] Start folder watcher (~/Downloads/Invoices/)' -ForegroundColor White;",
        "Write-Host '  [3] Exit' -ForegroundColor White;",
        "Write-Host '';",
        "`$c = Read-Host '  Select [1-3]';",
        "if (`$c -eq '3') { exit };",
        "if (`$c -eq '2') { python 'process_invoice.py' --watch; exit };",
        "Add-Type -AssemblyName System.Windows.Forms;",
        "`$d = New-Object System.Windows.Forms.OpenFileDialog;",
        "`$d.Filter = 'PDF Files (*.pdf)|*.pdf';",
        "`$d.Multiselect = `$true;",
        "`$d.Title = 'Select Invoice PDF(s)';",
        "if (`$d.ShowDialog() -ne 'OK') { Write-Host '  No file selected.'; exit };",
        "foreach (`$f in `$d.FileNames) {",
        "  Write-Host `"  Processing: `$f`" -ForegroundColor Yellow;",
        "  python 'flows\invoice_flow.py' `$f;",
        "  Write-Host '';",
        "};",
        "Read-Host '  Press Enter to exit'"
    ) -join ' '

    try {
        Start-Process powershell -ArgumentList $invoiceCmd
        Write-Host '  Invoice Automation window opened.'
    }
    catch {
        Write-Host '  ERROR: Could not launch Invoice Automation.' -ForegroundColor Red
    }
}

function Start-ChatGPTCLI {
    Write-Host "`n  Launching ChatGPT CLI ...`n"
    if (-not (Test-Path $WorkingDir)) {
        Write-Host "  ERROR: Working directory not found: $WorkingDir" -ForegroundColor Red
        return
    }

    # Codex CLI is a full-screen TUI — launch in a new PowerShell window
    # using the npm .ps1 shim so it gets direct terminal ownership.
    $codexScript = Join-Path $env:APPDATA 'npm\codex.ps1'
    if (-not (Test-Path $codexScript)) {
        Write-Host '  ERROR: Codex CLI not found. Install with: npm install -g @openai/codex' -ForegroundColor Red
        return
    }

    Write-Host "  Using: $codexScript"
    try {
        Start-Process powershell -ArgumentList "-ExecutionPolicy Bypass -NoExit -Command Set-Location '$WorkingDir'; & '$codexScript'"
        Write-Host '  Codex CLI window opened.'
    }
    catch {
        Write-Host '  ERROR: Could not launch Codex CLI.' -ForegroundColor Red
    }
}

function Start-Script ([int]$Index) {
    $entry    = $Scripts[$Index]
    $filename = $entry.File

    # Combined workflow — use the entry's WorkflowCmd directly
    if ($entry.IsWorkflow) {
        Write-Host "`n  Launching: $($entry.Desc)"
        Start-Process powershell -ArgumentList "-ExecutionPolicy Bypass -NoExit -Command $($entry.WorkflowCmd)"
        Write-Host '  Opened in new window.'
        return
    }

    # Invoice Automation — separate project directory
    if ($entry.IsInvoice) {
        Start-InvoiceAutomation
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
    elseif ($choice -eq 'G') {
        Start-ChatGPTCLI
        Read-Host "`n  Press Enter to continue..."
    }
    elseif ($choice -eq 'R') {
        Write-Host "`n  Restarting launcher ..."
        Start-Process powershell -ArgumentList "-ExecutionPolicy Bypass -File `"$($MyInvocation.MyCommand.Definition)`""
        exit
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
