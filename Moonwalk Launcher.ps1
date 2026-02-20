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
$DevTeamAgentDir = Join-Path (Split-Path $env:USERPROFILE) "$($env:USERNAME)\Downloads\DevTeam_Agent"
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
    @{ File = 'cleancloud_to_duckdb.py';     Desc = 'Rebuild analytics.duckdb from Parquet files';          Section = 'Utilities' }
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
    Write-Host '  [D]  DevTeam Agent - AI Planning Pipeline'
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

function Start-DevTeamAgent {
    Write-Host "`n  Launching DevTeam Agent - Planning Pipeline ...`n"
    if (-not (Test-Path $DevTeamAgentDir)) {
        Write-Host "  ERROR: DevTeam Agent directory not found: $DevTeamAgentDir" -ForegroundColor Red
        return
    }
    if (-not $PythonExe) {
        Write-Host '  ERROR: Python not found. Cannot launch DevTeam Agent.' -ForegroundColor Red
        return
    }

    $runPlanner = Join-Path $DevTeamAgentDir 'run_planner.py'
    if (-not (Test-Path $runPlanner)) {
        Write-Host "  ERROR: run_planner.py not found in: $DevTeamAgentDir" -ForegroundColor Red
        return
    }

    # Launch in a new PowerShell window with an interactive prompt loop
    $defaultRoot = $ScriptDir
    $agentCmd = @(
        "-ExecutionPolicy Bypass -NoExit -Command",
        "cd '$DevTeamAgentDir';",
        "Write-Host '';",
        "Write-Host '  ============================================================' -ForegroundColor Cyan;",
        "Write-Host '  DevTeam Agent - AI Planning Pipeline (v0.5.0)' -ForegroundColor Cyan;",
        "Write-Host '  ============================================================' -ForegroundColor Cyan;",
        "Write-Host '  Generates project roadmaps with Tick/Tock milestones,' -ForegroundColor Gray;",
        "Write-Host '  red-team risk assessment, and sprint sequencing.' -ForegroundColor Gray;",
        "Write-Host '';",
        "Write-Host '  Flags: --model MODEL | --root PATH | --redact-pii | --strict-schema | --json-summary' -ForegroundColor Gray;",
        "Write-Host '  Type `"exit`" to close this window.' -ForegroundColor Gray;",
        "Write-Host '';",
        "`$hasKey = [bool]`$env:OPENAI_API_KEY;",
        "if (`$hasKey) { Write-Host '  Mode: LLM (OpenAI API key detected)' -ForegroundColor Green }",
        "else { Write-Host '  Mode: Template (no API key - set OPENAI_API_KEY for LLM mode)' -ForegroundColor Yellow };",
        "Write-Host '';",
        "Write-Host '  Available models:' -ForegroundColor Gray;",
        "Write-Host '    [1] gpt-4o-mini  (fast, cheap - default)' -ForegroundColor Gray;",
        "Write-Host '    [2] gpt-4o       (balanced)' -ForegroundColor Gray;",
        "Write-Host '    [3] o3-mini      (reasoning)' -ForegroundColor Gray;",
        "Write-Host '    [4] Custom       (enter model name)' -ForegroundColor Gray;",
        "Write-Host '';",
        "`$defaultModel = if (`$env:OPENAI_MODEL) { `$env:OPENAI_MODEL } else { 'gpt-4o-mini' };",
        "`$mc = Read-Host `"  Select model [1-4] or Enter for `$defaultModel`";",
        "`$selectedModel = switch (`$mc) { '1' { 'gpt-4o-mini' } '2' { 'gpt-4o' } '3' { 'o3-mini' } '4' { Read-Host '  Enter model name' } default { `$defaultModel } };",
        "Write-Host `"  Using model: `$selectedModel`" -ForegroundColor Green;",
        "Write-Host '';",
        "`$projectRoot = Read-Host '  Project root for stack discovery [Enter for $defaultRoot]';",
        "if ([string]::IsNullOrWhiteSpace(`$projectRoot)) { `$projectRoot = '$defaultRoot' };",
        "Write-Host `"  Stack discovery root: `$projectRoot`" -ForegroundColor Green;",
        "Write-Host '';",
        "while (`$true) {",
        "  `$p = Read-Host `"  [`$selectedModel] Enter your project prompt (or exit)`";",
        "  if (`$p -eq 'exit') { exit };",
        "  if ([string]::IsNullOrWhiteSpace(`$p)) { continue };",
        "  Write-Host '';",
        "  if (`$hasKey) { & '$PythonExe' '$runPlanner' --prompt `$p --model `$selectedModel --root `$projectRoot --json-summary -v }",
        "  else { & '$PythonExe' '$runPlanner' --prompt `$p --use-template --root `$projectRoot --json-summary -v };",
        "  Write-Host '';",
        "}"
    ) -join ' '

    try {
        Start-Process powershell -ArgumentList $agentCmd
        Write-Host '  DevTeam Agent window opened.'
    }
    catch {
        Write-Host '  ERROR: Could not launch DevTeam Agent.' -ForegroundColor Red
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
    elseif ($choice -eq 'D') {
        Start-DevTeamAgent
        Read-Host "`n  Press Enter to continue..."
    }
    elseif ($choice -eq 'G') {
        Start-ChatGPTCLI
        Read-Host "`n  Press Enter to continue..."
    }
    elseif ($choice -eq 'R') {
        continue
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
