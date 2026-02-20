# purge_and_encrypt.ps1
# Run AFTER closing Claude Code (to release the DuckDB file lock).
#
# What this script does:
#   1. Swaps encrypted analytics.duckdb.tmp into analytics.duckdb
#   2. Installs git-filter-repo (if missing)
#   3. Purges ALL unencrypted analytics.duckdb from git history
#   4. Re-adds the GitHub remote (filter-repo removes it)
#   5. Commits the new encrypted analytics.duckdb
#   6. Force-pushes to origin/master
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File purge_and_encrypt.ps1

$ErrorActionPreference = "Stop"

$RepoDir   = $PSScriptRoot  # PythonScript/
$DataDir   = Split-Path $RepoDir -Parent  # Moonwalk Data/
$DbSource  = Join-Path $DataDir "analytics.duckdb.tmp"
$DbTarget  = Join-Path $DataDir "analytics.duckdb"
$DbInRepo  = Join-Path $RepoDir "analytics.duckdb"
$Remote    = "https://github.com/Mister-ams/moonwalk-analytics.git"

function Write-Step($msg)  { Write-Host "`n>>> $msg" -ForegroundColor Cyan }
function Write-Ok($msg)    { Write-Host "    [OK] $msg" -ForegroundColor Green }
function Write-Fail($msg)  { Write-Host "    [FAIL] $msg" -ForegroundColor Red; exit 1 }

# ── Step 0: Pre-flight checks ────────────────────────────────────────
Write-Step "Pre-flight checks"

if (-not (Test-Path $DbSource)) {
    Write-Fail "Encrypted DB not found at: $DbSource`nRun 'MOONWALK_DUCKDB_KEY=moonwalk-duckdb-2026 python cleancloud_to_duckdb.py' first."
}

$sourceSize = (Get-Item $DbSource).Length / 1MB
Write-Ok "Encrypted DB found: analytics.duckdb.tmp ($([math]::Round($sourceSize, 1)) MB)"

# Check git is clean (except untracked files)
Set-Location $RepoDir
$gitStatus = git status --porcelain 2>&1
$tracked = $gitStatus | Where-Object { $_ -notmatch '^\?\?' }
if ($tracked) {
    Write-Fail "Git has uncommitted changes. Commit or stash first:`n$tracked"
}
Write-Ok "Git working tree is clean"

# ── Step 1: Swap encrypted DB into place ─────────────────────────────
Write-Step "Swapping encrypted DB into place"

try {
    Copy-Item -Path $DbSource -Destination $DbTarget -Force
    Write-Ok "Copied analytics.duckdb.tmp -> analytics.duckdb ($([math]::Round((Get-Item $DbTarget).Length / 1MB, 1)) MB)"
} catch {
    Write-Fail "Cannot overwrite analytics.duckdb — is another process holding it?`nClose any DuckDB connections and try again.`nError: $_"
}

# ── Step 2: Verify encryption ────────────────────────────────────────
Write-Step "Verifying encryption (should FAIL without key)"

$verifyResult = python -c @"
import duckdb
try:
    con = duckdb.connect(r'$DbTarget', read_only=True)
    print('UNENCRYPTED')
    con.close()
except Exception as e:
    if 'encrypted' in str(e).lower() or 'Cannot open' in str(e):
        print('ENCRYPTED')
    else:
        print(f'ERROR:{e}')
"@ 2>&1

if ($verifyResult -eq "ENCRYPTED") {
    Write-Ok "Database is encrypted (cannot open without key)"
} elseif ($verifyResult -eq "UNENCRYPTED") {
    Write-Fail "Database is NOT encrypted! Something went wrong with the build."
} else {
    Write-Fail "Unexpected verification result: $verifyResult"
}

# ── Step 3: Install git-filter-repo if needed ────────────────────────
Write-Step "Checking git-filter-repo"

$hasFilterRepo = $null
try { $hasFilterRepo = git filter-repo --version 2>&1 } catch {}

if (-not $hasFilterRepo -or $LASTEXITCODE -ne 0) {
    Write-Host "    Installing git-filter-repo via pip..."
    pip install git-filter-repo --quiet
    if ($LASTEXITCODE -ne 0) {
        Write-Fail "Could not install git-filter-repo. Install manually: pip install git-filter-repo"
    }
    Write-Ok "git-filter-repo installed"
} else {
    Write-Ok "git-filter-repo already installed"
}

# ── Step 4: Purge analytics.duckdb from git history ──────────────────
Write-Step "Purging unencrypted analytics.duckdb from ALL git history"
Write-Host "    This rewrites every commit that touched analytics.duckdb..." -ForegroundColor Yellow

# Count commits that contain analytics.duckdb
$commitCount = (git log --all --oneline -- analytics.duckdb 2>&1 | Measure-Object).Count
Write-Host "    Found $commitCount commits containing analytics.duckdb"

git filter-repo --path analytics.duckdb --invert-paths --force 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Fail "git filter-repo failed. Check output above."
}
Write-Ok "History purged ($commitCount commits rewritten)"

# ── Step 5: Re-add remote ────────────────────────────────────────────
Write-Step "Re-adding GitHub remote"

# filter-repo removes the remote as a safety measure
$existingRemote = git remote 2>&1
if ($existingRemote -notcontains "origin") {
    git remote add origin $Remote
    Write-Ok "Remote added: $Remote"
} else {
    Write-Ok "Remote already exists"
}

# ── Step 6: Copy encrypted DB into repo and commit ───────────────────
Write-Step "Adding encrypted analytics.duckdb to repo"

Copy-Item -Path $DbTarget -Destination $DbInRepo -Force
$repoDbSize = [math]::Round((Get-Item $DbInRepo).Length / 1MB, 1)
Write-Ok "Copied to repo ($repoDbSize MB)"

git add analytics.duckdb
git commit -m "Add encrypted analytics.duckdb (AES-256, requires DUCKDB_KEY)"
if ($LASTEXITCODE -ne 0) {
    Write-Fail "Commit failed"
}
Write-Ok "Committed"

# ── Step 7: Force push ───────────────────────────────────────────────
Write-Step "Force-pushing to origin/master"
Write-Host "    WARNING: This rewrites remote history!" -ForegroundColor Yellow
Write-Host ""

$confirm = Read-Host "    Type 'yes' to force-push to origin/master"
if ($confirm -ne "yes") {
    Write-Host "    Aborted. You can push manually with:" -ForegroundColor Yellow
    Write-Host "    git push --force-with-lease origin master" -ForegroundColor White
    exit 0
}

git push --force-with-lease origin master 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Fail "Push failed. Try: git push --force-with-lease origin master"
}
Write-Ok "Pushed to origin/master"

# ── Step 8: Cleanup ──────────────────────────────────────────────────
Write-Step "Cleanup"

if (Test-Path $DbSource) {
    Remove-Item $DbSource -Force
    Write-Ok "Removed analytics.duckdb.tmp"
}

# ── Done ──────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=" * 70 -ForegroundColor Green
Write-Host "  DONE! Unencrypted DB purged from history, encrypted DB pushed." -ForegroundColor Green
Write-Host "=" * 70 -ForegroundColor Green
Write-Host ""
Write-Host "  NEXT: Add secrets in Streamlit Cloud" -ForegroundColor Yellow
Write-Host "    1. Go to: https://share.streamlit.io/ -> Your app -> Settings -> Secrets" -ForegroundColor White
Write-Host '    2. Add:' -ForegroundColor White
Write-Host '         DASHBOARD_PASSWORD = "moonwalk2026"' -ForegroundColor White
Write-Host '         DUCKDB_KEY = "moonwalk-duckdb-2026"' -ForegroundColor White
Write-Host '         STREAMLIT_CLOUD = "1"' -ForegroundColor White
Write-Host ""
Write-Host "  Then reboot the app from the Streamlit Cloud dashboard." -ForegroundColor White
Write-Host ""

Read-Host "Press Enter to close"
