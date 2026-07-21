# Automated monthly referral-moat refresh (durable layer).
# Registered as Windows scheduled task "TraderV3 referral-moat refresh"
# (2nd of month). Rebuilds scorecards + site, commits ONLY
# referral_moat/data + site, pushes -> Railway redeploys with fresh seed
# and git keeps the dated snapshot forever.
#
# Data-only commits are operator-authorized (2026-07-21); this script must
# never stage anything outside referral_moat/data and referral_moat/site.
# Prefer the GitHub Actions version (.github/workflows/referral-refresh.yml)
# once the git PAT has `workflow` scope — then delete the scheduled task:
#   Unregister-ScheduledTask "TraderV3 referral-moat refresh" -Confirm:$false

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $env:LOCALAPPDATA "TraderV3"
New-Item -ItemType Directory -Force $logDir | Out-Null
$log = Join-Path $logDir "referral_refresh.log"

function Log($msg) { "$(Get-Date -Format s)  $msg" | Add-Content $log }

# Native commands (git/python) write progress to stderr; in PowerShell 5.1
# a bare `2>&1` turns those lines into terminating ErrorRecords. Routing
# through `cmd /c "... 2>&1"` merges the streams safely; $LASTEXITCODE
# still carries the child's exit code.
function Run($cmdline) {
    cmd /c "$cmdline 2>&1" | Add-Content $log
    return $LASTEXITCODE
}

try {
    Set-Location $repo
    Log "=== refresh start ==="

    if ((Run "git fetch origin") -ne 0) { Log "ABORT: git fetch failed"; exit 1 }
    if ((Run "git merge --ff-only origin/main") -ne 0) { Log "ABORT: local main diverged from origin"; exit 1 }

    if ((Run "python referral_moat/build.py") -ne 0) { Log "ABORT: build.py failed"; exit 1 }
    if ((Run "python referral_moat/make_site.py") -ne 0) { Log "ABORT: make_site.py failed"; exit 1 }

    Run "git add referral_moat/data referral_moat/site" | Out-Null
    cmd /c "git diff --cached --quiet"
    if ($LASTEXITCODE -eq 0) {
        Log "No data changes - nothing to commit."
    } else {
        $stamp = Get-Date -Format yyyy-MM-dd
        if ((Run "git commit -m `"referral_moat: automated monthly data refresh ($stamp)`"") -ne 0) { Log "ERROR: commit failed"; exit 1 }
        if ((Run "git push origin main") -ne 0) { Log "ERROR: push failed"; exit 1 }
        Log "Pushed refresh ($stamp)."
    }
    Log "=== refresh done ==="
} catch {
    Log "ERROR: $_"
    exit 1
}
