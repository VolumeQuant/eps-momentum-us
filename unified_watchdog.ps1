# Unified signal watchdog v2.1 (2026-07-23) — GH cron is NOT a clock (1.5~4h delay measured daily).
# Weekday triggers 18:20/19:30 KST + WakeToRun + StartWhenAvailable catch-up. Machine-independent.
# NOTE: log strings are ASCII-only — PS5.1 misparses BOM-less UTF-8 Korean (2026-07-23 parse crash).
$log = Join-Path $env:USERPROFILE 'unified_watchdog.log'
function W($msg) { Add-Content $log "$(Get-Date -Format s) $msg" }
try {
    $repo = $PSScriptRoot
    Set-Location $repo
    $gh = if (Get-Command gh -ErrorAction SilentlyContinue) { 'gh' } else { 'C:\Program Files\GitHub CLI\gh.exe' }

    # wait for network up to 3 min (wake/boot catch-up runs)
    $net = $false
    foreach ($i in 1..18) {
        if (Test-Connection -ComputerName github.com -Count 1 -Quiet -ErrorAction SilentlyContinue) { $net = $true; break }
        Start-Sleep -Seconds 10
    }
    if (-not $net) { W 'ABORT - no network after 3min'; exit 0 }

    git fetch origin master 2>&1 | Out-Null
    $today = Get-Date -Format 'yyyy-MM-dd'

    # 1) today's ledger block already on origin -> done
    $ledger = git show origin/master:data_cache/unified_vm_log.csv 2>$null
    if ($ledger | Select-String -Pattern "^$today," -Quiet) { W 'OK - block exists'; exit 0 }

    # 2) in-progress or <15min-old runs -> skip (prevents double-send + push race)
    $json = & $gh run list --workflow=unified-signal.yml --limit 10 --json status,createdAt 2>$null
    $busy = 0
    if ($json) {
        $runs = $json | ConvertFrom-Json
        $cut = (Get-Date).ToUniversalTime().AddMinutes(-15)
        $busy = @($runs | Where-Object { $_.status -in @('in_progress', 'queued') -or ([datetime]$_.createdAt).ToUniversalTime() -gt $cut }).Count
    }
    if ($busy -gt 0) { W "SKIP - busy runs: $busy"; exit 0 }

    # 3) dispatch
    & $gh workflow run unified-signal.yml --ref master 2>&1 | Out-Null
    W "DISPATCHED - no block for $today"
    exit 0
} catch {
    try { W ("ERROR - " + $_.Exception.Message) } catch {}
    exit 1
}
