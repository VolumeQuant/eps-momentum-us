# Unified signal watchdog — GH cron 드랍 자가치유 (2026-07-20 설치)
# 매일 20:30 KST(평일): 오늘 원장 블록이 origin에 없으면 workflow_dispatch로 대신 발동.
$repo = 'C:\dev\claude-code\eps-momentum-us'
$log = 'C:\dev\claude-code\unified_watchdog.log'
Set-Location $repo
git fetch origin master 2>&1 | Out-Null
$today = Get-Date -Format 'yyyy-MM-dd'
$ledger = git show origin/master:data_cache/unified_vm_log.csv 2>$null
if ($ledger | Select-String -Pattern "^$today," -Quiet) {
    Add-Content $log "$(Get-Date -Format s) OK — 오늘 블록 존재, 개입 없음"
    exit 0
}
gh workflow run unified-signal.yml --ref master 2>&1 | Out-Null
Add-Content $log "$(Get-Date -Format s) DISPATCHED — 오늘($today) 블록 없어 수동 발동"
