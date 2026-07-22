# Unified signal watchdog v2 (2026-07-22) — GH cron은 시계가 아니다.
# 실측: GH가 09~11 UTC 스케줄을 매일 1.5~4h 지연(7/15~22 전수) → 워치독이 주 시계, GH cron은 백업.
# 트리거: 평일 18:20 + 19:30 (+켜질 때 캐치업). 머신 독립: 경로는 스크립트 위치 기준(집PC/회사PC 공용).
$repo = $PSScriptRoot
$log = Join-Path $env:USERPROFILE 'unified_watchdog.log'
$gh = if (Get-Command gh -ErrorAction SilentlyContinue) { 'gh' } else { 'C:\Program Files\GitHub CLI\gh.exe' }
Set-Location $repo
git fetch origin master 2>&1 | Out-Null
$today = Get-Date -Format 'yyyy-MM-dd'

# ① 오늘 블록이 이미 origin에 있으면 끝
$ledger = git show origin/master:data_cache/unified_vm_log.csv 2>$null
if ($ledger | Select-String -Pattern "^$today," -Quiet) {
    Add-Content $log "$(Get-Date -Format s) OK — 오늘 블록 존재" -Encoding UTF8
    exit 0
}
# ② 실행 중이거나 최근 15분 내 생성된 run이 있으면 대기 (이중발송 방지 — dispatch는 guard를 안 타고,
#    방금 끝난 run의 원장 push가 fetch보다 늦게 도착하는 레이스(7/22 실측)도 이 창으로 흡수)
$runs = & $gh run list --workflow=unified-signal.yml --limit 10 --json status,createdAt 2>$null | ConvertFrom-Json
$cut = (Get-Date).ToUniversalTime().AddMinutes(-15)
$busy = @($runs | Where-Object { $_.status -in @('in_progress', 'queued') -or ([datetime]$_.createdAt).ToUniversalTime() -gt $cut }).Count
if ($busy -gt 0) {
    Add-Content $log "$(Get-Date -Format s) SKIP — 진행/최근 run ${busy}건" -Encoding UTF8
    exit 0
}
# ③ 발동
& $gh workflow run unified-signal.yml --ref master 2>&1 | Out-Null
Add-Content $log "$(Get-Date -Format s) DISPATCHED — 오늘($today) 블록 없음" -Encoding UTF8
