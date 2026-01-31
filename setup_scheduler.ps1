# Windows Task Scheduler 등록 스크립트
# PowerShell 관리자 권한으로 실행 필요

$TaskName = "EPS_Momentum_Daily"
$TaskPath = "\EPS_Momentum\"
$ScriptPath = "C:\dev\claude-code\eps-momentum-us\run_daily.bat"

# 기존 태스크 삭제 (있으면)
Unregister-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath -Confirm:$false -ErrorAction SilentlyContinue

# 트리거: 매일 07:00 (한국 시간, 미국 장 마감 후)
$Trigger = New-ScheduledTaskTrigger -Daily -At "07:00"

# 액션: 배치 파일 실행
$Action = New-ScheduledTaskAction -Execute $ScriptPath -WorkingDirectory "C:\dev\claude-code\eps-momentum-us"

# 설정
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

# 태스크 등록
Register-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath -Trigger $Trigger -Action $Action -Settings $Settings -Description "EPS Momentum Daily Screening"

Write-Host "Task Scheduler 등록 완료: $TaskPath$TaskName"
Write-Host "매일 07:00에 자동 실행됩니다."
