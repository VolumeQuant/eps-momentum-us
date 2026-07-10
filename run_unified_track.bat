@echo off
rem ── 통합(US+KR) VM 트랙 18:10 schtask (회사PC) ──
rem 2026-07-10 감사수리 3: 구버전은 `git pull --ff-only >nul 2>&1` — 로컬 원장(unified_vm_log.csv,
rem   append만 하고 미커밋)이 원격 커밋과 어긋나는 순간 pull이 '조용히' 실패, 그때부터 영원히
rem   낡은 코드로 실행되는 구조였음. 수리: ①원장을 실행 전 커밋(작업트리 클린 보장) ②pull을
rem   --rebase로 + 결과를 로그에 남김(무음 금지) ③실행 후 원장 커밋+push(단일 작성자 원칙 —
rem   원장은 이 schtask만 커밋, 샘플/일회성 실행은 UNIFIED_NO_LOG=1).
cd /d "C:\dev\claude code\eps-momentum-us"
echo [%date% %time%] ===== run_unified_track start ===== >> logs\unified_track.log
git add data_cache/unified_vm_log.csv data_cache/ticker_names.json >nul 2>&1
git diff --cached --quiet || git commit -m "data: unified ledger (auto, pre-pull)" >> logs\unified_track.log 2>&1
git pull --rebase origin master >> logs\unified_track.log 2>&1
if errorlevel 1 (
    echo [%date% %time%] !! git pull FAILED - STALE CODE RISK, check conflicts >> logs\unified_track.log
    git rebase --abort >nul 2>&1
)
"C:\Users\user\miniconda3\envs\volumequant\python.exe" unified_vm_track.py --run >> logs\unified_track.log 2>&1
git add data_cache/unified_vm_log.csv data_cache/ticker_names.json >nul 2>&1
git diff --cached --quiet || git commit -m "data: unified ledger (auto)" >> logs\unified_track.log 2>&1
git push origin master >> logs\unified_track.log 2>&1
if errorlevel 1 echo [%date% %time%] !! git push FAILED - will retry next run via pre-pull commit >> logs\unified_track.log
echo [%date% %time%] ===== run_unified_track end ===== >> logs\unified_track.log
