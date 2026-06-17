@echo off
REM ============================================================
REM  US EPS Momentum — 로컬 실행 래퍼
REM  목적: GitHub Actions 공유 IP의 yfinance rate-limit 회피
REM        (집 PC 가정용 IP는 야후 throttle 거의 없음)
REM
REM  사용법:
REM    run_local.bat        → 프로덕션 (채널 발송 + git push, GH와 동일)
REM    run_local.bat test   → 테스트 (개인봇만, 채널/푸시 없음, signal_local.txt)
REM ============================================================
setlocal

cd /d "%~dp0"

REM --- 1) 시크릿 로드 ---
if not exist "%~dp0secrets.local.bat" (
    echo [ERROR] secrets.local.bat 없음 — 시크릿 파일을 먼저 채우세요.
    exit /b 1
)
call "%~dp0secrets.local.bat"

REM --- 2) 파이썬 (volumequant env = yfinance 0.2.66, 요구사항 충족) ---
set "PYTHON=C:\Users\user\miniconda3\envs\volumequant\python.exe"
if not exist "%PYTHON%" (
    echo [ERROR] python 없음: %PYTHON%
    exit /b 1
)

REM --- 3) 실행 환경 ---
set "TZ=America/New_York"
set "MESSAGE_VERSION=v3"
if not exist logs mkdir logs
set "LOGFILE=logs\local_%date:~0,4%%date:~5,2%%date:~8,2%.log"

REM --- 4) 최신 코드 동기화 (다른 PC 변경 반영) ---
git pull --rebase origin master

if /i "%~1"=="test" goto :TESTRUN

REM ============ 프로덕션: 채널 발송 + git push ============
REM  GITHUB_ACTIONS=true → 채널 발송 켜짐 + daily_runner는 자체 push 스킵
REM  (실제 GH 워크플로우와 100% 동일 동작) → 아래에서 직접 commit/push
set "GITHUB_ACTIONS=true"
echo [%date% %time%] 프로덕션 실행 시작 >> "%LOGFILE%"
"%PYTHON%" -u daily_runner.py >> "%LOGFILE%" 2>&1

REM --- 결과 커밋/푸시 (GH 워크플로우 commit 스텝과 동일) ---
git add -A
git diff --staged --quiet || git commit -m "Daily update (local): %date:~0,10%"
git pull --no-rebase -X ours origin master
git push
echo [%date% %time%] 완료 >> "%LOGFILE%"
goto :END

:TESTRUN
REM ============ 테스트: 개인봇만, 채널/푸시 없음 ============
REM  GITHUB_ACTIONS 미설정 → send_to_channel=False → 개인봇만 + signal_local.txt
echo [%date% %time%] 테스트 실행 (채널 미발송) >> "%LOGFILE%"
"%PYTHON%" -u daily_runner.py >> "%LOGFILE%" 2>&1
echo [%date% %time%] 테스트 완료 — logs/, signal_local.txt 확인 >> "%LOGFILE%"

:END
endlocal
