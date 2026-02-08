@echo off
chcp 65001 >nul 2>&1
REM EPS Momentum Daily Runner v9.0 - Windows Task Scheduler용

REM 작업 디렉토리 이동
cd /d C:\dev\claude-code\eps-momentum-us

REM 로그 디렉토리 없으면 생성
if not exist logs mkdir logs

REM Python 경로 자동 감지 (직장PC / 집PC)
set PYTHONIOENCODING=utf-8
if exist "C:\Users\jkw88\miniconda3\envs\volumequant\python.exe" (
    set PYTHON_PATH=C:\Users\jkw88\miniconda3\envs\volumequant\python.exe
) else if exist "C:\Users\user\miniconda3\envs\volumequant\python.exe" (
    set PYTHON_PATH=C:\Users\user\miniconda3\envs\volumequant\python.exe
) else (
    echo ERROR: Python not found
    exit /b 1
)

%PYTHON_PATH% daily_runner.py >> logs\daily_%date:~0,4%%date:~5,2%%date:~8,2%.log 2>&1

echo Done: %date% %time%
