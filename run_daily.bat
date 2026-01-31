@echo off
chcp 65001 >nul 2>&1
REM EPS Momentum Daily Runner - Windows Task Scheduler용
REM 매일 07:00 (미국 장 마감 후, 한국 시간) 실행 권장

REM 작업 디렉토리 이동
cd /d C:\dev\claude-code\eps-momentum-us

REM 로그 디렉토리 없으면 생성
if not exist logs mkdir logs

REM Python 스크립트 실행 (UTF-8 환경변수 설정)
set PYTHONIOENCODING=utf-8
C:\Users\jkw88\miniconda3\envs\volumequant\python.exe daily_runner.py >> logs\daily_%date:~0,4%%date:~5,2%%date:~8,2%.log 2>&1

echo Done: %date% %time%
