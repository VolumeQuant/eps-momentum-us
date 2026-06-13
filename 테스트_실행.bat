@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ============================================
echo   집 PC 테스트 실행
echo   (고객 채널에는 절대 안 나갑니다)
echo ============================================
echo.
echo [1/3] 데이터 수집 + 신호 계산 중...
echo       5~10분 걸려요. 창 닫지 말고 기다려주세요.
echo       (중간에 "텔레그램 401" 떠도 정상이에요 - 무시하세요)
echo.
python daily_runner.py
echo.
echo [2/3] 원상복구 중... (다음에 git pull 충돌 안 나게)
git checkout -- . 2>nul
echo.
echo [3/3] 완료! 오늘의 신호를 엽니다.
echo.
if exist signal_local.txt (
    start notepad signal_local.txt
) else (
    echo [!] 신호 파일이 안 만들어졌어요. 수집이 실패했을 수 있어요.
    echo     위에 빨간 글씨가 많으면 잠시 후 다시 시도해보세요.
)
echo.
echo 끝났습니다. 아무 키나 누르면 이 창이 닫혀요.
pause >nul
