# ETF Pulse 설정 가이드

## 1. 로컬 실행 (Quick Start)

```bash
# 디렉토리 이동
cd etf_pulse

# DB 초기화 (1회)
python db_schema.py

# 30일 history backfill (1회, 약 2분)
python backfill.py

# 일별 실행 (매일)
python run_daily.py
```

생성 결과:
- `etf_pulse.db` (SQLite DB)
- `content/pulse_YYYY-MM-DD.md` (Markdown 콘텐츠)
- `content/charts/*.png` (시각화)

## 2. 매일 자동 실행 — 3가지 옵션

### 옵션 A: 회사 PC cron (Windows Task Scheduler)
가장 간단. KR/US 시스템과 같은 패턴.

```powershell
# PowerShell 관리자 권한
$action = New-ScheduledTaskAction -Execute "python.exe" -Argument "C:\dev\claude-code\eps-momentum-us\etf_pulse\run_daily.py"
$trigger = New-ScheduledTaskTrigger -Daily -At 6:00AM
Register-ScheduledTask -TaskName "ETF Pulse Daily" -Action $action -Trigger $trigger
```

### 옵션 B: GitHub Actions (PC 없어도 자동)
`.github/workflows/etf_pulse_daily.yml` 이미 작성됨.

Settings → Secrets 등록:
- `GMAIL_USER` (선택)
- `GMAIL_APP_PASSWORD` (선택)
- `TO_EMAILS` (선택, 콤마 구분)

자동으로 매일 한국 시간 06:00 실행.

### 옵션 C: cron + 텔레그램 발행
```bash
# crontab -e
0 6 * * * cd /path/to/etf_pulse && python run_daily.py
```

## 3. 이메일 발행 설정 (Gmail)

```bash
# 1) Gmail 앱 비밀번호 생성
#    myaccount.google.com → 보안 → 2단계 인증 → 앱 비밀번호 → "메일" → 생성

# 2) config.json 작성
cat > etf_pulse/config.json <<EOF
{
  "gmail_user": "your.email@gmail.com",
  "gmail_app_password": "abcd efgh ijkl mnop",
  "to_emails": ["self@email.com", "subscriber@email.com"]
}
EOF

# 3) 테스트 발송
python etf_pulse/email_sender.py
```

## 4. 텔레그램 발행 설정

```bash
# 1) BotFather에서 봇 생성 → token 받기
# 2) 봇과 대화 시작 → chat_id 확인 (https://api.telegram.org/bot{TOKEN}/getUpdates)

# 3) config.json에 추가
{
  "telegram_bot_token": "1234567890:ABCDEF...",
  "telegram_chat_id": "1234567890"
}

# 4) 자동 발송
python etf_pulse/publisher.py
```

## 5. Substack 발행 (수동, 1분)

자동 API 없음. 매일 5분:

1. https://substack.com 본인 publication
2. New Post → Editor 열기
3. `etf_pulse/content/pulse_YYYY-MM-DD.md` 내용 복사 → 붙여넣기
4. 제목 자동 (`# 🌅 ETF Pulse — YYYY-MM-DD`)
5. Publish (Free or Paid 선택)

자동화 옵션: Substack 이메일 발행 endpoint 사용 (이메일로 발송 → Substack이 자동 변환).

## 6. 포트폴리오 추적 사용

```python
from etf_pulse.portfolio import add_holding, get_portfolio_pulse, gen_pulse_message

# 보유 등록 (1회)
add_holding('user1', 'VOO', 100, entry_price=550, entry_date='2026-04-01')
add_holding('user1', 'QQQ', 50, entry_price=480, entry_date='2026-04-15')
add_holding('user1', 'SOXX', 30, entry_price=520, entry_date='2026-05-01')

# 일별 펄스 (매일 자동)
pulse = get_portfolio_pulse('user1')
msg = gen_pulse_message(pulse)
print(msg)
```

## 7. 의존성

```
pip install yfinance pandas matplotlib
```

또는:
```
pip install -r etf_pulse/requirements.txt
```

`requirements.txt`:
```
yfinance>=0.2
pandas>=2.0
matplotlib>=3.7
```

## 8. 데이터 신뢰성 검증 (1주 dogfood)

매일 자동 실행 후 1주일 누적 → 다음 확인:

```bash
# DB 검사
sqlite3 etf_pulse/etf_pulse.db <<EOF
.headers on
SELECT date, COUNT(*) as n, AVG(volume_spike) as avg_spike,
       SUM(CASE WHEN estimated_flow IS NOT NULL THEN 1 ELSE 0 END) as flow_calc
FROM etf_daily GROUP BY date ORDER BY date;
EOF
```

확인 항목:
- 매일 228 ETF 일관 수집되나
- estimated_flow 계산되는 비율 (어제 데이터 있어야 가능)
- holdings 변동 빈도 (액티브 ETF가 더 자주 변동)

## 9. 다음 단계 (Roadmap)

### 즉시 가능
- [ ] cron 등록 (옵션 A or B)
- [ ] Substack 계정 생성 + 첫 발행
- [ ] X/Twitter 시드 audience 시작

### 1-2주 후
- [ ] 1주 dogfood 데이터로 신호 정확성 확인
- [ ] fund flow / holdings diff 신뢰성 검증
- [ ] 콘텐츠 quality 개선

### 1-3개월
- [ ] 구독자 100 → 1000
- [ ] paid tier (포트폴리오 추적 + 알림) 출시
- [ ] AI 챗봇 (Claude API)

### 6개월+
- [ ] B2B 영업
- [ ] 한국 ETF 추가
- [ ] 강의/교육 콘텐츠

## 10. 문제 해결

### yfinance 에러
- 일부 ETF 데이터 없음 → 자동 skip (정상)
- rate limit → `time.sleep()` 추가 (현재 코드에는 미반영)

### 텔레그램 401 Unauthorized
- bot token 만료 또는 잘못됨 → BotFather에서 재발급

### Gmail SMTP 535 인증 실패
- 일반 비밀번호가 아니라 **앱 비밀번호** 필요
- 2단계 인증 활성화 후 myaccount.google.com → 보안 → 앱 비밀번호 생성
