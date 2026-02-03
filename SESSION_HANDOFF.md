# SESSION_HANDOFF.md
## EPS Momentum System v7.0 - AI Insights + Dual Track Strategy

**Last Updated:** 2026-02-03 21:00
**Session:** v6.3.2 -> v7.0 대규모 업그레이드 완료

---

## 1. v7.0 핵심 변경사항

### A. TOP 10 액션 로직 변경 (핵심!)

**기존 문제점:**
- TOP 10에 선정된 종목에도 "관망" 태그가 붙음
- 이미 900개 중 선별된 종목인데 "지켜보라"는 건 모순

**v7.0 해결:**
```python
# TOP 10은 무조건 매수 관점 (관망 제거)
if RSI >= 60 or 신고가근처(-5%):
    return "🚀돌파매수"  # Momentum
else:
    return "🛡️분할매수"  # Dip/Accumulate
```

**결과:** TOP 10에서 "관망" 완전 제거

### B. AI Insights 추가

**기존:** 규칙 기반 해설 (PER, RSI 조건문)
**v7.0:** 웹 검색 기반 실시간 뉴스/인사이트

```
기존: 💡 EPS 전망치 완전 정배열, PER 13배 저평가
v7.0: 📰 금값 $5,000 돌파로 급등, UBS 목표가 $160 상향
```

### C. Sector Booster (ETF 추천)

TOP 10 중 동일 섹터 3개 이상 → 섹터 ETF 자동 추천

```
🔥 [HOT] 섹터 포착
👉 반도체 4개 → SMH/SOXL
```

### D. Exit Strategy (ATR 손절가)

```python
def calculate_stop_loss(price, atr, multiplier=2.0):
    return price - (atr * multiplier)
```

### E. Forward Fill (EPS 결측치 보정)

```python
def forward_fill_eps(current, d7, d30, d60):
    """EPS 7d/30d/60d가 NaN이면 Current로 채움"""
```

### F. Super Momentum Override

Quality >= 80 + RSI 70-85 → 자동 "슈퍼모멘텀" 부여

### G. Config 분리

하드코딩된 값들을 config.json으로 외부화:
- action_multipliers
- exit_strategy
- super_momentum
- sector_booster
- telegram_format

---

## 2. v7.0 실행 결과 (2026-02-03)

### TOP 10 (AI Insights 포함)

| # | 종목 | 점수 | 액션 | 핵심 인사이트 |
|---|------|------|------|---------------|
| 1 | NEM | 80.9 | 🛡️분할매수 | 금값 $5,000 돌파, UBS 목표가 $160 |
| 2 | AVGO | 70.8 | 🛡️분할매수 | OpenAI 4년간 AI칩 공급 계약 |
| 3 | EXEL | 62.9 | 🛡️분할매수 | 항암제 파이프라인 확대 |
| 4 | G | 59.5 | 🛡️분할매수 | RSI 31 과매도, AI BPO 확대 |
| 5 | KLAC | 56.4 | 🛡️분할매수 | Q2 사상최대, Citi 목표가 $1,800 |
| 6 | MU | 54.4 | 🚀돌파매수 | HBM 품절, FY26 EPS 4배 성장 |
| 7 | CRS | 53.6 | 🛡️분할매수 | 항공우주/방산 특수합금 수요 증가 |
| 8 | WMG | 53.2 | 🛡️분할매수 | 스트리밍 수익 증가세 |
| 9 | LRCX | 53.2 | 🛡️분할매수 | Q2 매출 +22% YoY, CEA-Leti R&D 협력 |
| 10 | CVNA | 52.1 | 🛡️분할매수 | 공매도 리포트 후 JPM "매수 기회" |

---

## 3. 수정된 파일

### config.json
- action_multipliers 추가
- exit_strategy, super_momentum, sector_booster 추가
- telegram_format (top_n: 10, watchlist_max: 25)

### daily_runner.py
- load_config(): UTF-8 인코딩 추가
- create_telegram_message(): v7.0 포맷
- TOP 10 액션 오버라이드 (돌파매수/분할매수)
- Sell Signal 섹션 추가
- killed_tickers, trend_exit_tickers 추적

### eps_momentum_system.py
- calculate_atr(): ATR(14) 계산
- calculate_stop_loss(): 동적 손절가
- forward_fill_eps(): 결측치 보정
- super_momentum_override(): Quality+RSI 기반

### sector_analysis.py
- SECTOR_ETF에 Semiconductor 추가 (SMH/SOXL)
- get_sector_etf_recommendation(): 섹터 집중 감지

### DOCUMENTATION.md
- v7.0 섹션 추가 (6장)
- 새 텔레그램 템플릿 문서화

---

## 4. 텔레그램 메시지 v7.0 포맷

```
🇺🇸 미국주식 퀀트 랭킹 v7.0
━━━━━━━━━━━━━━━━━━━━━━
📅 {Date} 마감 | 총 {Count}개 통과
📋 전략: EPS Growth + RSI Dual Track

🔥 [HOT] 섹터 포착
👉 반도체 4개 → SMH/SOXL
━━━━━━━━━━━━━━━━━━━━━━

🏆 TOP 10 추천주

🥇 NEM $113 | Newmont Corp
   [🛡️분할매수] 80.9점
   • 🍎맛: 71.7 | 💰값: 90 | RSI50
   • 📰 금값 $5,000 돌파, UBS 목표가 $160 상향
   • 2/19 실적발표 예정

━━━━━━━━━━━━━━━━━━━━━━
📋 관심 종목 (11~25위)
...

━━━━━━━━━━━━━━━━━━━━━━
🤖 EPS Momentum v7.0 + AI Insights
```

---

## 5. GitHub Actions 설정

`.github/workflows/daily_screening.yml` 추가

**필요 설정:**
1. GitHub Secrets에 추가:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`

2. 스케줄: 매일 07:00 KST (UTC 22:00)

---

## 6. 버전 히스토리

### v7.0 (2026-02-03 21:00)
- TOP 10 액션 로직 변경 (관망 제거)
- AI Insights (웹 검색 기반)
- Sector Booster (ETF 추천)
- Exit Strategy (ATR 손절가)
- Forward Fill (EPS 결측치)
- Super Momentum Override
- Config 분리
- GitHub Actions 워크플로우

### v6.3.2 (2026-02-03)
- Quality Score: score_321 직접 활용

### v6.3 (2026-02-03)
- Quality & Value Scorecard 분리

---

## 7. v7.0.3 추가 변경 (2026-02-03 22:49)

### 전체 종목 TOP 10 동일 포맷 + 합산점수

**변경사항:**
1. 11위~54위 모든 종목을 TOP 10과 완전 동일한 포맷으로 표시
2. 맛+값 합산점수 추가 (100점 만점)

**새 포맷:**
```
#11 WMG $30
   Warner Music Group Corp.
   [🛡️분할매수] 종합: 53.2점
   • 📊매수근거: EPS- + RSI40
   • 🍎맛: 51.5점(B급) | 💰값: 55점(적정가)
   • 📊합산: 53.2점/100 (맛+값 평균)
   • 📉대응: 손절가 $28.9 (ATR×2)
   • 통신서비스 | 고점-16%
   💡 EPS 전망 상향 추세, PER 17배 적정
```

### v7.0.2 변경 (2026-02-03 22:46)
- 액션별 분포 섹션 제거 (혼란 방지)

### v7.0.1 변경 (2026-02-03 21:04)
- 전체 종목 상세 표시 (watchlist_max 제한 제거)

### NVDA 탈락 이유 분석

| 지표 | 값 | 결과 |
|------|-----|------|
| EPS Current | 7.66 | - |
| EPS 7일전 | 7.66 | 변화 없음 |
| 모멘텀 점수 | 3점 | < 4.0 (탈락) |
| RSI | 50.8 | 중립 |
| 52주고점 대비 | -12.5% | - |

**결론:** EPS 상향 모멘텀 없음 → 정상 필터링

---

## 8. 다음 작업

- [ ] GitHub Secrets 설정 (Telegram 토큰)
- [ ] GitHub Actions 테스트 실행
- [ ] 백테스트: v6.3 vs v7.0 수익률 비교
- [ ] AI Insights 자동화 (yfinance news 활용)

---

*작성: Claude Opus 4.5 | 2026-02-03*
