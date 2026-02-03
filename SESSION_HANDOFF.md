# SESSION_HANDOFF.md
## EPS Momentum System v6.3 - Quality & Value Scorecard + RSI Momentum

**Last Updated:** 2026-02-03 08:15
**Session:** v6.2 -> v6.3 업그레이드 완료

---

## 1. 작업 요약

### v6.3 핵심 변경사항

#### A. Quality & Value Scorecard (맛/값 분리)

**핵심 철학:**
> "맛있는 사과를 좋은 값에" - Quality(맛) + Value(값) 분리 평가

**Quality Score (맛, 100점):**
```
- EPS 정배열: 30점
- ROE 품질: 25점 (30%+ S급, 20%+ A급, 10%+ B급)
- EPS 성장률: 20점 (20%+:20점, 10%+:15점, 5%+:10점)
- 추세 (MA200 위): 15점
- 거래량 스파이크: 10점
```

**Value Score (값, 100점):**
```
- PEG 평가: 35점 (<1.0:35점, <1.5:28점, <2.0:20점)
- Forward PER: 25점 (<15:25점, <25:20점, <40:12점)
- 52주 고점 대비: 25점 (-25%이상:25점, -15%이상:20점)
- RSI 눌림목: 15점 (30-45:15점, 45-55:10점)
```

**Actionable Score v6.3 공식:**
```
실전점수 = (Quality×0.5 + Value×0.5) × Action Multiplier
```

#### B. RSI Momentum Strategy (신규)

**핵심 철학:**
> RSI 70 이상을 무조건 진입금지로 처리하지 않음.
> 신고가 돌파 + 거래량 동반 = Super Momentum (최고 등급)

**Super Momentum 조건 (RSI 70-84):**
```python
if 70 <= RSI < 85:
    if 신고가근처(-5%이내) and 거래량스파이크:
        return "🚀강력매수 (돌파)"  # Action Multiplier: ×1.1
    elif 신고가근처:
        return "관망 (RSI🚀고점)"  # ×0.75
    else:
        return "관망 (RSI🚀)"      # ×0.75
```

**Extreme Overbought (진짜 위험):**
```python
if RSI >= 85:
    return "진입금지 (극과열)"  # ×0.3
```

**Action Multiplier (v6.3):**
| Action | Multiplier | 설명 |
|--------|------------|------|
| 🚀강력매수 | ×1.1 | 신고가 돌파 + 거래량 스파이크 |
| 적극매수/저점매수 | ×1.0 | 눌림목/과매도 |
| 매수적기 | ×0.9 | 건강한 추세 |
| 관망 (RSI🚀) | ×0.75 | RSI 과열이지만 추세 강함 |
| 관망 | ×0.7 | 진입 애매 |
| 진입금지 | ×0.3 | 과열/급등 |
| 추세이탈 | ×0.1 | MA200 하회 |

#### C. 추가 기능

**거래량 스파이크 감지:**
```python
# 최근 3일 중 20일 평균 × 1.5 초과
if any(vol_recent_3 > vol_avg_20 * 1.5):
    volume_spike = True
```

**실적 D-Day 표시:**
```
실적D-5  # 실적 발표 5일 전
실적D+1  # 실적 발표 1일 후
```

**Fake Bottom 경고:**
```python
# RSI 낮지만 MA200 아래 = 하락 추세 중 반등
if RSI < 40 and Price < MA200:
    fake_bottom = True
    # 경고 표시
```

**섹터 집중 경고:**
```python
# 특정 섹터가 50% 이상
if sector_pct >= 50:
    warning("섹터집중: {sector} {pct}%")
```

---

## 2. 텔레그램 메시지 포맷 (v6.3)

```
🍎 [02/03] EPS 모멘텀 v6.3 브리핑
━━━━━━━━━━━━━━━━━━━━━━
🚦 시장: 🟢 GREEN (상승장)
📍 SPY $595 | VIX 15.2
📅 2026-02-03 08:00 | 총 48개 통과
━━━━━━━━━━━━━━━━━━━━━━

📋 v6.3 Quality + Value Scorecard
"맛있는 사과를 좋은 값에" 🍎💰

🍎 맛(Quality) 100점
EPS정배열30 + ROE25 + 성장률20 + 추세15 + 수급10

💰 값(Value) 100점
PEG35 + PER25 + 고점대비25 + RSI눌림15

📊 실전점수 공식
= (맛×0.5 + 값×0.5) × Action배수
• 🚀강력/적극/저점: ×1.0~1.1 | 관망: ×0.7 | 금지: ×0.3

━━━━━━━━━━━━━━━━━━━━━━

🏆 TOP 3 SCORECARD

──────────────────────
🥇 PLTR $65 📈
┌ 🍎 맛: 90점 (S급)
├ 💰 값: 72점 (저평가)
├ 📊 실전: 89.1점
└ 기술 | 🚀RSI72 | 고점-2% | 실적D-8
   → 🚀강력매수
   💡 신고가 돌파 + 거래량 폭발! 지금이 제일 쌉니다 (⚠️손절 -5% 필수)

──────────────────────
🥈 NVDA $140 📈
┌ 🍎 맛: 98점 (S급)
├ 💰 값: 65점 (적정가)
├ 📊 실전: 67.5점
└ 반도체 | 🚀RSI75 | 고점-3%
   → 관망 (RSI🚀고점)
   💡 RSI 75 과열이지만 상승세 강함, 거래량 확인 필요
```

---

## 3. 수정된 파일

### daily_runner.py (v6.3)

**헤더 버전:**
```python
"""
EPS Momentum Daily Runner v6.3 - Quality & Value Scorecard System
"""
```

**신규 함수/변경:**
1. `get_action_label()`: RSI Momentum Strategy 추가
2. `run_screening()`: volume_spike, earnings_dday, fake_bottom 계산 추가
3. `create_telegram_message()`: v6.3 스코어카드 포맷
4. `generate_korean_rationale()`: RSI 모멘텀 해설 추가

### eps_momentum_system.py (v6.3)

**변경된 함수:**
1. `get_action_multiplier()`: 🚀강력매수 ×1.1, RSI🚀 ×0.75 추가

**기존 v6.3 함수 (이전 세션에서 추가):**
1. `calculate_quality_score()`: 맛 점수 계산
2. `calculate_value_score()`: 값 점수 계산

---

## 4. 다음 작업

### 필수
- [x] v6.3 코드 구현 완료
- [x] RSI Momentum Strategy 구현
- [ ] 테스트 실행 및 텔레그램 전송
- [ ] README.md 업데이트
- [ ] Git commit/push

### 선택적 개선
- [ ] 백테스트: 눌림목 vs 돌파 전략 수익률 비교
- [ ] Quality/Value 가중치 튜닝
- [ ] 실적 발표 D-Day 자동 필터

---

## 5. 설정 파일 정보

**config.json 위치:** `C:\dev\claude-code\eps-momentum-us\config.json`

주요 설정:
- `telegram_enabled`: true
- `git_enabled`: true
- `min_score`: 4.0
- `indices`: ["NASDAQ_100", "SP500", "SP400_MidCap"]

---

## 6. 디렉토리 구조

```
eps-momentum-us/
├── daily_runner.py          # 메인 실행 파일 (v6.3)
├── eps_momentum_system.py   # 핵심 로직 (v6.3)
├── config.json              # 설정
├── eps_momentum_data.db     # SQLite DB
├── eps_data/
│   └── screening_YYYY-MM-DD.csv
├── reports/
│   ├── report_YYYY-MM-DD.md
│   └── report_YYYY-MM-DD.html
└── logs/
    └── daily_YYYYMMDD.log
```

---

## 7. v6.3 버전 히스토리

### v6.3 (2026-02-03)
- Quality Score (맛) + Value Score (값) 분리 스코어카드
- RSI Momentum Strategy: 신고가 돌파 + 거래량 = 🚀강력매수
- RSI 85 이상만 진짜 과열 (기존: 70 이상)
- 거래량 스파이크 감지 (20일 평균 ×1.5)
- 실적 D-Day 표시
- Fake Bottom 경고 (RSI 낮지만 MA200 하회)
- 섹터 집중 경고 (50% 이상)

### v6.2 (2026-02-03)
- Action Multiplier: Hybrid Score × Action 배수
- RSI 과열 종목 자동 순위 하락

### v6.1 (2026-02-03)
- 가격위치 점수: 52주 고점 대비 위치
- Hybrid Ranking: M×0.5 + V×0.2 + P×0.3

### v6.0 (2026-02-02)
- Forward PER, ROE 지표 추가
- 3-Layer Filtering: Momentum -> Quality -> Safety

---

*작성: Claude Opus 4.5 | 2026-02-03*
