# EPS Momentum System v74 (US Stocks)

Forward 12개월 EPS(NTM EPS) 기반 모멘텀 시스템. **"파괴적 혁신 기업을 싸게 살래"** 철학으로, w_gap(가중 괴리율)을 기반으로 저평가 종목을 선별한다. 괴리율이 음수일수록 EPS 개선 대비 주가가 덜 반영된 상태(= 저평가). MA120 + 리스크 필터로 신뢰도를 높이고, AI(Gemini 2.5 Flash)가 위험 신호를 점검한 뒤 최대 3종목을 매수 후보로 제시한다. 최대 3종목 보유.

**v74 전략**: 진입 Top 3 / 이탈 11위 밖 / 슬롯 3개 / Breakout Hold (strict) — 강한 상승 추세 시 매도 신호 2일 유예. 백테스트 평균 +31.59% (33개 시작일), MDD 최악 -18.16%.

---

## 목차
1. [핵심 전략](#핵심-전략)
2. [NTM EPS 계산](#ntm-eps-계산)
3. [EPS 점수 체계](#eps-점수-체계)
4. [괴리율 (adj_gap → w_gap)](#괴리율)
5. [매수 후보 선정](#매수-후보-선정)
6. [진입·이탈 규칙 (v58b)](#진입이탈-규칙-v58b)
7. [리스크 필터](#리스크-필터)
8. [신용·변동성 모니터링 (HY+VIX)](#신용변동성-모니터링-hyvix)
9. [AI 리스크 분석 (Gemini)](#ai-리스크-분석-gemini)
10. [텔레그램 메시지](#텔레그램-메시지)
11. [데이터 흐름](#데이터-흐름)
12. [DB 스키마](#db-스키마)
13. [실행 방법](#실행-방법)
14. [프로젝트 구조](#프로젝트-구조)
15. [환경변수](#환경변수)
16. [버전 히스토리](#버전-히스토리)

---

## 핵심 전략

**한 줄 요약**: EPS 전망이 꾸준히 올라가는데 주가가 아직 안 따라간 종목(= 저평가)을 찾아 매수.

### 투자 철학
- **adj_gap**(방향 보정 괴리율)이 유일한 순위 신호. 음수가 클수록 저평가
- 혁신 없는 종목은 하드필터(매출 성장 ≥10%)로 사전 제거
- 추세가 살아있는 종목만(MA120 이상) 대상
- 매수 후보 Top 3, 최대 5종목 보유 (균등비중)
- 목록에 있으면 보유, 없으면 매도 검토

---

## NTM EPS 계산

**NTM = Next Twelve Months** — 향후 12개월 EPS를 하나의 숫자로 통일.

yfinance의 0y(현재 회계연도)/+1y(다음 회계연도) EPS를 **endDate 기반 시간 가중치**로 블렌딩한다.

```
예시: endDate=2026-12-31, 오늘=2026-03-22
  → 현재 연도 잔여일: 284일 → 0y 가중치 = 284/365 ≈ 0.78
  → +1y 가중치 = 1 - 0.78 = 0.22
  → NTM EPS = 0y × 0.78 + (+1y) × 0.22
```

**왜 NTM인가?**: 기존 +1y 컬럼은 종목마다 가리키는 연도가 달라 비교가 불가능했다. NTM으로 통일하면 모든 종목을 같은 시간축(오늘부터 12개월)에서 비교할 수 있다.

구현: `eps_momentum_system.py`의 `calc_ntm_eps()` — yfinance `stock._analysis._earnings_trend`에서 endDate를 추출.

---

## EPS 점수 체계

### Score (기본 점수)
90일을 4개 독립 구간으로 나눠 각 구간의 NTM EPS 변화율을 합산:

```
|----seg4----|----seg3----|----seg2----|--seg1--|
90d         60d         30d          7d      today

Score = seg1 + seg2 + seg3 + seg4   (세그먼트 캡: ±100%)
```

### adj_score (방향 보정 점수)
최근 추세(seg1+seg2)와 과거 추세(seg3+seg4)의 차이로 가속/감속 판단:

```
recent = (seg1 + seg2) / 2
old = (seg3 + seg4) / 2
direction = recent - old
adj_score = score × (1 + clamp(direction/30, -0.3, +0.3))
```

- 가속 (direction > 0) → adj_score 증가 (최대 +30%)
- 감속 (direction < 0) → adj_score 감소 (최대 -30%)
- Part 2 진입 필터에서 `adj_score > 9` 기준으로 사용

### eps_quality (v55b)
min_seg(4개 세그먼트 중 최솟값) 기반 **연속 함수** — cliff effect 제거:

```
min_seg = min(seg1, seg2, seg3, seg4)
eps_quality = 1.0 + 0.3 × clamp(min_seg / 2, -1, 1)
```

| min_seg | eps_quality | 의미 |
|---------|-------------|------|
| ≤ -2% | 0.7 | EPS 추세 불안정 → 괴리율 30% 할인 |
| 0% | 1.0 | 보통 |
| ≥ +2% | 1.3 | 전 구간 상승 → 괴리율 30% 할증 |

### 추세 아이콘 (5단계 × 12패턴)
4개 세그먼트의 방향과 크기를 날씨 아이콘으로 시각화 (과거→현재 순):

| 아이콘 | 기준 | 의미 |
|--------|------|------|
| 🔥 | > 20% | 폭등 |
| ☀️ | 5~20% | 강세 |
| 🌤️ | 1~5% | 상승 |
| ☁️ | ±1% | 보합 |
| 🌧️ | < -1% | 하락 |

12개 기본 패턴(횡보/전구간 상승/꾸준한 상승/상향 가속/최근 급상향/중반 강세/상향 둔화/반등/추세 전환 등)에 🔥 강도 수식어 조합.

---

## 괴리율

### fwd_pe_chg (기본 괴리율)
가중평균 Fwd P/E 변화율. 각 시점의 Fwd PE(= 주가/NTM EPS)를 비교:

```
fwd_pe_chg = 7d변화×40% + 30d변화×30% + 60d변화×20% + 90d변화×10%
```

음수 = EPS 개선 대비 주가 미반영 (저평가).

### adj_gap (방향 보정 괴리율)
```
adj_gap = fwd_pe_chg × (1 + clamp(direction/30, -0.3, +0.3)) × eps_quality
```

- 가속 EPS + 전 구간 상승 → 저평가 최대 강화 (×1.3 × 1.3 ≈ 1.69)
- 감속 EPS + 일부 하락 → 저평가 약화
- **유일한 순위 신호** — 다른 팩터(매출 등)는 하드필터로만 사용

### w_gap (가중 괴리율) — 3일 가중 adj_gap
일간 adj_gap의 노이즈를 완화하기 위해 3일 가중합산:

```
w_gap = adj_gap(T0) × 0.5 + adj_gap(T1) × 0.3 + adj_gap(T2) × 0.2
```

- T0=오늘, T1=어제, T2=2일전
- w_gap 오름차순 → part2_rank 1~30 부여 (가장 저평가 = 1위)
- 기존 일별 adj_gap 순위 가중 방식 대비 **절대 크기 차이 보존**

### 매력도 (Score 100)
```
매력도 = clamp((-adj_gap + 10) × 5, 0, 100)
```
adj_gap = -10 → 100점, adj_gap = +10 → 0점. Signal/Watchlist 메시지에 표시.

---

## 매수 후보 선정

### 유니버스
NASDAQ 100 + S&P 500 + S&P 400 MidCap = **916개 고정 종목** + `fetch_dynamic_tickers()`로 시가총액 $50억+ 종목 추가 (~1,260개 총 유니버스).

### Part 2 필터 (10개)
`get_part2_candidates()` — 모든 종목에 순서대로 적용:

| # | 필터 | 조건 | 근거 |
|---|------|------|------|
| 1 | EPS 모멘텀 | adj_score > 9 | 방향 보정 점수 최소 기준 |
| 2 | EPS 개선 | eps_change_90d > 0 | 90일간 NTM EPS가 실제로 상승 |
| 3 | Fwd PE 유효 | fwd_pe > 0 | 데이터 유효성 |
| 4 | 최소 주가 | price ≥ $10 | 페니스톡 제외 |
| 5 | MA120 추세 | price > MA120 (fallback MA60) | 하락 추세 종목 제외 |
| 6 | 매출 성장 | rev_growth ≥ 10% | 혁신 부족 기업 제외 |
| 7 | 커버리지 | num_analysts ≥ 3 | 소수 의견 데이터 불안정 |
| 8 | 하향 제한 | 하향 비율 ≤ 30% | 다수 애널리스트 동시 하향 |
| 9 | 저마진 제외 | OM<10% & GM<30%, 또는 OP<5% | 구조적 저수익 기업 |
| 10 | 원자재 제외 | 22개 원자재 업종 + 개별 티커(SQM, ALB) | commodity 가격 패스스루 |

매출 수집: eligible 상위 50종목에 대해서만 `fetch_revenue_growth()` 실행 (~12초).

### 순위 부여 (`save_part2_ranks()`)
1. 필터 통과 종목 중 **min_seg < -2% 제외** (EPS 추세 불안정)
2. 당일 adj_gap 오름차순 → `composite_rank` 부여 (전 eligible 종목)
3. w_gap(3일 가중 adj_gap) 오름차순 → 상위 30개에 `part2_rank` 1~30 부여

---

## 진입·이탈 규칙 (v74)

### 진입 조건
`select_display_top5()` — 매일 최대 3종목 매수 후보 선정, 누적 최대 3종목 보유:

1. w_gap 순위 **Top 3** 이내에서 탐색
2. **min_seg ≥ 0%** (전 구간 EPS 상승 확인)
3. **리스크 필터** 통과 (하향과반·저커버리지 차단)
4. 조건 충족 순서대로 **최대 3종목** 매수 후보 제시 (min_seg 미달 시 스킵 후 차순위)
5. **최대 3슬롯** 보유

### 이탈 조건
| 조건 | 기준 | 이탈 사유 표시 |
|------|------|----------------|
| 순위 밀림 | part2_rank > 11 (w_gap Top 11 밖) | [순위밀림] |
| 주가 선반영 | adj_gap > 5 | [주가선반영] |
| EPS 추세 악화 | min_seg < -2% | [추세둔화] |
| 추세 이탈 | price < MA120 | [MA120↓] |
| 매출 둔화 | rev_growth < 10% | [매출↓] |
| 저커버리지 | num_analysts < 3 | [저커버리지] |

### Breakout Hold (이탈 유예, v74 신규)
순위밀림/주가선반영 이탈 신호 발생 시 다음 4조건 모두 만족하면 **2일 매도 유예**:
1. 최근 20거래일 종가 +25% 이상
2. ntm_90d → ntm_current 순방향 (EPS 동행)
3. rev_up30 / num_analysts ≥ 0.4 (애널리스트 합의 상향)
4. 현재가 > MA60

이탈 사유에 `⏸️유예` 마커로 표시. 사용자는 메시지 보고 수동 매매.

> v74 백테스트: 41일 33개 시작일 multistart 평균 +31.59% (현재 v72 +22.58% 대비 +9.01%p), MDD 최악 -18.16% (동일), 33/33 양의 수익률.

이탈 종목은 Signal/Watchlist에서 사유별로 그룹핑 표시.

### 3일 상태 표시
기준: DB의 `part2_rank`(Top 30 소속 여부)가 존재하는 최근 3거래일 수

| 상태 | 조건 | 의미 |
|------|------|------|
| ✅ | 3일 연속 Top 30 | 검증됨 |
| ⏳ | 2일 Top 30 | 검증 중 |
| 🆕 | 1일만 Top 30 | 신규 진입 |

진입 조건에서 3일 검증(✅)은 **불요** — w_gap이 이미 3일 가중이므로 별도 검증 불필요.

### 포지션 사이징: 균등비중 (v71)
Top 3 검증 종목에 동일 비중 배분 (`100% / N`).

v71 전환 배경: 역변동성(5일 window)이 단기 변동에 과민반응하여 MU 71% 등 비합리적 집중 발생.
28일 검증 결과 균등비중(+19.7%, Sharpe 3.58) > 역변동성(+10.9%, Sharpe 2.72).

### L3 시장 동결
`concordance = both_warn`(HY+VIX 동시 경고) 시:
- 비검증(🆕⏳) 종목은 포트폴리오에서 제외
- ✅ 종목만 유지

---

## 리스크 필터

시스템 철학: adj_gap = 저평가 기회. 필터는 **데이터 자체의 신뢰성**만 검증하고, 주가/밸류에이션은 건드리지 않는다.

### 차단 필터 (2개)
| 플래그 | 조건 | 근거 |
|--------|------|------|
| 하향과반 | rev_down/(up+down) > 30%, 또는 down≥up 且 down≥2 | EPS 전망 신뢰 하락 |
| 저커버리지 | num_analysts < 3 | 소수 의견 → NTM EPS 불안정 |

### 경고 표시 (2개)
| 플래그 | 조건 | 처리 |
|--------|------|------|
| 고평가 | fwd_pe > 100 | (미구현) |
| 어닝 2주 이내 | yfinance `stock.calendar` 기반 | AI Risk 메시지에 ⚠️ 표시만 (포트폴리오 제외 안 함) |

`rev_up`, `rev_down`, `num_analysts`는 **max(0y, +1y)** — NTM 블렌딩에 맞춰 양쪽 기간 반영.

### 턴어라운드 (내부 처리)
`abs(NTM_current) < $1.00` 또는 `abs(NTM_90d) < $1.00`인 종목은 내부적으로 분리. 저 베이스 EPS로 인한 Score 왜곡 방지. 별도 메시지로 발송하지 않음.

### ⚠️ 주가 괴리 경고
주가 하락이 EPS 개선 대비 과도한 종목:
- 조건: EPS 가중평균 > 0, 주가 가중평균 < 0, |주가변화| / |EPS변화| > 5
- 종목명 옆에 ⚠️ 아이콘 표시

---

## 신용·변동성 모니터링 (HY+VIX)

### Layer 1: HY Spread (FRED BAMLH0A0HYM2)
US High Yield Spread 기반 Verdad 4분면 모델. `fetch_hy_quadrant()` — FRED API CSV 수집.

**HY 퍼센타일**: 2520일(10년) rolling rank.

### Layer 2: VIX (FRED VIXCLS)
`fetch_vix_data()` — 252일 rolling percentile 기반 (최소 126일):

| 퍼센타일 | 상태 | 의미 |
|----------|------|------|
| < 10th | 안일 (complacency) | 시장 과신 경계 |
| 10~67th | 정상 | 평소 수준 |
| 67~80th | 경계 | 변동성 증가 |
| 80~90th | 상승경보 | 위험 구간 |
| ≥ 90th | 위기 | 극단적 공포 |

### Concordance (교차 검증)
`get_market_risk_status()` — HY(메인) + VIX(보조) 교차:

| HY 방향 | VIX 방향 | concordance | VIX 가감 처리 |
|---------|---------|-------------|---------------|
| 경고 (Q3/Q4) | 경고 | both_warn | 전액 적용 |
| 안정 (Q1/Q2) | 안정 | both_stable | 그대로 |
| 경고 | 안정 | hy_only | 0% (HY가 이미 반영) |
| 안정 | 경고 | vix_only | 50%만 (일시적 쇼크) |

concordance/final_action은 **내부 로직(L3 동결 등)에만** 사용, 고객 메시지에는 미표시.

### 종합 판정 (v65 HY×VIX 조합, v71 교정)
HY 4분면 × VIX 4구간 = 16칸 매트릭스. 2000~2026 SPY 6,593거래일 20일 선행수익률 연환산 기반.

**RETURN_MATRIX** (v71 교정 — `bt_hy_vix_corrected.py` 검증):
```
         normal(<67p)  elevated(67-80)  high(80-90)  crisis(90+)
Q1 회복    +20.4%(673)   +23.7%(81)     +58.2%(24)   +39.6%(16)
Q2 성장     +9.8%(2029)  +14.8%(180)    +13.7%(75)   +15.2%(106)
Q3 과열     +7.6%(599)    +5.3%(186)     +1.3%(155)   +15.3%(224)
Q4 침체     +7.8%(333)   -12.1%(184)    +18.6%(155)   +18.9%(294)
```

신호등 판정:
| 과거 수익률 | 아이콘 | 판정 |
|-------------|--------|------|
| ≥ 8% | 🟢 | 과거 수익률이 좋았던 구간 |
| < 8% | 🟡 | 과거 수익률이 보통인 구간 |
| < 5% AND (VIX≥90p OR HY≥90p) | 🔴 | 과거 수익률이 낮았던 구간 |

VIX ≥ 95p이면 최소 🟡 (극단 공포 시 🟢 방지).

> **v71 교정 배경**: 기존 매트릭스의 Q3+crisis(+2.7→+15.3%)와 Q4+elevated(+12.1→-12.1%) 값이 부정확하여 거짓 🔴 발생. 6,593거래일 검증으로 교정.

---

## AI 리스크 분석 (Gemini)

**"검색은 코드가, 분석은 AI가"** — yfinance로 팩트 수집, Gemini 2.5 Flash는 데이터 해석에 집중.

SDK: `google-genai>=1.0.0` (NOT google-generativeai)

### 출력 섹션
| 섹션 | 내용 | 데이터 소스 |
|------|------|-------------|
| 📰 시장 동향 | 어제 미국 시장 마감 + 금주 이벤트 | Google Search 1회 |
| ⚠️ 매수 주의 | 위험 신호 기반 주의 종목 (없으면 "✅ 양호") | yfinance 데이터 |
| 📅 어닝 주의 | 2주 이내 실적발표 | yfinance `stock.calendar` 직접 조회 |

### AI 내러티브 (종목별)
Signal 메시지의 각 종목에 **2~3문장(120~150자)** AI 해설 추가. 인사말/서두/맺음말 금지.

### 검증
- 📰 또는 "시장" 키워드 없으면 자동 재시도
- `[SEP]` 마커 → `\n\n` 변환
- temperature 0.2, 데이터에 있는 정보만 사용

---

## 텔레그램 메시지

3개 메시지 + 시스템 로그 (개인봇만). 채널은 Cold Start(3일 미만) 후 자동 활성화.

| # | 메시지 | 내용 |
|---|--------|------|
| 1 | **Signal** | 성과 헤더 + 매수 후보 최대 3종목 + 알파 시그널 + 선정과정 + 종목별 근거 + 이탈 1줄 |
| 2 | **AI Risk** | 시장환경(지수) + 신용·변동성 + AI 시장동향 + 포트폴리오 경고 |
| 3 | **Watchlist** | Top 20 현황(w_gap 순위) + ⚠️추세둔화 섹션 + 이탈 섹션 + 운영 규칙 범례 |
| - | 시스템 로그 | DB 적재 결과, 분포 통계 (개인봇만) |

### Signal 성과 헤더
```
📈 시스템 누적 수익률 +11.1% (25거래일)
    같은 기간 S&P500은 -5.7%
```
- `_get_system_performance()`: DB 기반 복리 재투자 백테스트 리플레이 (균등비중)

### Signal 알파 시그널 (정보 표시용, 순위에 영향 없음)
종목별 근거 아래에 해당 시그널이 있을 때만 표시:
- **어닝 서프**: 최근 1Q surprisePercent > 0 → `어닝 서프 +X%`
- **어닝 쇼크**: 최근 1Q surprisePercent < 0 → `⚠️ 어닝 미스 X%`
- **공매도**: shortPercentOfFloat ≥ 8% → `공매도 X.X%` + MoM 변화(숏커버/급증)
- **경영진 매도**: C-suite Sale > $1M (Stock Award 14일 내 제외) → `⚠️ CEO 매도 $XM`

### Signal 종목 포맷 (4줄 + 알파 시그널)
```
✅ 1. 종목명(티커) 업종 · $123.45
EPS 전망 +N% · 매출성장 +N% · 매력도 86.0점
순위 3→4→1위 · 의견 ↑N↓N · 저평가 19일째
어닝 서프 +12% · 공매도 9.2%(숏커버 중)
AI가 생성한 2~3문장 내러티브
```

- **매력도**: `clamp((-adj_gap + 10) × 5, 0, 100)` — 매출성장 옆 배치
- **저평가 streak**: adj_gap < -7 연속일수 — 의견 옆 배치
- **의견**: 30일간 EPS 상향/하향 수정 애널리스트 수 (`↑N ↓N`)

### Watchlist 종목 포맷 (4줄)
```
✅ 1. 종목명(티커) 86.0점 업종
EPS추이 ☀️🔥🔥🌤️ 중반 급등
EPS 전망 +N% · 매출성장 +N%
의견 ↑N↓N · 순위 3→4→1위
```

### 용어 규칙 (v55)
- "EPS 전망 +X%", "매출성장 +X%"
- **괴리** (괴리율 → 괴리)

---

## 데이터 흐름

```
                           daily_runner.py main()
                                  │
    ┌─────────────────────────────┼─────────────────────────────┐
    │                             │                             │
    ▼                             ▼                             ▼
 1. 데이터 수집            2. 필터·순위 부여           3. 메시지 생성·발송
    │                             │                             │
    ├─ yfinance 전종목 수집       ├─ get_part2_candidates()     ├─ create_v3_signal()
    │  (NTM EPS, 가격,            │  (10개 하드필터)             │  (Top 3 추천)
    │   MA120, 업종 등)           │                             │
    │                             ├─ save_part2_ranks()         ├─ create_v3_ai_risk()
    ├─ fetch_revenue_growth()     │  (min_seg 필터 →            │  (시장+HY+VIX+AI)
    │  (상위 50종목 매출)          │   composite_rank →          │
    │                             │   w_gap Top 30)             ├─ create_v3_watchlist()
    ├─ fetch_hy_quadrant()        │                             │  (Top 20 현황)
    │  (FRED HY Spread)           ├─ select_display_top5()      │
    │                             │  (Top 7 탐색 →              ├─ 텔레그램 발송
    ├─ fetch_vix_data()           │   min_seg ≥ 0% →            │  (4000자 분할)
    │  (FRED VIX)                 │   리스크 필터 →              │
    │                             │   최대 3종목 후보)           └─ Git auto commit/push
    ├─ DB 저장                    │
    │  (ntm_screening)            └─ get_daily_changes()
    │                                (이탈 감지 + 사유 분류)
    └─ run_ai_analysis()
       (Gemini 2.5 Flash)
```

### 실행 순서
1. **DB 초기화** + SPY 기반 마켓 날짜 감지
2. **전 종목 NTM EPS 수집** (yfinance ~1,260종목, ~15분)
3. **DB 저장** (ntm_screening 테이블)
4. **매출+품질 수집** (yfinance `.info` — 상위 50종목)
5. **Part 2 필터 적용** → 순위 부여 (composite_rank + w_gap)
6. **시장 리스크** (HY Spread + VIX + Concordance)
7. **AI 분석** (Gemini — 시장동향 + 종목별 내러티브)
8. **메시지 생성** (Signal + AI Risk + Watchlist)
9. **텔레그램 발송** (개인봇 + 채널)
10. **Git 자동 커밋/푸시** (DB + 캐시)

---

## DB 스키마

### ntm_screening (핵심 테이블)
```sql
CREATE TABLE ntm_screening (
    date            TEXT,
    ticker          TEXT,
    rank            INTEGER,
    score           REAL,         -- 기본 Score (seg1+seg2+seg3+seg4)
    ntm_current     REAL,         -- 오늘 NTM EPS
    ntm_7d          REAL,         -- 7일전 NTM EPS
    ntm_30d         REAL,         -- 30일전 NTM EPS
    ntm_60d         REAL,         -- 60일전 NTM EPS
    ntm_90d         REAL,         -- 90일전 NTM EPS
    is_turnaround   INTEGER DEFAULT 0,
    adj_score       REAL,         -- 방향 보정 점수
    adj_gap         REAL,         -- 방향 보정 괴리율
    price           REAL,
    ma60            REAL,
    part2_rank      INTEGER,      -- w_gap 기준 Top 30 순위 (NULL = 미선정)
    composite_rank  INTEGER,      -- 당일 adj_gap 순수 순위
    PRIMARY KEY (date, ticker)
);
```

- 전 종목 매일 저장 (`INSERT ... ON CONFLICT DO UPDATE`)
- `save_part2_ranks()`: 저장 전 기존 rank 전부 NULL 초기화 → 잔여 rank 방지

### ai_analysis (AI 분석 저장)
```sql
CREATE TABLE ai_analysis (
    date            TEXT NOT NULL,
    analysis_type   TEXT NOT NULL,   -- 'market', 'stock' 등
    ticker          TEXT DEFAULT '__ALL__',
    content         TEXT NOT NULL,
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, analysis_type, ticker)
);
```

### portfolio_log (포트폴리오 추적)
```sql
CREATE TABLE portfolio_log (
    date        TEXT,
    ticker      TEXT,
    action      TEXT,       -- 'enter', 'hold', 'exit'
    price       REAL,
    weight      REAL,
    entry_date  TEXT,
    entry_price REAL,
    exit_price  REAL,
    return_pct  REAL,
    PRIMARY KEY (date, ticker)
);
```

**DB 파일**: `eps_momentum_data.db` (NOT eps_momentum.db)

---

## 실행 방법

### 로컬 실행
```bash
python daily_runner.py
```
또는 Windows 배치:
```bash
run_daily.bat
```

### GitHub Actions (자동)
- **일일 스케줄**: 평일 KST 06:15 (cron: `'15 21 * * 0-4'` UTC)
- **워크플로우**: `.github/workflows/daily-screening.yml`
- DB + 캐시 자동 커밋/푸시

### 테스트 (수동)
- **워크플로우**: `.github/workflows/test-private-only.yml` (수동 dispatch)
- 개인봇만 전송, DB 커밋 안 함 (프로덕션 오염 방지)
- `MARKET_DATE` 파라미터로 특정 날짜 지정 가능

### 빠른 메시지 테스트
```bash
python quick_test_v3.py
```
DB + 캐시 + mock 기반, 실제 yfinance 수집 없이 메시지 포맷 확인.

### 마켓 날짜
- SPY 최근 거래일 기준 자동 감지 (`yf.Ticker("SPY").history(period="5d")`)
- `MARKET_DATE` 환경변수로 오버라이드 가능
- 미국 공휴일은 yfinance 데이터 부재로 자연스럽게 skip

### Cold Start
- `is_cold_start()`: DB에 `part2_rank` 데이터 3일 미만 → 개인봇만 전송
- 3일 이상 축적 시 자동 전환 (날짜 하드코딩 없이 DB 상태 기반)

---

## 프로젝트 구조

```
eps-momentum-us/
├── eps_momentum_system.py     # 핵심: INDICES(916종목), INDUSTRY_MAP, NTM EPS 계산, get_trend_lights()
├── daily_runner.py            # 메인 실행: ~3,998줄, 데이터수집→필터→순위→AI→메시지→텔레그램
├── quick_test_v3.py           # DB+cache+mock 기반 빠른 v3 메시지 테스트
├── config.json                # 텔레그램 토큰, Gemini API 키, Git 설정
├── run_daily.bat              # Windows 로컬 실행 스크립트
├── requirements.txt           # pandas, yfinance, pytz, google-genai
├── SESSION_HANDOFF.md         # 설계 결정 히스토리 (v1~v68)
│
├── track_performance.py       # DB 기반 실전 성과 추적기 (복리 누적, SPY 대비)
├── research_alpha_signals.py  # Top30 알파 시그널 연구 (어닝/공매도/내부자/매출)
├── backtest/                  # 백테스트 스크립트 (17개)
│   ├── backtest.py            #   기본 프레임워크 (검증일수 × 보유기간 매트릭스)
│   ├── backtest_wgap_*.py     #   w_gap 관련 (grid, multistart, final, variants, startdate)
│   ├── backtest_compare_all.py
│   ├── backtest_current_strategy.py
│   ├── backtest_exit_compare.py
│   ├── backtest_grid.py
│   ├── backtest_v57b_revised.py
│   ├── bt_metrics.py          #   종합 성과 지표 (Sharpe/Sortino/Calmar/MDD/Kelly/PF)
│   ├── bt_funcs.py            #   공통 유틸리티
│   ├── bt_entry_filter_check.py
│   ├── bt_part1.py
│   └── research_buff_gridsearch.py
│
├── migrate/                   # DB 마이그레이션 스크립트 (9개)
│   ├── migrate_v44_ranks.py
│   ├── migrate_v52_ranks.py
│   ├── migrate_v54_eps_quality.py
│   ├── migrate_v54_rerank.py
│   ├── migrate_v55_eps_quality.py
│   ├── migrate_v55b_smooth_eps_q.py
│   ├── migrate_v57b_raw_adjgap.py
│   ├── migrate_v58_wgap_rank.py
│   └── migrate_v58b_min_seg_filter.py
│
├── scripts/                   # 유틸리티 스크립트
│   ├── update_etf_cache.py    #   ETF 전체 홀딩 캐시 갱신 (etf-scraper)
│   └── send_historical_messages.py  #   과거 메시지 재발송
│
├── .github/workflows/
│   ├── daily-screening.yml    # 일일 자동 실행 (KST 06:15)
│   └── test-private-only.yml  # 수동 테스트 (개인봇만, DB 미커밋)
│
├── eps_momentum_short.py      # Short 후보 스크리닝 (Long 전략의 역방향, 관찰용)
│
├── eps_momentum_data.db       # SQLite DB - US (자동 생성)
├── ticker_info_cache.json     # US 종목 이름/업종 캐시 (자동 생성)
└── etf_holdings_cache_v2.json # ETF 전체 홀딩 캐시 (자동 생성)
```

---

## 환경변수

| 변수 | 용도 | 필수 |
|------|------|------|
| `TELEGRAM_BOT_TOKEN` | 텔레그램 봇 토큰 | ✅ |
| `TELEGRAM_CHAT_ID` | 채널 ID | 선택 (없으면 채널 전송 안 함) |
| `TELEGRAM_PRIVATE_ID` | 개인봇 ID | ✅ |
| `GEMINI_API_KEY` | Google AI Studio API 키 | ✅ (없으면 AI 분석 스킵) |
| `FRED_API_KEY` | FRED 경제 데이터 API 키 | ✅ (없으면 CSV fallback) |
| `MARKET_DATE` | 마켓 날짜 오버라이드 (YYYY-MM-DD) | 선택 |
| `MESSAGE_VERSION` | 메시지 버전 (v3 고정) | 선택 |

GitHub Actions에서는 Secrets로 등록.

---

## 버전 히스토리

| 버전 | 날짜 | 주요 변경 |
|------|------|-----------|
| **v58b+** | **2026-03-22** | 역변동성 비중, 성과 헤더, 알파 시그널(어닝/공매도/내부자) 추가 |
| **v58b** | **2026-03-20** | Top7 탐색으로 3종목 보장 강화, min_seg<-2% 순위전 제외 |
| **v58** | **2026-03-16** | w_gap Top3/Top15 전략: 진입 w_gap Top3 + min_seg≥0%, 이탈 Top15 |
| **v55b** | **2026-03-14** | eps_quality 연속 함수: min_seg 기반 선형 보간 (cliff effect 제거) |
| **v55** | **2026-03-14** | eps_quality 도입: adj_gap에 EPS 추세 품질 반영 |
| **v52** | **2026-03-12** | adj_gap 절대값 전략 전환: z-score composite → adj_gap 직접 정렬 |
| v45 | 2026-02-28 | v3 전용: v2 코드 제거 (-471줄) |
| v44 | 2026-02-26 | Dynamic Universe + 원자재 제외 + OP<5% 필터 |
| v31 | 2026-02-19 | VIX Layer 2 + Concordance + L3 동결 |
| v22 | 2026-02-12 | 매출 필수화 + 섹터 분산 제거 |
| v21 | 2026-02-12 | Composite Score + AI 프롬프트 구조화 |
| v20 | 2026-02-11 | Simple & Clear 리팩토링: Top 30 통일, 리스크 철학 확립 |
| v19 | 2026-02-10 | Safety & Trend Fusion: MA60+3일 검증 |
| v18 | 2026-02-09 | adj_gap 도입 |
| v8~10 | 2026-02-07~08 | Gemini AI + NTM EPS 전환 |
| v1~7 | 2026-01~02 | 초기 구현, A/B 테스팅 |

### v58b 백테스트 근거 (21일, 2/10~3/12)
- 16개 시작일 평균: wTop3/wTop15 = **-0.5%** (모든 조합 중 0에 가장 가까움)
- Top3 >> Top5, 좁은 이탈선 >> 넓은 이탈선 (일관)
- 21일 데이터 한계: 확정 판단 불가, 데이터 축적 후 재검증 필요
