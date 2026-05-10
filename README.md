# EPS Momentum System v80.10 (US Stocks)

Forward 12개월 EPS(NTM EPS) 기반 모멘텀 시스템. **"파괴적 혁신 기업을 싸게 살래"** 철학으로, w_gap(가중 괴리율)을 기반으로 저평가 종목을 선별한다. 괴리율이 음수일수록 EPS 개선 대비 주가가 덜 반영된 상태(= 저평가). MA120 + 리스크 필터로 신뢰도를 높이고, AI(Gemini 2.5 Flash)가 위험 신호를 점검한 뒤 최대 3종목을 매수 후보로 제시한다. 최대 3종목 보유.

**v80.5 전략**: 진입 Top 3 / 이탈 8위 밖 / 슬롯 3개 / Breakout Hold (strict, 2일 유예) / FCF·ROE 품질 필터 / z-score 상한 제거 / **빈 날 penalty 기준 `part2_rank IS NULL`** / **✅ 약점 종목 슬라이드** / **β1 (cap 발동 시 +0.3 보너스)** + **opt4 (C4 sign flip)** 유지 / **Case 1 z-score 보너스 제거** — cr/score_100/part2_rank 정렬 일관성 회복. BT -2.84%p 양보(+62.95% → +60.11%), 메시지 비대칭(LNG cr=18 vs p2=2 같은) 사례 차단.

---

## 목차
1. [핵심 전략](#핵심-전략)
2. [NTM EPS 계산](#ntm-eps-계산)
3. [EPS 점수 체계](#eps-점수-체계)
4. [괴리율 (adj_gap → w_gap)](#괴리율)
5. [매수 후보 선정](#매수-후보-선정)
6. [진입·이탈 규칙 (v80.2)](#진입이탈-규칙-v802)
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
- **w_gap**(3일 가중 conviction adj_gap)의 z-score 순위가 매매 신호. 점수 높을수록 강한 저평가
- 혁신 없는 종목은 하드필터(매출 성장 ≥10%)로 사전 제거
- 추세가 살아있는 종목만(MA120 이상) 대상
- 매수 후보 Top 3, 최대 3종목 보유 (균등비중)
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

### conviction adj_gap (v75)
adj_gap에 애널리스트 합의·EPS 동행·매출 성장 보너스를 곱해 강도 증폭:

```
conviction = adj_gap × (1 + max(rev_up30/N, |Δntm_90d|/100) + rev_bonus)
rev_bonus = 0.3 if rev_growth ≥ 30% else 0       # V9h 매출 성장 보너스
```

- 양수 adj_gap(고평가)에 보너스가 페널티화되는 부호 결함은 **feature** (양수 종목 자동 차별이 알파의 일부)

### w_gap (가중 conviction) — 3일 가중 z-score (v71/v78/v79)
일별 conviction adj_gap을 z-score(30~∞)로 표준화 후 3일 가중평균:

```
점수(d) = max(30, 65 + (-(conv - mean) / std) × 15)   # v79: 상한 100 clamp 제거
점수(d) += 8  if NTM 30d > +1% AND 가격 30d < -1%     # v78 Case 1 보너스

w_gap = 점수(T0) × 0.5 + 점수(T1) × 0.3 + 점수(T2) × 0.2
```

- 빈 날(필터 탈락 등) = **30점 페널티** (v77 carry-forward 제거)
- w_gap 내림차순 → part2_rank 1~30 부여 (점수 높을수록 1위)
- **v79**: 상한 100 clamp 제거 (40/46일 z_raw≥100 발생, MU/VNOM 등 outlier 변별력 회복) — multistart 33시작일 +2.4%p

### 점수 표시 (1위=100 환산)
```
표시점수 = w_gap / top_w_gap × 100
```
1위는 항상 100점, 나머지는 1위 대비 비율. 종목 간 격차를 한눈에 파악 (예: MU 100, FIVE 60대 → "MU가 압도적").

---

## 매수 후보 선정

### 유니버스
NASDAQ 100 + S&P 500 + S&P 400 MidCap = **916개 고정 종목** + `fetch_dynamic_tickers()`로 시가총액 $50억+ 종목 추가 (~1,260개 총 유니버스).

### Part 2 필터 (11개)
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
| 11 | **품질 필터 (v79)** | **FCF<0 AND ROE<0 제외** | EPS 전망만 높고 실질 수익성 없는 종목(VNOM 패턴) — 단독 음수는 허용해 성장주 보호 |

매출/품질 수집: eligible 상위 종목에 대해 `fetch_revenue_growth()` + `.info` 실행. 수집 실패 시 **DB 캐시 fallback** (v76, 최근 7일 값 사용).

### 순위 부여 (`save_part2_ranks()`)
1. 필터 통과 종목 중 **min_seg < -2% 제외** (EPS 추세 불안정)
2. 당일 conviction adj_gap 오름차순 → `composite_rank` 부여 (전 eligible 종목)
3. w_gap(3일 가중 z-score + Case 1 보너스) 내림차순 → 상위 30개에 `part2_rank` 1~30 부여

---

## 진입·이탈 규칙 (v80.2)

### 진입 조건
`select_display_top5()` — 매일 최대 3종목 매수 후보 선정, 누적 최대 3종목 보유:

1. w_gap 순위 상위에서 탐색 (✅ 종목 슬라이드 방식)
2. **min_seg ≥ 0%** (전 구간 EPS 상승 확인)
3. **리스크 필터** 통과 (하향과반·저커버리지 차단)
4. **✅ 검증 + 진입조건 통과 종목만 슬롯 차지** (v80.2): ⏳/🆕뿐 아니라 ✅이지만 min_seg<0 / 하향과반 / 저커버리지 탈락 종목도 다음 정상 ✅ 후보로 슬라이드 → 빈 슬롯 방지
5. **최대 3슬롯** 보유

### 이탈 조건
| 조건 | 기준 | 이탈 사유 표시 |
|------|------|----------------|
| 순위 밀림 | part2_rank > 8 (v78에서 11→8로 강화) | [순위밀림] |
| EPS 추세 악화 | min_seg < -2% | [추세둔화] |
| 추세 이탈 | price < MA120 | [MA120↓] |
| 매출 둔화 | rev_growth < 10% | [매출↓] |
| 저커버리지 | num_analysts < 3 | [저커버리지] |
| 품질 악화 (v79) | FCF<0 AND ROE<0 | [품질↓] |

### Breakout Hold (이탈 유예, v74)
순위밀림 이탈 신호 발생 시 다음 4조건 모두 만족하면 **2일 매도 유예**:
1. 최근 20거래일 종가 +25% 이상
2. ntm_90d → ntm_current 순방향 (EPS 동행)
3. rev_up30 / num_analysts ≥ 0.4 (애널리스트 합의 상향)
4. 현재가 > MA60

이탈 사유에 `⏸️유예` 마커로 표시. 사용자는 메시지 보고 수동 매매.

> **v78 백테스트**: 45일, 시스템 누적 +34.6% → +49.8% (+15.2%p), MDD -16.1%→-12.9%, Sharpe 3.39→4.58. 81,880조합 그리드서치 + Walk-Forward + multistart + LOO 통과.
> **v79 검증**: z-score clamp 제거(A1) multistart 33시작일 +2.4%p. 기각: B1(계수 12) -7.2%p, C(missing 재정규화) -10.7%p. 사이드이펙트 없음.

이탈 종목은 Signal/Watchlist에서 사유별로 그룹핑 표시.

### 3일 상태 표시
기준: DB의 `part2_rank`(Top 30 소속 여부)가 존재하는 최근 3거래일 수

| 상태 | 조건 | 의미 |
|------|------|------|
| ✅ | 3일 연속 Top 30 | 검증됨, **Signal 진입 가능** |
| ⏳ | 2일 Top 30 | 검증 중, Watchlist만 표시 |
| 🆕 | 1일만 Top 30 | 신규 진입, Watchlist만 표시 |

v80.2부터: ⏳/🆕는 스킵, ✅이지만 진입조건(min_seg<0 / 하향과반 / 저커버리지) 탈락도 슬롯 못 차지하고 다음 정상 ✅ 후보로 슬라이드. 풀 슬롯(3개) 채울 때까지 자연 슬라이드.

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
US High Yield Spread 기반 Verdad 4분면 모델. `fetch_hy_quadrant()` — FRED API JSON + 로컬 장기 캐시 병합.

**HY 퍼센타일**: 2520일(10년) rolling rank.

**캐시 병합 (2026-04-21 도입)**: FRED가 2026-04부터 이 시리즈를 최근 3년으로 제한(series note 명시) → 10년 rolling 중위수(min 5년) 계산 불가. `data_cache/hy_spread.parquet`(1996~, 7,650일)에 FRED 최근분을 매일 오버레이하고 GA 워크플로우의 `git add -A`로 꼬리 자동 연장. `_load_merge_save_hy_cache()` 참조.

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
📈 시스템 누적 수익률 +48.8% (44거래일)
    같은 기간 S&P500은 +1.4%
```
- `_get_system_performance()`: DB 기반 복리 재투자 백테스트 리플레이 (균등비중)
- **벤치마크**: ^GSPC (S&P 500 지수, ETF 아닌 지수 자체) — 펀드 매니저 표준 관행
- yfinance `end=` exclusive 보정(+1일) 처리 — 마지막 날 누락 방지

### Signal 알파 시그널 (정보 표시용, 순위에 영향 없음)
종목별 근거 아래에 해당 시그널이 있을 때만 표시:
- **어닝 서프**: 최근 1Q surprisePercent > 0.3 → `어닝 서프 +X%`
- **어닝 쇼크**: 최근 1Q surprisePercent < 0 → `⚠️ 어닝 미스 X%`
- **공매도**: shortPercentOfFloat ≥ 8% → `공매도 X.X%`
- **경영진 매도**: 제거됨 (10b5-1 사전계획 매도 구분 불가)

### Signal 종목 포맷
```
1. Micron Technology(MU) · 반도체 · 100.0점
EPS 전망 +133% · 매출성장 +196%
순위 1→1→1위 · 의견 ↑26↓0
AI가 생성한 2~3문장 내러티브
```

- **점수**: `w_gap / top_w_gap × 100` (1위=100, v79) — 종목 간 격차 직관적 표시
- **의견**: 30일간 EPS 상향/하향 수정 애널리스트 수 (`↑N ↓N`)
- 알파 시그널(어닝 서프/공매도)이 있으면 의견 줄에 한 줄로 통합

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
    ├─ yfinance 전종목 수집       ├─ get_part2_candidates()     ├─ create_signal_message()
    │  (NTM EPS, 가격,            │  (11개 하드필터)             │  (✅ Top 3 추천)
    │   MA120, 업종 등)           │                             │
    │                             ├─ save_part2_ranks()         ├─ create_ai_risk_message()
    ├─ fetch_revenue_growth()     │  (min_seg 필터 →            │  (시장+HY+VIX+AI)
    │  (상위 종목 매출+품질)       │   composite_rank →          │
    │                             │   w_gap Top 30)             ├─ create_watchlist_message()
    ├─ fetch_hy_quadrant()        │                             │  (Top 20 현황)
    │  (FRED HY Spread)           ├─ select_display_top5()      │
    │                             │  (Top 3 탐색 →              ├─ 텔레그램 발송
    ├─ fetch_vix_data()           │   min_seg ≥ 0% →            │  (4000자 분할)
    │  (FRED VIX)                 │   리스크 필터 →              │
    │                             │   ✅ 기준 3종목)             └─ Git auto commit/push
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
- **일일 스케줄**: 화~토 **KST 05:58** (cron: `'58 20 * * 1-5'` UTC) — **v80 (2026-04-21)**: KST 20:00 → 05:58 변경
  - 이유: 백테스트는 T일 종가로 매매 시뮬. KST 20:00 실행 시 실제는 T+1 시가 매매(overnight gap). KST 05:58 (마감 +58분, 애프터마켓) → 사용자는 종가 근처 가격에 애프터마켓 매매 → **백테스트=실제 가격 일치**
  - DE 사례 재분석: v78 "마감 직후 수집 불안정" 진단은 과잉. 실제는 30일 rolling window 자연 현상 (수집 시점 무관)
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
├── daily_runner.py            # 메인 실행: ~5,126줄, 데이터수집→필터→순위→AI→메시지→텔레그램
├── quick_test_v3.py           # DB+cache+mock 기반 빠른 v3 메시지 테스트
├── config.json                # 텔레그램 토큰, Gemini API 키, Git 설정
├── run_daily.bat              # Windows 로컬 실행 스크립트
├── requirements.txt           # pandas, yfinance, pytz, google-genai
├── SESSION_HANDOFF.md         # 설계 결정 히스토리 (v1~v79)
├── Z_SCORE_FLAW_HANDOFF.md    # v79 z-score 결함 진단 + 검증 5가지 방안 + 실행원칙
│
├── track_performance.py       # DB 기반 실전 성과 추적기 (복리 누적, ^GSPC 대비)
├── recompute_ranks.py         # DB 전 일자 part2_rank 재계산 (전략 변경 후 사용)
├── research_alpha_signals.py  # Top30 알파 시그널 연구 (어닝/공매도/내부자/매출)
├── backtest/                  # 백테스트 통합 인프라
│   ├── bt_engine.py           #   통합 시뮬레이션 엔진 (모든 metric 반환)
│   ├── bt_metrics.py          #   Sharpe/Sortino/Calmar/MDD/Kelly/PF/WinRate
│   ├── backtest_v3.py         #   regenerate_part2_with_conviction (DB 복사본 + monkey-patch)
│   └── backtest_compare.py    #   다중 전략 비교 프레임워크
│
├── research/                  # v79 검증 스크립트 (캐시 재사용 패턴)
│   ├── zscore_cache.py        #   공통 데이터 캐시 (한 번 로드, 모든 변형 재사용)
│   ├── zscore_diagnosis.py    #   결함 정량화
│   ├── baseline_v78.py        #   v78 baseline metric
│   ├── step4_a2.py            #   방안 A2 단독 BT
│   ├── step7_a1_c.py          #   A1+C 조합 BT
│   └── step8_sideeffect.py    #   사이드이펙트 점검
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
| **v80.10** | **2026-05-10** | **PE long-tail — fwd_pe_chg 가중치 0.4/0.3/0.2/0.1 → 0.3/0.1/0.1/0.5** (90일 누적 PE 압축 강조). **계기**: AMD가 EPS +12% 폭등에도 순위 미진입 디버깅 → fwd_pe_chg 가중치 의문 → 4D 그리드 84조합 BT에서 production 80/84위 발견. **검증 종합** (3가지 BT, 모두 12시작일 또는 walk-forward 5 splits): (1) 4D 그리드 84조합 (production 80위), (2) walk-forward 5 splits (Top 10 모두 5/5 OOS 양수, production 항상 11/11), (3) seg-style 비교 (cumulative + long-tail이 모든 형태 best). **재현성 검증**: 5/2 commit msg "midweight +5.72%p" → OLD conviction 재실행 +6.17%p로 재현 확인 (0.45%p 차이는 데이터 누적). **인접 안정성** (research/bt_pe_weights_adjacency.py): A 후보 ±0.05 7변형 모두 5/5 splits OOS lift 양수 (+46~65%p) → plateau 확정. **전문가 평가** (퀀트 + 리스크 매니저 독립 의견): 둘 다 A 후보 (w_30_10_10_50) 권장. 60일 단일 강세장 multiple testing inflation 우려 + 7d 0.4→0.1 (B 후보) 절단 위험 + alpha decay 가능성. A는 변경 폭 작아 (0.4→0.3) 롤백 용이. **변경**: `daily_runner.py:632, 782` weights 두 곳 동시 수정. **DB 마이그레이션** (research/apply_v80_10.py): 60일 모두 adj_gap/cr/p2 재계산, backup `bak_pre_v80_10.db`. **모니터링 권장**: 5거래일 SPY 대비 알파 / MDD / Top3 교체율 모니터링. HY×VIX Q3 진입 시 baseline 복귀 검토. **caveat**: 60일 단일 강세장 sample, 약세장 미검증 |
| **v80.9** | **2026-05-05** | **X2 — eps_floor cap 1.0→3.0 + rev_bonus 비례** (cliff/cap 임의 임계값 제거, 경제학적 합리성). v80.8 위에서 `_apply_conviction` 두 줄 수정: (a) eps_floor cap `1.0 → 3.0` (NTM 100%+ 변동 정보 보존), (b) rev_bonus binary `(rg≥0.3) ? 0.3 : 0` → `min(min(rg, 0.5) × 0.6, 0.3)` smooth 비례 (30% 경계 cliff 제거). **BT 12시작일**: ret -0.44%p (미세), MDD/Sharpe/Sortino 미세 개선 — 60일 데이터에선 큰 차이 없음. **채택 이유**: 미래 환경 변화(매출 30% 경계 종목, NTM 200%+ 폭증) 대비 robustness. 사용자 직관 — "경제학적으로 합리적이고 더 나은 방식이면 운용하면서 문제 안 생기게 적용". **DB 마이그레이션** (research/apply_v80_9.py): 56일 모두 cr/p2 재계산, 53/56일 cr 변경, 54/56일 p2 변경, backup `bak_pre_v80_9.db`. **B-F 추가 단건 검증** (X2 base 위, 12시작일): B1~B4 (min_seg variants), E1~E2 (rev_bonus cap) 7개 변형 모두 X2 base 동일 → 보류 (테스트 워크플로우 후 재검증) |
| **v80.8** | **2026-05-05** | **rev_up30 ≥ 3 합의 강도 필터 추가** — 단일 분석가 의존 종목 차단 (WELL 같은 케이스). **계기**: 5/4 메시지 EDA에서 WELL p2=14에 num_analysts=3, rev_up30=1로 단일 분석가 의존 → 사용자 우려. **검증 여정 (7개 맹점 BT, 30+ 변형, 12시작일 multistart)**: (1) confidence-weighted ratio (-15~37%p), (2) max → avg/sum (-14~16%p), (3) 둘 다 강함 보너스 (단독 +4.80%p, base 위에선 0), (4) eps_floor cap 완화 (단독 +6.22%p, base 위에선 0), (5) rev_bonus 비례화 (단독 +3.64%p, base 위에선 0), (6) rev_down30 활용 (효과 0), (7) num_analysts=0 (영향 미미) — **모든 변형 합치면 -20%p 손실**. 본질 파악: 시스템 알파 = "약한 신호 종목 차단" 단일 차원, rev_up30 ≥ 3가 single point of action으로 다른 모든 알파 흡수. **BT 결과**: 6시작일 +8.51%p, 12시작일 +7.16%p, MDD -3.47%p 개선 (-13.12 → -9.65), Sharpe +1.07, Sortino +1.81, Calmar +78. **변경**: `get_part2_candidates`에 한 줄 (`filtered = filtered[filtered['rev_up30'] >= 3].copy()`). **DB 마이그레이션** (research/apply_v80_8.py): 58일 cr/p2 재계산, 1687건 제외, 58/58일 cr 변경, 55/58일 p2 변경, backup `bak_pre_v80_8`. 영향 종목: WELL/AGX/STRL 등 단일 분석가 의존 종목. 5/1 매수 후보 TER/SNDK/LRCX → SNDK/TER/LRCX (순서 미세 변경). **메모리**: `project_v80_8_validation_2026_05_05.md` |
| **v80.7** | **2026-05-02** | **누적 수익률 측정 정확화 + SPY 버그 수정** — 5/1 메시지 EDA에서 발견. (1) `_get_system_performance` SPY 가격 추출 시 `row.iloc[3]`이 Low(일중 최저가) → `df['Close'].iloc[i, 0]`로 수정 (yfinance auto_adjust=False 컬럼 [Adj Close, Close, High, Low, Open, Volume]). (2) day_ret 계산 순서 버그: 이탈/진입 후 day_ret → **이전 코드는 진입 종목의 매수 전 변동(어제→오늘)을 day_ret에 잘못 누적 + 이탈 종목의 마지막 변동을 누락**. 사용자 운영(메시지 받고 그 종가에 애프터마켓 매수/매도)과 일치하려면 **어제 portfolio 기준 day_ret 먼저 계산 → 그 다음 이탈/진입**. 적용 결과: 시스템 +57% → **+99.6%** (실제 trade-level 검증, SNDK +51%/MU +42%/TTMI +24% 등). SPY +4.6% → **+5.8%** (Close 기준). `daily_runner.py:_get_system_performance` + `backtest_s2_params.simulate` 동일 수정 (BT 변형 비교 결론 동일). |
| **v80.6 (시도/롤백)** | **2026-05-02** | **β1 제거 시도 → 즉시 롤백** — 5/1 메시지 EDA에서 사용자 우려(MU 5/1 cap 보너스로 매도 영역 강조)로 β1을 γ(cap → dir=0)로 제거했으나, **6시작일 multistart(50거래일+ 보장)에서 -18.20%p 손실 일관 확인 → 롤백**. 메모리 v80.5의 "β1 BT 효과 0"은 잘못된 결론 (33시작일 평균이 짧은 기간 시작일로 흐려졌음). 추가 검증 큐: midweight 가격 가중치(트레이드오프, 거부), Case 1 복원(-4.78%p, 거부), 저커버리지 컷오프 ↑(-9.63%p, 거부), 콤보 필터(대상 n=1, 거부), min_seg 임계값(영향 0, 현행 유지). **결론**: v80.5b가 6개 시도 모두 통과한 최적 정책. **DB 백업**: `bak_pre_v80_6.db` (롤백 source), `bak_post_v80_6.db` (실패한 v80.6 보존). **6시작일 multistart 표준화**: BT 신뢰성을 위해 데이터 시작 직후 6일 시작일 패턴 정착 (`research/bt_initial_multistart.py`). |
| **v80.5b** | **2026-05-01** | **`save_part2_ranks._conv_gap` 필드명 버그 수정** — results_df에서 `'ntm_current'` 키 읽고 있었으나 실제 키는 `'ntm_cur'`. 결과: nc=0 → eps_floor=1.0 cap → conviction 평탄화 → 사실상 adj_gap 기준 정렬 (rev_up30 합의도 무시). **사례**: 4/30 cron TER cr=2 / LRCX cr=3 (시스템 의도 반대). **수정**: `daily_runner.py:1490` `row.get('ntm_current')` → `row.get('ntm_cur')`. **영향**: 56일 중 1일(4/30)만 cr 순서 변경 (33건). 다른 55일은 우연히 conv_gap 정렬과 일치. part2_rank는 `_compute_w_gap_map`(DB 직접 조회) 사용으로 영향 없음. BT/시스템 수익률 +58.3% 변화 0. 검증: 정렬 위반 0건 |
| **v80.5** | **2026-05-01** | **Case 1 z-score 보너스 제거** — cr/score_100/part2_rank 정렬 일관성 회복. **계기**: LNG 사례 (part2_rank=8, score_100=98.8, cr=5 비대칭). 사용자 요구 "메시지에 cr/가중순위/점수 셋 다 표시 + 일관". **진단**: Case 1 보너스(+8 z-score)가 z-score 단계에만 적용 → part2_rank/score_100 영향 O, cr 영향 X. **BT 1차** (5시작일): v80.4(β1+opt4+Case1) +55.24% / no_case1 +60.11% / no_capopt(Case1만) +62.95% / pure_baseline +60.11%. **β1+opt4 BT 효과 0** (no_case1 = pure_baseline 동일), Case 1만이 +2.84%p 알파. **BT 2차** (Case 1을 adj_gap 단계로 이동 시도): factor 1.05/1.10 효과 0, 1.15+ 악화 (-7.71%p). **z-score 단계만이 알파 살림** (3일 가중 안정성). **사용자 결정**: β1+opt4 유지 + Case 1 제거 — BT -2.84%p 양보, 메시지 일관성 회복, β1+opt4는 미래 안전장치. **변경**: daily_runner.py 3곳 (`_compute_w_gap_map` / `_build_score_100_map` / 성과 추적 `_w_gap`) Case 1 보너스 블록 제거. **DB 마이그레이션**: 54일 part2_rank 재계산, 31/54일 변경, backup `bak_pre_v80_5`. **결과**: LNG 4/28 p2=8 → 18, cr=5 그대로 (자연스러운 1일 vs 3일 가중 차이) |
| **v80.4** | **2026-04-30** | **v80.3 γ 대체** — β1 (cap 발동 시 dir=+0.3 보너스, 어닝 비트 강한 신호 강화) + opt4 (정상 영역 C4 sign flip, 양수 차별 정직화) + score_100 Case 1 보너스 동기화 (`_build_score_100_map`에 +8점 추가, part2_rank↔score_100 정렬 일관) + Watchlist 저커버리지(num_analysts<3) 필터. **근거**: 사용자 직관 ("고평가+둔화 buggy 보너스 차단", "어닝 비트 보너스") + memory v75 ("양수 종목 자동 차별이 알파") 일치. **4사분면 EDA**: C1 매수 강화 / C2 약화 / C3 매수 멀리 / C4만 fix (sign flip). **검증**: DB 54일 재계산, β1 위반 0건, score_100↔part2_rank 정렬 100% 일관. **SNDK 의존성**: SNDK 제외 multistart에서 모든 변형 완전 동일(+36.17%) → 차이는 SNDK 한 종목 의존, 미래 데이터 누적 시 사라질 가능성. **v80.5에서 Case 1 sync 제거** (β1+opt4는 유지) |
| **v80.3** | **2026-04-30** | Segment cap 발동 시 direction 무효화 (γ). 어닝 같은 점프 이벤트가 lookback 30일 경계를 가로지르며 한 segment가 ±100% cap에 걸리면 direction 부호 반전 → adj_score 폭락 부작용 차단. MU 4/28 사례: adj_score 폭락 -43% → -18%로 robust. 매매 BT (54일/159 trades): baseline +57.43% → γ +60.51% (+3.08%p), MDD -15.34→-15.84%, Sharpe 4.14→4.20. γ''(partial direction) +1.79%p 차선·δ(dir 제거) -6.11%p 폐기. 모든 일자 DB row γ로 재계산 (backup 보존) |
| **v80.2** | **2026-04-29** | Signal 슬롯 채움 — `ENTRY_THRESHOLD=3` 인공 캡 제거. ⏳/🆕뿐 아니라 ✅이지만 min_seg<0 / 하향과반 / 저커버리지 탈락도 다음 정상 ✅로 슬라이드(4위/5위 자동 대체). 빈 슬롯 발생 차단. 54일 BT 발동 0건이라 과거 BT 결과 변화 없음(+64.80% 동일). LNG 04-29 ✅ 진입 임박 — 첫 발동 케이스 실증 예정. 저커버리지 단독 필터 BT는 -21.8%p 악화로 비채택(TTMI 4명 winner 차단) |
| **v80.1** | **2026-04-24** | w_gap/score penalty 기준 `composite_rank` → `part2_rank`. ⏳(2일)/🆕(1일) 종목이 3일치 실제 데이터로 계산되던 논리 모순 해소. 최근 30일 BT에서 ✅ 진입 3종목 변경 0건(실거래 영향 없음), Top 8 변화 5일(⏳/🆕 종목만 뒤로 밀림). TSM 4/21 사례: 3위→7위, ASML 4위→3위 |
| **v79** | **2026-04-17** | z-score 상한 100 clamp 제거(outlier 변별력 회복) + FCF·ROE 품질 필터 + Signal ✅ 3종목 보장 + 점수 표시 1위=100 환산. multistart 33시작일 +2.4%p / B/C/A1+C 기각. 벤치마크 SPY→^GSPC 전환(end-exclusive 버그 동시 수정) |
| **v78** | **2026-04-16** | E5/X12→E3/X8 + Case 1 보너스(NTM 30d>+1% AND 가격 30d<-1% → z-score +8). 81,880조합 그리드서치. 시스템 +34.6%→+49.8% (+15.2%p), MDD 개선. 스케줄 KST 06:15→20:00 |
| **v77** | **2026-04-15** | carry-forward 제거(빈 날 무조건 30점) + fallback DB UPDATE. FAF 🆕인데 rank 3 모순 해결 |
| **v76** | **2026-04-14** | 재무 필드 DB 캐시 fallback (yfinance `.info` 17~99% 편차 방어). AI 내러티브 누락 종목 재요청 |
| **v75** | **2026-04-11** | 매출 성장 보너스(V9h): conviction에 `+0.3 if rev_growth ≥ 30%`. multistart +1.84%p |
| **v74** | **2026-04-11** | E3/X11/S3 + Breakout Hold strict (강한 상승 추세 시 매도 신호 2일 유예). 백테스트 +31.59% (33시작일), 33/33 양수 |
| **v71** | **2026-03-30** | 역변동성→균등비중. 일별 z-score(30~100)→3일 가중점수. composite_rank=당일순위, part2_rank=3일가중순위 |
| **v68** | **2026-03-20** | 톤 통일: ~해요/~예요 → ~입니다 |
| **v65** | **2026-03-15** | HY×VIX 조합 매트릭스 (16칸, 24.4년 분석) |
| **v58b+** | **2026-03-22** | 역변동성 비중, 성과 헤더, 알파 시그널(어닝/공매도/내부자) 추가 |
| **v55** | **2026-03-14** | eps_quality 도입: adj_gap에 EPS 추세 품질 반영 |
| **v52** | **2026-03-12** | adj_gap 절대값 전략 전환 |
| v45 | 2026-02-28 | v3 전용: v2 코드 제거 (-471줄) |
| v44 | 2026-02-26 | Dynamic Universe + 원자재 제외 + OP<5% 필터 |
| v31 | 2026-02-19 | VIX Layer 2 + Concordance + L3 동결 |
| v22 | 2026-02-12 | 매출 필수화 + 섹터 분산 제거 |
| v20 | 2026-02-11 | Simple & Clear 리팩토링: Top 30 통일 |
| v19 | 2026-02-10 | Safety & Trend Fusion: MA60+3일 검증 |
| v18 | 2026-02-09 | adj_gap 도입 |
| v8~10 | 2026-02-07~08 | Gemini AI + NTM EPS 전환 |
| v1~7 | 2026-01~02 | 초기 구현, A/B 테스팅 |

### v79 검증 결과 (multistart 33시작일, 2026-02-10~04-16)
- **A1 채택** (z-score 상한 무제한): ret +2.4%p, MDD +1.5%p 악화(허용), Sharpe +0.39
- 기각 변형: B1(계수 12) -7.2%p, C(missing 재정규화) -10.7%p, A1+C -9.3%p
- 사이드이펙트: Top 20 안정성 동등, L3/breakout/⚠️ 트리거 무관
- 검증 패턴: 캐시 재사용(research/zscore_cache.py + 6개 스크립트, 각 Step 30분 이내)
