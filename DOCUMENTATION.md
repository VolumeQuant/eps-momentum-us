# EPS Revision Momentum Strategy v7.0 - 상세 기술 문서

## 목차

1. [프로젝트 개요](#1-프로젝트-개요)
2. [전략 철학](#2-전략-철학)
3. [시스템 아키텍처](#3-시스템-아키텍처)
4. [핵심 알고리즘](#4-핵심-알고리즘)
5. [v6.3 Quality & Value Scorecard](#5-v63-quality--value-scorecard)
6. [v7.0 신규 기능](#6-v70-신규-기능)
7. [데이터 흐름](#7-데이터-흐름)
8. [모듈별 상세](#8-모듈별-상세)
9. [설정 가이드](#9-설정-가이드)
10. [설치 및 실행](#10-설치-및-실행)
11. [백테스팅 설계](#11-백테스팅-설계)
12. [트러블슈팅](#12-트러블슈팅)

---

## 1. 프로젝트 개요

### 1.1 목적

미국 주식 시장에서 **애널리스트 EPS 컨센서스 상향 조정**을 추적하여 모멘텀이 있는 종목을 자동으로 스크리닝하고, 실전 매매에 활용할 수 있는 액션 신호를 제공하는 시스템.

### 1.2 핵심 가설

> "애널리스트들이 Forward EPS 전망치를 지속적으로 상향 조정하는 종목은 향후 주가 상승 확률이 높다"

이 가설은 다음 논리에 기반:
- EPS 상향 = 실적 개선 기대
- 실적 개선 = 주가 상승 촉매
- 모멘텀 지속 = 추세 추종 전략 유효

### 1.3 투 트랙 시스템

| Track | 목적 | 출력 |
|-------|------|------|
| **Track 1** | 실시간 트레이딩 | 텔레그램 알림, 매수 후보 리스트 |
| **Track 2** | 백테스트 데이터 축적 | SQLite DB (Point-in-Time) |

---

## 2. 전략 철학

### 2.1 EPS Revision Momentum이란?

애널리스트들이 제출하는 Forward EPS 전망치의 **시간에 따른 변화**를 추적:

```
EPS Trend 데이터 (Yahoo Finance)
├── Current: 현재 컨센서스
├── 7 Days Ago: 7일 전 컨센서스
├── 30 Days Ago: 30일 전 컨센서스
├── 60 Days Ago: 60일 전 컨센서스
└── 90 Days Ago: 90일 전 컨센서스
```

**핵심 통찰**: Current > 7d > 30d > 60d 패턴(정배열)은 지속적인 상향 조정을 의미

### 2.2 왜 EPS Revision인가?

| 지표 | 장점 | 단점 |
|------|------|------|
| **EPS Revision** | 선행 지표, 전문가 합의 | 데이터 접근성 |
| Price Momentum | 단순, 검증됨 | 후행 지표 |
| Earnings Surprise | 강력한 신호 | 분기 1회만 |
| Insider Trading | 내부 정보 반영 | 노이즈 많음 |

### 2.3 필터 철학 변화 (v4 → v5)

**v4**: 펀더멘털 중심 (저평가/성장 엄격)
```
문제: 좋은 종목도 펀더멘털 데이터 없으면 제외
```

**v5**: 기술적 + 펀더멘털 복합
```
해결: MA200이 1차 필터 → 펀더멘털 조건 완화
논리: 장기 상승 추세(MA200↑) 자체가 품질 신호
```

---

## 3. 시스템 아키텍처

### 3.1 전체 구조

```
┌─────────────────────────────────────────────────────────────┐
│                    daily_runner.py (메인)                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   Track 1    │    │   Track 2    │    │   Report     │  │
│  │  스크리닝    │    │  데이터축적  │    │   생성       │  │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘  │
│         │                   │                   │          │
│         ▼                   ▼                   ▼          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │ eps_momentum │    │   SQLite     │    │  HTML/MD     │  │
│  │ _system.py   │    │     DB       │    │   Files      │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│                      출력 채널                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  Telegram    │    │    Git       │    │    CSV       │  │
│  │   알림       │    │  Push        │    │   저장       │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 파일 구조

```
eps-momentum-us/
├── daily_runner.py          # 메인 실행 파일 (1,613줄)
│   ├── run_screening()      # Track 1: 실시간 스크리닝
│   ├── run_data_collection()# Track 2: 데이터 축적
│   ├── get_action_label()   # 액션 분류 (v5.1)
│   ├── create_telegram_message() # 텔레그램 포맷 (v5.2)
│   └── main()               # 전체 오케스트레이션
│
├── eps_momentum_system.py   # 코어 로직 (879줄)
│   ├── INDICES              # 종목 유니버스 (917개)
│   ├── SECTOR_MAP           # 섹터 매핑
│   ├── calculate_momentum_score_v3() # 스코어링
│   └── calculate_slope_score()       # A/B 테스트용
│
├── sector_analysis.py       # 섹터/테마 분석
│   ├── SECTOR_ETF           # 섹터별 ETF 매핑
│   ├── THEME_ETF            # 테마별 ETF 매핑
│   └── analyze_sector_theme()
│
├── config.json              # 설정 파일
├── eps_momentum_data.db     # SQLite DB (백테스트용)
├── eps_data/                # 일일 스크리닝 CSV
├── reports/                 # HTML/MD 리포트
├── logs/                    # 실행 로그
└── run_daily.bat            # Windows 스케줄러용
```

---

## 4. 핵심 알고리즘

### 4.1 모멘텀 스코어 계산 (v3)

```python
def calculate_momentum_score_v3(current, d7, d30, d60, d90=None):
    """
    가중치 기반 스코어링 + 정배열 보너스

    기본 점수:
    - Current > 7d: +3점 (최신, 가장 중요)
    - 7d > 30d: +2점
    - 30d > 60d: +1점
    - 역방향: -1점

    변화율 보너스:
    - 60일 변화율 / 5 (5%당 1점)

    정배열 보너스:
    - 완전 정배열 (C>7d>30d>60d): +3점
    - 부분 정배열 (C>7d>30d): +1점
    """
```

**예시 계산**:
```
MU (Micron): Current=8.5, 7d=8.2, 30d=7.0, 60d=4.0

기본 점수:
- 8.5 > 8.2 (C>7d): +3
- 8.2 > 7.0 (7d>30d): +2
- 7.0 > 4.0 (30d>60d): +1
= 6점

변화율 보너스:
- (8.5-4.0)/4.0 = 112.5%
- 112.5/5 = 22.5점

정배열 보너스:
- 완전 정배열: +3점

총점: 6 + 22.5 + 3 = 31.5점
```

### 4.2 Kill Switch 로직

```python
# 7일 대비 -1% 이상 하락시 제외
if current < d7 * 0.99:
    return None  # 스크리닝 탈락
```

**목적**: 모멘텀 꺾임 감지
- 너무 엄격하면 (0%): 일시적 변동에도 제외
- 너무 느슨하면 (-5%): 하락 추세 포착 못함
- **-1%**: 균형점

### 4.3 시장 국면 3단계 진단 시스템 v5.4

**문제**: 개별 종목이 완벽해도 시장 전체가 폭락장이면 성공 확률 급락

```python
def check_market_regime():
    """
    SPY + VIX 기반 3단계 진단

    진단 기준 (우선순위 순):
    🔴 RED: SPY < MA50 OR VIX >= 30
    🟡 YELLOW: SPY < MA20 OR VIX >= 20
    🟢 GREEN: 위 조건에 해당하지 않음

    Returns:
        dict: {
            'regime': 'RED' | 'YELLOW' | 'GREEN',
            'reason': str,
            'spy_price': float,
            'spy_ma20': float,
            'spy_ma50': float,
            'vix': float
        }
    """
```

**3단계 대응**:

| 항목 | 🟢 GREEN | 🟡 YELLOW | 🔴 RED |
|------|----------|-----------|--------|
| 조건 | 정상 | SPY<MA20 OR VIX>=20 | SPY<MA50 OR VIX>=30 |
| Score | >= 4.0 | >= 6.0 | 스크리닝 중단 |
| PEG | < 2.0 | < 1.5 | - |
| 액션 | 적극 매매 | 신중 매매 | Cash is King |
| 텔레그램 | 상승장 | 경계 모드 | 경고만 전송 |

### 4.4 v5.3 스크리닝 필터 파이프라인

```
[FILTER 0] 시장 국면 체크 (v5.3)
    │
    ├── SPY > MA200: 🟢 BULL → 기본 필터
    └── SPY < MA200: 🔴 BEAR → 필터 강화 (Score 6.0, PEG 1.5)
    │
    ▼
917개 유니버스
    │
    ▼ [FILTER 1] EPS 데이터 존재
    │  제외: ~100개 (no_eps)
    │
    ▼ [FILTER 2] Kill Switch (7d -1%)
    │  제외: ~100개 (killed)
    │
    ▼ [FILTER 3] Score >= 4.0 (하락장: 6.0)
    │  제외: ~400개 (low_score)
    │
    ▼ [FILTER 4] Dollar Volume >= $20M
    │  제외: ~50개 (low_volume)
    │
    ▼ [FILTER 5] Price > MA200
    │  제외: ~50개 (below_ma200)
    │
    ▼ [FILTER 6] Earnings Blackout (D-5~D+1)
    │  제외: ~20개 (earnings_blackout)
    │
    ▼ [FILTER 7] Quality & Value (OR)
    │  A. Quality Growth: 매출↑5% & 영업↑>=매출
    │  B. Reasonable Value: PEG < 2.0 (하락장: 1.5)
    │  C. Technical Rescue: Price > MA60
    │  제외: ~30개 (no_quality_value)
    │
    ▼
~70개 통과 (하락장: 더 적음)
```

### 4.4 액션 분류 알고리즘 (v6.3 - RSI Momentum Strategy)

```python
def get_action_label(price, ma_20, ma_200, rsi, from_52w_high, volume_spike=False):
    """
    v6.3 RSI Momentum Strategy 기반 액션 분류

    핵심 철학:
    - RSI 70 이상을 무조건 진입금지로 처리하지 않음
    - 신고가 돌파 + 거래량 동반 = Super Momentum (🚀강력매수)
    """
```

**v6.3 액션 우선순위**:
```
1. 추세이탈: Price < MA200 → 즉시 제외 (×0.1)
2. 극과열: RSI >= 85 → 진입금지 (×0.3)
3. RSI 70-84 구간 (Super Momentum 조건부):
   - 신고가근처(-5%) + 거래량스파이크 → 🚀강력매수 (×1.1)
   - 신고가근처(-5%) → 관망(RSI🚀고점) (×0.75)
   - 기타 → 관망(RSI🚀) (×0.75)
4. 단기급등: MA20 +8% 이상 → 진입금지 (×0.3)
5. 저점매수: RSI <= 35 & 52주高 -20% 이상 → ×1.0
6. 적극매수: 52주高 -10%~-25% & RSI 35-55 → ×1.0
7. 매수적기: 정배열 & RSI 40-65 → ×0.9
8. 관망: 기타 → ×0.7
```

**거래량 스파이크 감지**:
```python
volume_spike = False
if len(hist_1m) >= 20:
    vol_avg_20 = hist_1m['Volume'].tail(20).mean()
    vol_recent_3 = hist_1m['Volume'].tail(3)
    if any(vol_recent_3 > vol_avg_20 * 1.5):
        volume_spike = True
```

---

## 5. v6.3 Quality & Value Scorecard

### 5.1 핵심 개념

v6.3에서는 종목을 **맛(Quality)**과 **값(Value)**으로 분리 평가:

| 점수 | 의미 | 100점 만점 |
|------|------|------------|
| **Quality Score** | "이 사과가 맛있는가?" | EPS 모멘텀, ROE, 성장률, 추세, 수급 |
| **Value Score** | "지금 가격이 적절한가?" | PEG, PER, 52주 고점대비, RSI 눌림목 |

### 5.2 Quality Score (맛, 100점)

```python
def calculate_quality_score(is_aligned, roe, eps_chg, above_ma200, volume_spike, momentum_score=None):
    score = 0

    # v6.3.2: score_321 직접 활용 (정배열 보너스 중복 제거)
    if momentum_score is not None and momentum_score > 0:
        score += min(30, momentum_score)  # 최대 30점

    # ROE 품질 (25점)
    if roe and roe >= 0.30:
        score += 25
    elif roe and roe >= 0.20:
        score += 20
    elif roe and roe >= 0.10:
        score += 15

    # EPS 성장률 (20점)
    if eps_chg and eps_chg >= 20:
        score += 20
    elif eps_chg and eps_chg >= 10:
        score += 15
    elif eps_chg and eps_chg >= 5:
        score += 10

    # 추세 (15점)
    if above_ma200:
        score += 15

    # 수급 (10점)
    if volume_spike:
        score += 10

    return score
```

### 5.3 Value Score (값, 100점)

```python
def calculate_value_score(peg, fwd_per, from_52w_high, rsi):
    score = 0

    # PEG 평가 (35점)
    if peg and peg < 1.0:
        score += 35
    elif peg and peg < 1.5:
        score += 28
    elif peg and peg < 2.0:
        score += 20

    # Forward PER (25점)
    if fwd_per and fwd_per < 15:
        score += 25
    elif fwd_per and fwd_per < 25:
        score += 20
    elif fwd_per and fwd_per < 40:
        score += 12

    # 52주 고점대비 (25점) - 떨어진 것 = 세일
    if from_52w_high and from_52w_high <= -25:
        score += 25
    elif from_52w_high and from_52w_high <= -15:
        score += 20
    elif from_52w_high and from_52w_high <= -10:
        score += 15

    # RSI 눌림목 (15점)
    if rsi and 30 <= rsi <= 45:
        score += 15
    elif rsi and 45 < rsi <= 55:
        score += 10

    return score
```

### 5.4 Actionable Score 공식

```python
actionable_score = (quality_score * 0.5 + value_score * 0.5) * action_multiplier
```

### 5.5 Action Multiplier

| Action | Multiplier | 조건 |
|--------|------------|------|
| 🚀강력매수 (돌파) | ×1.1 | RSI 70-84 + 신고가 + 거래량 |
| 적극매수/저점매수 | ×1.0 | 눌림목/과매도 |
| 매수적기 | ×0.9 | 건강한 추세 |
| 관망 (RSI🚀) | ×0.75 | RSI 70-84 (거래량 미동반) |
| 관망 | ×0.7 | 진입 애매 |
| 진입금지 | ×0.3 | RSI 85+ / 단기급등 |
| 추세이탈 | ×0.1 | MA200 하회 |

### 5.6 score_321 계산 (Quality Score의 EPS 모멘텀)

```python
def calculate_momentum_score_v3(current, d7, d30, d60):
    score = 0

    # 가중치 기반 (최근일수록 높은 점수)
    if current > d7:   score += 3  # 최신
    if d7 > d30:       score += 2
    if d30 > d60:      score += 1

    # 역전 페널티
    if d7 < d30:       score -= 1
    if d30 < d60:      score -= 1

    # 60일 변화율 보너스
    eps_chg_60d = (current - d60) / d60 * 100 if d60 else 0
    score += eps_chg_60d / 5

    # 정배열 보너스
    if current > d7 > d30 > d60:
        score += 3  # 완전 정배열
    elif current > d7 > d30:
        score += 1  # 부분 정배열

    return score
```

### 5.7 v6.3.2 중복 제거 히스토리

**v6.3.1 문제점:**
```python
# 정배열 여부로 20점 + 모멘텀 강도로 10점 = 중복
if is_aligned:
    score += 20  # 정배열 보너스
    score += min(10, momentum_score)  # 모멘텀 강도
```

**v6.3.2 해결:**
```python
# score_321에 이미 정배열 보너스 포함되어 있으므로 직접 활용
score += min(30, momentum_score)
```

---

## 6. v7.0 신규 기능

### 6.1 Super Momentum Override

Quality >= 80 + RSI 70-85 조건 충족시 자동으로 "🚀슈퍼모멘텀" 액션 부여:

```python
def super_momentum_override(quality_score, rsi, action, config):
    """Quality >= 80 AND RSI 70-85 → 무조건 슈퍼모멘텀"""
    sm_config = config.get('super_momentum', {})
    if not sm_config.get('enabled', True):
        return action, False

    threshold = sm_config.get('quality_threshold', 80)
    rsi_min = sm_config.get('rsi_min', 70)
    rsi_max = sm_config.get('rsi_max', 85)

    if quality_score >= threshold and rsi_min <= rsi < rsi_max:
        return "🚀슈퍼모멘텀", True
    return action, False
```

### 6.2 Exit Strategy (ATR 손절가 + 추세 이탈)

동적 손절가 계산 (ATR × 2):

```python
def calculate_atr(hist, period=14):
    """Average True Range 계산"""
    high = hist['High']
    low = hist['Low']
    close = hist['Close'].shift(1)
    tr = pd.concat([
        high - low,
        (high - close).abs(),
        (low - close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean().iloc[-1]

def calculate_stop_loss(price, atr, multiplier=2.0):
    """손절가 = Close - (ATR × multiplier)"""
    return price - (atr * multiplier)
```

추세 이탈 감지:
- Track A (Momentum): MA50 이탈시 경고
- Track B (Dip Buy): MA20 이탈시 경고

### 6.3 Forward Fill (EPS 결측치 보정)

EPS 7d/30d/60d가 NaN일 경우 Current 값으로 채움:

```python
def forward_fill_eps(current, d7, d30, d60):
    """EPS 결측치를 Current로 채움"""
    filled_7d = d7 if pd.notna(d7) else current
    filled_30d = d30 if pd.notna(d30) else current
    filled_60d = d60 if pd.notna(d60) else current
    was_filled = pd.isna(d7) or pd.isna(d30) or pd.isna(d60)
    return filled_7d, filled_30d, filled_60d, was_filled
```

### 6.4 Sector Booster (ETF 추천)

TOP 10 중 동일 섹터 3개 이상 → 섹터 ETF 추천:

```python
SECTOR_ETF = {
    'Semiconductor': {'1x': 'SMH', '3x': 'SOXL'},
    'Technology': {'1x': 'XLK', '3x': 'TECL'},
    'Healthcare': {'1x': 'XLV', '3x': 'LABU'},
    # ...
}

def get_sector_etf_recommendation(screening_df, top_n=10, min_count=3):
    """섹터 집중시 ETF 추천"""
    sector_counts = screening_df.head(top_n)['sector'].value_counts()
    recommendations = []
    for sector, count in sector_counts.items():
        if count >= min_count and sector in SECTOR_ETF:
            recommendations.append({
                'sector': sector,
                'count': count,
                'etf_1x': SECTOR_ETF[sector]['1x'],
                'etf_3x': SECTOR_ETF[sector].get('3x')
            })
    return recommendations
```

### 6.5 Config 분리

하드코딩된 값들을 config.json으로 외부화:

```json
{
  "action_multipliers": {
    "돌파매수": 1.1, "슈퍼모멘텀": 1.1,
    "적극매수": 1.0, "저점매수": 1.0, "분할매수": 1.0,
    "매수적기": 0.9, "RSI관망": 0.75, "관망": 0.7,
    "진입금지": 0.3, "추세이탈": 0.1
  },
  "exit_strategy": {
    "atr_period": 14, "atr_multiplier": 2.0,
    "track_a_ma": 50, "track_b_ma": 20
  },
  "super_momentum": {
    "enabled": true, "quality_threshold": 80,
    "rsi_min": 70, "rsi_max": 85
  },
  "sector_booster": {
    "enabled": true, "min_sector_count": 3, "top_n": 10
  },
  "telegram_format": {
    "top_n": 10, "watchlist_max": 25
  }
}
```

### 6.6 DB 스키마 확장

신규 컬럼 6개:

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `atr` | REAL | ATR(14) |
| `stop_loss` | REAL | 손절가 |
| `action_type` | TEXT | 액션 분류 |
| `industry` | TEXT | 업종 |
| `is_filled` | INTEGER | Forward Fill 적용 여부 |
| `ma_50` | REAL | 50일 이평선 |

### 6.7 텔레그램 템플릿 v7.0

```
🇺🇸 미국주식 퀀트 랭킹 v7.0
━━━━━━━━━━━━━━━━━━━━━━
📅 {Date} 마감 | 총 {Count}개 통과
📋 전략: EPS Growth + RSI Dual Track

🔥 [HOT] 섹터 포착: {Sector}
👉 ETF 추천: {ETF_1x} / {ETF_3x}
━━━━━━━━━━━━━━━━━━━━━━

🏆 TOP 10 추천주

🥇 {Name} ({Ticker}) ${Price}
   [{Action}] 종합점수: {Score}점
   • 📊매수근거: EPS↗ + RSI {RSI}
   • 🍎맛: {Q}점 | 💰값: {V}점
   • 📉대응: 손절가 ${Stop_Loss} (ATR×2)
   • {Sector} | 고점{High}%
   💡 {Rationale}

━━━━━━━━━━━━━━━━━━━━━━
📋 관심 종목 (11~25위)
11. {Ticker} | {Name} | {Score}점
...

━━━━━━━━━━━━━━━━━━━━━━
🚨 보유 종목 긴급 점검 (Sell Signal)
🔻 {Ticker}: 펀더멘털 훼손 (EPS -1% 하향)
🔻 {Ticker}: 기술적 이탈 (MA{20/50} 붕괴)
```

---

## 7. 데이터 흐름

### 7.1 일일 실행 플로우

```
07:00 KST (미장 마감 후)
    │
    ▼
[1] config.json 로드
    │
    ▼
[1.5] check_market_regime() (v5.3)
    ├── SPY MA200 체크
    ├── BULL/BEAR 판단
    └── 하락장시 필터 강화 설정
    │
    ▼
[2] Track 1: run_screening(market_regime)
    ├── Yahoo Finance API 호출 (917개)
    ├── 8개 필터 적용 (시장국면 포함)
    ├── 액션 분류
    └── screening_YYYY-MM-DD.csv 저장
    │
    ▼
[3] Track 2: run_data_collection()
    ├── 전 종목 데이터 수집
    └── SQLite DB 저장 (Point-in-Time)
    │
    ▼
[4] generate_report()
    ├── report_YYYY-MM-DD.html
    └── report_YYYY-MM-DD.md
    │
    ▼
[5] get_portfolio_changes()
    ├── 전일 대비 편입/편출 계산
    │
    ▼
[6] create_telegram_message()
    ├── 액션별 그룹화
    └── 텔레그램 전송
    │
    ▼
[7] git_commit_push()
    └── 자동 커밋/푸시
```

### 6.2 데이터 소스

| 데이터 | 소스 | API |
|--------|------|-----|
| EPS Trend | Yahoo Finance | `stock.eps_trend` |
| 가격/거래량 | Yahoo Finance | `stock.history()` |
| 재무제표 | Yahoo Finance | `stock.quarterly_financials` |
| 기업 정보 | Yahoo Finance | `stock.info` |
| 실적발표일 | Yahoo Finance | `stock.calendar` |

### 6.3 출력 파일

```
eps_data/
└── screening_2026-02-01.csv
    ├── ticker, index, score_321, score_slope
    ├── eps_chg_60d, peg, price, ma_20, ma_200
    ├── rsi, dollar_vol_M, sector
    ├── is_aligned, is_quality_growth, is_reasonable_value
    ├── pass_reason, from_52w_high, action
    └── ... (25개 컬럼)

reports/
├── report_2026-02-01.html  # 웹 리포트
└── report_2026-02-01.md    # 마크다운 리포트

logs/
└── daily_20260201.log      # 실행 로그
```

---

## 8. 모듈별 상세

### 7.1 daily_runner.py

**주요 함수**:

| 함수 | 역할 | 줄 수 |
|------|------|-------|
| `run_screening()` | Track 1 메인 | 250줄 |
| `get_action_label()` | 액션 분류 v5.1 | 100줄 |
| `run_data_collection()` | Track 2 메인 | 190줄 |
| `create_telegram_message()` | 텔레그램 v5.2 | 210줄 |
| `analyze_fundamentals()` | 펀더멘털 분석 | 90줄 |
| `analyze_technical()` | 기술적 분석 | 80줄 |

### 7.2 eps_momentum_system.py

**주요 상수**:

```python
INDICES = {
    'NASDAQ_100': [...],  # 101개
    'SP500': [...],       # 503개
    'SP400_MidCap': [...] # 400개
}  # 중복 제거 후 917개

SECTOR_MAP = {
    'NVDA': 'Semiconductor',
    'AAPL': 'Tech',
    ...
}
```

**주요 함수**:

| 함수 | 역할 |
|------|------|
| `calculate_momentum_score_v3()` | 스코어링 (가중치+정배열) |
| `calculate_slope_score()` | A/B 테스트용 스코어 |
| `check_technical_filter()` | MA20 필터 (레거시) |
| `get_peg_ratio()` | PEG 계산 |

### 7.3 sector_analysis.py

**ETF 매핑**:

```python
SECTOR_ETF = {
    'Technology': {'1x': 'XLK', '3x': 'TECL'},
    'Semiconductor': {'1x': 'SMH', '3x': 'SOXL'},
    ...
}

THEME_ETF = {
    'Semiconductors': {'1x': 'SMH', '3x': 'SOXL'},
    'Gold': {'1x': 'GDX', '3x': 'NUGT'},
    ...
}
```

---

## 9. 설정 가이드

### 8.1 config.json 상세

```json
{
  "python_path": "C:\\...\\python.exe",  // Python 경로

  "git_enabled": true,           // Git 자동 커밋
  "git_remote": "origin",
  "git_branch": "master",

  "telegram_enabled": true,      // 텔레그램 알림
  "telegram_bot_token": "...",   // BotFather에서 발급
  "telegram_chat_id": "...",     // @userinfobot으로 확인

  "run_time": "07:00",           // 실행 시간 (참고용)

  "indices": [                   // 스크리닝 대상 지수
    "NASDAQ_100",
    "SP500",
    "SP400_MidCap"
  ],

  "min_score": 4.0,              // 최소 모멘텀 점수
  "kill_switch_threshold": -0.01, // Kill Switch (-1%)
  "earnings_blackout_days": 5    // 실적발표 블랙아웃
}
```

### 8.2 텔레그램 봇 설정

1. **봇 생성**: @BotFather → `/newbot`
2. **토큰 획득**: 생성 후 토큰 복사
3. **Chat ID 확인**: @userinfobot에 메시지 → ID 확인
4. **config.json 설정**:
   ```json
   "telegram_bot_token": "123456:ABC-DEF...",
   "telegram_chat_id": "7580571403"
   ```

### 8.3 필터 조정

**더 엄격하게**:
```json
"min_score": 6.0,
"kill_switch_threshold": -0.005  // -0.5%
```

**더 느슨하게**:
```json
"min_score": 3.0,
"kill_switch_threshold": -0.02  // -2%
```

---

## 10. 설치 및 실행

### 9.1 요구사항

```
Python 3.8+
패키지: yfinance, pandas, numpy
```

### 9.2 설치

```bash
# 클론
git clone https://github.com/VolumeQuant/eps-momentum-us.git
cd eps-momentum-us

# 패키지 설치
pip install yfinance pandas numpy

# 설정 파일 수정
# config.json에서 텔레그램 토큰/Chat ID 설정
```

### 9.3 실행

```bash
# 수동 실행
python daily_runner.py

# Windows 스케줄러 등록
schtasks /create /tn "EPS_Momentum_Daily" /tr "C:\...\run_daily.bat" /sc daily /st 07:00
```

### 9.4 개별 모듈 실행

```bash
# 스크리닝만
python eps_momentum_system.py screen

# 데이터 축적만
python eps_momentum_system.py collect

# 축적 현황 확인
python eps_momentum_system.py stats
```

---

## 11. 백테스팅 설계

### 10.1 Point-in-Time 원칙

```
❌ Look-Ahead Bias
"2026-01-31 시점에 2026-02-01 데이터 사용"

✅ Point-in-Time
"2026-01-31 시점에 그 날 사용 가능한 데이터만 사용"
```

**Track 2 설계**:
- 매일 전 종목(917개) 데이터 저장
- 스크리닝 통과 여부와 관계없이 저장
- Survivorship Bias 방지

### 10.2 DB 스키마

```sql
CREATE TABLE eps_snapshots (
    id INTEGER PRIMARY KEY,
    date TEXT NOT NULL,           -- 스냅샷 날짜
    ticker TEXT NOT NULL,
    index_name TEXT,

    -- EPS 데이터
    eps_current REAL,
    eps_7d REAL,
    eps_30d REAL,
    eps_60d REAL,
    eps_90d REAL,

    -- 가격 데이터
    price REAL,
    ma_20 REAL,
    ma_200 REAL,
    dollar_volume REAL,

    -- 스코어
    score_321 REAL,
    score_slope REAL,

    -- 플래그
    passed_screen INTEGER,        -- 스크리닝 통과 여부
    is_aligned INTEGER,           -- 정배열 여부

    -- 펀더멘털
    peg REAL,
    from_52w_high REAL,
    rsi REAL,
    rev_growth_yoy REAL,
    op_growth_yoy REAL,

    UNIQUE(date, ticker)
);
```

### 10.3 백테스트 쿼리 예시

```sql
-- 특정 날짜 스크리닝 통과 종목
SELECT ticker, score_321, eps_chg_60d
FROM eps_snapshots
WHERE date = '2026-01-31' AND passed_screen = 1
ORDER BY score_321 DESC;

-- 정배열 종목의 평균 성과 (6개월 후)
SELECT
    a.ticker,
    a.price as entry_price,
    b.price as exit_price,
    (b.price - a.price) / a.price * 100 as return_pct
FROM eps_snapshots a
JOIN eps_snapshots b ON a.ticker = b.ticker
WHERE a.date = '2026-01-31'
  AND b.date = '2026-07-31'
  AND a.is_aligned = 1;
```

### 10.4 A/B 테스트 설계

```
Score_321 (가중치 방식)
├── 기본 점수: 3-2-1 가중치
├── 정배열 보너스: +3점
└── 변화율 보너스: %/5

Score_Slope (변화율 가중 평균)
├── Δ7d × 0.5
├── Δ30d × 0.3
└── Δ60d × 0.2
```

**검증 방법**: 6개월 데이터 축적 후 두 스코어의 예측력 비교

---

## 12. 트러블슈팅

### 11.1 일반적인 오류

| 오류 | 원인 | 해결 |
|------|------|------|
| `No EPS data` | Yahoo Finance 데이터 없음 | 정상 (일부 종목 제외) |
| `Rate limit` | API 호출 제한 | 1일 1회 실행 권장 |
| `Telegram error` | 토큰/Chat ID 오류 | config.json 확인 |
| `Git push failed` | 인증 오류 | Git credential 확인 |

### 11.2 데이터 품질 이슈

```
문제: 일부 종목 재무 데이터 누락
해결: Technical Rescue (Price > MA60 시 통과)

문제: EPS 데이터 이상치 (200% 이상 변화)
해결: 이상치 필터 (-80% ~ +200%)

문제: 52주 고점 데이터 없음
해결: 기본값 -10% 가정
```

### 11.3 성능 최적화

```
현재 실행 시간: ~15분 (917개 종목)

최적화 방법:
1. 병렬 처리 (yfinance 제한으로 어려움)
2. 캐싱 (일부 데이터 재사용)
3. 종목 수 축소 (NASDAQ100만 사용)
```

---

## 부록: 용어 정의

| 용어 | 정의 |
|------|------|
| **EPS Revision** | 애널리스트 EPS 전망치 변화 |
| **Forward EPS** | 향후 1년 예상 EPS |
| **EPS 정배열** | EPS 전망치 Current > 7d > 30d > 60d (지속적 상향) |
| **MA 정배열** | 가격 > MA20 > MA200 (상승 추세) |
| **Kill Switch** | 모멘텀 꺾임 감지 장치 |
| **MA200** | 200일 이동평균선 |
| **PEG** | P/E ÷ 성장률 |
| **Point-in-Time** | 특정 시점 사용 가능 데이터만 사용 |
| **Survivorship Bias** | 생존 종목만 분석하는 오류 |

---

*문서 버전: v6.3.3 | 최종 업데이트: 2026-02-03*
