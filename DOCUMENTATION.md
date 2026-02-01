# EPS Revision Momentum Strategy - 상세 기술 문서

## 목차

1. [프로젝트 개요](#1-프로젝트-개요)
2. [전략 철학](#2-전략-철학)
3. [시스템 아키텍처](#3-시스템-아키텍처)
4. [핵심 알고리즘](#4-핵심-알고리즘)
5. [데이터 흐름](#5-데이터-흐름)
6. [모듈별 상세](#6-모듈별-상세)
7. [설정 가이드](#7-설정-가이드)
8. [설치 및 실행](#8-설치-및-실행)
9. [백테스팅 설계](#9-백테스팅-설계)
10. [트러블슈팅](#10-트러블슈팅)

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

### 4.3 시장 국면 (Market Regime) 필터 v5.3

**문제**: 개별 종목이 완벽해도 시장 전체가 폭락장이면 성공 확률 급락

```python
def check_market_regime():
    """
    SPY(S&P 500 ETF)의 MA200 위치 체크

    Returns:
        'BULL': SPY > MA200 (상승장)
        'BEAR': SPY < MA200 (하락장)
    """
```

**하락장 대응**:

| 항목 | 상승장 (BULL) | 하락장 (BEAR) |
|------|---------------|---------------|
| Score 기준 | >= 4.0 | >= 6.0 (강화) |
| PEG 기준 | < 2.0 | < 1.5 (강화) |
| 텔레그램 | 🟢 상승 추세 | 🚨 시장 경보 |
| 권장 | 정상 매매 | 현금 비중 확대 |

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

### 4.4 액션 분류 알고리즘 (v5.1)

```python
def get_action_label(price, ma_20, ma_200, rsi, from_52w_high):
    """
    52주 고점 대비 위치 기반 실전 매매 액션

    핵심 원칙:
    1. 고점 근처(-5% 이내)는 상승여력 제한 → 진입금지
    2. 진짜 눌림목 = 충분한 조정(-10%~-25%) + RSI 중립
    3. RSI만으로 판단하지 않음
    """
```

**액션 우선순위**:
```
1. 추세이탈: Price < MA200 → 즉시 제외
2. 진입금지:
   - RSI >= 70 (과열)
   - 52주高 -5% 이내 (고점근처)
   - MA20 +8% 이상 (단기급등)
3. 저점매수: RSI <= 35 & 52주高 -20% 이상
4. 적극매수: 52주高 -10%~-25% & RSI 35-55 & MA20 근처
5. 매수적기: 정배열 & RSI 40-65 & 52주高 -5%~-15%
6. 관망: 기타
```

---

## 5. 데이터 흐름

### 5.1 일일 실행 플로우

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

### 5.2 데이터 소스

| 데이터 | 소스 | API |
|--------|------|-----|
| EPS Trend | Yahoo Finance | `stock.eps_trend` |
| 가격/거래량 | Yahoo Finance | `stock.history()` |
| 재무제표 | Yahoo Finance | `stock.quarterly_financials` |
| 기업 정보 | Yahoo Finance | `stock.info` |
| 실적발표일 | Yahoo Finance | `stock.calendar` |

### 5.3 출력 파일

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

## 6. 모듈별 상세

### 6.1 daily_runner.py

**주요 함수**:

| 함수 | 역할 | 줄 수 |
|------|------|-------|
| `run_screening()` | Track 1 메인 | 250줄 |
| `get_action_label()` | 액션 분류 v5.1 | 100줄 |
| `run_data_collection()` | Track 2 메인 | 190줄 |
| `create_telegram_message()` | 텔레그램 v5.2 | 210줄 |
| `analyze_fundamentals()` | 펀더멘털 분석 | 90줄 |
| `analyze_technical()` | 기술적 분석 | 80줄 |

### 6.2 eps_momentum_system.py

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

### 6.3 sector_analysis.py

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

## 7. 설정 가이드

### 7.1 config.json 상세

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

### 7.2 텔레그램 봇 설정

1. **봇 생성**: @BotFather → `/newbot`
2. **토큰 획득**: 생성 후 토큰 복사
3. **Chat ID 확인**: @userinfobot에 메시지 → ID 확인
4. **config.json 설정**:
   ```json
   "telegram_bot_token": "123456:ABC-DEF...",
   "telegram_chat_id": "7580571403"
   ```

### 7.3 필터 조정

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

## 8. 설치 및 실행

### 8.1 요구사항

```
Python 3.8+
패키지: yfinance, pandas, numpy
```

### 8.2 설치

```bash
# 클론
git clone https://github.com/VolumeQuant/eps-momentum-us.git
cd eps-momentum-us

# 패키지 설치
pip install yfinance pandas numpy

# 설정 파일 수정
# config.json에서 텔레그램 토큰/Chat ID 설정
```

### 8.3 실행

```bash
# 수동 실행
python daily_runner.py

# Windows 스케줄러 등록
schtasks /create /tn "EPS_Momentum_Daily" /tr "C:\...\run_daily.bat" /sc daily /st 07:00
```

### 8.4 개별 모듈 실행

```bash
# 스크리닝만
python eps_momentum_system.py screen

# 데이터 축적만
python eps_momentum_system.py collect

# 축적 현황 확인
python eps_momentum_system.py stats
```

---

## 9. 백테스팅 설계

### 9.1 Point-in-Time 원칙

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

### 9.2 DB 스키마

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

### 9.3 백테스트 쿼리 예시

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

### 9.4 A/B 테스트 설계

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

## 10. 트러블슈팅

### 10.1 일반적인 오류

| 오류 | 원인 | 해결 |
|------|------|------|
| `No EPS data` | Yahoo Finance 데이터 없음 | 정상 (일부 종목 제외) |
| `Rate limit` | API 호출 제한 | 1일 1회 실행 권장 |
| `Telegram error` | 토큰/Chat ID 오류 | config.json 확인 |
| `Git push failed` | 인증 오류 | Git credential 확인 |

### 10.2 데이터 품질 이슈

```
문제: 일부 종목 재무 데이터 누락
해결: Technical Rescue (Price > MA60 시 통과)

문제: EPS 데이터 이상치 (200% 이상 변화)
해결: 이상치 필터 (-80% ~ +200%)

문제: 52주 고점 데이터 없음
해결: 기본값 -10% 가정
```

### 10.3 성능 최적화

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
| **정배열** | Current > 7d > 30d > 60d |
| **Kill Switch** | 모멘텀 꺾임 감지 장치 |
| **MA200** | 200일 이동평균선 |
| **PEG** | P/E ÷ 성장률 |
| **Point-in-Time** | 특정 시점 사용 가능 데이터만 사용 |
| **Survivorship Bias** | 생존 종목만 분석하는 오류 |

---

*문서 버전: v5.3 | 최종 업데이트: 2026-02-02*
