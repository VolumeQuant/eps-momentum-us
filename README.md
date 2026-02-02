# EPS Revision Momentum Strategy v6.1 (US Stocks)

미국 주식 대상 **Value-Momentum Hybrid** 전략 시스템

> 🍎💰 **핵심 철학**: "좋은 사과(A등급)를 싸게 사는 것이 최고 사과(S등급)를 비싸게 사는 것보다 낫다"

## 버전 히스토리

| 버전 | 날짜 | 주요 변경 |
|------|------|----------|
| **v6.1** | 2026-02-03 | **Option A**: 가격위치(Position) 점수 추가 - 52주 고점 대비 조정폭 반영 |
| v6.0 | 2026-02-02 | Value-Momentum Hybrid System: 3-Layer Filtering + Hybrid Ranking |
| v5.4 | 2026-02-02 | 시장 국면 3단계 진단 (RED/YELLOW/GREEN) + VIX 추가 |
| v5.3 | 2026-02-02 | 시장 국면(Market Regime) 필터 추가 - SPY MA200 기반 |
| v5.2 | 2026-02-01 | 텔레그램 메시지 액션별 그룹화, 메시지 길이 63% 감소 |
| v5.1 | 2026-02-01 | 액션 로직 전면 개편 - 52주 고점 기반 판단 |
| v5 | 2026-02-01 | MA200 필터 + Quality/Value 조건 + 인덱스 917개 확장 |
| v4 | 2026-01-31 | 저평가/성장 필터, 정배열 보너스, 펀더멘털 분석 추가 |
| v3 | 2026-01-31 | 기술적 분석 (RSI, 200일선), 상세 텔레그램 포맷 |
| v2 | 2026-01-30 | Kill Switch, 거래대금 필터, 가중치 스코어링 |
| v1 | 2026-01-30 | 초기 버전 |

---

## v6.0 Value-Momentum Hybrid System

### 핵심 개념

기존 EPS 모멘텀 전략에 **밸류에이션(Forward PER)** 및 **품질(ROE)** 필터를 추가하여 "성장 + 가치" 복합 전략으로 업그레이드.

### 3-Layer Filtering

```
Layer 1 [Momentum]: EPS Trend Alignment
├── Kill Switch: 7일 대비 -1% 이상 하락시 탈락
├── Score >= 4.0 (YELLOW 시장: >= 6.0)
└── EPS 정배열: Current > 7d > 30d

Layer 2 [Quality]: ROE > 10%
├── 저품질 성장 필터링
└── 예외: ROE 데이터 없으면 통과 (Technical Rescue 대상)

Layer 3 [Safety]: Forward PER < 60
├── 버블 종목 제외
└── 예외: 고모멘텀(Score >= 8) 시 PER 80까지 허용
```

### Hybrid Ranking (v6.1 - Option A)

```python
Hybrid Score = (Momentum × 0.5) + ((100 / PER) × 0.2) + (Position × 0.3)
```

**Position Score**: 52주 고점 대비 가격 위치
```python
Position Score = 100 - (현재가 / 52주고점 × 100)
# 범위: 0~50
```

**목표**: "A등급 싸게 사기" - 조정받은 좋은 종목 상위 랭크

**예시**:
| 종목 | Momentum | PER | 52w고점대비 | Position | Hybrid Score |
|------|----------|-----|-------------|----------|--------------|
| S급비싼 | 32 | 10 | -5% | 5 | 32×0.5 + 10×0.2 + 5×0.3 = **19.5** |
| A급싼 | 25 | 15 | -20% | 20 | 25×0.5 + 6.7×0.2 + 20×0.3 = **19.8** ✅ |

→ **조정받은 A급 종목이 고점 근처 S급 종목보다 상위 랭크**

### v6.0 공식 (이전)

```python
# v6.0 (참고용)
Hybrid Score = (Momentum × 0.7) + ((100 / Forward PER) × 0.3)
```

### 신규 지표 (v6.1)

| 지표 | 계산식 | 용도 |
|------|--------|------|
| Forward PER | 현재가 / Forward EPS | 밸류에이션 |
| ROE | `ticker.info['returnOnEquity']` | 품질 필터 |
| PEG (계산) | Forward PER / EPS 성장률(%) | 성장 대비 가치 |
| **Position Score** | 100 - (현재가/52주고점×100) | **가격 위치 (v6.1)** |
| Hybrid Score | M×0.5 + V×0.2 + P×0.3 | 최종 랭킹 (v6.1) |

### 텔레그램 메시지 분리

**User Briefing (Track 1)**:
```
🏆 TOP 3 PICKS
─────────────────────
🥇 AVGO $331
   Hybrid: 8.5 | 모멘텀: 11.7⬆
   PER 23 | ROE 52% | 반도체
   💡 EPS 전망치 완전 정배열, PER 23배 적정

🥈 NEM $112
   Hybrid: 8.2 | 모멘텀: 11.7⬆
   PER 13 | ROE 18% | 소재
   💡 EPS 전망 +14% 상향, PER 13배 저평가
```

**Admin Log (Track 2)**:
```
🔧 [02/02] EPS v6.0 Admin Log
━━━━━━━━━━━━━━━━━━━━━━
📊 Track 2 (Data Collection)
Status: ✅ SUCCESS
• 수집: 845개 종목
• 실행시간: 423.5초

📈 Track 1 (Screening) 통계
• 총 스캔: 917개
• ROE < 10%: 127개
• PER > 60: 45개
```

### DB 스키마 (v6 신규 필드)

```sql
fwd_per REAL,        -- Forward PER
roe REAL,            -- ROE (0~1 범위)
peg_calculated REAL, -- 직접 계산된 PEG
hybrid_score REAL,   -- 하이브리드 점수
```

---

## 전략 개요 (v5.4 이전)

**핵심 아이디어**: 애널리스트들의 Forward EPS 컨센서스 상향 조정 + 기술적/펀더멘털 복합 필터

### v5.3 스크리닝 필터

```
0. 시장 국면 체크 (v5.3 신규)
   ├ SPY > MA200: 🟢 상승장 → 기본 필터 적용
   └ SPY < MA200: 🔴 하락장 → 필터 2배 강화
      ├ Score 4.0 → 6.0
      └ PEG 2.0 → 1.5

1. 인덱스 유니버스 (917개)
   ├ NASDAQ100: 101개
   ├ S&P500: 503개
   └ S&P400: 400개 (중복 제거 후 917개)

2. EPS 모멘텀 (필수)
   ├ Kill Switch: 7일 대비 -1% 이상 하락시 제외
   ├ 모멘텀 점수 >= 4.0 (하락장: >= 6.0)
   ├ 거래대금 >= $20M
   └ 실적발표 D-5~D+1 제외

3. 기술적 필터 (필수)
   └ Price > MA200 (장기 상승 추세 필수)

4. Quality & Value Filter (OR 조건)
   ├ A. Quality Growth: 매출 >= 5% AND 영업익 >= 매출증가율
   ├ B. Reasonable Value: PEG < 2.0 (하락장: < 1.5)
   └ C. Technical Rescue: 데이터 없으면 Price > MA60
```

### 시장 국면 3단계 진단 시스템 v5.4

개별 종목이 완벽해도 **시장 전체가 하락장(Bear Market)**이면 성공 확률이 급격히 떨어집니다.

**문제**: 2008년, 2022년 같은 하락장에서도 "매수 신호"가 발생
**해결**: SPY + VIX 기반 3단계 진단 시스템

```
진단 기준 (우선순위 순):

🔴 RED (위험/매매중단)
├── 조건: SPY < MA50 OR VIX >= 30
├── 액션: 스크리닝 즉시 중단, 빈 리스트 반환
└── 텔레그램: "⛔ 오늘의 추천 종목 없음 (Cash is King)"

🟡 YELLOW (경계/기준강화)
├── 조건: SPY < MA20 OR VIX >= 20
├── 액션: 필터 강화 (Score 6.0, PEG 1.5)
└── 텔레그램: "⚠️ 경계 모드: 필터 강화 적용중"

🟢 GREEN (정상/적극매매)
├── 조건: 위 조건에 해당하지 않음
├── 액션: 기본 필터 (Score 4.0, PEG 2.0)
└── 텔레그램: "🟢 시장 상태: GREEN (상승장)"
```

**VIX (공포지수) 기준**:
- VIX < 20: 정상 (시장 안정)
- VIX 20-30: 경계 (변동성 확대)
- VIX >= 30: 공포 (매매 중단)

### 정배열 보너스

EPS가 꾸준히 상승하는 종목에 추가 점수:

```
정배열: Current > 7일 > 30일 > 60일
├ 완전 정배열: +3점 보너스
└ 부분 정배열: +1점 보너스
```

### v4 → v5 필터 비교

| 항목 | v4 (이전) | v5 (현재) |
|------|-----------|-----------|
| **기술적 필터** | Price > MA20 | Price > MA200 (더 엄격) |
| **펀더멘털 필터** | 저평가 OR 성장 (엄격) | Quality & Value (완화) |
| **저평가 조건** | PEG < 1 또는 52주高-10% | PEG < 2.0 (Reasonable Value) |
| **성장 조건** | 매출 +10% AND 영업 +5% | 매출 +5% AND 영업 >= 매출 (Quality Growth) |
| **데이터 없음** | 통과 (좋은 종목 놓치지 않음) | Technical Rescue (Price > MA60) |
| **유니버스** | 433개 | 917개 |

**핵심 변경점:**
- MA200 필터 추가로 **장기 하락 추세 종목 제외**
- 펀더멘털 조건 완화 → **MA200이 더 강력한 1차 필터 역할**
- Technical Rescue로 **데이터 없는 종목도 기술적 조건 충족시 통과**

---

## 액션 분류 (v5.1)

52주 고점 대비 위치를 핵심 기준으로 실전 매매용 액션 판단:

### 진입금지 조건 (하나라도 해당시)

| 조건 | 설명 |
|------|------|
| RSI >= 70 | 🚫 과열 |
| 52주 고점 -5% 이내 | 🚫 고점근처 |
| MA20 대비 +8% 이상 | 🚫 단기급등 |

### 매수 신호 조건

| 액션 | 조건 | 의미 |
|------|------|------|
| 🚀 적극매수 (눌림목) | 52주高 -10%~-25% + RSI 35-55 + MA20 근처 | 의미있는 조정, 진짜 기회 |
| 💎 저점매수 (과매도) | RSI <= 35 + 52주高 -20% 이상 | 과매도 반등 기회 |
| 🟢 매수적기 (추세) | 정배열 + RSI 40-65 + 52주高 -5%~-15% | 건강한 상승 추세 |

### 관망/이탈 조건

| 액션 | 조건 | 의미 |
|------|------|------|
| 👀 관망 (과열경계) | RSI 65-70 | 매수 대기 |
| 👀 관망 (조정부족) | 52주高 -5%~-10% | 추가 조정 대기 |
| 📉 추세이탈 | Price < MA200 | 매수 금지 |

---

## 텔레그램 메시지 포맷 (v5.2)

**액션별 그룹화**로 메시지 길이 63% 감소, 메시지 개수 50% 감소:

```
🚀 [02/01] EPS 모멘텀 일일 브리핑
━━━━━━━━━━━━━━━━━━━━━━
📅 2026-02-01 07:00 | 총 71개 통과

🚀 적극매수 (눌림목) - 9개
━━━━━━━━━━━━━━━━━━━━━━
의미있는 조정 후 반등 구간

• TEAM $254 | 점수 7.3 | RSI 51 | 高-16%
  └ 52주高 -10~25%, RSI 35-55, MA20 근처
• PANW $401 | 점수 5.5 | RSI 48 | 高-12%
  └ 52주高 -10~25%, RSI 35-55, MA20 근처

💎 저점매수 (과매도) - 1개
━━━━━━━━━━━━━━━━━━━━━━
RSI 과매도 반등 기회

• SMCI $35 | 점수 12.1 | RSI 32 | 高-72%
  └ RSI <= 35, 52주高 -20% 이상

🟢 매수적기 (추세) - 4개
━━━━━━━━━━━━━━━━━━━━━━
건강한 상승 추세 (정배열)

• AMD $236 | 점수 9.5 | RSI 55 | 高-8%

👀 관망 - 26개
━━━━━━━━━━━━━━━━━━━━━━
진입 대기 (조정/과열경계)

• NVDA $134 | RSI 67 | 高-4%
• MSFT $445 | RSI 62 | 高-6%

🚫 진입금지 - 31개
━━━━━━━━━━━━━━━━━━━━━━
과열/고점/급등 (매수 금지)

• MU $414 | RSI 72 | 高-3% (과열)
• META $682 | RSI 68 | 高-2% (고점근처)
```

### 액션별 표시 순서

1. 🚀 **적극매수** - 지금 매수 적기
2. 💎 **저점매수** - 과매도 반등 기회
3. 🟢 **매수적기** - 건강한 상승 추세
4. 👀 **관망** - 진입 대기
5. 🚫 **진입금지** - 매수 금지

### 태그 설명 (v5)

| 태그 | 의미 | 기준 |
|------|------|------|
| ⬆ | 정배열 표시 | 점수 옆 표시 (C > 7d > 30d > 60d) |
| 🌱 Quality Growth | 품질 성장주 | 매출 >= +5% AND 영업익 >= 매출증가율 |
| 💎 Reasonable Value | 합리적 가치 | PEG < 2.0 |
| 🔧 Technical Rescue | 기술적 구제 | 재무데이터 없음 & Price > MA60 |
| 高-N% | 52주 고점 대비 | 현재가가 52주 고점 대비 N% 하락 |

---

## 파일 구조

```
eps-momentum-us/
├── daily_runner.py          # 자동화 시스템 (스크리닝 + 텔레그램 + Git)
├── eps_momentum_system.py   # 코어 로직 (스코어링, 필터)
├── sector_analysis.py       # 섹터 분석 + ETF 추천
├── run_daily.bat            # Windows 작업 스케줄러용
├── config.json              # 설정 (텔레그램, Git 등)
├── eps_momentum_data.db     # SQLite DB (백테스트용)
├── eps_data/                # 일일 스크리닝 CSV
├── reports/                 # HTML/MD 리포트
└── logs/                    # 실행 로그
```

---

## 데이터베이스 스키마 (v5)

Track 2에서 백테스팅용으로 저장하는 데이터:

### 기본 필드

| 필드 | 타입 | 설명 |
|------|------|------|
| eps_current~eps_90d | REAL | EPS 추세 데이터 |
| score_321, score_slope | REAL | 스코어 (A/B 테스트) |
| price, ma_20, ma_200 | REAL | 가격 및 이동평균선 |
| dollar_volume | REAL | 일평균 거래대금 |

### v5 스크리닝 필드

| 필드 | 타입 | 설명 |
|------|------|------|
| is_aligned | INTEGER | EPS 정배열 여부 (C>7d>30d>60d) |
| is_quality_growth | INTEGER | Quality Growth 통과 (매출↑5%+ & 영업익>=매출) |
| is_reasonable_value | INTEGER | Reasonable Value 통과 (PEG < 2.0) |
| is_technical_rescue | INTEGER | Technical Rescue 통과 (데이터없음 & Price>MA60) |
| pass_reason | TEXT | 통과 사유 문자열 |

### 펀더멘털 필드 (백테스트용)

| 필드 | 타입 | 설명 |
|------|------|------|
| peg, forward_pe | REAL | 밸류에이션 |
| from_52w_high | REAL | 52주 고점 대비 (%) |
| rsi | REAL | RSI(14) |
| rev_growth_yoy | REAL | 매출 성장률 (YoY) |
| op_growth_yoy | REAL | 영업이익 성장률 (YoY) |
| is_undervalued | INTEGER | 저평가 여부 (PEG<1 또는 52주高-10%) |
| is_growth | INTEGER | 고성장 여부 (매출+10% & 영업+5%) |

---

## 사용법

```bash
# 전체 자동 실행 (스크리닝 + 데이터 축적 + 텔레그램)
python daily_runner.py

# Windows 작업 스케줄러 등록
schtasks /create /tn "EPS_Momentum_Daily" /tr "C:\...\run_daily.bat" /sc daily /st 07:00
```

---

## 필터 변경 이력

### v6.0 변경 (2026-02-02)

**Value-Momentum Hybrid System:**

v5.x에서 모멘텀 위주 필터 → v6.0에서 밸류에이션+품질 필터 추가

**새 기능:**
1. **3-Layer Filtering**
   - Layer 1: Momentum (기존 유지)
   - Layer 2: Quality - ROE > 10%
   - Layer 3: Safety - Forward PER < 60

2. **Hybrid Ranking**
   - `Score = Momentum×0.7 + (100/PER)×0.3`
   - 성장 + 저평가 종목 상위 랭크

3. **신규 지표**
   - Forward PER, ROE, PEG (계산), Hybrid Score
   - DB 스키마 자동 마이그레이션

4. **텔레그램 분리**
   - User Briefing: Top 3 Picks + 한국어 추천 문구
   - Admin Log: 시스템 상태 + v6 필터 통계

**변경 파일:**
- `eps_momentum_system.py`: DB 스키마 + 신규 함수 4개
- `daily_runner.py`: 3-Layer 필터 + Hybrid Ranking + 텔레그램 분리

### v5.4 변경 (2026-02-02)

**시장 국면 3단계 진단 시스템:**

v5.3 개선:
- 2단계(BULL/BEAR) → 3단계(RED/YELLOW/GREEN) 확장
- VIX(공포지수) 추가로 더 정밀한 진단

새 기능:
1. `check_market_regime()` 함수 업그레이드
   - SPY MA20/MA50 + VIX 기반 진단
   - 데이터 실패시 보수적으로 YELLOW 반환

2. 3단계 진단:
   - 🔴 RED: SPY < MA50 OR VIX >= 30 → 스크리닝 중단
   - 🟡 YELLOW: SPY < MA20 OR VIX >= 20 → 필터 강화
   - 🟢 GREEN: 정상 → 기본 필터

3. 텔레그램 헤더 개선:
   - RED: "⛔ 오늘의 추천 종목 없음 (Cash is King)"
   - YELLOW: "⚠️ 경계 모드: 필터 강화 적용중"
   - GREEN: "🟢 시장 상태: GREEN (상승장)"

### v5.3 변경 (2026-02-02)

**시장 국면(Market Regime) 필터 추가:**

기존 문제점:
- 개별 종목은 완벽해도 시장 전체가 폭락장이면 확률 급락
- 2008년, 2022년 같은 하락장에서도 매수 신호 발생

해결책:
1. `check_market_regime()` 함수 추가
2. SPY(S&P 500 ETF)의 MA200 위치 체크
3. 하락장 진입시 필터 2배 강화:
   - Score 4.0 → 6.0
   - PEG 2.0 → 1.5
4. 텔레그램 헤더에 시장 상태 표시:
   - 🟢 상승장: "시장 상승 추세 유지"
   - 🚨 하락장: "시장 경보: 하락장 진입" + 현금 비중 확대 권장

### v5.2 변경 (2026-02-01)

**텔레그램 메시지 최적화:**
- 종목을 액션별로 그룹화하여 표시
- 적극매수 → 저점매수 → 매수적기 → 관망 → 진입금지 순서
- 메시지 길이 63% 감소 (12,920자 → 4,596자)
- 메시지 개수 50% 감소 (4개 → 2개)
- 매수 신호만 상세 사유 표시

### v5.1 변경 (2026-02-01)

**액션 로직 전면 개편 - 실전 매매용:**

기존 문제점:
- RSI + MA20만으로 판단, 52주 고점 위치 무시
- 고점 근처 종목도 "강력매수" 표시되는 문제

개선사항:
1. `get_action_label()` 함수 전면 재작성
2. 52주 고점 대비 위치 기반 판단 추가
3. 새 진입금지 조건: 고점 -5% 이내, 단기급등 +8% 이상

**결과 (71개 중):**
- 진입금지: 31개 (고점18 + RSI과열12 + 급등1)
- 관망: 26개
- 적극매수 (눌림목): 9개 ← 진짜 기회
- 매수적기: 4개
- 저점매수: 1개

### v5 변경 (2026-02-01)

**인덱스 유니버스 확장:**
- 433개 → 917개 (Wikipedia에서 최신 구성종목)
- NASDAQ100: 91 → 101개
- S&P500: 262 → 503개
- S&P400: 149 → 400개

**필터 개편:**
1. [NEW] Price > MA200 (장기 상승 추세 필수)
2. Quality & Value Filter (OR 조건)
   - A. Quality Growth: 매출 >= 5% AND 영업익 >= 매출증가율
   - B. Reasonable Value: PEG < 2.0
   - C. Technical Rescue: 데이터 없으면 Price > MA60

**결과 (2026-02-01):**
- 스캔: 917개 → MA200↓ 제외: 48개 → 최종 통과: 71개

### v4 변경 (2026-01-31)

**추가된 필터:**
1. **저평가/성장 엄격 필터**: 저평가 OR 성장 중 하나 충족 필수
2. **정배열 보너스**: 완전 정배열 +3점, 부분 정배열 +1점
3. **MA20 필터 제거**: 저평가 필터로 대체

**결과:**
- 기존: 74개 → 변경: 약 25~35개 (엄격)
- 노이즈 감소, 품질 향상

### Kill Switch 완화 (2026-01-31)

- 기존: 0.01%라도 하락시 제외 (너무 엄격)
- 변경: -1% 이상 하락시에만 제외 (일시적 변동 허용)

---

## 투 트랙 시스템

```
Track 1 (실시간 트레이딩)
├── 매일 스크리닝 실행
├── EPS 모멘텀 + MA200 + Quality/Value 필터
├── 정배열 보너스 적용
├── 52주 고점 기반 액션 분류
└── 텔레그램 알림 (액션별 그룹화)

Track 2 (백테스트 데이터)
├── 917개 전체 종목 저장 (통과/불통과 모두)
├── 펀더멘털 지표 저장 (PEG, 성장률, RSI 등)
├── 6개월 후 Point-in-Time 백테스트 가능
└── Survivorship Bias 방지
```

---

## 의존성

```bash
pip install yfinance pandas numpy
```

## 데이터 소스

- Yahoo Finance API (`yfinance`)
- EPS Trend: `stock.eps_trend` (Forward 1 Year)
- Quarterly Financials: `stock.quarterly_financials`

---

## 주의사항

1. **야후 파이낸스 데이터 한계**: 중소형주 재무 데이터 누락 가능
   - 해결: Technical Rescue (Price > MA60 시 통과)

2. **API 호출 제한**: 너무 빈번한 호출 시 제한될 수 있음
   - 해결: 일 1회 실행 권장

3. **투자 책임**: 본 시스템은 참고용이며, 투자 결정은 본인 책임

---

## 향후 과제

1. **v6 백테스팅**: Hybrid Score 기반 성과 분석
2. **가중치 최적화**: Momentum/Value 비율 튜닝 (현재 0.7/0.3)
3. **ROE/PER 임계값**: 시장 상황별 동적 조정
4. **Point-in-Time 백테스트**: 6개월 데이터 축적 후 검증
5. **포지션 사이징**: Hybrid Score 기반 비중 배분
6. **실시간 알림**: 장중 Kill Switch 발동 시 알림
