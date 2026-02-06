# Session Handoff: 전략 개선 논의

> **v1**: 2026-02-06 직장 PC — 구조적 문제 발견, score_321 개선 논의
> **v2**: 2026-02-06 집 PC — NTM EPS 전환 결정, DB/Score/랭킹 전면 재설계
> **v3**: 2026-02-06 집 PC — 풀 유니버스 시뮬레이션, 이상치 처리, Part 2 재설계
> **v4**: 2026-02-06 집 PC — 텔레그램 포맷 확정, 발송 채널 분리, 업종 매핑

---

## Phase 1: 문제 발견 (직장 PC)

### 1-1. 시작점: "마이크로소프트는 왜 안 나오는 거야?"

텔레그램 메시지에서 MSFT를 한 번도 본 적 없다는 의문에서 시작.

### 1-2. Track 1 vs Track 2 필터 불일치

**Track 1 (텔레그램)**: 10단계 필터 → ~24개 통과
**Track 2 (DB)**: 2단계 필터 → ~265개 passed

MSFT는 EPS 모멘텀(score 9.5)은 좋지만, **Price < MA200**에서 Track 1 탈락.
→ "무엇을 보유할까"와 "언제 살까"가 같은 필터에 혼재된 구조적 문제.

### 1-3. score_321 공식의 문제

```python
# 기존 방식: binary 방향 + 임의 스케일링 + 90d 미사용
if current > d7:  score += 3    # 0.01% 올라도 +3, 10% 올라도 +3
score += eps_chg_60d / 5        # 임의 스케일링
# 90d는 아예 안 봄 → TTWO(90d -34%)를 못 잡음
```

---

## Phase 2: 근본적 재설계 (집 PC)

### 2-1. +1y 컬럼의 치명적 문제 발견

현재 시스템은 `trend.loc['+1y']`로 EPS를 가져오는데, **`+1y`가 가리키는 실제 연도가 종목마다 다름:**

| 종목 | +1y endDate | 0y endDate | 원인 |
|------|-----------|-----------|------|
| AMZN | **2027**-12-31 | 2026-12-31 | FY2025 발표 완료, 롤오버 |
| CRWV | **2026**-12-31 | 2025-12-31 | FY2025 미발표 |
| AAPL | **2027**-09-30 | 2026-09-30 | 9월 결산 |

**→ +1y끼리 비교하면 2026년과 2027년 EPS가 뒤섞인 엉터리 랭킹**

**확인 방법**: `stock._analysis._earnings_trend` 리스트의 각 항목에 `endDate` 필드 존재

### 2-2. 해결: NTM (Forward 12M) EPS 도입

**0y와 +1y를 endDate 기반 시간 가중치로 블렌딩:**

```python
# 각 시점(ref_date)마다 앞으로 12개월 윈도우를 계산
window_start = ref_date
window_end = ref_date + 365일

# 0y, +1y 각각의 겹치는 기간으로 가중치 산출
w0 = overlap(window, 0y_fiscal_year) / total_overlap
w1 = overlap(window, +1y_fiscal_year) / total_overlap

NTM_EPS = w0 × (0y EPS) + w1 × (+1y EPS)
```

**핵심: 5개 시점 각각 가중치를 재계산**
- NTM_current: 오늘 기준 가중치로 블렌딩
- NTM_7d: 7일 전 기준 가중치로 블렌딩
- NTM_30d: 30일 전 기준 가중치로 블렌딩
- NTM_60d: 60일 전 기준 가중치로 블렌딩
- NTM_90d: 90일 전 기준 가중치로 블렌딩

**검증 결과 (MSFT, 6월 결산):**
```
시점       0y(FY26)    w0      +1y(FY27)   w1      NTM EPS
current    17.202    39.6%  +  19.025    60.4%  =  18.304
7d ago     17.235    41.5%  +  18.997    58.5%  =  18.266
30d ago    15.678    47.8%  +  18.545    52.2%  =  17.175
60d ago    15.633    56.0%  +  18.531    44.0%  =  16.907
90d ago    15.661    64.3%  +  18.557    35.7%  =  16.696
```
- **NTM 모멘텀: +9.63%** (기존 +1y만: +2.52%) — 0y의 강한 상향이 반영됨

### 2-3. Score 공식 (NTM 기반, 내부 로직)

```python
seg1 = (NTM_current - NTM_7d)  / |NTM_7d|  × 100   # 최근 7일
seg2 = (NTM_7d - NTM_30d)     / |NTM_30d| × 100   # 7~30일 구간
seg3 = (NTM_30d - NTM_60d)    / |NTM_60d| × 100   # 30~60일 구간
seg4 = (NTM_60d - NTM_90d)    / |NTM_90d| × 100   # 60~90일 구간

score = seg1 + seg2 + seg3 + seg4
```

**4개 구간이 겹치지 않는 독립 구간으로 90일 전체를 커버:**
```
|----seg4----|----seg3----|----seg2----|--seg1--|
90d         60d         30d          7d      today
```

**주의**: Score는 내부 DB 저장/필터링용. 고객 표시에는 **90일 이익변화율** 사용 (아래 Phase 4 참고).

### 2-4. 패턴 (추세 화살표)

seg 방향을 화살표로 시각화. **순서: 과거→현재 (왼→오)**
```
추세(90d/60d/30d/7d)
     ↑   ↑   ↑   ↑
    seg4 seg3 seg2 seg1
```

주요 패턴:
| 패턴 | seg 방향 | 의미 |
|------|---------|------|
| 꾸준한 상향 | ↑↑↑↑ | 가장 신뢰 높은 시그널 |
| V자 회복 | ↓↑↑↑ | 턴어라운드 |
| 턴어라운드 | ↓↓↑↑ | 최근 전환, 초기 단계 |
| 꺾임 | ↑↑↑↓ | 모멘텀 하락 시작. 경고 |
| 하락전환 | ↑↑↓↓ | 하락 가속 |
| 역배열 | ↓↓↓↓ | 지속 하향. 회피 |
| 혼조 | 기타 | 방향 불명확 |

---

## Phase 3: 시뮬레이션 & Part 2 재설계 (집 PC)

### 3-1. 유니버스 정리

- **현행 유지**: NASDAQ 100 + S&P 500 + S&P 400 MidCap = **915개**
- **FOX 제거**: FOX(Class B)는 eps_trend 데이터 없음, FOXA(Class A)가 커버 → `INDICES['SP500']`에서 제거 완료
- GOOG/GOOGL 둘 다 유지 (Class A/C 별도 상장)

### 3-2. 이상치 처리: |NTM EPS| < $1.00 분리

**문제 발견**: 1차 시뮬레이션에서 ALB 스코어 54,065 (NTM_90d ≈ $0.01 → 분모 폭발)

**검토한 대안들:**
| 방법 | 결과 | 문제점 |
|------|------|--------|
| 세그먼트 캡 (±200%) | ALB 800점 | 여전히 SNDK(322)보다 높음 — 부당 |
| 최소 분모 ($0.50) | ALB 521점 | 여전히 비정상적으로 높음 |
| Z-Score 정규화 | 분포 기반 | 1-2개 이상치가 전체 분포를 왜곡 |
| **|NTM| < $1.00 분리** ✅ | **깔끔하게 해결** | 없음 |

**최종 결정: $1.00 최소 EPS 기준**
- 5개 NTM 값(current, 7d, 30d, 60d, 90d) 중 **하나라도** |EPS| < $1.00이면 → **"턴어라운드" 카테고리**로 분리
- 메인 랭킹에서 제외, 별도 섹션으로 표시
- **근거**: NTM EPS가 $1 미만인 종목은 성장률 계산이 의미 없음 (0.01→0.02가 +100%)

### 3-3. 풀 유니버스 시뮬레이션 결과 (2026-02-06)

**기본 통계:**
```
전체 유니버스: 916개 (FOX 제거 전)
데이터 있음:   913개
데이터 없음:     0개
에러:            3개 (COKE, L, NEU — endDate 파싱 에러)
```

**$1 필터 적용 후:**
```
메인 랭킹:    861개 (|NTM| >= $1.00)
턴어라운드:    52개 (|NTM| < $1.00)
```

**Score 분포 (메인 861개):**
```
Score >  0:  596 (69%)
Score >  1:  506 (59%)
Score >  2:  389 (45%)
Score >  3:  310 (36%)
Score >  5:  177 (21%)
Score > 10:   74 (9%)
Score > 20:   31 (4%)
Min=-98.19, Max=322.15, Median=1.26
정배열: 236
```

### 3-4. Part 2: Forward P/E 변화율 (= 괴리율)

**기존 Part 2 (폐기):** MA200, RSI 기반 기술적 진입 타이밍
**새 Part 2:** EPS 개선이 아직 주가에 반영 안 된 종목 찾기

**핵심 지표: Forward P/E 90일 변화율 (고객 표시명: "괴리율")**
```python
Fwd_PE_now = Price_now / NTM_current
Fwd_PE_90d = Price_90d / NTM_90d
괴리율 = (Fwd_PE_now - Fwd_PE_90d) / Fwd_PE_90d × 100
```

**해석:**
- **괴리율 마이너스** = EPS 상향 > 주가 상승 → "아직 덜 반영됨" → **매수 기회**
- **괴리율 플러스** = 주가 상승 > EPS 상향 → "이미 선반영됨" → 추격 매수 위험

---

## Phase 4: 텔레그램 포맷 & 발송 채널 확정 ← NEW

### 4-1. Part 1 순위 기준 변경

**기존**: Score(seg합산) 기준 정렬
**변경**: **90일 이익변화율** 기준 정렬
```python
이익변화 = (NTM_current - NTM_90d) / |NTM_90d| × 100
```

**이유**: Score는 % 단위가 아니라 고객에게 혼란. 90일 변화율이 직관적.
추세 화살표(↑↑↑↑)가 구간별 패턴을 이미 보여주므로 Score의 구간별 장점은 유지됨.

**Score는 내부 로직으로만 사용**: DB 저장, Part 2 필터링(Score > 3) 등.

### 4-2. 업종 분류

- yfinance의 `industry` 필드 사용 (130개 고유값)
- 한글 축약 매핑 테이블 1회 생성 (예: Semiconductors → 반도체, Software-Application → 응용SW)
- 매핑 안 된 건 영어 그대로 표시

### 4-3. 텔레그램 메시지 포맷

**Part 1: 이익 모멘텀 랭킹** (Top 30)
```
📊 이익 모멘텀 랭킹

 #  종목명(티커)          업종     이익변화  추세(90d/60d/30d/7d)
 1  Sandisk(SNDK)        반도체   +660.5%      ↑ ↑ ↑ ↑
 2  Micron(MU)           반도체   +116.0%      ↑ ↑ ↑ ↑
 3  MicroStrategy(MSTR)  응용SW    +96.6%      ↑ ↓ ↑ ↑
 5  Southwest(LUV)       항공      +32.1%      ↑ ↑ ↑ ↑
22  Nvidia(NVDA)         반도체    +24.1%      ↑ ↑ ↑ ↑
```

**Part 2: 매수 후보** (Top 30, Score > 3 필터, 괴리율 순 정렬)
```
💰 매수 후보 (이익↑ 주가 덜 반영)

 #  종목명(티커)          업종     이익변화  주가변화  괴리율   추세(90d/60d/30d/7d)
 1  MicroStrategy(MSTR)  응용SW    +96.6%   -49.0%  -74.1%      ↑ ↓ ↑ ↑
 2  Sandisk(SNDK)        반도체   +660.5%  +153.8%  -66.6%      ↑ ↑ ↑ ↑
 3  Palantir(PLTR)       인프라SW  +36.6%   -24.3%  -44.6%      ↑ ↑ ↑ ↑
```

**턴어라운드 섹션** (Top 10)
```
⚡ 턴어라운드 주목 (|EPS|<$1 구간)

 종목명(티커)          업종     EPS(90일전→현재)  추세(90d/60d/30d/7d)
 Palantir(PLTR)       인프라SW  -$0.50 → $0.20       ↑ ↑ ↑ ↑
 Cleveland(CLF)       철강     -$0.30 → $0.20       ↓ ↓ ↑ ↑
```

**시스템 로그**
```
🔧 시스템 실행 로그 (2026-02-06 07:30 KST)

실행환경: GitHub Actions / Local
소요시간: 12분 34초

[데이터 수집]
유니버스: 915개
성공: 912 | 에러: 3
에러 종목: COKE, L, NEU

[DB 적재 - ntm_screening]
컬럼: date, ticker, rank, score, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, is_turnaround
메인: 860건 | 턴어라운드: 52건 | 합계: 912건

[스코어 분포]
이익변화 > 0: 596 (69%)
이익변화 > 3: 310 (36%)
정배열(↑↑↑↑): 236

[발송 결과]
Part1 모멘텀 랭킹 30건 → 개인봇 ✓
Part2 매수후보 30건 → 채널 ✓
턴어라운드 10건 → 채널 ✓
```

### 4-4. 발송 채널

| 메시지 | 로컬 실행 | GitHub Actions |
|--------|----------|---------------|
| 시스템 로그 | 개인봇 | 개인봇 |
| Part 1 (모멘텀 랭킹) | 개인봇 | 개인봇 |
| Part 2 (매수 후보) | 개인봇 | **채널** |
| 턴어라운드 | 개인봇 | **채널** |

### 4-5. 실행 스케줄

- **GitHub Actions**: 매일 KST 07:30 (미국 장마감 ET 16:00 = KST 06:00, 데이터 안정화 후 1.5시간)
- **로컬**: 수동 실행 (개인봇에만 발송)

---

## 결정된 사항 ✅

1. **+1y → NTM EPS 전환**: endDate 기반 시간 가중 블렌딩
2. **Score = seg1+seg2+seg3+seg4**: 내부 DB 저장/필터링용
3. **고객 표시는 90일 이익변화율**: (NTM_cur - NTM_90d) / |NTM_90d| × 100
4. **Part 1 정렬: 90일 이익변화율 순** (Score 아님)
5. **Part 2 정렬: 괴리율 순** (Fwd P/E 90일 변화율)
6. **Part 2 필터: Score > 3** (310개, 상위 36%)
7. **DB는 전 종목 저장**: 915개 전체 (나중에 순위 진입/이탈 추적 가능)
8. **패턴은 별도 저장 불필요**: 5개 NTM 값에서 재계산
9. **|NTM EPS| < $1.00 → 턴어라운드 분리**: 메인 랭킹에서 제외, 별도 표시
10. **FOX 제거**: eps_trend 없음, FOXA가 커버
11. **Fwd PE / 괴리율은 DB 미저장**: NTM 값 + Yahoo 주가에서 파생 가능
12. **업종: yfinance industry** → 한글 축약 매핑
13. **종목 표기: 종목명(티커)** 형식
14. **추세 헤더: 추세(90d/60d/30d/7d)** — 화살표 순서 = 과거→현재
15. **Part 1: Top 30, 로컬/개인봇에만 발송**
16. **Part 2: Top 30, GitHub Actions시 채널 발송**
17. **턴어라운드: Top 10, GitHub Actions시 채널 발송** (EPS 절대값 표시)
18. **시스템 로그: 개인봇에만 발송** (DB 적재 컬럼명 포함)
19. **실행 스케줄: KST 07:30 GitHub Actions**
20. **에러 종목: skip** (로그만 남김)
21. **기존 eps_snapshots 테이블: 삭제**

---

## 미결 사항 ❓

### 1. 기존 코드 마이그레이션
- eps_momentum_system.py: `calculate_momentum_score_v3()` → NTM 기반으로 전면 교체
- daily_runner.py: `run_screening()`, `run_data_collection()` 수정
- 에러 종목(COKE, L, NEU) endDate=None 처리

### 2. 업종 한글 매핑 테이블
- 130개 industry → 한글 축약 매핑 생성 필요
- 구현 시 초안 자동 생성 후 수동 보정

---

## 폐기 대상 (기존 시스템)

| 항목 | 이유 |
|------|------|
| score_321 | NTM Score로 대체 |
| quality_score (100점) | NTM Score로 대체 |
| Kill Switch (7d -1%) | seg1이 자연 감점 |
| 이상치 필터 (60d > 200%) | $1 기준으로 턴어라운드 분리 |
| passed_screen 플래그 | score 랭킹으로 대체 |
| 기존 eps_snapshots 테이블 | ntm_screening으로 대체, **삭제** |
| MA200/RSI/action 로직 | Part 2에서 괴리율로 대체 |
| get_action_label() | 폐기 |
| is_actionable() | 폐기 |

---

## DB 스키마 (새)

```sql
CREATE TABLE ntm_screening (
    date        TEXT,     -- 스크리닝 날짜
    ticker      TEXT,     -- 종목
    rank        INTEGER,  -- 그날의 순위 (90일 이익변화율 기준)
    score       REAL,     -- seg1+seg2+seg3+seg4 (내부 필터링용)
    ntm_current REAL,     -- NTM EPS 현재 추정치
    ntm_7d      REAL,     -- NTM EPS 7일 전 추정치
    ntm_30d     REAL,     -- NTM EPS 30일 전 추정치
    ntm_60d     REAL,     -- NTM EPS 60일 전 추정치
    ntm_90d     REAL,     -- NTM EPS 90일 전 추정치
    is_turnaround INTEGER DEFAULT 0,  -- |NTM| < $1.00 여부
    PRIMARY KEY (date, ticker)
);
```

**설계 근거:**
- 원본 5개 NTM 값 보존 → 나중에 공식 바꿔도 재계산 가능
- 전 종목 저장 → 순위 진입/이탈 추적, 3개월 후 수익률 상관관계 분석
- 패턴/괴리율은 5개 값에서 파생 → 별도 컬럼 불필요
- is_turnaround 플래그로 메인/턴어라운드 구분

---

## 기술 참고

### 데이터 접근 방법

```python
# NTM 계산에 필요한 데이터
stock = yf.Ticker(ticker)
eps_trend = stock.eps_trend                    # 5개 시점 × 4개 기간
raw_trend = stock._analysis._earnings_trend    # endDate 포함

# endDate 추출
for item in raw_trend:
    period = item['period']      # '0y', '+1y'
    end_date = item['endDate']   # '2026-12-31'

# 업종 정보
industry = stock.info.get('industry', 'N/A')   # ex: "Semiconductors"
```

### NTM 계산 핵심 코드

```python
# 각 snapshot별 시간 가중 NTM 계산
snapshots = {'current': 0, '7daysAgo': 7, '30daysAgo': 30, '60daysAgo': 60, '90daysAgo': 90}
for col, days_ago in snapshots.items():
    ref = today - timedelta(days=days_ago)
    we = ref + timedelta(days=365)
    o0d = max(0, (min(we, fy0_end) - max(ref, fy0_start)).days)
    o1d = max(0, (min(we, fy1_end) - max(ref, fy1_start)).days)
    total = o0d + o1d
    ntm[col] = (o0d/total) * eps_0y + (o1d/total) * eps_1y

# 턴어라운드 판별
is_turnaround = any(abs(v) < 1.0 for v in ntm.values())

# 고객 표시용 90일 이익변화율 (Part 1 정렬 기준)
이익변화 = (ntm_current - ntm_90d) / abs(ntm_90d) * 100

# 괴리율 (Part 2 정렬 기준)
fwd_pe_now = price_now / ntm_current
fwd_pe_90d = price_90d / ntm_90d
괴리율 = (fwd_pe_now - fwd_pe_90d) / fwd_pe_90d * 100
```

### 시뮬레이션 스크립트 (참고용)
| 스크립트 | 용도 |
|---------|------|
| `ntm_simulation.py` | 1차 시뮬레이션 (이상치 미처리) |
| `ntm_sim.py` | 1차 정리 버전 |
| `ntm_sim2.py` | 2차 시뮬레이션 ($1 필터 + Part 2 Fwd PE) |
| `collect_industries.py` | 유니버스 업종 수집 (130개 고유 industry) |
| 직장 PC: `new_score_sim.py` | 새 점수 공식 시뮬레이션 |
| 직장 PC: `check_outliers.py` | SNDK, ALB, MSTR, MU, TTWO EPS 상세 |

---

*v1 작성: Claude Opus 4.6 | 2026-02-06 직장 PC*
*v2 업데이트: Claude Opus 4.6 | 2026-02-06 집 PC*
*v3 업데이트: Claude Opus 4.6 | 2026-02-06 집 PC — 시뮬레이션 결과 & Part 2 재설계*
*v4 업데이트: Claude Opus 4.6 | 2026-02-06 집 PC — 텔레그램 포맷 & 발송 채널 확정*
