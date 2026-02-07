# Session Handoff: 전략 개선 논의

> **v1**: 2026-02-06 직장 PC — 구조적 문제 발견, score_321 개선 논의
> **v2**: 2026-02-06 집 PC — NTM EPS 전환 결정, DB/Score/랭킹 전면 재설계
> **v3**: 2026-02-06 집 PC — 풀 유니버스 시뮬레이션, 이상치 처리, Part 2 재설계
> **v4**: 2026-02-06 집 PC — 텔레그램 포맷 확정, 발송 채널 분리, 업종 매핑
> **v5**: 2026-02-07 집 PC — 모바일 UI 리디자인, 고객 친화적 말투
> **v6**: 2026-02-07 집 PC — 트래픽 라이트, ⚠️ 경고, Part 2 EPS>0 필터, 코드 정리

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

### 2-4. 추세 표시 (트래픽 라이트)

seg 방향을 트래픽 라이트로 시각화. **순서: 과거→현재 (왼→오)**
```
추세(90d/60d/30d/7d)
    🟢  🟡  🔴  🟢
    seg4 seg3 seg2 seg1
```

**임계값:**
- 🟢 강한 개선: 세그먼트 변화 > 2%
- 🟡 소폭 개선: 세그먼트 변화 0~2%
- 🔴 하락: 세그먼트 변화 < 0%

**15개 말 설명 카테고리** (트래픽 라이트와 함께 표시):
| 패턴 | 말 설명 |
|------|---------|
| 🟢🟢🟢🟢 | 강세 지속 |
| 🟢🟢🟢🟡 | 소폭 감속 |
| 🟢🟢🟢🔴 | 최근 꺾임 |
| 🟢🟢🟡🟡 | 둔화 |
| 🔴🟢🟢🟢 | 반등 |
| 🔴🔴🟢🟢 | 가속 |
| 🟡🟡🟡🟡 | 거의 정체 |
| 🔴🔴🔴🔴 | 하락세 |
| 기타 (🟢 ≥ 3) | 소폭 개선 |
| 기타 (🔴 ≥ 3) | 최근 약세 |
| 기타 (상승세) | 회복 중, 변동 개선 등 |
| 기타 | 혼조, 등락 반복 등 |

**결정 근거 (v5):** 기존 ↑↓ 화살표는 방향만 표시. ▲△ 시도했으나 모바일에서 크기 다른 문제.
트래픽 라이트는 방향+속도를 동시에 표현하며 크기 일관됨. 말 설명 추가로 고객 이해도 향상.

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
- `abs(NTM_current) < $1.00` 또는 `abs(NTM_90d) < $1.00`이면 → **"턴어라운드" 카테고리**로 분리 (양 끝점만 체크, 중간 시점 이상치 방지)
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

### 4-3. 텔레그램 메시지 포맷 (v5 모바일 최적화)

**설계 원칙:**
- 모바일 가로폭 우선 (테이블/고정폭 헤더 폐기)
- 한 줄 또는 두 줄로 종목 하나 표현
- HTML bold/italic으로 시각적 구분, 구분선(---) 제거
- 날짜 + 부제로 맥락 전달

**Part 1: 이익 모멘텀 Top 30**
```
📊 이익 모멘텀 Top 30
2026.02.07 · 90일 이익추정치 변화율 순

1. SNDK +660.5% ↑↑↑↑ 반도체
2. MU +116.0% ↑↑↑↑ 반도체
3. MSTR +96.6% ↑↓↑↑ 응용SW

↑↓ = 90d · 60d · 30d · 7d 구간 방향
```

**Part 2: 매수 후보 Top 30** (Score > 3 필터, 괴리율 순)
```
💰 매수 후보 Top 30
2026.02.07 · 이익은 올랐는데 주가가 덜 따라간 종목

1. MSTR 응용SW ↑↓↑↑
    이익 +96.6% · 주가 -49.0% · 갭 -74.1%
2. SNDK 반도체 ↑↑↑↑
    이익 +660.5% · 주가 +153.8% · 갭 -66.6%

갭 = Fwd PE 변화율, 마이너스일수록 저평가
```

**턴어라운드 주목** (Top 10)
```
⚡ 턴어라운드 주목
2026.02.07 · 적자축소·흑자전환 신호 (|EPS| < $1)

1. PLTR 인프라SW ↑↑↑↑
    $-0.50 → $0.20
2. CLF 철강 ↓↓↑↑
    $-0.30 → $0.20

90일 전 EPS → 현재 EPS
```

**시스템 로그**
```
🔧 시스템 로그
2026.02.07 07:30 KST · GitHub Actions

수집 913/916 (에러 0)
├ 메인 861
└ 턴어라운드 52

Score > 0: 523 (61%)
Score > 3: 312 (36%)
정배열 ↑↑↑↑: 187

소요: 14분 35초
```

### 4-4. 발송 채널

| 메시지 | 로컬 실행 | GitHub Actions |
|--------|----------|---------------|
| Part 1 (모멘텀 랭킹) | 개인봇 | **채널** |
| Part 2 (매수 후보) | 개인봇 | **채널** |
| 턴어라운드 | 개인봇 | **채널** |
| 시스템 로그 | 개인봇 | 개인봇 |

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
9. **턴어라운드 분리**: `abs(current) < $1.00 OR abs(90d) < $1.00` → 양 끝점만 체크 (중간 시점 이상치 방지)
10. **FOX 제거**: eps_trend 없음, FOXA가 커버
11. **Fwd PE / 괴리율은 DB 미저장**: NTM 값 + Yahoo 주가에서 파생 가능
12. **업종: yfinance industry** → 한글 축약 매핑
13. **종목 표기: 티커만** (v5에서 변경 — 모바일 가로폭 절약)
14. **추세: 트래픽 라이트 🟢🟡🔴** — 순서 = 90d→60d→30d→7d (과거→현재), 15개 말 설명 동반
15. **Part 1: Top 30, GitHub Actions시 채널 발송** (로컬은 개인봇)
16. **Part 2: Top 30, GitHub Actions시 채널 발송, EPS 변화 > 0 필터 적용**
17. **턴어라운드: Top 10, GitHub Actions시 채널 발송** (EPS 절대값 표시)
18. **시스템 로그: 개인봇에만 발송** (DB 적재 컬럼명 포함)
19. **실행 스케줄: KST 07:30 GitHub Actions**
20. **에러 종목: skip** (로그만 남김)
21. **기존 eps_snapshots 테이블: 삭제**
22. **모바일 UI 리디자인 (v5)**: 테이블/고정폭 헤더 폐기, 한 줄/두 줄 compact 포맷, 고객 친화적 부제
23. **트래픽 라이트 🟢🟡🔴 (v5)**: ↑↓ 화살표 → 🟢(>2%)🟡(0~2%)🔴(<0%) + 15개 말 설명
24. **⚠️ 경고 시스템 (v6)**: Part 2에서 |주가변화|/|이익변화| > 5일 때 표시, 하드 필터 아닌 소프트 경고
25. **Part 2 EPS>0 필터 (v6)**: 이익 변화율 음수 종목은 매수 후보에서 제외
26. **읽는 법 헤더 배치 (v6)**: 모든 메시지에서 데이터 목록 위에 해석 가이드 배치
27. **Part 1 채널 발송 (v6)**: GitHub Actions 실행 시 Part 1도 채널로 발송

---

## 미결 사항 ❓

(모두 해결됨 — 코드 마이그레이션 완료, 업종 매핑 130개 구현 완료)

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

# 턴어라운드 판별 (양 끝점만 체크)
is_turnaround = abs(ntm_current) < 1.0 or abs(ntm_90d) < 1.0

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
*v5 업데이트: Claude Opus 4.6 | 2026-02-07 집 PC — 모바일 UI 리디자인, 고객 친화적 말투*
*v6 업데이트: Claude Opus 4.6 | 2026-02-07 집 PC — 트래픽 라이트, ⚠️ 경고, EPS>0 필터, 코드 정리*
