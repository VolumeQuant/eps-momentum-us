# EPS Momentum System v19 - Safety & Trend Fusion (US Stocks)

Forward 12개월 EPS(NTM EPS) 기반 모멘텀 시스템. 이익 추정치 변화율로 종목을 선별하고, MA60 + 3일 연속 검증으로 신뢰도를 높인 매수 후보를 제시. AI(Gemini)가 위험 신호를 점검하고 최종 포트폴리오를 추천.

## 핵심 전략

### NTM (Next Twelve Months) EPS
yfinance의 0y/+1y EPS를 **endDate 기반 시간 가중치**로 블렌딩하여 Forward 12개월 EPS를 산출.
기존 +1y 컬럼은 종목마다 가리키는 연도가 달라 비교가 불가능했으나, NTM으로 통일.

### 매수 후보 선정 (adj_gap 기반)
adj_gap 순 (방향 보정 괴리율) — EPS 개선이 아직 주가에 반영 안 된 종목.

**필터 (6개)**:
| 필터 | 조건 | 근거 |
|------|------|------|
| EPS 모멘텀 | adj_score > 9 | 방향 보정 점수 최소 기준 |
| EPS 개선 | eps_change_90d > 0 | EPS가 실제로 올라야 함 |
| Fwd PE 유효 | fwd_pe > 0 | 데이터 유효성 |
| **MA60** | price > 60일 이동평균 | 하락 추세 종목 제외 (기술적 안전장치) |
| **adj_gap ≤ 0** | adj_gap > 0이면 제외 | 주가가 EPS를 이미 초과 반영 → 기회 아님 |
| **$10** | price ≥ $10 | 페니스톡 제외 |

**정렬**: adj_gap 오름차순 (더 음수 = EPS 대비 주가 저평가)
- adj_gap = fwd_pe_chg × (1 + clamp(direction/30, -0.3, +0.3))
- fwd_pe_chg = 가중평균 Fwd PE 변화 (7d×40% + 30d×30% + 60d×20% + 90d×10%)
- 방향 보정: 가속 EPS → 저평가 강화, 감속 EPS → 저평가 약화

**표시 4줄**: 순위+✅/🆕+종목명 / 업종+추세+패턴 / EPS%·주가%·괴리 / 의견 ↑N ↓N

### 3일 연속 검증
- Part 2 eligible 종목에 `part2_rank` 부여 → DB 저장
- 최근 3일 모두 Part 2에 있는 종목 = ✅ (검증됨)
- 오늘만 진입한 종목 = 🆕 (관찰)
- 포트폴리오는 ✅ 종목에서만 선정

### Death List (탈락 알림)
- 어제 Part 2에 있었지만 오늘 빠진 종목 자동 감지
- 사유: MA60↓, 괴리+, 점수↓, EPS↓ 등
- Part 2 메시지 하단 `🚨 탈락 종목` 섹션

### EPS 점수: Score → adj_score (방향 보정)
**기본 Score** = seg1 + seg2 + seg3 + seg4
90일을 4개 독립 구간으로 나눠 각 구간 변화율 합산 (세그먼트 캡: ±100%).
```
|----seg4----|----seg3----|----seg2----|--seg1--|
90d         60d         30d          7d      today
```

**방향 보정 (adj_score)**:
```
recent = (seg1 + seg2) / 2
old = (seg3 + seg4) / 2
direction = recent - old
adj_score = score × (1 + clamp(direction / 30, -0.3, +0.3))
```
- Part 2 진입 필터(adj_score > 9)에 사용
- 고객 표시명: "EPS 점수"

### 괴리율: fwd_pe_chg → adj_gap (방향 보정)
**기본 fwd_pe_chg** = 가중평균 Fwd P/E 변화율 (7d×40% + 30d×30% + 60d×20% + 90d×10%)
각 시점의 Fwd PE(= 주가/NTM EPS)를 비교. 음수 = EPS 개선 대비 주가 미반영 (저평가).

**방향 보정 (adj_gap)**:
```
adj_gap = fwd_pe_chg × (1 + clamp(direction / 30, -0.3, +0.3))
```
- 가속 EPS(direction > 0) → 저평가 강화 (더 음수)
- 감속 EPS(direction < 0) → 저평가 약화 (덜 음수)
- Part 2 정렬 기준, 포트폴리오 비중 배분에 사용
- 고객 표시명: "괴리"

### 추세 표시: 5단계 아이콘 + 12패턴
4개 세그먼트의 방향과 크기를 아이콘으로 시각화 (순서: 과거→현재):
- 🔥 폭등 (>20%) — 폭발적 상승
- ☀️ 강세 (5~20%)
- 🌤️ 상승 (1~5%)
- ☁️ 보합 (±1%)
- 🌧️ 하락 (<-1%)

🔥가 있으면 해당 구간에서 폭등이 발생했다는 것을 즉시 파악 가능.
12개 기본 패턴(횡보/하락/전구간 상승/꾸준한 상승/상향 가속/최근 급상향/중반 강세/상승 등락/상향 둔화/반등/추세 전환/등락 반복)에 🔥 강도 수식어(폭발적~/급~/중반 급등/폭발적 등락/급등 후 둔화 등) 조합.

### 의견 (EPS Revision Breadth)
30일간 EPS 상향/하향 수정 애널리스트 수 표시: `의견 ↑N ↓N`
yfinance `epsRevisions.upLast30days/downLast30days` (0y) 데이터 사용, 추가 API 호출 없음.

### ⚠️ 경고 시스템
주가 하락이 EPS 개선 대비 과도한 종목 감지:
- 조건: EPS 가중평균 > 0, 주가 가중평균 < 0, |주가변화| / |EPS변화| > 5
- ⚠️ 아이콘 표시 (텍스트 없이 아이콘만)

### 리스크 필터 (포트폴리오 & AI 브리핑 공통)
시스템 철학(adj_gap = 저평가 기회)과 충돌하지 않는 4가지 필터만 적용:
| 플래그 | 조건 | 근거 |
|--------|------|------|
| 하향 | rev_down ≥ 3 | 다수 애널리스트 동시 하향 → EPS 전망 신뢰 하락 |
| 저커버리지 | num_analysts < 3 | 소수 의견 → NTM EPS 데이터 불안정 |
| 고평가 | fwd_pe > 100 | PE 비정상 → 데이터 품질 문제 |
| 어닝 | 2주 이내 실적발표 | 추정치 리셋 리스크 |

### 턴어라운드 (내부 처리)
`abs(NTM_current) < $1.00` 또는 `abs(NTM_90d) < $1.00`인 종목은 내부적으로 분리.
저 베이스 EPS로 인한 Score 왜곡 방지 목적. 별도 메시지로 발송하지 않음.

### AI 브리핑 (Gemini 2.5 Flash)
Part 2 매수 후보 데이터를 Gemini 2.5 Flash가 분석하여 투자 브리핑 작성.
"검색은 코드가, 분석은 AI가" — yfinance로 팩트 수집, AI는 데이터 해석에 집중.
- 📰 시장 동향: 어제 미국 시장 마감 + 금주 주요 이벤트 (Google Search 1회)
- ⚠️ 매수 주의: 위험 신호 기반 주의 종목
- 📅 어닝 주의: yfinance `stock.calendar`에서 2주내 실적발표 직접 조회 (AI 비의존)
- 데이터에 있는 정보만 사용, 없는 내용 생성 금지, temperature 0.2

## 유니버스

NASDAQ 100 + S&P 500 + S&P 400 MidCap = **916개 종목** (중복 제거)

## 텔레그램 메시지

| 메시지 | 내용 | 로컬 | GitHub Actions |
|--------|------|------|---------------|
| [1/3] 매수 후보 | adj_gap 순, ✅3일검증/🆕신규/🚨탈락 | 개인봇 | **채널+개인봇** |
| [2/3] AI 브리핑 | 시장 동향 + 위험 신호 분석 + 어닝 | 개인봇 | **채널+개인봇** |
| [3/3] 포트폴리오 | ✅ 종목 중 리스크 필터 통과 Top 5 | 개인봇 | **채널+개인봇** |
| 시스템 로그 | DB 적재 결과, 분포 통계 | 개인봇 | 개인봇 |

발송 순서: [1/3] **매수 후보** → [2/3] AI 브리핑 → [3/3] **포트폴리오** → 시스템 로그

3개 메시지가 순차적 단계를 거쳐 최종 포트폴리오에 도달하는 흐름. 각 메시지에 다음 단계 예고(👉).
모든 메시지에 **"읽는 법" 가이드를 헤더**에 배치하여 고객이 데이터 전에 해석법을 먼저 파악.

### 포트폴리오 추천
매수 후보 중 ✅ (3일 검증) + 리스크 필터 통과 종목 → adj_gap 순 → 상위 5개 선정.
비중: abs(adj_gap) 비례, 5% 단위. Gemini가 종목별 선정 이유 생성.
✅ 종목 부족 시 있는 만큼만 추천. 0개면 "관망 권장" 메시지.

## DB 스키마

```sql
CREATE TABLE ntm_screening (
    date TEXT, ticker TEXT, rank INTEGER, score REAL,
    ntm_current REAL, ntm_7d REAL, ntm_30d REAL, ntm_60d REAL, ntm_90d REAL,
    is_turnaround INTEGER DEFAULT 0,
    adj_score REAL, adj_gap REAL,
    price REAL, ma60 REAL,
    part2_rank INTEGER,
    PRIMARY KEY (date, ticker)
);
```

전 종목 매일 저장. part2_rank는 3일 교집합 쿼리에 사용.

## 실행

```bash
# 로컬 실행
python daily_runner.py

# GitHub Actions
# KST 07:15 자동 실행 (cron: '15 22 * * 0-4' UTC)
```

### 영업일/휴장 처리
- 3일 교집합은 **DB에 있는 distinct date** 기준 → 주말/공휴일 별도 처리 불필요
- 미국 공휴일은 yfinance 데이터 부재로 자연스럽게 skip

### Cold Start 자동 전환
- DB에 `part2_rank` 데이터가 3일 미만 → **개인봇만** 전송 (채널 비활성화)
- 3일 이상 축적되면 자동으로 **채널 + 개인봇** 전송 시작
- 날짜 하드코딩 없이 DB 상태 기반 자동 판단 (`is_cold_start()`)

### 환경변수 (GitHub Secrets)
- `TELEGRAM_BOT_TOKEN`: 텔레그램 봇 토큰
- `TELEGRAM_CHAT_ID`: 채널 ID
- `TELEGRAM_PRIVATE_ID`: 개인봇 ID
- `GEMINI_API_KEY`: Google AI Studio API 키

## 파일 구조

```
eps_momentum_system.py    # INDICES, INDUSTRY_MAP, NTM 계산 함수, get_trend_lights()
daily_runner.py           # 데이터 수집, MA60, 3일 검증, AI 분석, 텔레그램, main()
config.json               # 텔레그램 토큰, Gemini API 키, Git 설정 (.gitignore)
run_daily.bat             # Windows 로컬 실행 스크립트
requirements.txt          # Python 패키지 의존성
ticker_info_cache.json    # 종목 이름/업종 캐시 (자동 생성)
eps_momentum_data.db      # SQLite DB (자동 생성)
SESSION_HANDOFF.md        # 설계 결정 히스토리 (v1~v19)
```

## 버전 히스토리

| 버전 | 날짜 | 변경 |
|------|------|------|
| v19 | 2026-02-10 | Safety & Trend Fusion: MA60+3일검증+Death List, Part 1 제거, 메시지 3개 축소, adj_gap≤0 필터, cron 22:15 |
| v9.4 | 2026-02-09 | adj_gap 도입, 리스크 필터 정비 (모순 제거 + 저커버리지 추가) |
| v9.3 | 2026-02-09 | 날씨 아이콘 전환 (6단계 신호등→5단계 날씨), 포트폴리오 비중 단순화 |
| v9.2 | 2026-02-08 | AI 브리핑 전환: 뉴스 스캐너→데이터 분석, 어닝 yfinance 직접 조회 |
| v9.1 | 2026-02-08 | 트래픽 라이트 12패턴 확장 (피크 위치 기반 분류) |
| v9.0 | 2026-02-08 | 트래픽 라이트 8패턴 리디자인, Part 2 괴리율+의견 표시 |
| v8.x | 2026-02-08 | 방향 보정(adj_score), NTM EPS 전환, 이중 랭킹 |
| v7.x | 2026-02 | 밸류+가격 100점, 채널/봇 분리, GitHub Actions |
| v1~6 | 2026-01 | 초기 구현, A/B 테스팅, 기술적 필터, Value-Momentum Hybrid |
