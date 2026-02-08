# EPS Momentum System v9.0 - NTM EPS (US Stocks)

Forward 12개월 EPS(NTM EPS) 기반 모멘텀 시스템. 이익 추정치 변화율로 종목을 선별하고, 주가 미반영 종목을 매수 후보로 제시. AI(Gemini)가 매수 후보의 리스크를 소거법으로 스캔.

## 핵심 전략

### NTM (Next Twelve Months) EPS
yfinance의 0y/+1y EPS를 **endDate 기반 시간 가중치**로 블렌딩하여 Forward 12개월 EPS를 산출.
기존 +1y 컬럼은 종목마다 가리키는 연도가 달라 비교가 불가능했으나, NTM으로 통일.

### 이중 랭킹 시스템
- **Part 1 (EPS 모멘텀 랭킹)**: adj_score 순 — EPS 전망치 개선 크기 + 방향 보정
- **Part 2 (매수 후보)**: 괴리율 순 (가중평균 Fwd P/E 변화) — EPS 개선이 아직 주가에 반영 안 된 종목
  - 필터: adj_score > 9, EPS 변화율 > 0, Fwd PE > 0
  - 정렬: 가중평균 Fwd PE 변화 (7d×40% + 30d×30% + 60d×20% + 90d×10%)
  - 표시 4줄: 순위+종목명 / 업종+신호등+패턴 / EPS%·주가%·괴리율 / 의견 ↑N ↓N

### EPS 점수: Score → adj_score (방향 보정)
**기본 Score** = seg1 + seg2 + seg3 + seg4
90일을 4개 독립 구간으로 나눠 각 구간 변화율 합산 (세그먼트 캡: ±100%).
```
|----seg4----|----seg3----|----seg2----|--seg1--|
90d         60d         30d          7d      today
```

**방향 보정 (adj_score)**:
최근 모멘텀과 과거 모멘텀을 비교하여 가속/감속 보정.
```
recent = (seg1 + seg2) / 2
old = (seg3 + seg4) / 2
direction = recent - old
adj_score = score × (1 + clamp(direction / 30, -0.3, +0.3))
```
- 1σ(3.67) 가속 → ~12% 보너스, 감속 → ~12% 패널티
- 최대 ±30% 캡
- Part 1 정렬 기준, Part 2 진입 필터(adj_score > 9)에 사용
- 고객 표시명: "EPS 점수"

### 추세 표시: 6단계 트래픽 라이트 + 8패턴
4개 세그먼트의 방향과 크기를 트래픽 라이트로 시각화 (순서: 과거→현재):
- 🟩 폭발적 상승 (>20%) — 네모 = 변동폭 큰 구간
- 🟢 상승 (2~20%)
- 🔵 양호 (0.5~2%)
- 🟡 보합 (0~0.5%)
- 🔴 하락 (0~-10%)
- 🟥 급락 (<-10%) — 네모 = 변동폭 큰 구간

8개 기본 패턴(횡보/하락/전구간 상승/상향 가속/상향 둔화/반등/추세 전환/등락 반복)에 🟩🟥 강도 수식어(폭발적~/급~/급등락 등) 조합.

### 의견 (EPS Revision Breadth, Part 2)
30일간 EPS 상향/하향 수정 애널리스트 수 표시: `의견 ↑N ↓N`
yfinance `epsRevisions.upLast30days/downLast30days` (0y) 데이터 사용, 추가 API 호출 없음.

### ⚠️ 경고 시스템 (Part 2)
주가 하락이 EPS 개선 대비 과도한 종목 감지:
- 조건: EPS 가중평균 > 0, 주가 가중평균 < 0, |주가변화| / |EPS변화| > 5
- ⚠️ 아이콘 표시 (텍스트 없이 아이콘만)

### 턴어라운드 (내부 처리)
`abs(NTM_current) < $1.00` 또는 `abs(NTM_90d) < $1.00`인 종목은 내부적으로 분리.
저 베이스 EPS로 인한 Score 왜곡 방지 목적. 별도 메시지로 발송하지 않음.

### AI 리스크 체크 (Gemini 2.5 Flash)
Part 2 매수 후보 30종목을 Gemini 2.5 Flash + Google Search Grounding으로 리스크 스캔.
AI 역할: **리스크 스캐너** (소거법) — 30종목 중 위험한 종목을 걸러내고, 리스크 미발견 종목이 진짜 매수 후보.
- 🚫 주의: 최근 1~2주 실제 리스크 뉴스 (소송, 내부자 매도, 실적 미스, 신용등급 하향 등)
- 📅 어닝 주의: 2주 내 실적발표 임박 종목
- ✅ 리스크 미발견: 나머지 종목 나열
- 상시 리스크/일반론/호재 보고 금지, temperature 0.2
- 🚫 섹션 종목 간 구분선 후처리 삽입
- 출력: 1500자 이내

## 유니버스

NASDAQ 100 + S&P 500 + S&P 400 MidCap = **916개 종목** (중복 제거)

## 텔레그램 메시지

| 메시지 | 내용 | 로컬 | GitHub Actions |
|--------|------|------|---------------|
| Part 1 EPS 모멘텀 랭킹 | Top 30, EPS 점수 순 | 개인봇 | **채널** |
| Part 2 매수 후보 (핵심) | Top 30, 괴리율 순 | 개인봇 | **채널** |
| AI 리스크 체크 | 매수 후보 리스크 소거법 | 개인봇 | 개인봇 |
| 시스템 로그 | DB 적재 결과, 분포 통계 | 개인봇 | 개인봇 |

발송 순서: Part 1 → **Part 2** → AI 리스크 체크 → 시스템 로그

모든 메시지에 **"읽는 법" 가이드를 헤더**에 배치하여 고객이 데이터 전에 해석법을 먼저 파악.

## DB 스키마

```sql
CREATE TABLE ntm_screening (
    date TEXT, ticker TEXT, rank INTEGER, score REAL,
    ntm_current REAL, ntm_7d REAL, ntm_30d REAL, ntm_60d REAL, ntm_90d REAL,
    is_turnaround INTEGER DEFAULT 0,
    PRIMARY KEY (date, ticker)
);
```

전 종목 매일 저장. 괴리율/패턴 등은 5개 NTM 값에서 파생 가능.

## 실행

```bash
# 로컬 실행
python daily_runner.py

# GitHub Actions
# KST 07:30 자동 실행 (cron: '30 22 * * 0-4' UTC)
```

### 환경변수 (GitHub Secrets)
- `TELEGRAM_BOT_TOKEN`: 텔레그램 봇 토큰
- `TELEGRAM_CHAT_ID`: 채널 ID
- `TELEGRAM_PRIVATE_ID`: 개인봇 ID
- `GEMINI_API_KEY`: Google AI Studio API 키

## 파일 구조

```
eps_momentum_system.py    # INDICES, INDUSTRY_MAP, NTM 계산 함수, get_trend_lights()
daily_runner.py           # 데이터 수집, AI 분석, 텔레그램 메시지, main()
config.json               # 텔레그램 토큰, Gemini API 키, Git 설정 (.gitignore)
run_daily.bat             # Windows 로컬 실행 스크립트
requirements.txt          # Python 패키지 의존성
ticker_info_cache.json    # 종목 이름/업종 캐시 (자동 생성)
eps_momentum_data.db      # SQLite DB (자동 생성)
SESSION_HANDOFF.md        # 설계 결정 히스토리 (v1~v12)
```

## 버전 히스토리

| 버전 | 날짜 | 변경 |
|------|------|------|
| v9.0 | 2026-02-08 | 트래픽 라이트 8패턴 리디자인, Part 2 괴리율+의견 표시, 개발 스크립트 정리 |
| v8.2 | 2026-02-08 | 방향 보정(adj_score) 도입, Part 2 필터 adj_score > 9 |
| v8.1 | 2026-02-08 | AI 리스크 스캐너 소거법 전환, 구분선, temperature 0.2 |
| v8.0 | 2026-02-07 | NTM EPS 전환, 이중 랭킹, 6단계 트래픽 라이트, Gemini AI 뉴스 스캐너 |
| v7.x | 2026-02 | 밸류+가격 100점, 채널/봇 분리, GitHub Actions |
| v1~6 | 2026-01 | 초기 구현, A/B 테스팅, 기술적 필터, Value-Momentum Hybrid |
