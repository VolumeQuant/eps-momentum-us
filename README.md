# EPS Momentum System v8.0 - NTM EPS (US Stocks)

Forward 12개월 EPS(NTM EPS) 기반 모멘텀 시스템. 이익 추정치 변화율로 종목을 선별하고, 주가 미반영 종목을 매수 후보로 제시.

## 핵심 전략

### NTM (Next Twelve Months) EPS
yfinance의 0y/+1y EPS를 **endDate 기반 시간 가중치**로 블렌딩하여 Forward 12개월 EPS를 산출.
기존 +1y 컬럼은 종목마다 가리키는 연도가 달라 비교가 불가능했으나, NTM으로 통일.

### 이중 랭킹 시스템
- **Part 1 (이익 모멘텀 랭킹)**: 90일 이익변화율 순 — 어떤 기업의 이익이 가장 빠르게 개선되고 있는가
- **Part 2 (매수 후보)**: 괴리율 순 (가중평균 Fwd P/E 변화) — 이익 개선이 아직 주가에 반영 안 된 종목
  - 필터: Score > 3, EPS 변화율 > 0, Fwd PE > 0

### Score = seg1 + seg2 + seg3 + seg4
90일을 4개 독립 구간으로 나눠 각 구간 변화율 합산. 내부 필터링용 (Part 2 진입 기준: Score > 3).
```
|----seg4----|----seg3----|----seg2----|--seg1--|
90d         60d         30d          7d      today
```

### 추세 표시: 트래픽 라이트 🟢🟡🔴
4개 세그먼트 방향을 트래픽 라이트로 시각화 (순서: 과거→현재):
- 🟢 강한 개선 (세그먼트 변화 > 2%)
- 🟡 소폭 개선 (세그먼트 변화 0~2%)
- 🔴 하락 (세그먼트 변화 < 0%)

15개 카테고리의 말 설명 함께 표시 (예: "강세 지속", "최근 꺾임", "반등" 등).

### ⚠️ 경고 시스템 (Part 2)
주가 하락이 이익 대비 과도한 종목 감지:
- 조건: 이익 > 0, 주가 < 0, |주가변화| / |이익변화| > 5
- ⚠️ 아이콘 + "주가 하락이 이익 대비 과도합니다" 경고문 표시

### 턴어라운드 분리
`abs(NTM_current) < $1.00` 또는 `abs(NTM_90d) < $1.00`인 종목은 별도 카테고리.
양 끝점(현재 & 90일 전) 모두 체크하여 저 베이스 왜곡 방지. EPS 절대값(90d→현재) 표시.

## 유니버스

NASDAQ 100 + S&P 500 + S&P 400 MidCap = **916개 종목** (중복 제거)

## 텔레그램 메시지

| 메시지 | 내용 | 로컬 | GitHub Actions |
|--------|------|------|---------------|
| Part 1 모멘텀 랭킹 | Top 30, 이익변화율 순 | 개인봇 | **채널** |
| Part 2 매수 후보 | Top 30, 괴리율 순 | 개인봇 | **채널** |
| 턴어라운드 | Top 10, EPS 절대값 | 개인봇 | **채널** |
| 시스템 로그 | DB 적재 결과, 분포 통계 | 개인봇 | 개인봇 |

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

## 파일 구조

```
eps_momentum_system.py   # INDICES, INDUSTRY_MAP, NTM 계산 함수, get_trend_lights()
daily_runner.py          # 데이터 수집, 텔레그램 메시지, main()
config.json              # 텔레그램 토큰, Git 설정
run_daily.bat            # Windows 로컬 실행 스크립트
requirements.txt         # Python 패키지 의존성
ticker_info_cache.json   # 종목 이름/업종 캐시 (자동 생성)
SESSION_HANDOFF.md       # 설계 결정 히스토리 (v1~v6)
```

## 버전 히스토리

| 버전 | 날짜 | 변경 |
|------|------|------|
| v8.0 | 2026-02-07 | NTM EPS 전환, 이중 랭킹, 턴어라운드 분리, 트래픽 라이트 UI, ⚠️ 경고 시스템 |
| v7.2 | 2026-02-05 | 밸류+가격 100점 체계, GitHub Actions 자동화 |
| v7.1 | 2026-02-03 | 텔레그램 채널/봇 분리, 메시지 간소화 |
| v7.0 | 2026-02-01 | Super Momentum, Exit Strategy, Sector Booster |
| v6.x | 2026-01 | Value-Momentum Hybrid, RSI Action Multiplier |
| v1~5 | 2026-01 | 초기 구현, A/B 테스팅, 기술적 필터 |
