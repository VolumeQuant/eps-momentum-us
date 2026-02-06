# EPS Momentum System v8.0 - NTM EPS (US Stocks)

Forward 12개월 EPS(NTM EPS) 기반 모멘텀 시스템. 이익 추정치 변화율로 종목을 선별하고, 주가 미반영 종목을 매수 후보로 제시.

## 핵심 전략

### NTM (Next Twelve Months) EPS
yfinance의 0y/+1y EPS를 **endDate 기반 시간 가중치**로 블렌딩하여 Forward 12개월 EPS를 산출.
기존 +1y 컬럼은 종목마다 가리키는 연도가 달라 비교가 불가능했으나, NTM으로 통일.

### 이중 랭킹 시스템
- **Part 1 (이익 모멘텀 랭킹)**: 90일 이익변화율 순 — 어떤 기업의 이익이 가장 빠르게 개선되고 있는가
- **Part 2 (매수 후보)**: 괴리율 순 (Fwd P/E 90일 변화) — 이익 개선이 아직 주가에 반영 안 된 종목

### Score = seg1 + seg2 + seg3 + seg4
90일을 4개 독립 구간으로 나눠 각 구간 변화율 합산. 내부 필터링용 (Part 2 진입 기준: Score > 3).
```
|----seg4----|----seg3----|----seg2----|--seg1--|
90d         60d         30d          7d      today
```

### 턴어라운드 분리
|NTM EPS| < $1.00인 종목은 변화율 계산이 무의미하므로 별도 카테고리. EPS 절대값 표시.

## 유니버스

NASDAQ 100 + S&P 500 + S&P 400 MidCap = **916개 종목** (중복 제거)

## 텔레그램 메시지

| 메시지 | 내용 | 로컬 | GitHub Actions |
|--------|------|------|---------------|
| Part 1 모멘텀 랭킹 | Top 30, 이익변화율 순 | 개인봇 | 개인봇 |
| Part 2 매수 후보 | Top 30, 괴리율 순 | 개인봇 | **채널** |
| 턴어라운드 | Top 10, EPS 절대값 | 개인봇 | **채널** |
| 시스템 로그 | DB 적재 결과, 분포 통계 | 개인봇 | 개인봇 |

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
eps_momentum_system.py  # INDICES, INDUSTRY_MAP, NTM 계산 함수
daily_runner.py         # 데이터 수집, 텔레그램 메시지, main()
config.json             # 텔레그램 토큰, Git 설정
SESSION_HANDOFF.md      # 설계 결정 히스토리 (v1~v4)
```

## 버전 히스토리

| 버전 | 날짜 | 변경 |
|------|------|------|
| v8.0 | 2026-02-07 | NTM EPS 전환, 이중 랭킹, 턴어라운드 분리, 전면 재설계 |
| v7.2 | 2026-02-05 | 밸류+가격 100점 체계, GitHub Actions 자동화 |
| v7.1 | 2026-02-03 | 텔레그램 채널/봇 분리, 메시지 간소화 |
| v7.0 | 2026-02-01 | Super Momentum, Exit Strategy, Sector Booster |
| v6.x | 2026-01 | Value-Momentum Hybrid, RSI Action Multiplier |
| v1~5 | 2026-01 | 초기 구현, A/B 테스팅, 기술적 필터 |
