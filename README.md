# EPS Revision Momentum Strategy (US Stocks)

미국 주식 대상 EPS 리비전 모멘텀 전략 시스템

## 전략 개요

**핵심 아이디어**: 애널리스트들의 Forward EPS 컨센서스 상향 조정이 향후 주가 상승을 예측

| 항목 | 설정값 |
|------|--------|
| EPS 기준 | Forward +1 Year (내년 예상 EPS) |
| 변화 측정 기간 | 60일 (Sweet Spot) |
| 최소 EPS 변화율 | +5% |
| 최소 모멘텀 점수 | 4.0 |

## 스크리닝 필터

```
1. Kill Switch:      Current < 7daysAgo → 즉시 제외 (최근 하향 = 위험)
2. EPS 변화율:       60일 대비 +5% 이상
3. 모멘텀 점수:      가중치 기반 4.0점 이상
   - Current > 7d:   +3점 (가장 중요)
   - 7d > 30d:       +2점
   - 30d > 60d:      +1점
   - EPS변화율/5:    보너스
4. Dollar Volume:    $20M 이상 (유동성)
5. Technical:        Price > 20-day MA (추세)
```

## 지수별 검증 결과

| 지수 | Sharpe Ratio | 평가 |
|------|-------------|------|
| **S&P 500** | **1.23** | 최적 |
| S&P 400 MidCap | 0.91 | 양호 |
| NASDAQ 100 | 0.78 | 양호 |
| S&P 600 SmallCap | 0.57 | 보통 |
| DOW 30 | 0.00 | 부적합 |

**결론**: S&P 500 + MidCap + NASDAQ 100 조합이 최적

## Broad vs Narrow 신호 전략

| 신호 유형 | 특징 | ETF 선택 |
|----------|------|---------|
| **BROAD** | 섹터 내 다양한 업종에 분포 | 섹터 ETF (DUSL, FAS) |
| **NARROW** | 특정 테마에 집중 | 테마 ETF (SOXL, COPX) |

**판단 기준**: HHI(허핀달지수) >= 0.25 또는 주도 테마 비중 >= 50%

**예시**:
- Tech 신호가 반도체에 집중 → SOXL (Semiconductor 3x)
- Industrials 신호가 골고루 분포 → DUSL (Industrials 3x)

## 파일 구조

```
eps-momentum-us/
├── eps_momentum_system.py   # 메인 시스템 (Track 1 + Track 2)
├── sector_analysis.py       # 섹터 분석 + Broad/Narrow + ETF 추천
├── daily_eps_screener.py    # 일일 스크리닝 (편입/편출 추적)
├── eps_data/                # 데이터 저장
│   ├── screening_*.csv      # 일일 스크리닝 결과
│   └── track2_*.csv         # 백테스트용 전체 데이터
└── README.md
```

## 사용법

```bash
# 일일 스크리닝 (개별 종목)
python eps_momentum_system.py

# 섹터 분석 + ETF 추천
python sector_analysis.py
```

## 투 트랙 시스템

```
Track 1 (실시간 트레이딩)
├── 매일 스크리닝 실행
├── 통과 종목 = 매수 후보
├── 탈락 종목 = 매도 검토
└── Broad/Narrow 분석 → ETF 추천

Track 2 (백테스트 데이터 축적)
├── 모든 종목 EPS 데이터 저장 (통과/불통과 모두)
├── 6개월~1년 후 Point-in-Time 백테스트 가능
└── Survivorship Bias 방지
```

## 핵심 발견 및 수정사항

| 이슈 | 문제 | 해결 |
|------|------|------|
| **Look-ahead Bias** | 오늘 EPS 변화로 과거 수익률 예측 | Track 2로 데이터 축적 후 검증 |
| **Volume 필터** | 주당 거래량은 고가주 불리 | Dollar Volume($20M)으로 변경 |
| **동일 가중치** | 모든 기간 동일 가중치 | 최신 변화에 높은 가중치 부여 |
| **Broad ETF 오류** | 반도체 신호에 TECL 추천 | Narrow 감지 → SOXL 추천 |

## 의존성

```bash
pip install yfinance pandas numpy
```

## 향후 과제

1. **Point-in-Time 백테스트**: 6개월 후 Track 2 데이터로 실제 검증
2. **리밸런싱 주기 최적화**: 주간 vs 월간
3. **포지션 사이징**: 점수 기반 비중 배분
4. **리스크 관리**: 섹터 집중도 제한, 손절 규칙

## 데이터 소스

- Yahoo Finance API (`yfinance`)
- EPS Trend: `stock.eps_trend` (Forward 1 Year)
