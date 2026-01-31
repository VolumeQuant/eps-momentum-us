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
6. Earnings Blackout: 실적 발표 D-5 ~ D+1 진입 금지
```

## 기술적 분석 신호 (v3 신규)

RSI(14)와 이동평균선(20일/200일)을 활용한 진입 타이밍 분석:

| 우선순위 | 조건 | 액션 |
|---------|------|------|
| 1 | 현재가 < 200일선 | 📉 추세이탈 (200일선↓) |
| 2 | RSI ≥ 70 | ✋ 진입금지 (과열) |
| 3 | RSI 50~65 & 20일선 근처(-2%~+3%) | 🚀 강력매수 (눌림목) |
| 4 | RSI < 40 & 현재가 > 200일선 | 🟢 저점매수 (반등) |
| 5 | 현재가 > 20일선 | 🟢 매수적기 (추세) |
| 6 | 기타 | 👀 관망 (20일선 이탈) |

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
├── daily_runner.py          # 자동화 시스템 (스크리닝 + 텔레그램 + Git)
├── eps_momentum_system.py   # 메인 시스템 (Track 1 + Track 2)
├── sector_analysis.py       # 섹터 분석 + Broad/Narrow + ETF 추천
├── daily_eps_screener.py    # 일일 스크리닝 (편입/편출 추적)
├── run_daily.bat            # Windows 작업 스케줄러용 배치 파일
├── config.json              # 설정 (텔레그램, Git 등)
├── eps_momentum_data.db     # SQLite DB (백테스트용 데이터)
├── eps_data/                # 데이터 저장
│   └── screening_*.csv      # 일일 스크리닝 결과
├── reports/                 # 리포트 저장
│   ├── report_*.md          # Markdown 리포트
│   └── report_*.html        # HTML 리포트
└── logs/                    # 실행 로그
```

## 사용법

```bash
# 전체 자동 실행 (스크리닝 + 데이터 축적 + 리포트 + 텔레그램)
python daily_runner.py

# 일일 스크리닝 (개별 종목)
python eps_momentum_system.py screen

# 데이터 축적
python eps_momentum_system.py collect

# 섹터 분석 + ETF 추천
python sector_analysis.py
```

## 자동화 설정

### Windows 작업 스케줄러

1. `작업 스케줄러` 열기
2. 새 작업 생성:
   - 트리거: 매일 07:00 (미국 장 마감 후, 한국 시간)
   - 동작: `C:\dev\claude-code\eps-momentum-us\run_daily.bat`
   - 시작 위치: `C:\dev\claude-code\eps-momentum-us`

### 텔레그램 알림 설정

`config.json`:
```json
{
  "telegram_enabled": true,
  "telegram_bot_token": "YOUR_BOT_TOKEN",
  "telegram_chat_id": "YOUR_CHAT_ID"
}
```

## 텔레그램 메시지 포맷 (v3)

```
🚀 [01/31] EPS 모멘텀 일일 브리핑
━━━━━━━━━━━━━━━━━━━━━━
📅 2026-01-31 07:00 | 총 74개 종목 통과

1. MU Micron Technology ($414.9)
   ├ 📊 점수 28.8 | EPS +114% | PEG 0.1 | 반도체
   └ 🎯 ✋ 진입금지 (과열) (RSI 72 | $15.2B)

2. LUV Southwest Airlines ($47.5)
   ├ 📊 점수 11.0 | EPS +25% | PEG 0.2 | 산업재
   └ 🎯 🟢 매수적기 (추세) (RSI 60 | $478M)

3. FCX Freeport-McMoRan ($60.2)
   ├ 📊 점수 10.8 | EPS +24% | PEG 0.4 | 소재
   └ 🎯 🚀 강력매수 (눌림목) (RSI 60 | $1.5B)
...

━━━━━━━━━━━━━━━━━━━━━━
📊 시장 테마 분석
• Semiconductor (🎯Narrow): 10종목
  └ ETF 추천: SMH (1x) / SOXL (3x)
• Industrials (📈Broad): 8종목
  └ ETF 추천: XLI (1x) / DUSL (3x)

━━━━━━━━━━━━━━━━━━━━━━
📈 스크리닝 통계
• 총 스캔: 433개
• Kill Switch 제외: 45개
• 거래량 부족: 12개
• MA20 하회: 85개
• 실적 블랙아웃: 3개
• 최종 통과: 74개
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

## A/B 테스팅 (스코어링 방식 비교)

| 방식 | 설명 | 특징 |
|------|------|------|
| **Score_321** | 가중치 기반 (3-2-1) | 현재 사용 중, 안정적 |
| **Score_Slope** | 변화율 가중 평균 | 가속도 측정, 테스트 중 |

3개월 후 어떤 방식이 더 효과적인지 검증 예정

## 핵심 발견 및 수정사항

| 이슈 | 문제 | 해결 |
|------|------|------|
| **Look-ahead Bias** | 오늘 EPS 변화로 과거 수익률 예측 | Track 2로 데이터 축적 후 검증 |
| **Volume 필터** | 주당 거래량은 고가주 불리 | Dollar Volume($20M)으로 변경 |
| **동일 가중치** | 모든 기간 동일 가중치 | 최신 변화에 높은 가중치 부여 |
| **Broad ETF 오류** | 반도체 신호에 TECL 추천 | Narrow 감지 → SOXL 추천 |
| **진입 타이밍** | EPS 좋아도 과열 상태 진입 | RSI + 이평선 기술적 분석 추가 |

## 의존성

```bash
pip install yfinance pandas numpy
```

## 향후 과제

1. **Point-in-Time 백테스트**: 6개월 후 Track 2 데이터로 실제 검증
2. **리밸런싱 주기 최적화**: 주간 vs 월간
3. **포지션 사이징**: 점수 기반 비중 배분
4. **리스크 관리**: 섹터 집중도 제한, 손절 규칙
5. **A/B 테스트 결과 분석**: Score_321 vs Score_Slope 비교

## 데이터 소스

- Yahoo Finance API (`yfinance`)
- EPS Trend: `stock.eps_trend` (Forward 1 Year)

## 최근 업데이트

- **2026-01-31**: 기술적 분석 함수 추가 (RSI, 200일선), 텔레그램 상세 카드형 포맷 적용
- **2026-01-30**: 초기 버전 배포
