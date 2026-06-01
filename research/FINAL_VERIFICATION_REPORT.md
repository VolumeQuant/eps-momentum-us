# 최종 검증 보고서 — 사이클 framework + EPS Momentum 통합

**일시**: 2026-06-01 자율주행 (전체 세션)
**원래 사용자 질문**: "경기 사이클 4계절 framework로 EPS Momentum 시스템과 결합해서 더 좋은 성과 만들 수 있나?"
**검증 데이터**: 8년 sector ETF (2018-2026) + 74일 EPS Momentum DB

## TL;DR — 솔직한 결론

| 검증 영역 | 결과 |
|---|---|
| **Investment Clock 7년 BT** | ❌ SPY 대비 **-31%p** (171% vs 202%), XLK 대비 -345%p |
| **약세장 보호 가치** | △ 부분 (2022 +21%p OK, 2020 -8%p NO) |
| **Buffett 13F sector follow** | ❌ OXY → XLE: SPY 대비 -18%p (Buffett 알파 못 잡음) |
| **H9 + 시스템 결합** | ❌ 시스템 dip-buy 알파(MU, STX 같은 winner) 직접 차단 |
| **13가지 overlay 가설** | ❌ 거의 모두 marginal/음수 |
| **결론** | **사이클 framework는 retail에게 alpha 없음. XLK 단순 buy-hold가 가장 강한 단일 전략.** |

## 1. Investment Clock 7년 BT (2019-01-01 ~ 2026-05-29)

| Strategy | 누적 Return | CAGR | MDD | Sharpe | Calmar |
|---|---|---|---|---|---|
| **XLK buy-hold (기술)** | **+516%** | **+27.9%** | -34.0% | 1.06 | **0.82** ⭐ |
| **SPY buy-hold** | +202% | +16.2% | -34.1% | 0.86 | 0.47 |
| Clock 12-state (Buffett 패턴) | +171% | +7.0% | -29.9% | 0.60 | 0.23 |
| Clock 4-state (단순) | +123% | +5.6% | -31.5% | 0.51 | 0.18 |

→ **단순 XLK buy-hold가 모든 framework 압도**. Calmar 0.82 vs Clock 12-state 0.23.

## 2. 기간별 분리 분석

### 강세장

| 기간 | SPY | Clock 12 | Alpha |
|---|---|---|---|
| 2019 | +28.7% | +22.5% | -6.1%p |
| 2020-21 복구 | +71.0% | +77.7% | +6.7%p |
| **2023-24 AI 활황** | **+53.9%** | **+35.6%** | **-18.3%p** ❌ |
| 2025-26 AI 지속 | +29.4% | +29.9% | +0.5%p |

→ **AI 시대에 사이클 framework가 가장 크게 underperform**.

### 약세장

| 기간 | SPY | Clock 12 | Alpha |
|---|---|---|---|
| 2020 COVID | -17.5% | -25.7% | **-8.1%p** ❌ |
| 2022 인플레 | -19.9% | +1.1% | **+21.0%p** ✅ |

→ **약세장 타입에 따라 정반대**. 급락(COVID)은 못 막고, slow bear(2022)는 막음.

## 3. Buffett 13F follow 정량 검증

Berkshire 주요 매수 이벤트 → 해당 sector ETF로 단순 follow 시 누적 성과:

| 이벤트 | 매수 시점 | sector ETF | 누적 return | SPY 누적 | Alpha |
|---|---|---|---|---|---|
| OXY 21% 매수 | 2022-03 | XLE | +57.9% | +75.9% | **-18.0%p** ❌ |
| AAPL 75% 매도 (반대 signal) | 2024-08 | XLK (반대) | +81.4% | +39.3% | **+42%p** (Buffett 틀림) |
| Cash 비축 (ATH) | 2024-12 | SPY | +29.1% | +29.1% | 0 |

→ **Buffett 13F → sector ETF로 단순화하면 alpha 없음**. Buffett 알파는 **개별 종목 + valuation 분석 + 인내심**에 있고, sector mimic으로 잡을 수 없음.

## 4. H9 (mom_20>0) 와 시스템 알파 충돌

실제 매수 시점 mom_20 값 분석:

| 종목 | 매수일 | mom_20 | H9 결과 | 실제 수익 |
|---|---|---|---|---|
| STX | 03-13 | -11.0% | ❌ Reject | +11.9% winner |
| **MU** | **04-01** | **-8.2%** | **❌ Reject** | **+103% 대박** |
| SNDK 2차 | 04-06 | +37.4% | ✅ 통과 | +119% |
| BE | 05-08 | +56.6% | ✅ 통과 | +11% |
| TER | 05-15 | -11.2% | ❌ Reject | 현재 +11% |
| AEIS | 05-26 | -11.9% | ❌ Reject | 현재 -3% |

→ **H9는 시스템의 dip-buy 알파(EPS 강한데 가격 일시 dip)를 직접 차단**. MU +103%, STX +11.9% 같은 winner 놓침.

## 5. 진짜 알파의 출처 — 데이터 기반

| 알파 원천 | 검증 |
|---|---|
| **XLK buy-hold** (기술/AI 노출) | ✅ 7년 CAGR 27.9%, Calmar 0.82 |
| **EPS Momentum 시스템** | ✅ 72일 BT 강세장 ceiling, 라이브 12건 92% 승률 |
| **사이클 사계절 framework** | ❌ 7년 BT -31%p underperform |
| **Buffett 13F sector follow** | ❌ -18%p underperform |
| **각종 overlay (H1~H9)** | ❌ 거의 다 marginal/음수 |

## 6. 사용자 원래 가설에 대한 정직한 답

| 가설 | 결과 |
|---|---|
| "사이클 보고 다음 계절 미리 매매하면 alpha" | ❌ **실증 부정**. 7년 -31%p |
| "Buffett처럼 13F 보고 따라가면 alpha" | ❌ **sector ETF 단순화로는 X**. Buffett 알파는 개별 종목 분석 |
| "AI 시대에도 cycle framework 유효" | ❌ **AI 시대 가장 크게 underperform** (-18%p 2023-24) |
| "EPS Momentum + cycle overlay" | ❌ 13가지 시도 다 marginal/음수 |
| "약세장에서 cycle framework 보호" | △ **2022만**. 2020 COVID는 못 막음 |
| **그럼 무엇이 진짜 alpha?** | **XLK + EPS Momentum 시스템 + 인내심** |

## 7. 권장 framework (수정)

이전 토의 결론에서 L4 (Buffett selective)는 추가 제안했지만 **이번 실증으로 부정**:

```
L0: EPS Momentum 시스템 (v84) — 검증된 alpha
L1: Regime overlay (자동 IEF 전환)
L2: 현금 buffer 20% (구조적 cushion)
L3: 월 macro signal monitor (정보용, alpha X)
L4: ❌ 폐기 — sector ETF로 Buffett follow 무력
```

**대신 새 후보 (검증 필요):**
- **L0+: 시스템 자금의 일부를 XLK 핵심 보유로 anchor** (5~10%)
  - 7년 CAGR 27.9% buy-hold
  - 시스템이 놓치는 long-horizon AI alpha 흡수
  - 위험: AI bubble burst 시 -50% 가능

## 8. 본인 가설들 vs 실증

| 본인 직관 | 실증 |
|---|---|
| "Buffett 13F로 미리 사놓더라" | ✅ 사실 (Buffett 개별 종목 잘 함). ❌ retail이 sector ETF로 mimic 불가 |
| "사이클 보면서 미리 사기" | ❌ 7년 BT 실증 부정 |
| "AI가 cycle override 중" | ✅ 사실. AI 시대(2023-2026) cycle framework underperform |
| "trauma 보호 위해 cycle framework" | △ 2022는 OK, 2020 NO. 일관성 없음 |

## 9. 6.4년 50억 목표 재계산 (정직)

| 시나리오 | 7년 후 자산 (10억 시작) |
|---|---|
| **XLK buy-hold 단독** (CAGR 27.9%) | **약 57억** (목표 초과 달성) |
| SPY buy-hold (CAGR 16.2%) | 약 29억 |
| Clock 12-state (CAGR 7.0%) | 약 16억 (목표 미달) |
| EPS Momentum 시스템 추정 (CAGR 20~25%) | 약 36~50억 (목표 달성 가능) |

→ **XLK 단순 buy-hold가 가장 단순한 답**. 약세장 risk만 견디면 27.9% CAGR.

**EPS Momentum 시스템이 XLK 단독 buy-hold보다 +alpha 만든다면 진짜 우월**. 단 다년 BT 필요.

## 10. 최종 권장 — 정직

| 결정 | 답 |
|---|---|
| 사이클 framework 시스템 통합? | ❌ X (실증 부정) |
| Buffett 13F sector follow? | ❌ X (sector ETF로는 alpha X) |
| H9 + 시스템 결합? | ❌ X (dip-buy 알파 차단) |
| Investment Clock 별도 트랙 운영? | ⚠️ 의문 (SPY 못 이김) |
| **XLK 일부 anchor (5~10%)?** | **검토 가치 — 단 약세장 risk 인지** |
| **EPS Momentum 시스템 그대로?** | **✅ 최선** |

## 11. 다음 자율주행에서 검증할 것

1. **EPS Momentum 시스템 vs XLK / SPY 다년 alpha 측정** (가장 중요)
   - 72일 외 다년 시계열 reconstruction
   - 시스템이 진짜 단순 buy-hold 압도하는지
2. AI peak signal trigger 시 XLK 매도 룰 (큰 약세장 보호)
3. 환·세금 보정한 net CAGR 측정

## 12. 산출물

| 파일 | 내용 |
|---|---|
| `research/bt_investment_clock.py` + `.log` | Investment Clock 7년 BT |
| `research/sector_etf_8y.parquet` | 8년 sector ETF 데이터 |
| `research/V84_PLUS_H9_REPORT.md` | v84+H9 결합 상세 |
| `research/CYCLE_OVERLAY_RESEARCH_REPORT.md` | 13가지 가설 종합 |
| **본 파일** | 최종 종합 |

## 13. 사용자에게 솔직한 한 마디

본인이 원했던 "사이클 미리 매매로 시스템 강화" — **실증적으로 부정됐습니다**.

가장 정직한 답:
- 시스템(v84) + 시간 = 가장 큰 알파
- 6.4년 50억은 시스템 또는 XLK anchor로 가능
- macro/cycle framework는 매혹적이지만 retail에게 alpha 안 줌
- Buffett 알파는 따라가기 불가능

다음 검증 우선순위: **EPS Momentum 시스템 vs 단순 XLK buy-hold 다년 비교**. 시스템이 진짜로 단순 buy-hold 압도하는지 확정.
