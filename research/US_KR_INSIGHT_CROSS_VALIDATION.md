# US-KR Cross-Validation 보고서 — KR 인사이트 US 적용 검증

**일시**: 2026-06-01 자율주행 (사용자 KR 자율주행 결과 보고 후 US 적용)
**배경**: KR 자율주행에서 발견된 인사이트 4가지를 US v84에 적용·검증

## TL;DR

| KR 인사이트 | US 검증 결과 |
|---|---|
| **Slot 확대 (3→5/7) Cal +30%** | ❌ 정반대 (-170~-207%p) |
| Universe 필터 (KP200+KQ150 best) | ⭐ **시총 30B+ filter +17.9%p (90/100 wins)** |
| Fragility (Top 5 -29%) | ✅ **동일 패턴** (Top 6 -91%p) |
| Defense 모드 가치 | ✅ v84 regime overlay 이미 적용 |

→ **시총 30B+ filter 도입 = 새로운 alpha 후보**. Slot 확대는 US엔 비적합.

## 1. KR과 US 시스템 본질 차이 (slot 확대 정반대 이유)

| 차원 | KR (v80.22) | US (v84) |
|---|---|---|
| 회전 | high-turnover (median 8일) | medium (74일 BT 26일) |
| Weight | equal 균등 | dynamic 2step_t15 |
| Slot 본질 | 분산이 alpha (slot 5/7로 +30%) | 집중이 alpha (slot 2로 +alpha) |
| 핵심 알파 | 다양한 sector momentum surfer | dd_30_25 + dynamic weight + concentration |

→ **두 시스템은 본질이 다름**. KR 인사이트 그대로 적용 위험.

## 2. ⭐ 시총 30B+ Filter — 핵심 발견

| 시총 cap | 누적 | MDD | vs baseline | wins |
|---|---|---|---|---|
| 시총 무제한 (baseline) | +259.9% | -4.91% | — | — |
| 시총 5B+ | +251.6% | -8.66% | -8.3%p | 9/100 |
| **시총 30B+** ⭐ | **+277.8%** | **-10.28%** | **+17.9%p** | **90/100 ✓✓** |
| 시총 100B+ | +194.7% | -5.30% | -65.2%p | 2/100 |
| 시총 500B+ | +55.3% | -1.73% | -204.6%p | 0/100 |

→ **30B+가 sweet spot** (90/100 wins paired). Robust 우월.

**왜 30B+가 best?**
- < 30B: 시총 너무 작아 변동성·noise ↑
- 30B-100B: mid-large cap, EPS revision 신호 깨끗
- \> 100B: 너무 커서 EPS revision에 시장 미반응

## 3. Slot 확대 — US에선 정반대

| Slot | 누적 | MDD | vs baseline | wins |
|---|---|---|---|---|
| **slot 2 (baseline)** | **+259.9%** | -4.91% | — | — |
| slot 3 equal | +89.2% | -6.54% | -170.7%p | 0/100 ❌ |
| slot 4 equal | +68.2% | -5.96% | -191.8%p | 0/100 ❌ |
| slot 5 equal | +52.5% | -6.50% | -207.4%p | 0/100 ❌ |

→ **KR과 정반대**. US v84의 dynamic 2step_t15 + dd_30_25가 집중 alpha 핵심. 분산하면 강한 picks의 alpha 희석.

## 4. Fragility — KR과 동일 패턴

| 제외 종목 | 누적 | vs full | 의미 |
|---|---|---|---|
| NONE | +259.9% | — | baseline |
| AEIS만 제외 | +267.8% | +7.9%p | 영향 미미 |
| MU 제외 | +217.1% | -42.8%p | 큰 winner |
| SNDK 제외 | +197.1% | -62.8%p | 큰 winner |
| **MU+SNDK** | +160.4% | **-99.5%p** | 두 winner 핵심 |
| Top 4 제외 | +166.4% | -93.5%p | |
| **Top 6 제외** | +168.5% | **-91.4%p** | KR Top 5 -29% 패턴과 동일 |

→ **US도 winner-dependent**. 5-6개 winner가 alpha 절반 견인. 시스템 본질.

## 5. 변경 권고 — 행동 결정

| 변경 | 권장 | 근거 |
|---|---|---|
| **시총 30B+ filter 도입** | ⭐ **검토** | +17.9%p, 90/100 wins |
| Slot 2 → 3/5 확대 | ❌ X | -170~-207%p 손실 |
| KR식 V/Q/G/M 다중팩터 적용 | ❌ X | US는 EPS revision z-score 기반 |
| dd_30_25 그대로 | ✅ 유지 | 본인 BT에서 +8.73%p (제 simulator는 측정 어려움) |
| 2step_t15 그대로 | ✅ 유지 | dynamic weight 효과 |

## 6. 시총 30B+ filter 추가 검증 필요

| 항목 | 검증 |
|---|---|
| 74일 boost regime만 | 약세장 효과 미검증 |
| 30B+ paired wins 90/100 | robust 시사하지만 표본 한계 |
| 100B+에선 -65%p | sensitivity 큼 → 30B threshold 정확? |
| 90/100 vs random noise | 통계적 유의 |
| Adjacent stability (25B+ vs 35B+) | 다음 검증 |

→ **다음 자율주행에서 시총 cap sensitivity sweep + paired BT** 권장.

## 7. KR + US 비교 요약

| | KR | US |
|---|---|---|
| 시스템 본질 | high-turnover momentum surf | dd_30_25 + dynamic concentration |
| 슬롯 최적 | 5/7 분산 | 2 집중 |
| Universe 최적 | KP200+KQ150 결합 | **30B+ filter** |
| Winner-dependent | Top 5 -29% | Top 6 -91%p (US가 더 fragile) |
| Defense | KP MA20<MA80 + cash | SPY MA200 + IEF |

→ **두 시스템은 별개 architecture**. 인사이트 검증 후 선택적 적용.

## 8. 다음 자율주행 후보

1. **🥇 시총 30B+ filter sensitivity** (25/30/35/40B 비교, adjacent stability)
2. 시총 30B+ + dd_30_25 + 2step_t15 결합 시 paired BT
3. 시총 필터 시기별 효과 (약세장 vs 강세장)
4. Universe filter + slot 변형 결합 (slot 2 + 30B+ vs slot 3 + 30B+)

## 산출물

- `research/bt_us_slot_expansion_kr_inspired.py` + `.log`
- 본 보고서

## 한 줄 결론

**KR 인사이트 중 시총 30B+ universe filter가 US에 적용 가능한 새 alpha 후보 (+17.9%p, 90/100 wins). Slot 확대는 US 본질과 충돌. fragility는 KR과 동일 패턴 (winner-dependent).**
