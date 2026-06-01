# Cycle Overlay 자율연구 보고서

**일시**: 2026-06-01 (자율주행 세션)
**목적**: 경기 사이클 사계절 framework를 US EPS Momentum 시스템(v84)에 통합해 성과 개선 방법 탐색
**검증 기간**: 2026-02-12 ~ 2026-05-29 (74거래일, 100% boost regime)
**검증 방법**: entry_fixed simulator (total_cash pool 모델), 100 seeds × 3 samples paired BT

## TL;DR

**9개 가설 검증 결과 — 시스템 v84는 이 표본에서 거의 ceiling. 추가 overlay는 모두 marginal 또는 음수.**
사이클 사계절 framework는 **이론적으로 합리**하지만 **단일 boost 기간 BT에선 효과 검증 불가**. 약세장 데이터 누적 후 재검증 필요.

## 1. 가설 검증 결과 종합

| # | 가설 | avg_lift | wins | 평가 |
|---|---|---|---|---|
| baseline | v84 (dd_30_25 + 2step_t15 dynamic) | 0 | — | ★ ceiling |
| H_SL_-10 | 개별 종목 -10% 손절 | -9.42%p | 0/100 | ✗ 열세 (whipsaw) |
| H_SL_-15 | -15% 손절 | 0%p | 5/100 | trigger 안 됨 |
| H_SL_-20 | -20% 손절 | 0%p | 5/100 | trigger 안 됨 |
| H_TechCap50 | Tech sector 50% 상한 | -4.94%p | 0/100 | ✗ 열세 |
| H5_HighConvOnly | 2step gap≥15만 진입 (gap<15 시 cash) | -11.09%p | 21/100 | ✗ 열세 |
| H6_revup≥5 | 분석가 합의 임계 ↑ | -0.47%p | 2/100 | ~ 동등 |
| H6_revup≥10 | 더 strict | -21.84%p | 12/100 | ✗ 열세 |
| H7_cash20 | 항시 cash 20% buffer | -21.67%p | 0/100 | trade-off |
| H7_cash30 | 항시 cash 30% buffer | -32.51%p | 0/100 | trade-off |
| H_static90_10 | v83.3 static 90/10 (no dynamic) | +8.92%p | 79/100 | ✓✓ but caveat ↓ |

## 2. 5가지 핵심 발견

### 발견 1: SL은 강세장에서 winner를 잘라낸다

- SL -10%: 평균 -9.4%p 손실. SNDK가 보유 중 -10.7% 빠졌다가 +27.6%로 청산된 사례 등 winner cut.
- SL -15/-20%: 이 기간 그렇게 큰 단일종목 손실 없어서 trigger 0회.
- **권장: 개별 SL 도입 안 함.** v84 exit rule (rank>10 또는 min_seg<-2%)가 이미 효과적.

### 발견 2: AI 활황기 Tech cap 디버시는 알파를 깎는다

- Tech 57% 비중 → 50% 상한 강제 = -4.94%p 손실.
- 사이클 사계절 framework로 "여름엔 Tech 비중 ↓" 추천하지만 **현재 표본에선 AI capex 활황으로 Tech가 알파 원천**.
- **권장: 사이클 기반 sector tilt는 약세장 진입 신호 명확할 때만**. 무차별 cap은 손해.

### 발견 3: High-conviction filter는 Sharpe 개선 (Calmar 비슷)

- H5 (gap≥15만 진입, gap<15 시 cash): avg return -11%p but **Sharpe 1.66→2.11 (+0.45)**.
- MDD 동일. Calmar 비슷 (baseline 4.52 vs H5 4.06).
- **잠재 가치**: 약세장에서 cash 보유 시간 증가 → drawdown 보호. **하지만 강세장 알파 손실 명확.** 표본 1개로는 채택 불가.

### 발견 4: 현재 cash buffer 룰 (20%)이 정직한 절충

- H7 cash 20%: return -22%p, MDD -19% (baseline -24% 대비 +5%p 개선)
- H7 cash 30%: return -33%p, MDD -17% (+7%p 개선)
- H7 cash 10%: return -11%p, MDD -22%
- **trade-off 명확**: cash% 늘릴수록 보험비 ↑.
- **현재 프로젝트 룰 (US 80:20, KR 70:30)이 합리적 절충.** 30%까지 늘릴 가치는 보이지 않음 (강세장 한정 BT).

### 발견 5: dynamic weight (2step_t15) vs static 90/10 — simulator 가정 차이

- 본인 채택 BT (bt_weight_grid_entry_fixed, slot별 cash 모델): 2step_t15 + dd_30_25 결합이 90/10 baseline 대비 +11.45%p
- 내 BT (total_cash pool 모델, dd_30_25 미포함): static 90/10이 2step_t15 대비 +8.92%p 우월
- **두 simulator 모두 production과 다른 가정**일 가능성. simulator 가정 검증 또 한번 필요.

→ **권장**: dd_30_25 filter가 진짜 알파. 2step_t15 dynamic vs static은 simulator 가정에 sensitive. **production 정확한 동작 grep 필요** ([[feedback_simulator_production_mismatch]] 강화).

## 3. 사이클 framework 검증 한계

이 BT의 본질적 한계:

| 한계 | 영향 |
|---|---|
| 74거래일 단일 표본 | 사이클 transition 0회 |
| 100% boost regime | 봄/여름/가을/겨울 다 검증 불가 |
| AI capex 활황기 | tech tilt가 알파 — cycle penalty 부정적 |
| VIX 데이터 못 받음 | 동적 cash buffer (L3) 검증 못 함 |
| 약세장 데이터 없음 | SL 효과 양방향 측정 불가 |

**즉**: 이 표본만으로는 "사이클 framework가 시스템에 도움되는지" 측정 못 함. 약세장 1회 이상 포함 BT 데이터 필요.

## 4. 권장 사항

### 즉시 적용 — 없음

현재 시스템 (v84 + cash buffer + L1 regime overlay)이 이 표본의 거의 ceiling. 변경 권장 없음.

### 약세장 데이터 누적 후 재검증할 것

1. **H5 (HighConvOnly)** — Sharpe 우위 robust한지 약세장 포함 BT
2. **H7 cash20** — 약세장에서 trauma 보호 효과 정량화
3. **사이클 sector tilt** — fall/winter 신호 발현 시 tech ↓ / 방어 ↑

### 다음 검증 후보 (H8~H10 — future work)

| # | 가설 | 검증 데이터 필요 |
|---|---|---|
| H8 | 약세장 1회 포함 BT (2022 데이터 reconstruction) | DB 확장 |
| H9 | 가격 momentum 추가 신호 (mom_20_z) — KR v80.20 패턴 | 가격 시계열 충분 |
| H10 | EPS revision speed (revisions/day) 추가 | 시계열 데이터 |
| H11 | AI cycle peak 신호 trigger 시 tech reduce — NVDA YoY <50% | 분기별 NVDA 데이터 |
| H12 | sector momentum (sector ETF return)로 사이클 ranking | sector ETF 가격 |

## 5. 결론

**경기 사이클 사계절 framework는 retail 투자자에게 지적으로 매력적이지만, 강세장 단일 표본 BT에서 EPS Momentum 시스템에 통합 시 알파 X 또는 음수.**

이게 의미하는 바:

1. **시스템(L0)이 이미 sector rotation을 EPS revision channel로 자동 수행**. 사이클 awareness가 별도 layer 불필요.
2. **약세장 검증 부재**가 결정적 limit. 사이클 framework의 진짜 가치는 약세장 회피인데 검증 못함.
3. **AI cycle override 현실**: 2023~2026 AI capex 활황으로 전통 사이클 신호가 시장 lead 못 함.
4. **L3 (monthly macro monitor) overlay는 검증 안 됐지만 cost 거의 0**: 매월 1시간 신호 체크 → 동시 발현 시 cash↑. 약세장 진입 시 효과 가능성 + boost 기간 비용 0.
5. **사이즈 작게 + Buffett 13F follow (L4) 가능**: 시스템과 무관한 18-36개월 horizon 베팅. 검증 못 했지만 사이즈 5%면 portfolio 영향 제한.

## 6. 추가 BT — H9: Price momentum filter (KR v80.20 패턴 application)

KR에서 검증된 mom_10 + vol_low 신팩터 가산을 US 버전으로 단순화 — `mom_X > threshold` filter (음수 momentum 종목 reject).

| 변형 | avg ret | MDD | Sharpe | Calmar | paired vs baseline |
|---|---|---|---|---|---|
| baseline_v84 | +108.36% | -23.90% | 1.66 | 4.52 | — |
| **H9_mom10>0** | +108.48% | **-30.31%** | 1.94 | 3.58 | +0.11%p, **62/100 ✓** |
| H9_mom10>0.02 | +107.82% | -28.70% | 1.96 | 3.76 | -0.54%p, 54/100 |
| **H9_mom20>0** | +91.16% | **-16.61%** ✓ | **2.20** | **5.49** ✓ | -17.20%p, 21/100 |
| H9_mom20>0.05 | +89.82% | -17.25% | **2.32** ✓ | 5.21 | -18.55%p, 24/100 |

### H9 발견

**mom_20 > 0 filter**:
- **MDD -23.9% → -16.6% (+7%p 개선)** ← 가장 강력한 개선
- Sharpe 1.66 → 2.20 (+0.54)
- Calmar 4.52 → 5.49 (+21%)
- BUT absolute return -17%p 손실
- → **risk-adjusted 우월, return ↓**. 사용자 저-MDD 철학과 정합.

**mom_10 > 0 filter** (가벼운):
- return 거의 동일 (+0.11%p)
- Sharpe 향상 (+0.28)
- BUT MDD 악화 (-23.9% → -30.3%)
- → 미묘한 결과, paired wins 62/100 (weak ✓)

### H9 종합 평가

**약세장 가정 시 가장 promising 후보**. 강세장 표본만으로는 return cost 큼 (-17%p) but 약세장에서 MDD -7%p 보호는 정확히 사용자가 원하는 것.

**채택 권고**: ⚠️ **약세장 데이터 1회 이상 포함 BT 후 결정**. 강세장만 보고 채택하면 alpha 17%p 손실 risk.

## 7. 다음 자율주행 세션에서 시도할 것

1. **약세장 reconstruction (최우선)**: yfinance 2022 데이터로 EPS revision 시계열 복원 → H9_mom20 약세장 BT
2. H10: EPS revision speed (revisions/day acceleration)
3. H11: AI cycle peak 신호 trigger (NVDA YoY <50%)
4. H12: sector momentum (sector ETF return) — 사이클 ranking

---

## 부록: 실행 파일

- `research/bt_cycle_overlay_hypotheses.py` — v2 (SL, sector cap)
- `research/bt_cycle_overlay_v3.py` — v3 (H5, H6, H7)
- `research/bt_cycle_overlay_results.log` — v2 결과
- `research/bt_cycle_overlay_v3.log` — v3 결과
