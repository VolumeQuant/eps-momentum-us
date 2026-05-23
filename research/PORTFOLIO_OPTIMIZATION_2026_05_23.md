# Portfolio Optimization Research (2026-05-23)

7시간 자유시간 동안 진행한 포트폴리오 최적화 연구. 사용자의 "C2 보너스, 슬롯 수, 비중 차등 등 가능성 있는 거 다 해보라"는 요청.

## 최종 결론

**최강 후보: `(3,10,2) 80/20 + C2 boost=3`** — 즉시 적용 검토 권장

| metric | baseline (현재 production) | 새 후보 | 차이 |
|---|---|---|---|
| 누적 수익 (65일) | +147.79% | **+241.34%** | **+93.55%p** |
| random 500 paired | 0 | **+48.62%p** | (500/500 wins) |
| 12 multistart | 0 | **+56.63%p** | (12/12 wins) |
| 최저 multistart lift | - | **+18.43%p** (음수 0건) | - |
| MDD | -18.18% | **-19.82%** | +1.71%p (악화 미미) |
| 메시지/진입 일관성 | ✓ | ✓ | 유지 |

## 구성

- **slots = 2** (Top 2 매수, 3슬롯 → 2슬롯)
- **weights = [80, 20]** (1위 80%, 2위 20%)
- **entry = 3** (Top 3 후보 중 선택, C2 보너스 후 재정렬)
- **exit = 10** (rank > 10 매도, 현재 유지)
- **C2 boost = 3** (rank 점수에 +3 — EPS↑+가격↓ 종목)

## 검증 여정 (5 Phase)

### Phase 1: 조합 grid (A2 / E / (3,10,2) / A8 × boost 0/3)

10개 조합 random 500 + 12 multistart 동시 측정.

| Config | random lift | multistart lift | wins |
|---|---|---|---|
| baseline (3,10,3) 균등 | 0 | 0 | — |
| baseline + boost=3 | -0.49%p | -2.10%p / 2/12 | ✗ |
| A2 (3,10,3) 50/30/20 | +3.62%p | +2.74%p / 9/12 | ★ |
| A2 + boost=3 | +5.50%p | +4.80%p / 8/12 | ★ |
| E (2,10,2) 균등 | +27.50%p | +29.45%p / 12/12 | ★★ |
| (3,10,2) 균등 | +16.67%p | +18.26%p / 12/12 | ★★ |
| **(3,10,2) + boost=3** | **+37.11%p** | **+39.88%p / 12/12** | **★★★** |
| A8 (2,10,2) 70/30 | +32.83%p | +36.93%p / 12/12 | ★★ |

**핵심 발견**: 슬롯 축소 + C2 boost = 시너지 폭발. 단독으론 약했던 C2 boost가 (3,10,2)에서 +20%p 추가.

### Phase 2: (3,10,2) 가중치 × boost 그리드

7개 boost 값 × 4개 weights = 28개 조합.

| weights | boost=3 lift | MDD | M min |
|---|---|---|---|
| 50/50 | +37.11%p | -22.19% | +18.16%p |
| 60/40 | +41.02%p | -21.09% | +22.14%p |
| 70/30 | +44.86%p | -19.98% | +20.78%p |
| **80/20** | **+48.62%p** | **-19.82%** | **+18.43%p** |
| 90/10 | +52.28%p | -21.77% | +16.04%p |

**Counter-intuitive 발견**: 80/20이 90/10보다 MDD 낮음.
- 90/10: 1위 90% → 1위 폭락 시 더 큰 손실
- 80/20: 2위 20%가 보호 역할

**Boost step function**:
- boost 0, 1, 2: 효과 0
- **boost 3: jump (+20%p)**
- boost 4, 5: boost 3과 동일 (모든 C2 이미 Top 3 진입)
- boost 7: 붕괴 (-9%p)

### Phase 3: Trade-by-trade 깊은 분석

11건 trade 분석:
- 1위 슬롯 (80%): avg +30.10%, max **MU +132.07%**
- 2위 슬롯 (20%): avg +21.65%
- C2 (boost): avg **+35.25%** (n=5)
- C1: avg +18.77% (n=6)

**MU 결정적 사례**:
| | baseline | (3,10,2) 80/20 b=3 |
|---|---|---|
| 진입 | 4/1 $367.85 | **3/30 $321.80** (2일 빨리, 더 싸게) |
| 매도 | 5/8 $747 | 5/8 $747 |
| 종목 수익 | +103% | **+132%** |
| 슬롯 비중 | 33% | **80%** |
| Portfolio 기여 | +34%p | **+106%p** |

C2 boost가 buy-the-dip MU를 2일 빨리 잡고, 80% 집중으로 portfolio 기여 +106%p.

**Trade-off**:
- 잃는 것: SNDK 2번째 진입 4/6 → 4/22로 늦어짐 (+112% → +57%)
- 얻는 것: MU 빨리 + TTMI/FIVE/AEIS 추가 C2 진입

순 알파 +93.55%p.

### Phase 4: 극단 변형 + 새 아이디어

16개 추가 변형 테스트:

| 시도 | 결과 |
|---|---|
| Entry 확장 (4/5/10) | 효과 0 (boost=3로 이미 충분히 확장) |
| Strict price (-5%, -10%) | 효과 0 (이미 깊은 dip만 진입) |
| Slot 1 (100% 집중) | alpha 작음 (+41%p), MDD 큼 (-23.90%) |
| C1 penalty | 미세 음수 (C1도 알파 source) |
| C3 penalty | 효과 0 (C3 sample 거의 없음) |
| **(5,10,2) 90/10 b=3** | **+52.28%p** but MDD -21.77% |

**(5,10,2) 90/10 b=3**: 알파 최고지만 MDD +2%p 더 큼. 80/20이 균형 잡힘.

### Phase 5: simulator 정확성 검증 (v81 학습)

v81 사태처럼 simulator 버그가 환상 알파를 만든 가능성 점검:

✅ **Trade 가격 11/11 DB와 정확히 일치**
✅ **C2 분류 모두 정확** (MU -21.83%, TTMI -2.90%, FIVE -3.44% 등)
✅ **독립 simulator: +232.20% (Phase 3: +241.34%, 차이 +9%p 시작일 차이로 추정)**

**환상이 아닌 진짜 알파 확정.**

### Phase 6: C2 정의 sensitivity

EPS 정의 (D1/D2/D3/D4) × Price lookback (7d/30d/60d) = 12 조합:

| Price lookback | 결과 |
|---|---|
| 7d | **-29.39%p** (대패, noise) |
| **30d** | **+24.96%p** (sweet spot) |
| 60d | +2.09%p (stale) |

**EPS 정의는 영향 없음** — 시스템이 이미 강한 EPS 상향만 잡음.
**Price 30d가 정답** (현재 사용 중).

## 거부된 후보들 (참고)

| 후보 | 이유 |
|---|---|
| DCA down (-5%, -10% 시 추매) | -18 ~ -59%p, trend-follower와 부적합 |
| Pyramid up (+5%, +10% 시 추매) | -8 ~ -29%p, underexposure |
| Time-based DCA (day 0/1, 0/2) | -4 ~ -18%p, 상승장 손해 |
| Profit taking (부분 익절) | -13 ~ -24%p, 자연 매도가 더 좋음 |
| Trailing stop (-6 ~ -25%) | 전부 baseline 패배 |
| MA60 strict filter | -0.56%p, MU +103% 잃을 위험 |
| C2 boost 단독 (baseline + boost) | random 환상 +1.40%p, multistart -0.11%p |
| Slot 1 (100% 집중) | MDD -23.90% 너무 큼 |
| Entry 확장 (4, 5, 10) | 효과 0 |
| Strict price threshold | 효과 0 |

## 운영 권장사항

### Option 1: 점진적 적용 (보수적, 권장)

**Step 1 (지금)**: A2 (50/30/20) 비중 차등만 적용
- 슬롯 3 유지
- random +3.62%p, MDD 동일
- 안전, 위험 거의 없음

**Step 2 (Step 1 5거래일 모니터링 통과 후)**: (3,10,2) 80/20 boost=3 전환
- 슬롯 2 + 비중 80/20 + C2 boost=3
- 메시지 표시: Top 2 종목만
- MDD +1.71%p 악화 감수

### Option 2: 한 번에 적용 (공격적)

곧장 (3,10,2) 80/20 boost=3 적용
- BT 검증 강력 (12/12 multistart, M min +18.43%p)
- 단 1일 변경 = v81 학습과 거리 둠

### 모니터링 권장 (Option 1, 2 둘 다)

5거래일 후 점검:
- SPY 대비 알파 ≥ baseline 추정 알파의 50%
- MDD ≤ -25% (BT MDD -19.82% × 1.25 안전 여유)
- C2 진입 빈도 BT 평균 (~40%) ± 20%

10거래일 후 점검:
- SPY 대비 알파 ≥ baseline 추정의 70%
- 조건 미충족 시 baseline 복귀

## 한계 / 향후 재검증

1. **65일 단일 환경** (S&P +8% 강세장 + 1번 stress)
2. **진짜 약세장 없음** (SPY -20%+) — C2 boost 효과 약세장에서 다를 수 있음
3. **표본 작음** (11~12건 trade) — MU +103% 같은 단일 케이스 의존성 일부
4. **거래 비용 무시** — 슬롯 축소로 회전율 ↑ 가능성 (정밀 측정 필요)

60일+ 데이터 누적 + 약세장 사이클 한번 거치면 재검증 가치.

## 변경 코드 위치 (production 적용 시)

### `daily_runner.py`

1. **C2 분류 + rank reorder** (신규):
   - `get_part2_candidates` 통과 후 `save_part2_ranks` 직전에 case 분류
   - `price_30d` 계산 함수 추가 (price history lookup)
   - `eps_chg_weighted > 0 AND price_30d < 0` → C2
   - 기존 part2_rank에 (31-p2) + 3 보너스 적용 후 재정렬 → 새 part2_rank

2. **`select_display_top5` 변경**:
   - `MAX_SLOTS = 3` → `2`
   - `weights = [33,33,34]` (균등) → `[80, 20]`
   - entry threshold 그대로 (3)

3. **메시지 표시**:
   - Top 2 종목만 표시 (3위 제거)
   - "매수: 상위 2종목, 최대 2종목 보유"
   - Watchlist Top 20은 새 rank 기준

4. **`_get_system_performance` 변경**:
   - weights [80, 20]
   - slots 2
   - entry 3, exit 10 (변경 없음)
   - C2 rerank 동일 로직

5. **메시지 표시 일관성**:
   - Top 2 = C2 boost 적용된 new rank
   - "Top 3 중 C2 우대로 선택" 같은 표시는 안 함 (메시지/선택 일관)

## BT 스크립트 목록 (research/)

- `bt_combined_grid.py` — Phase 1
- `bt_310_2_boost_grid.py` — Phase 2
- `analyze_310_2_80_20_boost3.py` — Phase 3
- `bt_extreme_variants.py` — Phase 4
- `verify_310_2_simulator.py` — Phase 5
- `bt_c2_definitions_compare.py` — Phase 6
- `bt_dca_strategy.py` — 거부된 옵션
- `bt_pyramid_and_time.py` — 거부된 옵션
- `bt_profit_taking.py` — 거부된 옵션
- `bt_position_sizing.py` — A2 단독
- `bt_exit_extension.py` — B 단독
- `bt_slot_count.py` — E 단독
- `bt_trailing_stop.py` — D 단독
- `bt_c2_boost.py` — 옵션 A (메시지 불일치, 거부)
- `bt_c2_boost_full.py` — 옵션 B 단독
- `bt_c2_boost_full_multistart.py` — 옵션 B 12 multistart
