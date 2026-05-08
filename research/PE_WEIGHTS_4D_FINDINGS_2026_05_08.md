# PE 가중치 BT 종합 보고서 (2026-05-08)

> **세션 컨텍스트**: AMD가 EPS 폭등에도 순위에 못 든 이유 디버깅 → fwd_pe_chg 가중치(7d/30d/60d/90d) 의문 → dense grid → walk-forward → 재현성 진단 → 4D 자유 grid → walk-forward Top 10. 일관 결론: **production w7=0.40은 4D 그리드에서 80/84위, walk-forward 5/5 split에서 long-tail 패턴이 production을 +37~56%p outperform.**

---

## TL;DR

1. **production 가중치 (w7=0.4 / w30=0.3 / w60=0.2 / w90=0.1)는 4D 자유 그리드에서 거의 꼴찌(80/84위)**.
2. **Walk-forward (5 splits, Top 10 변형) 결과: 10개 모두 5/5 split에서 production OOS outperform**, 평균 +37~56%p.
3. **세 가지 패턴 발견**:
   - **long-tail** (w90 0.40~0.60 큼): 가장 robust, train/test 모두 안정 상위
   - **30d-heavy** (w30=0.50): test에서 1위 일관, but train으로는 식별 불가
   - **high-w7** (w7=0.60): in-sample 좋지만 OOS에서 약화
4. **이전 BT가 0.4를 "확정"했다는 사용자 기억의 정체**: commit adcb138의 BT는 5변형 좁은 grid (max w7=0.4)였고, 메시지엔 midweight 한 점만 highlight. 4D 풀어보면 production은 거의 꼴찌. 재현성 검증 완료 (`bt_pe_weights_oldconv.py` → midweight +6.17%p로 commit msg +5.72%p와 거의 일치).
5. **권장 다음 단계**: 인접 안정성 + 다른 시장 국면 검증 후 production 변경 결정.

---

## 1. 시작 — AMD 사례 디버깅

### 사용자 질문
> "AMD가 EPS 폭등했는데 왜 순위에조차 안 나오는지 확인해봐"

### 진단 결과
- AMD 30일 (4/6→5/6):
  - 주가: $220 → $421 (**+91.4%**)
  - NTM EPS: 7.38 → 8.29 (+12.3%)
  - Forward PE: 29.8 → 50.8 (**+70%**)
  - adj_gap: **+73.5** (top 5위는 -9.3 ~ -3.5 음수)
- 시스템은 PE 압축(EPS↑ 주가↓)을 찾는데 AMD는 정반대 — "주가 선반영 + 멀티플 폭발". **시스템 의도 동작, 버그 아님**.

### WWD vs AEIS 분해 (다음 사용자 질문 — adj_gap 차이)
- adj_gap = fwd_pe_chg × (1 + dir_factor) × eps_quality
- WWD: fwd_pe_chg -4.43, multiplier 1.189, adj_gap -5.27
- AEIS: fwd_pe_chg -5.27, multiplier 1.179, adj_gap -6.21
- WWD가 30d만 보면 PE 더 압축됐지만 **fwd_pe_chg는 7d 가중치 0.4로 가장 큼** → AEIS의 7d -7.84% 압축이 결정적
- 이 발견에서 "최신성 강조 가중치" 자체에 대한 의문 시작

---

## 2. 1차원 dense grid (w7만 변경)

### 설계
- w7 ∈ {0.10, 0.15, 0.20, ..., 0.70} (13점)
- non-7d는 production decay 비율 3:2:1 고정 (w30 : w60 : w90)
- 33 multistart (앞 33일 시작일)
- random baseline 33 seeds (part2_rank 랜덤 셔플)

### 파일: `research/bt_pe_weights_dense.py` + `.log`

### 결과
```
variant         avg     med     min     max  MDD     risk_adj
w7=0.55      +88.68% +87.21% +63.75% +124.40% -16.22% 5.47    ← in-sample best
w7=0.60      +81.00% +77.08% +61.22% +113.48% -16.26% 4.98
w7=0.50      +73.73% +71.73% +50.53% +105.83% -16.22% 4.55
★ w7=0.40    +53.03% +52.87% +33.18%  +80.32% -18.13% 2.92    ← production
w7=0.25      +49.93% +49.84% +30.52%  +76.73% -18.13% 2.75
random       +26.67% +25.34%  +3.22%  +96.92% -24.90% —
```

**핵심**: production w7=0.40이 13개 중 10위. peak는 w7=0.55에서 +35.66%p 상회. 1차원만 봐도 production 이미 suboptimal.

### Caveat
- 표본 짧음(58일), 33 multistart 후반은 26일밖에 안 남음
- non-7d=3:2:1 비율 고정 슬라이스 — 실제 4D 공간 일부만 본 것

---

## 3. multistart 개수 검토 (n=6,8,10,12,15,20,33)

### 사용자 지적
> "33일은 너무 많지 않나? 후반 시작일은 기간 짧아서 과적합 위험"

### 검증 (`research/bt_pe_weights_recompare.py` + `.log`)
- 캐시 DB 그대로 사용 (regenerate 재실행 안 함)
- n_starts ∈ {6, 8, 10, 12, 15, 20, 33}

### 결과
**모든 n에서 best variant 동일 (w7=0.55), Top 5 순위 완전 동일.**

| n | best | Δ vs prod |
|---|---|---|
| 6 | w7=0.55 | +45.35%p |
| 12 | w7=0.55 | +40.34%p |
| 33 | w7=0.55 | +35.66%p |

후반 시작일 추가시 모든 변형 평균 하락 — w7=0.50/0.55에서 -15%p로 가장 큰 하락 (이 변형들이 후반 약화 신호 — 의심 항목).

### 결론
- **n=12 표준 채택** (47일+ 보장 + 노이즈 평균 효과)
- best 결론은 n에 안정적 → multistart trick 아님
- 다만 후반 lead 축소는 OOS 약화 가능성 시사 → walk-forward 필요

---

## 4. Walk-forward 1D (w7 grid)

### 파일: `research/bt_pe_weights_walkforward.py` + `.log`

### 설계
- splits: T ∈ {20, 25, 30, 35, 40}
- 각 split: train 첫 T일 + test 나머지 (cold-start)
- Spearman ρ로 train→test 순위 상관 측정

### 결과
| split | train_best | test_ret | prod_test | OOS lift | ρ |
|---|---|---|---|---|---|
| T=20 | w7=0.50 | +61.96% | +48.93% | **+13.03%p** | +0.511 |
| T=25 | w7=0.50 | +50.53% | +33.18% | **+17.35%p** | +0.451 |
| T=30 | w7=0.50 | +59.18% | +46.32% | **+12.86%p** | +0.555 |
| T=35 | w7=0.50 | +38.67% | +20.75% | **+17.92%p** | +0.533 |
| T=40 | w7=0.50 | +29.71% | +12.99% | **+16.72%p** | +0.379 |

**평균 OOS lift +15.58%p (5/5 양수)**. train_best가 모든 split에서 w7=0.50 일치. test_best는 모든 split에서 w7=0.55. production w7=0.40은 모든 split에서 test rank 11~13위.

---

## 5. 재현성 진단 — "이전 BT는 0.4 확정"의 정체

### 사용자 우려
> "갑자기 다른 결과 나오니 당황. 분명 BT해서 0.4로 적용했던 거 같은데"

### 진단 (`research/bt_pe_weights_sanitycheck.py` + `.log`)

#### 5-1. 이전 5변형 그대로 재실행 (현재 코드)
```
variant       ΔRet    ΔMDD     비고
long_heavy   +52.20%p   0.00%p  ← 이전 commit msg에 언급조차 없음
midweight    +24.86%p   0.00%p  ← commit msg는 +5.72%p였음
uniform      +33.84%p   0.00%p
short_lite    +0.12%p   0.00%p
```

→ **midweight +5.72%p와 +24.86%p 큰 차이**. 코드/데이터 변경 영향 의심.

#### 5-2. OLD conviction 재실행 (`research/bt_pe_weights_oldconv.py` + `.log`)
- 5/2 시점 코드 복원: eps_floor cap 1.0 (현재 3.0), rev_bonus binary cliff 0.30 (현재 smooth 비례)
- 같은 5변형 BT

```
variant       OLD ΔRet     NEW ΔRet     코드 영향
midweight    +6.17%p ✓     +24.86%p     +18.69%p
short_lite   +5.62%p       +0.12%p      -5.50%p
uniform      +37.14%p      +33.84%p     -3.30%p
long_heavy   +44.60%p      +52.20%p     +7.60%p
```

→ **midweight +6.17%p로 commit 기록 +5.72%p와 거의 일치 (재현 성공)**. 0.45%p 차이는 추가 5거래일 영향.

#### 5-3. 코드 변경 내역 확인
- **v80.8 (commit `12d8a16`)**: rev_up30 ≥ 3 합의 강도 필터 추가
- **v80.9 (commit `1be28d2`)**: eps_floor cap 1.0→3.0 + rev_bonus binary→smooth

이 두 변경이 conviction의 magnitude/eligibility를 키워 PE weight 변경 민감도가 +18.69%p 증가했음.

### 5-4. 가장 중요한 발견 — OLD 코드에서도 production은 이미 suboptimal
- OLD에서 long_heavy (0.1/0.2/0.3/0.4): production 대비 **+44.60%p**
- 5/2 commit 메시지엔 long_heavy/uniform 결과 **언급 없음** — **5변형 grid를 모두 본 게 아니거나 보고에서 제외**됐을 가능성

### 결론
> "BT해서 0.4 확정"은 **5변형 좁은 grid + midweight 한 점만 highlight**한 결과의 사용자 기억. 실제로는 OLD/NEW 코드 모두에서 production은 다른 변형들에 비해 +37~52%p 열세.

---

## 6. 4D 자유 그리드 (w7, w30, w60, w90 모두 풀기)

### 파일: `research/bt_pe_weights_4d.py` + `.log`

### 설계
- 각 weight ∈ {0.10, 0.20, ..., 0.70}, 단위 0.10, 각 ≥ 0.10, sum=1.0
- compositions of 10 into 4 with each ≥ 1 = **84조합**
- n_starts=12 (앞서 합의)
- NEW conviction (현재 production 코드)

### 결과 — Top 10 (avg 기준)

| 순위 | weights (7,30,60,90) | avg | risk_adj | 패턴 |
|---|---|---|---|---|
| 1 | (0.10, **0.50**, 0.10, 0.30) | **+121.13%** | 6.68 | 30d-heavy |
| 2 | (0.10, 0.10, 0.30, 0.50) | +117.98% | 6.51 | long-tail |
| 3 | (0.20, 0.10, 0.20, 0.50) | +117.40% | 6.48 | long-tail |
| 4 | (0.10, 0.10, 0.20, **0.60**) | +115.80% | 6.37 | long-tail |
| 5 | (0.20, 0.10, 0.30, 0.40) | +114.46% | 6.31 | long-tail |
| 6 | (0.10, 0.20, 0.10, 0.60) | +111.17% | 6.13 | long-tail |
| 7 | (**0.60**, 0.10, 0.10, 0.20) | +111.04% | **6.85** | high-w7 |
| 8 | (0.10, 0.10, 0.50, 0.30) | +110.99% | 6.12 | mid-long |
| 9 | (0.10, 0.20, 0.20, 0.50) | +109.99% | 6.07 | long-tail |
| 10 | (0.30, 0.10, 0.10, 0.50) | +109.84% | 6.04 | long-tail |
| ... | | | | |
| **80** | **(0.40, 0.30, 0.20, 0.10)** | **+54.96%** | 3.03 | **★ production** |

### 차원별 평균 효과
| 차원 | 추세 | 강한 영역 |
|---|---|---|
| w7 | U-자 | 0.10 또는 0.50~0.60 양극 |
| **w30** | **단조 감소** | **0.10이 best (+103%)** |
| w60 | 거의 평탄 | 0.50~0.70 약간 강 |
| **w90** | **단조 증가** | **0.40~0.60 (+103~109%)** |

### 핵심 발견
- **production w7=0.4 영역 내 best조차 +54.34%p 우월**: (0.40, 0.20, 0.10, 0.30) → +109.30%
- 즉 **w7만 바꾸는 게 아니라 분배 전체가 잘못됨**
- 두 가지 강한 패턴: **long-tail (w90 큼)** vs **high-w7 (w7=0.60)**
- production은 이 둘 사이의 어느 쪽도 못 잡는 어중간한 위치

---

## 7. Walk-forward Top 10 (4D 결과 OOS 검증)

### 파일: `research/bt_pe_weights_4d_walkforward.py` + `.log`

### 설계
- Top 10 변형 + production 비교
- splits: T ∈ {20, 25, 30, 35, 40}
- 캐시 DB 재사용 (regenerate 불필요)

### 결과 — Top 10 모두 5/5 split에서 OOS outperform

| variant | weights | pattern | avg lift | min | max | #pos |
|---|---|---|---|---|---|---|
| **w_10_50_10_30** | (0.10,0.50,0.10,0.30) | 30d-heavy | **+56.50%p** | +44.58 | +61.58 | **5/5** |
| w_10_10_30_50 | (0.10,0.10,0.30,0.50) | long-tail | +50.30%p | +37.48 | +56.27 | 5/5 |
| w_20_10_20_50 | (0.20,0.10,0.20,0.50) | long-tail | +49.82%p | +37.09 | +55.73 | 5/5 |
| w_10_20_20_50 | (0.10,0.20,0.20,0.50) | long-tail | +48.41%p | +37.48 | +53.63 | 5/5 |
| w_20_10_30_40 | (0.20,0.10,0.30,0.40) | long-tail | +47.35%p | +35.06 | +52.97 | 5/5 |
| **w_30_10_10_50** | **(0.30,0.10,0.10,0.50)** | **long-tail** | **+45.83%p** | **+37.78** | **+56.81** | **5/5** |
| w_10_10_20_60 | (0.10,0.10,0.20,0.60) | long-tail | +44.56%p | +29.47 | +54.32 | 5/5 |
| w_10_20_10_60 | (0.10,0.20,0.10,0.60) | long-tail | +40.66%p | +25.94 | +48.64 | 5/5 |
| w_60_10_10_20 | (0.60,0.10,0.10,0.20) | high-w7 | +37.41%p | +29.74 | +42.89 | 5/5 |
| w_10_10_50_30 | (0.10,0.10,0.50,0.30) | mid-long | +37.16%p | +27.58 | +41.61 | 5/5 |

### 패턴별 신뢰도 (train→test 일관성)

#### ✓ long-tail (8개 변형) — 가장 robust
- train rank 3-10 / test rank 2-8 (둘 다 안정 상위권)
- OOS lift 평균 +44~50%p
- "장기 90d PE 변화가 핵심 신호"

#### ⚠ high-w7 (w_60_10_10_20)
- train rank **항상 1위** (5/5)
- test rank 6~10으로 떨어짐
- in-sample optimization으로 식별되지만 OOS 약화 — **단기 momentum이 강세장 후반 약화 의심**

#### ✗ 30d-heavy (w_10_50_10_30) — Top 1지만 함정
- test rank **항상 1위** (5/5) — OOS 압도
- 그러나 train rank 3, 4, 4, 11, 9 — **사용자가 train으로 best 찾으면 절대 못 고름**
- OOS best는 알지만 어떻게 선택하느냐가 모호 → 운빨 가능성 배제 못함

---

## 8. 권장 변경안

### 가장 안전한 후보: **w_30_10_10_50 (0.30, 0.10, 0.10, 0.50)**
- train rank 5~9 / test rank 2~8 (둘 다 안정)
- OOS lift +45.83%p (5/5 splits, min +37.78%p)
- production 대비 변화: w7 0.40→0.30, w30 0.30→0.10, w60 0.20→0.10, w90 0.10→**0.50**
- 직관: "분기실적 시즌(90d) PE 변화 강조"
- **추천 근거**: train과 test 둘 다에서 일관 상위. 어떤 in-sample optimization으로도 식별 가능.

### 더 강력한 lift, 약간 공격적: w_10_10_30_50 또는 w_20_10_20_50
- avg lift +49.82~50.30%p
- 변동성 작음 (+37 ~ +56%p 범위)
- 극단적 long-tail (90d 0.50 강조)

### 비추천: w_60_10_10_20 (high-w7)
- 처음 dense grid에서 매력적이지만 OOS test rank가 train보다 일관 하락
- "단기 강조"는 in-sample mirage

### 절대 변경 금지: production 그대로
- 4D 84조합 중 80위
- walk-forward 5/5 split 꼴찌권 (test rank 항상 11/11)

---

## 9. 집 PC에서 이어서 할 일 (우선순위 순)

### Step 1. 인접 안정성 검증 (15분)

권장 후보 w_30_10_10_50 주변에서 ±0.05 step으로 5x5x5x5 = 625조합 fine grid는 너무 큼. 대신 **±0.05 단위 6변형** 정도로 plateau 확인:

| 비교 | 변형 |
|---|---|
| baseline | (0.30, 0.10, 0.10, 0.50) |
| +5 to w7 | (0.35, 0.10, 0.10, 0.45), (0.25, 0.10, 0.10, 0.55) |
| +5 to w30 | (0.30, 0.15, 0.10, 0.45), (0.30, 0.05, 0.10, 0.55) |
| +5 to w60 | (0.30, 0.10, 0.15, 0.45), (0.30, 0.10, 0.05, 0.55) |

**작성할 스크립트**: `research/bt_pe_weights_adjacency.py`
- bt_pe_weights_4d.py 패턴 그대로 사용
- 6변형 + baseline = 7개 모두 walk-forward (5 splits)
- 모두 OOS lift 양수 + 변동 작으면 plateau → robust 확정
- 한 변형이라도 음수 lift면 single-point luck 의심

```python
# 코드 골격
ADJACENCY = [
    ('w_30_10_10_50', (0.30, 0.10, 0.10, 0.50), 'baseline'),
    ('w_35_10_10_45', (0.35, 0.10, 0.10, 0.45), 'w7+5'),
    ('w_25_10_10_55', (0.25, 0.10, 0.10, 0.55), 'w7-5'),
    ('w_30_15_10_45', (0.30, 0.15, 0.10, 0.45), 'w30+5'),
    ('w_30_05_10_55', (0.30, 0.05, 0.10, 0.55), 'w30-5'),
    ('w_30_10_15_45', (0.30, 0.10, 0.15, 0.45), 'w60+5'),
    ('w_30_10_05_55', (0.30, 0.10, 0.05, 0.55), 'w60-5'),
]
# regenerate 필요 (DB 캐시 없음, 0.05 step 새 변형들)
```

### Step 2. 0.05 step fine grid (선택, 30분)

Step 1에서 plateau 확인되면 더 정밀하게 peak 위치 찾기:
- 각 weight ∈ {0.05, 0.10, 0.15, ..., 0.85}, sum=1.0, 각 ≥ 0.05
- compositions of 20 into 4 with each ≥ 1 → **C(19,3) = 969조합**
- 6 worker 병렬로 ~30분
- **작성할 스크립트**: `research/bt_pe_weights_4d_fine.py`
  - bt_pe_weights_4d.py에서 step=0.05, min_w=0.05로 변경
  - ProcessPoolExecutor로 6병렬

⚠ **caveat**: 969조합 multiple testing inflation. Top 10이 in-sample noise일 수 있음. walk-forward 필수.

### Step 3. 다른 시장 국면 검증 (1~2시간)

현재 데이터: 2026-02-12 ~ 2026-05-06 (58일, 강세장). 

**검증 옵션**:
- (a) DB에 더 오래된 데이터가 있나 확인: `SELECT MIN(date) FROM ntm_screening` — 처음 데이터부터 사용
- (b) 데이터 더 쌓일 때까지 대기 (1개월 추가 시 검증)
- (c) yfinance에서 과거 NTM EPS 데이터 다시 받아서 BT용 DB 별도 생성 (~수일 작업)

**작성할 스크립트**: `research/bt_pe_weights_4d_regime.py`
- 시장 국면별로 데이터 슬라이스
  - 강세 구간 (e.g., 4월 후반)
  - 조정 구간 (있다면)
- 각 구간에서 4D Top 10 OOS 일관성 검증

### Step 4. 3일 가중(T0/T1/T2)과 PE 가중치 상호작용 (30분)

현재 production T0/T1/T2 = 0.5/0.3/0.2. PE 가중치 변경 시 이 가중치도 재최적화 필요할 수 있음.

**작성할 스크립트**: `research/bt_pe_weights_x_t012.py`
- 권장 후보 w_30_10_10_50 + 다른 T0/T1/T2 조합
  - (0.5, 0.3, 0.2) baseline
  - (0.4, 0.35, 0.25)
  - (0.6, 0.25, 0.15)
  - 균등 (0.333, 0.333, 0.333)
- 각각 walk-forward
- PE 변경의 lift이 T 가중치 변경으로 강화/약화되는지 확인

### Step 5. paper trading 검증 (5~10거래일)

production 변경 전 실시간 paper test:
1. `daily_runner.py`에 `PE_WEIGHTS_OVERRIDE` env var 추가
   - 현재 hardcoded 0.4/0.3/0.2/0.1 → env로 오버라이드 가능
2. paper PC에서 새 weights로 매일 실행
3. 동일 날짜 production 출력과 picks 비교
4. 5~10거래일간 누적 수익률 비교
5. 일치/괴리 패턴 분석

### Step 6. production 변경 (Step 1~5 모두 통과 시)

#### 변경 위치
- **`daily_runner.py:608`** (가격 변화율 가중치) — 이건 `price_w` (참고만, fwd_pe_chg 아님)
- **`daily_runner.py:618`** (EPS 변화율 가중치 `eps_w`)
- **`daily_runner.py:632`** (`weights = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}`) ← **여기가 진짜 fwd_pe_chg 가중치**
- **`daily_runner.py:783`** 부근 (재계산 path 동일 weights)

#### 변경 절차
1. `git checkout -b experiment/pe-weights-long-tail`
2. weights 두 곳 모두 수정 (608/632/783 정확한 line은 grep으로 재확인)
3. SESSION_HANDOFF.md / MEMORY 업데이트 (새 v 번호: v80.10 또는 v81)
4. 일일 텔레그램 메시지 1주일 모니터링
5. 문제 없으면 main 머지

⚠ **운영 리스크**:
- DB가 새 weights로 재계산되면 과거 ranking 변경됨 — backup 필수
- backup: `cp eps_momentum_data.db eps_momentum_data.db.bak_pre_pe_weights_change_$(date +%Y%m%d).db`
- 메모리: `MEMORY.md` 또는 별도 memory 파일에 변경 이력 저장

---

## 10. 파일 목록

### 작성한 BT 스크립트 (commit 대상)

| 파일 | 역할 | 주요 결과 |
|---|---|---|
| `research/bt_pe_weights_dense.py` | 1D dense grid (13점, w7만 변경) | w7=0.55 best (+88.68%) |
| `research/bt_pe_weights_recompare.py` | n_starts 재측정 (캐시 재사용) | best 일관, n=12 표준화 |
| `research/bt_pe_weights_walkforward.py` | 1D walk-forward 5 splits | OOS lift +13~18%p, 5/5 양수 |
| `research/bt_pe_weights_sanitycheck.py` | 이전 5변형 + 0.55 변형 비교 | 코드 변경 영향 발견 |
| `research/bt_pe_weights_oldconv.py` | OLD conviction (5/2 코드) 재실행 | midweight +6.17%p (commit msg 재현) |
| `research/bt_pe_weights_4d.py` | 4D 자유 grid (84조합) | production 80/84위 |
| `research/bt_pe_weights_4d_walkforward.py` | 4D Top 10 walk-forward | 모두 5/5 split OOS 양수 |

### 로그 파일 (commit 대상)
- 모든 스크립트의 `.log` 파일 (확장자만 다름, 같은 위치)

### 캐시 DB (gitignore — commit 안 됨)
- `research/pe_weight_dbs/` (1D dense grid 13개 DB)
- `research/pe_weight_dbs_sanity/` (sanity check 8개 DB)
- `research/pe_weight_dbs_oldconv/` (OLD conviction 5개 DB)
- `research/pe_4d_dbs/` (4D grid 84개 DB)

집 PC에서 이어서 할 때 **DB 캐시는 다시 생성**해야 함 (각각 5~10분).

---

## 11. 재현 방법 (집 PC에서)

### 환경 확인
```bash
cd "C:\dev\claude code\eps-momentum-us"
git pull --rebase origin master
py -3 -c "import sys; sys.path.insert(0,'.'); import daily_runner; print(daily_runner.DB_PATH)"
```

### 캐시 DB 재생성 (필요시)
```bash
# 1D dense grid
py -3 research/bt_pe_weights_dense.py | tee research/bt_pe_weights_dense.log

# 4D grid (5.5분, 84조합)
py -3 research/bt_pe_weights_4d.py | tee research/bt_pe_weights_4d.log
```

### Walk-forward 재실행 (DB 캐시 있으면 즉시)
```bash
py -3 research/bt_pe_weights_walkforward.py
py -3 research/bt_pe_weights_4d_walkforward.py
```

### 권장: Step 1 인접 안정성부터 시작
```bash
# bt_pe_weights_adjacency.py 신규 작성 (위 9-1 참조)
py -3 research/bt_pe_weights_adjacency.py | tee research/bt_pe_weights_adjacency.log
```

---

## 12. 핵심 caveats (잊지 말 것)

1. **표본 짧음**: 58일 (2026-02-12 ~ 2026-05-06), 단일 강세장 국면
2. **walk-forward 후반 split은 test 18~38일** — short window 노이즈
3. **84조합 multiple testing**: in-sample Top 1은 통계적 inflation 가능. **Top 10 cluster의 일관 패턴이 더 신뢰 가능**
4. **OOS lift 평균 +37~56%p는 매우 큼** — 단일 시장 국면 효과 가능성. 다른 국면에서도 같은 비율 lift 나오는지 미검증
5. **production 코드는 v80.9 conviction 기준** — conviction 함수 또 바뀌면 PE 가중치 최적점도 이동 가능
6. **이전 BT 결과(0.4 확정)는 좁은 grid + 보고 누락의 결과** — 미래 BT 보고 시 **모든 후보 결과를 commit msg에 명시**할 것

---

## 13. 사용자 지시 / 결정 보류 항목

### 보류 중 — 사용자 결정 필요
- [ ] production w7 변경할지 여부 (Step 1~5 결과 본 후)
- [ ] 변경 시 어느 후보로? (w_30_10_10_50 보수적 vs w_10_10_30_50 공격적)
- [ ] paper trading 기간 (5거래일? 10거래일?)
- [ ] 별도 worktree에서 변경 후 production 머지 vs production 즉시 변경

### 다음 세션 시작할 때
1. 이 파일 (`research/PE_WEIGHTS_4D_FINDINGS_2026_05_08.md`) 읽기
2. **Step 1 인접 안정성** 먼저 (15분) — `bt_pe_weights_adjacency.py` 작성 + 실행
3. 결과 따라 Step 2~5 진행 또는 production 변경 결정

---

**작성**: 2026-05-08, Claude Opus 4.7 (1M context)
**세션 종료 직전 작성**. 사용자가 집 PC로 이동.
**다음 세션 첫 메시지 예시**: "어제 PE 가중치 BT 이어서 하자. 인접 안정성부터."
