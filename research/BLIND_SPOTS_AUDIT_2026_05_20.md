# Blind Spots Audit (2026-05-20)

v81 롤백 다음날 production 코드 맹점 전수 점검 보고서.

## 요약

| 항목 | 결과 | commit |
|---|---|---|
| Tier 1 #1: line 4088 X8 → X10 | ✅ fix | `6d71491` |
| A: day_ret divisor 동형 버그 | ✅ fix (예방적) | `1301577` |
| Tier 2 #2: 3일 검증 K=3 | 검증 후 변경 X | - |
| Tier 2 #3: 어닝 차단 | 검증 후 변경 X | - |
| Tier 2 #4: MA60 fallback / MA60 경고 | 검증 후 변경 X | - |
| Tier 2 #5: Score 100 압축 | 검증 후 변경 X | - |

## 확실한 버그 fix (2건)

### 1. line 4088 X8 → X10 (commit `6d71491`)

**v80.10b (5/11) 업데이트 누락분.** `_get_system_performance` 누적 수익률 계산에서 exit threshold가 X8 (v78 옛값) 그대로 남아있음.

- production strategy = X10 (v80.10b commit 메시지 5곳 명시: `2903, 4381, 4660, 4699, 4724`)
- 표시 시뮬레이션 = X8 (line 4088 누락) → 표시 숫자만 부정확

**측정 (5/19 production DB)**:

| | X8 (이전) | X10 (fix) | 차이 |
|---|---|---|---|
| 시스템 누적 | +117.23% | **+125.10%** | +7.87%p |
| 알파 (vs SPY) | +109.77%p | +117.64%p | +7.87%p |
| wins/losses | 10/0 | 9/0 | X10이 1번 덜 매도 |

메모리 v80.10b BT 예측 (+7.08%p)와 거의 일치. **검증 스크립트**: `verify_x8_x10_perf.py`.

### 2. day_ret divisor 동형 버그 fix (commit `1301577`)

bt_breakout_hold OLD simulator의 pool-exit price masking 패턴이 production 코드에도 존재.

```python
# Before (line 4060-4066)
day_ret = 0
if portfolio:
    w = 1.0 / len(portfolio)  # ← 보유 수로 나눔 (가격 누락 종목 포함)
    for tk in portfolio:
        cur = prices.get(tk)
        prev = prev_prices.get(tk)
        if cur and prev and prev > 0:
            day_ret += w * (cur - prev) / prev * 100

# After
day_ret = 0
if portfolio:
    n_valid = 0
    for tk in portfolio:
        cur = prices.get(tk)
        prev = prev_prices.get(tk)
        if cur and prev and prev > 0:
            day_ret += (cur - prev) / prev * 100
            n_valid += 1
    if n_valid > 0:
        day_ret /= n_valid
```

production 65일 데이터에서는 실제 가격 누락 0건이라 영향 +0%p. **예방적 fix** (universe rotation / delisting 대비). 의의: v81 발견 후 같은 패턴이 production 코드에도 1건 더 있었음 → 신규 코드 추가 시 표준 강제.

## 검증 후 변경 안 함 (5건)

### 3. 3일 검증 K=3 sliding window

**code**: `daily_runner.py:1731` `get_3day_status`. 최근 3거래일 모두 `part2_rank IS NOT NULL` → ✅.

**의문**: 한번 빠지면 reset되나? K=3 sweet spot인가?

**BT** (`bt_consecutive_K.py`):
- random 500 K=1/2/3/4/5 비교
- K=4 random +1.96%p / 404/500 wins (◐) — 미세 우월
- K=2 random +0.36%p / 442/500 — 거의 동등

**12 multistart** (`bt_consecutive_K_multistart.py`):
- Early 6/6 양수 (+4.85 ~ +5.59%p)
- Mid 혼재
- **Late 5/5 음수 (-0.33 ~ -2.79%p)** ← 시점 의존성 강함

**결론**: K=3 robust. K=4는 환경 의존적 (Early uplift, Late drag) → 거부.

**실 데이터 확인**: LRCX, BE, FORM 모두 빠졌다 회복 후 3거래일 만에 ✅ → 슈퍼위너 진입점 잡음. carryforward 없는 reset 정상 동작.

### 4. 어닝 2주 이내 차단 vs 경고

**code**: `daily_runner.py:2868-2875` `_build_portfolio_entry`. 경고 표시만 (📅5/22), 차단 없음.

**히스토리 분석** (`analyze_earnings_proximity.py`):
- 12건 진입 중 어닝 2주 이내 진입 = **FORM 1건** (3/18 진입 → 3/31 어닝 13일 전 → **+33.37% 어닝 비트**)
- 어닝 사고 (어닝 미스로 큰 손실) **0건**

**결론**: 차단 안 함. 이유:
- 시스템 자체 보호 메커니즘 (`min_seg < -2`, `rank > 10`) 충분
- 사용자 risk profile (MDD -60% 감내, 수익 우선)과 일치
- FORM 같은 어닝 직전 비트가 잠재 알파

### 5. MA60 fallback (MA120 strict로 갈까)

**code**: `daily_runner.py:1200` `get_part2_candidates`. `price > MA120 OR (MA120 None AND price > MA60)`.

**dense grid BT** (`bt_ma_dense_NEW.py`, NEW simulator):

| MA period | avg ret | lift vs current | wins |
|---|---|---|---|
| ma40 | +64.07% | -16.81%p | 0/100 ✗ |
| ma50 | +79.27% | -1.60%p | 38/100 ✗ |
| **ma60** | +80.31% | -0.56%p | 42/100 ~동등 |
| ma100 | +76.61% | -4.26%p | 0/100 ✗ |
| **ma120 (current)** | +80.87% | 0 | ★ sweet spot |
| ma130 | +68.80% | -12.07%p | 0/100 ✗ |
| ma150 | +72.69% | -8.18%p | 0/100 ✗ |

**MDD 상세** (`bt_ma_mdd_detail.py`): ma60 avg MDD -10.49% vs ma120 -10.65% — 0.16%p 미세. worst MDD 동일 (-18.18%, 2/17 LITE/SNDK/STX 동시 진입, 시점에서 MA60 ≈ MA120 → 어떤 필터로도 못 막음).

**Buy-the-dip 분석** (`analyze_buy_dip_history.py`): 시스템 시작 ~ 5/19, 12건 진입 중 2건 buy-the-dip (price < MA60 at entry)

| ticker | entry | MA60 차이 | exit | ret | days |
|---|---|---|---|---|---|
| **MU** | 4/1 | -6.6% | 5/8 (rank>10) | **+103.02%** | 26 |
| AEIS | 5/15 | -6.0% | OPEN (5/19) | -6.37% | 2 |

**결론**: MA120 유지. MA60 strict 시 **MU +103% buy-the-dip 알파 통째로 손실**. 데이터로 명확.

### 6. MA60 이탈 경고 (display)

**현재**: 매수 후보가 price < MA60 종목이어도 메시지에 표시 안 됨 (`MA120↓`만 이탈 사유로 표시).

**5/19 사례**: Top 20 중 AEIS (-11.8% from MA60), TER (-3.8% from MA60) — Signal 매수 후보 1, 3위. 사용자는 모름.

**옵션 검토**:
1. Signal 종목 옆 ⚠️ 마커 + "📉 단기조정" 표시
2. Watchlist에만 중립 표시
3. 변경 안 함

**사용자 self-awareness**: "경고 보면 의식해서 안 사고 4등 살 거 같다" → behavioral bias로 알파 갉아먹음.

**결론**: 경고도 추가 안 함. 이유:
- 시스템이 이미 종합 판단 (MA60 상태 간접 고려 via w_gap, conviction)
- 별도 경고는 redundant + counter-productive
- MU +103% 같은 buy-the-dip이 핵심 알파

### 7. Score 100 표시 압축

**code**: `daily_runner.py:3909-3913` `_build_score_100_map`. `score = ws / max_wgap × 100`.

**5/19 사례**: AEIS 100.0 / SNDK 96.6 / TER 95.2 — 실제 w_gap 95.48 / 92.28 / 90.88 (차이 3.2/4.6점).

**대안 검증 (min-max 정규화)**:

| rank | ticker | 현재 (비율) | min-max | 의미 |
|---|---|---|---|---|
| 1 | AEIS | 100.0 | 100.0 | 1위 |
| 2 | SNDK | **96.6** | 89.4 | w_gap 차이 3.2 |
| 3 | TER | **95.2** | 84.8 | w_gap 차이 4.6 |
| 4 | MU | 90.0 | 68.6 | |
| 20 | NTRS | 68.3 | 0.0 | |

**min-max 문제**:
- 미세 차이 과장 ("AEIS 압도적" 잘못된 인상)
- NTRS 20위 = 0 → "매우 약한 신호"로 보임 (실제는 Top 20 좋은 종목)
- max/min 매일 변동 → 불안정

**결론**: 현재 비율 방식이 정확한 정보. "1, 2, 3위 거의 동등한 강한 신호"가 5/19의 진실. 변경 X.

## 핵심 인사이트

1. **MU +103% buy-the-dip이 production 알파의 살아있는 증거**. MA60 strict / 단기조정 경고 둘 다 이걸 죽이는 변경.

2. **simulator 버그 패턴은 production 코드에도 존재**. v81 발견 후 일관성 점검 필수. 1건 더 발견 (line 4060). 신규 코드 추가 시 `price_full` fallback + `n_valid` 분모 표준 강제.

3. **"경고가 알파 갉아먹는다"** 원칙. risk-tolerant 사용자에게 시스템 추천 외 별도 경고는 behavioral bias 트리거. system trust 우선.

4. **v80.10b 같은 multi-line update는 누락 위험**. 미래 룰 변경 시 grep으로 모든 occurrence 확인 + 변경 후 그 함수 직접 호출 test 필수.

## 검증 스크립트

- `research/verify_x8_x10_perf.py` — X8 vs X10 누적 수익률 비교
- `research/bt_consecutive_K.py` — K=1~5 random 500 paired
- `research/bt_consecutive_K_multistart.py` — K=3 vs K=4 12 multistart
- `research/analyze_buy_dip_history.py` — 시스템 시작 ~ 5/19 12건 진입 분류 + 수익률
- `research/analyze_earnings_proximity.py` — 어닝 근접도 yfinance 분석
- `research/bt_ma_mdd_detail.py` — MA60 vs MA120 MDD 상세 (avg/median/worst)
- `research/bt_ma_dense_NEW.py` — ma40~ma200 dense grid NEW simulator
