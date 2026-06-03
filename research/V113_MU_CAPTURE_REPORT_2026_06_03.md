# V113 — MU 잡는 진짜 전략 (2026-06-03 자율주행)

## 사용자 비판 (정확)

"MU 400→1000달러 동안 시스템 수익 못 냈으면 폐기. 정확히 다시 연구해."

**판정: 100% 정확.** 사용자 비판이 production BUG 정확히 잡음.

## 진짜 진단 (4단계)

### 1단계: V110 시뮬 매매 trace
```
04-02 MU 매수 $366.24
05-28 MU 매도 $366.24 (수익 0%)  ← 가짜 매도
```
진짜 가격: 5/27 $928 → 06-02 $1064 (+19%) 못 잡음.

### 2단계: DB 무결성 점검
| 날짜 | 종목 수 | MU |
|------|---------|-----|
| 5/22 | 1223 | $751 cr=36 p2=20 |
| 5/26 | 1237 | $895 cr=65 p2=30 |
| 5/27 | 1243 | $928 cr=67 p2=None |
| **5/28** | **600** | **MISSING** ← 부분 fetch 실패 |
| **5/29** | **315** | **MISSING** ← 더 심각 |
| 6/01 | 1248 | $1035 cr=66 p2=None |
| 6/02 | 1238 | $1064 cr=62 p2=None |

5/28에 종목 649개 누락. 5/29에 932개 누락. yfinance cron 부분 fetch 실패.

### 3단계: yfinance retry 검증
```
MU 5/28: $923.52 (yfinance에 정상 존재)
MU 5/29: $971.00 (yfinance에 정상 존재)
```
DB 누락 = cron fetch 실패만 (network/rate limit). 데이터 자체는 정상.

### 4단계: sim/production BUG
**Sim**: `info is None → del held[tk]` (즉시 매도). 매도가 fallback = ep ($366.24 매수가) → 가짜 0%.

**Production** (daily_runner.py:3404-3406):
```python
row = cand_by_tk.get(t)  # Part 2 풀 (composite_rank ≤ 30)
if row is None:
    continue  # eligible 탈락 → 자연 매도
```
MU cr=65,67,MISSING → row None → 매도 발동.

## V113 fix logic

```python
# 1. info None 시 carryover
if info is None:
    continue  # 데이터 fetch 실패 = holding 유지

# 2. 가격 fallback 어제 가격
pn = pf[d].get(tk) or pp  # 오늘 없으면 어제

# 3. 메가 시그니처 유지 시 holding (composite_rank 무관)
if is_mega(info):
    if info.get('rev_growth') < 0.25:
        del held[tk]
    continue
```

## BT 결과 (start=0 Full)

| 변형 | cum | diff |
|------|-----|------|
| v86e+ | +369.3% | baseline |
| v112 (eligible 무관 carry but info None 매도) | +369.3% | 0p |
| **v113 (info None carryover)** | **+407.2%** | **+38p** |

### V113 매매 trace
1. 02-17 BUY SNDK $590.59 (보유, +190.6%)
2. 02-18 BUY LITE $594.26 → 03-13 SELL $622.50 (+4.8%)
3. 03-13 BUY STX $383.71 → 04-02 SELL $429.36 (+11.9%)
4. **04-02 BUY MU $366.24 (보유, +190.5%)** ★

**SNDK + MU 둘 다 +190% 보유.**

### Multistart 100×3 paired
| 시나리오 | v86e+ | v113 | diff | wins |
|---------|-------|------|------|------|
| 전체 | +202.2% | **+229.0%** | **+26.7p** | 255/300 |
| -SNDK | +121.9% | **+150.5%** | **+28.6p** | 258/300 |
| -MU | +154.6% | +154.6% | 0p | 0/300 |
| -SNDK-MU | +67.8% | +67.8% | 0p | 0/300 |

★ -MU/-SNDK-MU 효과 0 = **V113 알파 source 100% MU carryover**. SNDK는 5/28-29 PRESENT라 영향 없음.

## Production fix 옵션

### 옵션 A: daily_runner.py carryover 확장 (logic fix)
- row is None일 때 전체 ntm_screening fetch
- 메가 시그니처 (PEG<0.22) 유지 시 carryover
- **장점**: 코드 한 곳 수정 (line 3404-3406)
- **위험**: production logic 변경, test 필요

### 옵션 B: cron fetch retry/sticky (인프라 fix)
- yfinance fetch 실패 시 retry
- 부분 실패 시 어제 데이터 carryover (sticky)
- **장점**: 데이터 무결성 근본 해결
- **위험**: 데이터 stale 가능

### 옵션 C: A + B 조합 (권고)
- production logic robust + 데이터 인프라 robust
- 둘 다 fix하면 미래 동일 사고 방지

## 권고

★ **옵션 C 채택 권고**:
1. daily_runner.py:3406 carryover 확장 (메가 시그니처 종목)
2. cron yfinance fetch retry + sticky fallback
3. 5/28~5/29 DB 데이터 복구 (yfinance retry)
4. test workflow 검증 후 production 적용

## Caveat (정직)

1. V113 BT 76일 — N=2 (SNDK + MU)
2. MU 진짜 production cron 보유 여부 확인 필요 (V86e+ 시작이 6/02 — MU 이미 매도 후)
3. -MU 시나리오에서 +0 = MU 단독 효과 (다른 메가 종목 영향 없음 = 일반화 어려움)
4. fetch retry는 cron 실행 시간 + API throttle 위험
