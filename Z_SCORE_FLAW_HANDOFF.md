# Z-Score 설계 결함 — 다음 세션 핸드오프

> **작성**: 2026-04-17 집 PC
> **이어서**: 회사 PC에서 곧바로 시작
> **상태**: 문제 진단 완료, 해결 방안 후보 3가지 도출, **백테스트 미실행 / 코드 변경 0건**

---

## 1. 한 줄 요약

**가중순위(w_gap)를 만드는 z-score 공식에 두 가지 결함이 있어서, "2일밖에 검증 안 된 종목"이 가중순위 2위로 올라오는 모순이 발생했다.**

이게 단순 UI 문제가 아니라 **알파 시그널 자체에 들어가 있는 로직 결함**임. 현재 백테스트 +49.8%(v78)는 이 결함이 모든 날짜에 일관되게 적용된 결과라 신뢰도를 다시 검증해야 함.

---

## 2. 어떻게 발견했나 (실제 사례)

2026-04-16 시장 데이터로 돌린 메시지에 **VNOM(Viper Energy)**이 다음과 같이 나옴.

```
⏳ 2. Viper Energy(VNOM) · 석유미드스트림 ⚠️
   순위 -→2→2위 · 점수 84.9
```

**이상한 점**:
- ⏳ 아이콘 = "2일만 검증됨" (3일 검증 안 됨)
- 순위 추이 `-→2→2` = 2일 전(4/14)엔 순위 없음, 1일 전(4/15) 2위, 오늘(4/16) 2위
- 그런데 가중순위는 2위 (점수 84.9)

같은 화면의 FIVE는:
```
✅ 3. Five Below(FIVE) · 전문소매
   순위 2→4→5위 · 점수 78.2
```

**FIVE는 3일 모두 검증됐는데 VNOM보다 낮음.** 직관적으론 "3일 다 본 종목이 위여야" 맞는데 반대.

v77에서 "빈 날은 30점 페널티"를 도입했음에도 이런 결과가 나옴 → 페널티가 사실상 작동 안 한다는 뜻.

---

## 3. z-score 공식 재확인

`daily_runner.py` 1551~1564, 1605~1614 의 핵심 코드.

```python
# 매일 conviction adj_gap 분포에서 z-score 계산 → 30~100 범위로 변환
score = min(100.0, max(30.0, 65 + (-(v - mean_v) / std_v) * 15))
```

**계수 의미**:
- 평균(mean) 위치 = 65점
- 1σ 떨어질 때마다 ±15점
- 최저 30점, 최고 100점으로 자름(clamp)
- 빈 날(필터 탈락 등) = 30점 페널티

가중치: `T0(오늘)×0.5 + T-1×0.3 + T-2×0.2`

---

## 4. 결함 #1 — 상한 100 clamp가 outlier 변별력 죽임

**4/15 실데이터** (eligible 40종목):
- 평균 conv = 4.72
- 표준편차 = 22.50

| 순위 | 종목 | conv adj_gap | (v−mean)/std | z_raw | clamp 후 |
|------|------|--------------|--------------|-------|----------|
| 1    | MU   | -92.90       | -4.34        | **130.07** | **100** |
| 2    | VNOM | -48.36       | -2.36        | **100.38** | **100** |
| 3    | SNDK | -21.72       | -1.18        | 82.62 | 82.62 |
| 4    | FIVE | -12.76       | -0.78        | 76.65 | 76.65 |

**문제**:
- MU는 평균에서 **-4.34σ** 떨어진 극단 outlier
- VNOM은 **-2.36σ** (강한 outlier)
- 둘 다 100으로 잘려서 **rank 1과 rank 2가 동점**
- 실제로 MU의 conviction은 VNOM의 약 2배인데, 이 정보가 완전히 사라짐

**왜 100을 도달하기 쉬운가**:
- z_raw = 100 = 65 + 35 → 35/15 = **2.33σ만 넘으면 천장**
- conv adj_gap의 std가 20~30 수준인데 outlier 종목은 -50~-100까지 가서 너무 쉽게 도달

---

## 5. 결함 #2 — Missing day penalty 30점이 사실상 무력

**VNOM 가중순위 86점이 어떻게 만들어졌나**:

```
w_gap = T-2(missing) × 0.2 + T-1(rank 2) × 0.3 + T0(rank 2) × 0.5
      = 30 × 0.2 + 100 × 0.3 + 100 × 0.5
      = 6 + 30 + 50
      = 86점
```

**3일 다 발생했다고 가정** (예: T-2도 z=95 정도였다면):
```
w_gap = 95 × 0.2 + 100 × 0.3 + 100 × 0.5
      = 19 + 30 + 50
      = 99점
```

**페널티 효과 = 99 − 86 = 13점**. 근데 FIVE의 78.2점과 비교하면 여전히 8점 차이로 VNOM이 위.

**왜 페널티가 약한가**:
1. T-2 가중치가 0.2라서, 30점 페널티의 실효 차감은 (95−30)×0.2 = **약 13점** (정상 점수 대비)
2. VNOM의 T-1, T0 z-score가 100 천장에 박혀 있어서, 상단의 변별력은 이미 죽은 상태
3. 결과: missing day 1개가 있어도 강한 outlier 종목은 거의 손실 없이 상위에 올라옴

---

## 6. 두 결함이 결합되면 (왜 VNOM이 2위가 됐나)

1. VNOM의 conviction adj_gap이 -48로 강한 outlier → z=100으로 인플레이션
2. 1일 missing이 있어도 페널티 13점만 차감
3. FIVE는 3일 모두 정상이지만, conviction이 약해서 z-score 자체가 76~89 범위
4. 86점(VNOM) > 78점(FIVE) → **2일 검증 종목이 3일 검증 종목을 이김**

---

## 7. 왜 심각한가 — Signal에도 같은 결함

표면적으론 Signal 진입은 ✅(3일 검증)만 가능하니 안전해 보이지만, **✅ 종목들끼리도 같은 z-score 위에서 비교됨**:

- ✅ 종목 중 outlier가 여러 개면 모두 100으로 묶여 1·2·3위 변별 안됨
- 매수 비중·이탈 판단·Top 11 기준선 모두 같은 왜곡된 점수 위에서 결정
- v74·v78 백테스트 성과(+49.8%)는 결함이 일관되게 적용된 덕분이지, **z-score가 옳다는 증거가 아님**

즉, "어쩌다 잘 되는 것처럼 보였을 뿐"일 가능성. 진짜 알파를 찾으려면 점수 자체를 고쳐야 함.

---

## 8. 해결 방안 — 5가지 옵션 + 조합

각 옵션마다 **장점**, **단점**, **맹점**(놓치기 쉬운 부작용)을 모두 정리. 백테스트로 검증 전엔 최종 채택 불가.

---

### 방안 A — Clamp 상한 제거 또는 완화 (가장 본질적)

**변경**:
```python
# 기존
score = min(100.0, max(30.0, 65 + (-(v - mean_v) / std_v) * 15))

# 변경안 A1: 상한만 제거 (하한은 유지)
score = max(30.0, 65 + (-(v - mean_v) / std_v) * 15)

# 변경안 A2: 상한을 200으로 완화
score = min(200.0, max(30.0, 65 + (-(v - mean_v) / std_v) * 15))
```

**장점**:
- MU 130, VNOM 100, SNDK 82 같이 진짜 강도 차이 보존
- 1위 종목이 압도적으로 강한 날엔 가중순위에 그 강도가 그대로 반영됨
- z-score의 통계적 의미를 가장 충실히 살림 (clamp = 정보 절단)

**단점**:
- 한 종목이 너무 강하면 다른 종목들의 상대적 score가 작아 보여 매수 추천 1종목 쏠림 가능
- 점수 수치가 100을 넘게 되면 메시지 표시("78.2점")가 어색해짐 → UX 조정 필요

**맹점 (반드시 검토)**:
1. **단일 outlier가 mean/std 자체를 왜곡**
   - 4/15 데이터: 전체 mean=4.72, std=22.50
   - MU 한 종목 빼면: mean≈7, std≈10.5 (std 약 2배 차이)
   - 즉 **outlier 한 개가 다른 모든 종목의 z-score를 끌어내림** — clamp 풀어도 다른 종목들 점수가 정확하지 않음
2. **임계값 룰의 일괄 재조정 필요**
   - L3 동결, breakout hold, ⚠️ 추세주의 등 점수 임계값 기반 룰 다수 존재
   - 분포가 바뀌면 임계값도 다 같이 바꿔야 일관성 유지 — 누락 시 silent regression
3. **백테스트 score 컬럼 호환성 깨짐**
   - DB의 과거 score 컬럼은 clamp된 값으로 저장됨 → 새 로직과 비교 불가
   - 전 일자 재계산(`recompute_ranks.py`) 필수, 안 하면 v77/v78과 비교 불가능

---

### 방안 B — 계수 축소 (`*15 → *10` 또는 `*8`)

**변경**:
```python
score = min(100.0, max(30.0, 65 + (-(v - mean_v) / std_v) * 10))
```

**장점**:
- 1줄 수정으로 끝남 (가장 단순)
- z_raw=100 도달 임계가 2.33σ → 3.5σ로 까다로워짐 → outlier 천장 도달 확률 ↓
- 점수 범위 그대로(30~100) 유지 → 메시지/임계값 룰 영향 적음

**단점**:
- 분산이 좁아져서 종목 간 차이가 작아 보임 (UX상 "다 비슷해 보임")
- outlier 강한 날의 진짜 신호 강도가 약해짐

**맹점**:
1. **계수 결정의 임의성**
   - 왜 10인지 12인지 8인지 데이터 기반 근거 약함
   - 그리드서치로 결정해도 백테스트 41일은 표본 작아서 과적합 위험
2. **분산 압축 = 가중치 의미 약화**
   - T0×0.5 + T-1×0.3 + T-2×0.2의 weighted average가 좁은 분산 위에서 계산됨
   - 결국 종목 간 w_gap 차이가 1~2점 수준으로 미세해져 순위가 노이즈에 흔들림
3. **여전히 outlier는 outlier**
   - 4σ 넘는 극단 종목(MU 같은)은 *15에서도 *10에서도 결국 100 도달
   - 즉 **결함 #1의 본질을 해결하지 못하고 발생 빈도만 줄임**

---

### 방안 C — Missing day = 제외 + 가중치 재정규화

**변경**:
```python
# T-2 missing이면 가중치를 [0.3, 0.5]로 재정규화
weights_present = [w for w, d in zip(weights, dates) if score_by_date.get(d, {}).get(tk) is not None]
total = sum(weights_present)
weights_normalized = [w / total for w in weights_present]
```

**장점**:
- "3일 검증 원칙"을 로직 차원에서 강제 → UI(✅/⏳/🆕)와 일관성 확보
- v77 의도("🆕=낮음")의 자연스러운 확장
- VNOM 같은 케이스 자동 후순위화

**단점**:
- 신규 진입 종목이 며칠간 Watchlist 상위에서 못 뜸
- 여전히 z-score 자체의 outlier 결함은 미해결

**맹점**:
1. **신규 슈퍼위너 발굴 차단**
   - SNDK 같은 신규 진입 케이스(과거 +559% 슈퍼위너)를 늦게 발견 → 알파 손실
   - v75 검증에서 "SNDK rank 3은 진짜 시그널"로 결론 났음. C를 적용하면 이런 케이스가 묻힘
2. **부분 missing의 강도 무시**
   - T-2 missing이지만 T-1, T0 모두 강함 = 신규 강세 종목
   - VNOM(나쁜 케이스)와 SNDK(좋은 케이스)를 같은 룰로 차별 → 좋은 신호도 같이 손해
3. **재정규화가 또 다른 편향 유발**
   - [0.3, 0.5] → [0.375, 0.625]로 바뀌면 T0 비중이 더 커짐
   - 단일 날짜(특히 오늘) 변동에 더 민감 → 과거 평균을 보려는 가중평균 의도 훼손
4. **"missing"의 정의가 모호**
   - 일시 필터탈락(MA120 -0.03%) vs 진짜 신규 발견 vs 데이터 수집 실패 — 모두 똑같이 missing
   - 원인별로 다르게 처리하려면 추가 로직 필요

---

### 방안 D — Robust z-score (median/MAD 기반)

**변경**: 평균/표준편차 대신 중앙값(median)과 MAD(Median Absolute Deviation) 사용.

```python
import numpy as np
median_v = np.median(vals)
mad_v = np.median(np.abs(vals - median_v)) * 1.4826  # 정규분포 환산 계수
score = min(100.0, max(30.0, 65 + (-(v - median_v) / mad_v) * 15))
```

**장점**:
- 단일 outlier가 통계 자체를 흔드는 문제 근본 해결 (방안 A의 맹점 #1 해결)
- 분포가 비대칭적일 때 더 robust

**단점**:
- MAD는 mean/std보다 직관적이지 않음 (코드 가독성 ↓)
- 중앙값 기반이라 outlier의 magnitude를 통계 자체에서 무시함 → outlier 효과 자체를 죽일 위험

**맹점**:
1. **MAD가 0이 되는 케이스**
   - 종목 수가 적고 conv 값이 동일한 날 (희박하지만 가능)
   - 0으로 나누기 방지 코드 필요
2. **MAD 기반 z-score는 의미가 다름**
   - mean/std 기반의 표준 z-score와 같은 통계적 해석 불가
   - 임계값 튜닝 처음부터 다시 해야 함
3. **Outlier 자체를 무시한다는 것 = 강한 종목 신호 약화**
   - MU 같은 진짜 outlier가 평범한 종목과 비슷한 점수 받게 될 수도 있음
   - 알파 손실 위험 (가장 강한 종목을 약화시키는 것)

---

### 방안 E — Winsorization 후 z-score

**변경**: 상하위 5% conv 값을 분위수로 capping한 뒤 z-score 계산.

```python
import numpy as np
vals = np.array(list(conv_gaps.values()))
lo, hi = np.percentile(vals, [5, 95])
vals_winsorized = np.clip(vals, lo, hi)
mean_v = np.mean(vals_winsorized)
std_v = np.std(vals_winsorized)
# 단, 점수 계산 자체는 winsorize 안 한 원래 v로
score = min(100.0, max(30.0, 65 + (-(v - mean_v) / std_v) * 15))
```

**장점**:
- 통계는 깨끗(D 효과), 점수는 그대로 강도 반영(A 효과) — 둘의 절충
- 정규분포 가정이 더 잘 성립

**단점**:
- 5% 임계값의 임의성
- 종목 수 40개일 땐 상하위 2개 정도만 winsorize → 효과 미미할 수 있음

**맹점**:
1. **종목 수 변동 시 cutoff 의미 변동**
   - 어떤 날은 38종목, 어떤 날은 50종목 → 5% cutoff가 다른 효과
2. **winsorize한 mean/std로 outlier z-score 계산 → 극단값 발생**
   - MU의 진짜 conv는 -92인데 winsorize한 mean=5, std=15라면 z = (-92-5)/15 = -6.5σ → z_raw=162.5
   - 결국 clamp 안 하면 또 100 천장, 풀면 너무 큰 점수 (방안 A 문제 재발)

---

### 조합 후보들 (실전 채택 가능성 높음)

| 조합 | 구성 | 기대 효과 | 우려 |
|------|------|-----------|------|
| **A2 + B1** | 상한 200 + 계수 12 | outlier 변별력 + 일반 구간 노이즈 ↓ | 두 변수 동시 변경, disambiguation 어려움 |
| **D + missing 강화** | robust z-score + penalty 0 | 통계 깨끗 + missing 강제 후순위 | 알파 손실 위험 (outlier 약화 + 신규 차단) |
| **E + A1** | winsorize + 상한 무제한 | 통계 깨끗 + magnitude 보존 | 구현 복잡, 그리드서치 차원 ↑ |
| **B1 + Missing weight ↑** | 계수 12 + T-2 weight 0.3 | 점진적 개선, 변경 폭 작음 | 본질 해결 안 됨 |

---

### 우선 시도 순서 추천

1. **A2 (상한 200)** 단독 BT — 가장 본질적이면서 단순
2. 결과 좋으면 **A1 (상한 무제한)** 시도
3. A 계열 모두 fail이면 **D (robust z-score)** 시도
4. 그래도 fail이면 **C (missing 제외)** 추가 — 룰 결합
5. B는 단독으론 약함, 조합 보조용으로만 활용

**중요**: 어떤 변형을 채택하든 **41일 BT만으론 부족**. 멀티스타트(33시작일) + Walk-Forward + Leave-one-out까지 통과해야 production 채택 가능.

---

## 9. 검증 계획 — 실행 원칙 (필수 준수)

### 핵심 실행 원칙

**반드시 지킬 원칙 (과거 대화에서 합의된 룰 + 이번 세션 추가)**:

1. **각 Step은 30분 이내**
   - 30분 초과 시 그 Step은 설계가 잘못된 것 — 더 작게 쪼개라
   - 진단·BT·비교 단계가 1시간 넘게 걸리면 캐시 설계가 틀린 것

2. **캐시 한 번 로드해서 재사용 (효율성)**
   - DB, conv_gaps, z-score 분포, 종목 info는 **스크립트 시작 시 한 번만 로드**
   - 각 변형 BT에서 재로드 금지 → 변형별 계산 부분만 분리
   - 예: `load_all_data()` → dict/DataFrame 캐시 → 모든 변형이 이걸 참조

3. **CAGR 환산 금지**
   - 41일 raw return으로만 비교 (CAGR 환산하면 41일 +60% → 3000% 노이즈)
   - 누적 수익률 그대로 보고

4. **41일 데이터는 multistart만 사용**
   - Walk-Forward는 41일에선 부적합 (train/test 양쪽 다 너무 작음)
   - Leave-one-out도 41일에선 multistart와 중복 효과 → multistart만
   - 60거래일+ 축적되면 그때 walk-forward 추가

5. **모든 risk metric 필수**
   - multistart (33시작일): **평균 / 중앙값 / std / min / max** 5개 모두
   - **MDD 평균 + worst** (단일 MDD가 아닌 분포)
   - Sharpe (일간 수익 std 기반)
   - Sortino (하방 위험만)
   - **위험조정 = 평균 / |worst MDD|** (raw return 기반, Calmar 아님)
   - 거래 수, 승률, PF, 회전율

6. **차분 측정 (Differential Measurement)**
   - sim 100% 정확성은 본질적으로 불가능 (v71 이전 데이터 호환 안 됨)
   - 같은 sim 환경에서 변형끼리만 비교 → 차이는 정확
   - 변형 절대값은 참고, **변형 간 delta가 진짜 신호**

7. **single-variable change**
   - 한 번에 한 변수만 (A2 상한 / B 계수 / C missing 중 하나만)
   - 조합안은 단독 검증 모두 통과 후에만 시도
   - 두 변수 동시 변경 시 어느 변수가 효과 냈는지 disambiguation 불가

8. **사이드 이펙트 점검**
   - z-score 변경이 임계값 룰(L3 동결, breakout hold, ⚠️ 추세주의)에 silent regression 일으키는지 확인
   - Watchlist Top 20 안정성도 확인

9. **슬롯 1개 몰빵은 함정**
   - 단일 BT에서 CAGR/Sortino 압도적으로 좋아 보이면 의심
   - 슈퍼위너 1개 우연히 잡은 결과 → 재현성 없음
   - raw return + worst MDD + 위험조정으로 볼 것

10. **Strict > Moderate/Loose 패턴**
    - 트리거 조건은 까다롭게 (strict)
    - 느슨하게 하면 false positive → 손실
    - "많이 잡는 것"보다 "확신 케이스만"

11. **baseline(v78) 먼저 확정**
    - 변형 BT 전에 v78 성과표부터 동일 sim 환경에서 측정
    - 모든 변형은 **v78 대비 delta**로 평가

12. **검증 실패 시 production 변경 0건**
    - v75 검증 사례(2026-04-11): 5개 변형 모두 baseline 미만 → 변경 0건
    - 결함 자체는 인정해도, **알파 보존이 더 중요하면 v78 유지**가 정답
    - 결과 기록은 SESSION_HANDOFF.md에 남김

13. **DB 재계산 후 비교**
    - 과거 score 컬럼은 v78 clamp 값이라 새 로직과 비교 안 됨
    - 변형 BT마다 `recompute_ranks.py`로 재계산

14. **알파시그널 수집은 기존 스크리닝 방해 금지**
    - z-score 변경은 rev/OM/GM 등 기존 필터에 영향 주면 안 됨
    - 순수하게 w_gap 계산만 건드릴 것

15. **투자 성향 (MDD -60% 감내, 수익 우선)**
    - 수익 깎아서 MDD 줄이는 변형은 거부
    - win-win 아니면 채택 금지 (역변동성 때처럼)

---

### 단계별 실행 (Step당 30분 이내, 캐시 재사용)

#### Step 0 — 4/16 데이터 로컬 보강 (15분)

```bash
unset TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID TELEGRAM_PRIVATE_ID
python daily_runner.py
# → MAX(date) = 2026-04-16 확인
```

#### Step 1 — 공통 데이터 로더 작성 (30분)

**캐시 재사용의 핵심**. 한 번 돌려서 전 단계가 쓸 자료 전부 뽑아두기.

`research/zscore_cache.py` 신규:
```python
# 한 번 실행해서 아래 전부 pickle 저장
CACHE = {
    'conv_gaps_by_date': {...},     # 전 일자 × 티커 → conv adj_gap
    'composite_rank_by_date': {...},
    'ntm_data_by_date': {...},      # ntm_current, ntm_30d, price 캐시
    'eligible_tickers_by_date': {...},
}
# → research/zscore_cache.pkl 로 저장
```

**이후 모든 Step은 이 pickle만 로드해서 씀**. yfinance/DB 재조회 0회.

#### Step 2 — 진단 리포트 (30분)

`research_zscore_diagnosis.py`:
- 캐시 로드 (1초)
- 전 일자 conv 분포 (mean/std/min/max/skew/kurtosis)
- z_raw ≥ 100 종목 수 / 날짜
- 1·2·3위 z_raw 차이
- "missing day + outlier" 케이스 (VNOM 패턴) 발생 횟수
- → `research/zscore_diagnosis.md`

**판정**: 100 clamp 발생률이 전체 < 1%면 결함 영향 작음 → production 변경 불필요 결론 가능.

#### Step 3 — Baseline v78 metric (30분)

기존 `backtest_v3.py` 활용, **캐시를 입력으로 넘겨** BT 함수 재구성.

측정 (raw return 기준):
- 33시작일 multistart: 평균 / 중앙값 / std / min / max
- MDD: 평균 / worst
- Sharpe / Sortino
- 위험조정 = 평균 / |worst MDD|
- 거래 수, 승률, PF

→ `research/baseline_v78.md` 표로 기록.

#### Step 4 — 방안 A2 단독 BT (30분)

변경: `_compute_w_gap_map` + `_build_score_100_map` 에서 `min(100, ...)` → `min(200, ...)`.

- 캐시 재사용 (재로드 금지)
- multistart 33시작일 돌림
- Step 3과 동일 metric
- vs baseline delta 표 작성 (절대값 X, **차분만**)

**판정 (MDD 악화 금지, 수익 우선 원칙)**:
- 평균 delta ≥ 0 AND worst MDD 동등 이하 → 통과
- 수익만 좋고 MDD 악화면: 투자 성향(수익 우선)상 검토, but MDD 2%p 이상 악화면 기각
- slot 몰빵 의심 (단일 BT max만 높으면 거부)

#### Step 5 — 방안 A1 단독 BT (A2 통과 시, 30분)

상한 무제한. Step 4와 동일 절차.

#### Step 6 — 방안 D 단독 BT (A 모두 fail 시, 30분)

median/MAD 기반. Step 4와 동일.

#### Step 7 — 방안 C 추가 (단독 통과 변형 + missing 강화, 30분)

가중치 재정규화. single-variable 원칙 상, "통과한 변형 + C" 하나만 조합.

#### Step 8 — 사이드 이펙트 점검 (30분)

채택 후보에 대해:
- L3 동결 트리거 빈도 변화
- breakout hold 발동 빈도 변화
- ⚠️ 추세주의 표시 빈도 변화
- Watchlist Top 20 일별 변동률 (안정성)

캐시로 변형별 결과 재계산해서 빈도 비교.

#### Step 9 — 채택 or 기각 결정 (15분)

**채택 조건 (모두 만족)**:
- multistart 평균 ≥ v78
- worst MDD ≤ v78 + 2%p 이내 (악화 허용 범위)
- 위험조정 ≥ v78
- 사이드 이펙트 없음

**기각 조건 (하나라도)**:
- 평균이 v78보다 떨어짐
- worst MDD 2%p 이상 악화
- 사이드 이펙트 (L3/breakout 빈도 급변)

#### Step 10 — production 적용 or 기록 (30분)

**채택**: daily_runner.py 수정 → DB 재계산 → SESSION_HANDOFF.md v79 추가 → MEMORY.md 업데이트 → commit/push.

**기각**: SESSION_HANDOFF.md에 "v79 검증: 방안 X/Y/Z 모두 v78 미만 (또는 MDD 악화) → production 변경 0건" 기록 → commit/push.

---

### 시간 예상 (각 Step 30분, 캐시 재사용)

- Step 0~3 (준비 + 진단 + baseline): **1.5시간** — 첫 세션 여기까지
- Step 4~7 (변형 4개): **2시간** — 다음 세션
- Step 8~10 (점검 + 결정 + 적용): **1시간** — 세 번째 세션

**총 4.5시간** (캐시 재사용 기준). 캐시 없으면 BT마다 데이터 재로드로 3배 넘게 걸림.

---

## 10. 참고 — 이전에 시도되었던 것

`MEMORY.md` v73 percentile rank 시도/롤백 항목 참고:
- z-score 대신 percentile rank 쓰면 outlier 변별력은 살지만 magnitude 정보 손실
- 40일 BT에서 -8.6%p 열세
- 즉, **percentile은 답이 아님 → z-score 자체를 개선해야 함**

이번 작업은 v73과 다른 접근: **z-score는 유지하되, clamp/계수/penalty를 재설계**.

---

## 11. 액션 아이템 체크리스트 (각 Step 30분 이내)

### 첫 세션 (회사 PC, 1.5시간)
- [ ] Step 0 (15분): 4/16 데이터 로컬 생성 (`python daily_runner.py`)
- [ ] Step 1 (30분): `research/zscore_cache.py` — 공통 데이터 캐시 한 번만 생성 (pickle)
- [ ] Step 2 (30분): `research/zscore_diagnosis.md` — 결함 정량화
- [ ] Step 3 (30분): `research/baseline_v78.md` — multistart 33시작일 모든 metric

### 두 번째 세션 (2시간, 캐시 재사용)
- [ ] Step 4 (30분): 방안 A2 단독 BT → vs baseline delta
- [ ] Step 5 (30분): 방안 A1 단독 (A2 통과 시)
- [ ] Step 6 (30분): 방안 D 단독 (A 모두 fail 시)
- [ ] Step 7 (30분): 방안 C 추가 (단독 통과 변형 + missing 강화)

### 세 번째 세션 (1시간)
- [ ] Step 8 (30분): 사이드 이펙트 점검 (L3, breakout, ⚠️, Watchlist 안정성)
- [ ] Step 9 (15분): 채택 또는 기각 결정
- [ ] Step 10 (30분): production 적용 또는 기록

### 채택 시
- daily_runner.py 수정 + DB 재계산 + commit/push
- SESSION_HANDOFF.md에 v79 항목 추가
- MEMORY.md 업데이트

### 기각 시
- SESSION_HANDOFF.md에 "production 변경 0건 + 사유" 기록
- 검증 결과 표 첨부 (미래 동일 가설 반복 방지)

---

### Metric 표 템플릿 (모든 변형에 동일하게 사용)

```
| 변형      | 평균  | 중앙값 | std   | min   | max   | MDD avg | MDD worst | Sharpe | Sortino | 위험조정 |
|-----------|-------|--------|-------|-------|-------|---------|-----------|--------|---------|----------|
| baseline  | 49.8% | ?      | ?     | ?     | ?     | ?       | -16.1%    | 4.58   | ?       | ?        |
| A2 (200)  | ?     | ?      | ?     | ?     | ?     | ?       | ?         | ?      | ?       | ?        |
| ...       | ...   |        |       |       |       |         |           |        |         |          |
```

**주의**: 모든 수치는 **raw return** (CAGR 환산 X), **차분** 컬럼 별도로 추가.

---

## 12. 관련 파일 위치

- **메인 로직**: `daily_runner.py`
  - `_compute_w_gap_map` (1513~1616): 매매 시그널용 w_gap
  - `_build_score_100_map` (3753~3830): 디스플레이용 score_100
  - `_apply_conviction` (검색해서 위치 찾기): conviction adj_gap 산식
- **백테스트**: `backtest/backtest_v3.py`, `bt_engine.py`, `bt_metrics.py`
- **재계산**: `recompute_ranks.py`
- **DB**: `eps_momentum_data.db` (max date 2026-04-15)
- **메모리**: `~/.claude/projects/C--dev-claude-code-eps-momentum-us/memory/MEMORY.md`
