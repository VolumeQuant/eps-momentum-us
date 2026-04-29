# Segment Noise Fix — 2026-04-30 작업 핸드오프

**작성**: 2026-04-30
**다음 작업자**: 회사PC의 클로드코드 (자기 자신)
**긴급도**: 메인 워크플로우 중단 상태. 결정 후 빠르게 재활성화 필요.

---

## 🚨 현재 상태 (즉시 인지)

### 1. Production 메인 워크플로우 **중단됨**
```bash
gh workflow list
# Daily EPS Momentum Screening — disabled
# Test Private Bot Only — active (그대로 유지)
```
**복구**: 결정 후 `gh workflow enable "Daily EPS Momentum Screening"`

### 2. Production 코드 **변경 없음**
이번 세션에서 production daily_runner.py 등 전혀 안 건드림. 모든 실험은 분리 디렉토리(`C:\dev\claude-code\eps-momentum-us-multifactor`)에서만.

### 3. Production v80.2 정상 작동 중
- 4/28까지 메시지 정상 송출
- DB 4/28까지 채워짐
- 4/29 데이터는 push 안 됨 (메인 워크플로우 disabled 상태)

---

## 📌 사용자 요구사항 (핵심)

사용자는 **MU 4/27 → 4/28 → 4/29의 큰 순위 변동**에 당황해서 분석 요청:
- 4/27 cr=1, p2=1
- 4/28 cr=13, p2=1
- 4/29 cr=26, p2=7 (사용자가 4/29 테스트 메시지에서 본 값)

**사용자의 본질적 우려**:
> "당일 EPS 전망 수치 자체는 큰 변화 없는데 이렇게 순위가 크게 변경되는 게 말이 안되잖아"
> "고객이 보면 갑자기 당일 EPS 전망이 크게 하향된 줄 알 거 아냐"

**핵심 의문 정확함**: ntm_current는 거의 변화 없는데($86.23 → $86.37) 시스템이 흔들림.

---

## 🔍 진단 결과 — 진짜 원인

### 3/18 MU 어닝 비트가 lookback window를 가로지르며 발생한 시점 효과

| 발표일 | 예상 EPS | 실제 EPS | 서프라이즈 |
|---|---|---|---|
| 2026-03-18 | $9.16 | **$12.20** | **+33.2%** 🚀 |

**작동 메커니즘**:
1. 3/18 어닝 폭탄 → NTM EPS 컨센서스 대폭 상향 ($52 → $81 정도)
2. 4/27 측정: 30영업일 전 ≈ 3/16 (어닝 발표 **직전**, NTM 낮음 = $52)
3. 4/28 측정: 30영업일 전 ≈ 3/17 (어닝 발표 **직후**, NTM 높음 = $81)
4. 단 하루 차이로 lookback 시점이 어닝 발표 경계 넘음
5. 시스템 입장: "30일 전엔 이미 81이었네. 지금은 더 안 올랐고. 가속 둔화네" → **페널티**
6. 진짜 사실: 3/18 호재가 매우 큰 사건이었을 뿐. EPS 상태는 여전히 좋음

**구조적 결함**:
- score = seg1 + seg2 + seg3 + seg4 (4개 segment 변화율 합)
- direction = (seg1+seg2)/2 - (seg3+seg4)/2 → **양수에서 음수 점프 가능**
- adj_score = score × (1 + clamp(direction/30, -0.3, +0.3)) → factor 1.3× → 0.7×
- adj_gap도 부호 전환 (-14.50 → +0.92)
- composite_rank: 1 → 13 → 26 점진 하락
- 단 part2_rank는 3일 가중 w_gap 덕분에 1 → 1 → 7 (느리게 따라감)

→ **모든 강한 어닝 비트 종목에서 발생 가능한 일반적 문제**. MU만의 특수 사례 아님.

### 정확한 데이터 (MU 4/27 vs 4/28)
```
4/27: nc=86.23 n7=83.49 n30=52.39 n60=38.75 n90=36.17
  seg1=+3.28 seg2=+59.35 seg3=+35.20 seg4=+7.12
  recent_avg=+31.32, old_avg=+21.16, direction=+10.15
  score=104.96, adj_score=136.45 (mult=1.30), adj_gap=-14.50

4/28: nc=86.37 n7=85.51 n30=81.08 n60=39.14 n90=36.96  ← n30 점프!
  seg1=+1.00 seg2=+5.47 seg3=+100.00(cap!) seg4=+5.91
  recent_avg=+3.24, old_avg=+52.95, direction=-49.72
  score=112.38, adj_score=78.67 (mult=0.70), adj_gap=+0.92
```

---

## 🛠 검토한 해결 방법들 + 평가

### A. Segment cap 강화 (±100% → ±50%)
- 부작용 큼 (진짜 큰 변화 종목도 잘림)
- ❌

### B. ntm_current 추세 추가 보조 점수
- BT 영향 큼, 새 가중치 결정 필요
- ❌

### C. Lookback 점프 격리 (임계값 기반)
- 핀포인트 좋지만 임계값 자의적
- △

### D. EMA smoothing
- 모든 종목 신호 둔화
- ❌

### 옵션 1 (= C 변형): cap에 걸린 segment 제외
- 시스템에 이미 ±100% cap 룰이 있음
- cap 걸렸다 = "노이즈 신호" → 그 segment 빼고 합산
- **사용자 정확한 지적**: "100%는 검증 안 된 임의 수치"

### 🌟 중앙값 사용 (median) — 사용자 추가 검토 후 추천
**가장 부작용 적고 본질적**:
- 4 segment 중 max/min 제거, 중간 2개 평균
- 임계값 없음 (100%냐 90%냐 고민 X)
- 양방향 outlier 자동 격리
- 코드 한 줄 변경
- 통계학적으로 검증된 robust statistic
- KR 인사이트 #3 ("단순 회귀") 정신 부합

```python
# 지금
score = seg1 + seg2 + seg3 + seg4

# 변경 (중앙값 기반)
sorted_segs = sorted([seg1, seg2, seg3, seg4])
score = (sorted_segs[1] + sorted_segs[2]) * 2  # 4개 기준 보정
```

**MU 4/28 시뮬레이션**:
- segments: +1.00, +5.47, **+100(cap)**, +5.91
- 정렬: +1.00, +5.47, +5.91, +100
- 중앙값 = (+5.47 + +5.91)/2 = +5.69
- score = 5.69 × 4 = +22.76 (현재 112.38 대비 훨씬 작음, 노이즈 격리됨)

### 회귀선 기울기 (선형회귀)
- 5점(nc, n7, n30, n60, n90) 선형회귀 기울기
- 본질적 해결, 단 코드 복잡

### ntm_current 시계열 직접 사용
- yfinance lookback 의존 제거
- DB self-reference로 한 번 저장된 값 고정
- **가장 본질적 해결, 단 90일 데이터 누적 후 가능** (현재 80일 부족)

---

## 🧪 BT 시도 결과 (신뢰성 부족)

분리 환경에서 cap 제외 + outlier 검출 변형 BT 실행. 결과:

| Variant | avgRet | risk_adj |
|---|---|---|
| **Baseline (production)** | **+33.91%** | **2.23** |
| capX100 | +10.93% | 0.75 |
| capX150 | +13.66% | 0.93 |
| zscore2.0 | +16.24% | 0.78 |
| iqr2.0 | +12.49% | 1.11 |

**모든 변형이 -20%p 폭락** — 결과 신뢰 못 함. 이유:

1. **part2_rank 단순화**: BT 코드에서 part2_rank를 composite_rank로 그대로 대체. 정확하게는 w_gap (3일 가중) 기준
2. **adj_gap 재계산이 비례식**: `adj_gap_new = adj_gap_old × (adj_score_new / adj_score_old)`. fwd_pe_chg는 변하지 말아야 하는데 같이 비례 변동됨
3. **conviction 단순화**: rev_up30/num_analysts만 사용

→ **DB 재생성 자체의 단순화가 베이스라인 -20%p의 주범. segment 처리 효과는 측정 안 됨.**

**파일**: `eps-momentum-us-multifactor/segcap_backtest.py` (분리 환경)

---

## 🧠 4인 전문가 패널 컨설팅 결과 (2026-04-30 추가)

사용자 요청으로 4명 전문가 페르소나(시니어 퀀트 / 시계열 분석 / 통계학자 / 시스템 운영) 토론 받음. 우리가 검토한 8개 해법 외에 **놓친 접근법** 4개 발굴.

### 새로 발견된 접근법

#### 🥇 Hampel filter (시계열 전문가)
```python
threshold = median(segs) ± 3 × MAD(segs)
# |seg - median| > threshold면 median으로 대체
```
- **임계값 자의적이지 않음** — MAD 기반 데이터 적응적 (k=3 = 가우시안 99.7% 표준값)
- Industrial outlier 검출의 정통 기법 (Hampel 1974)
- n=4의 한계는 forward test로 확인 필요

#### 🥈 Time-normalized differencing (시계열 전문가)
**시스템의 진짜 bug 지적**:
- seg2(7→30d) = 23일 윈도우, seg3(30→60d) = 30일 윈도우
- 그런데 cap=100을 동등하게 취급 → seg2가 단위시간당 23/30배 압축
- "이건 fix가 아니라 bug fix" 강한 주장
```python
seg1_per_day = (ntm_current - ntm_7d) / 7
seg2_per_day = (ntm_7d - ntm_30d) / 23
seg3_per_day = (ntm_30d - ntm_60d) / 30
seg4_per_day = (ntm_60d - ntm_90d) / 30
```
- 단 이건 v75~v79 가중치(cap=100 기준 튜닝됨) 모두 재튜닝 필요 → major migration

#### 🥉 Tukey biweight M-estimator (통계학자)
- Median(robust) + Mean(efficient) 장점 결합
- 가우시안 efficiency 95% 보장 (Holland & Welsch 1977)
- c=4.685 표준값. median보다 우월(outlier 없을 때 신호 안 깎임)
- 단 코드 복잡도 +5줄

#### 🏅 Cross-sectional rank within segment (퀀트)
- 각 segment를 그날 universe percentile로 변환 후 합산
- BARRA 같은 전통 팩터 모델 표준 기법
- v73 percentile 시도(-8.6%p 롤백)와 다른 layer (segment 단계)
- 어닝시즌 노이즈는 universe 전체가 동시 노출되므로 cross-sectional rank가 자연스럽게 흡수

### 토론에서 도출된 핵심 경고

**"코드 한 줄이라 안전하다"는 환상**:
- median으로 base 변경 시 v75 conviction, v77 G 다층화, v79 z-clamp 제거 모든 layer 재튜닝 필요
- v75~v79 누적 +15.2%p 알파를 base 변경으로 깨뜨릴 위험

**BT 신뢰성 부족이 진짜 위기**:
- 어떤 통계적으로 우월한 해법도 BT 검증 없이 배포 불가
- 우리 BT의 -20%p 폭락은 코드 결함(재계산 단순화)이지 변형 자체 결함 아님
- backtest_v3.py 패턴으로 정확한 BT 인프라 재구축 필수

### 4인 합의된 권고

**즉시 (오늘~이번 주)**:
1. **Display layer fix** — `eps_chg_weighted` → `ntm_current/ntm_90d - 1`. 알파 무변경, 고객 우려 80% 즉시 해결. (운영매니저 추천, 패널 만장일치)
2. **Shadow deployment 인프라** — DB에 challenger 테이블 추가, base + challenger 동시 실행 후 part2_rank 별도 저장. 실시간 forward test 30일 누적 후 비교.
3. **BT 코드 결함 진단** — 검증 인프라 신뢰 회복이 우선

**DON'T**:
- 알파 로직(score, adj_gap, eps_quality, conviction) 즉시 변경 금지
- "코드 한 줄이니 배포해보자" 식 접근 금지

**단기 (1~4주)** Shadow에서 3개 challenger 병행:
| Challenger | 우선순위 | 근거 |
|---|---|---|
| **C1 Median** | 🥇 | 4명 중 3명 추천. 단순, 롤백 쉬움. n=4 한계 forward test로 확인 |
| **C2 Hampel filter** | 🥈 | 임계값 자의적 X (MAD 기반). C1보다 sophisticated |
| **C3 Time-normalized + median** | 🥉 | segment 정의 자체 fix (bug fix). v75 가중치 재튜닝 필요할 수 있음 |

**검증 기준**: 30일 forward test 후 (a) MU 같은 lookback 점프 케이스에서 part2_rank 안정성, (b) 평균 일별 part2_rank 변동성, (c) 실제 매수 신호 종목의 30일 forward return이 champion 대비 우월한지

**장기 (3개월+)**:
- ntm_current 시계열 직접 사용 (90+ 영업일 누적 후)
- BT 인프라 재작성 (backtest_v3.py 패턴)
- Cross-sectional rank within segment 재시도

### 핵심 통찰
사용자 직관 "EPS 전망 거의 안 변했는데 순위 큰 변화 = 말이 안 됨"은 **monotonicity violation**으로 정확히 진단됨. fix는 **표시 + 알파 두 layer로 분리**:
- 표시는 오늘 fix 가능 + 안전 (고객 우려 해결)
- 알파는 shadow 검증 후 fix (수익률 보호)

8개 해법 + 4개 새 발견 중 **median이 4인 합의**지만, **즉시 배포가 아니라 shadow challenger로 검증 후 배포**가 정답.

---

## 🎯 다음 단계 — 두 가지 길

### 길 1: 정확한 BT 다시 (30~60분 추가)
- **backtest_v3.py 패턴 그대로** 활용 (production daily_runner의 진짜 함수 직접 호출)
- segment 처리만 monkey-patch
- fwd_pe_chg와 w_gap을 production 그대로 사용
- **변형 1순위**: 중앙값 (median)
- 추가 비교: cap 제외, 회귀선 기울기

**구현 가이드**:
```python
import daily_runner as dr

# segment 계산 함수 monkey-patch
original_calc = dr.calculate_score  # 또는 비슷한 이름. eps_momentum_system.py에 있을 수도
def patched_calc(...):
    # 4 segment raw 계산
    # 중앙값 적용
    # score = (정렬 후 2번째 + 3번째) × 2
    # direction은 남은 2개로 재계산
    # adj_score, eps_q, adj_gap 재계산
    return ...

dr.calculate_score = patched_calc
# 그 다음 backtest_v3.py처럼 DB 복사 후 재계산
```

근거 파일:
- `eps-momentum-us/backtest_v3.py` — DB 복사 + monkey-patch 패턴
- `eps-momentum-us/eps_momentum_system.py:392-410` — 현재 segment + score + direction + adj_score 계산 위치
- `eps-momentum-us/daily_runner.py:615-660` — fwd_pe_chg + adj_gap 계산 위치

### 길 2: 메시지만 정직화 (즉시 가능, 알고리즘 안 건드림)

알고리즘은 production 그대로. 메시지 표시만 변경:

**변경 전**:
```
EPS 전망 +59% (eps_chg_weighted, 4구간 가중 변화율 기반)
```

**변경 후 옵션**:
1. `eps_chg_weighted` → `(ntm_current_today - ntm_current_7d_ago) / ntm_current_7d_ago` 사용
2. 라벨 변경: "EPS 전망" → "EPS 모멘텀 점수" 또는 "EPS 가속도"
3. 보조 표시: "EPS 전망 +40% (실제 NTM EPS 7일간 +0.2%)"

**대상 파일**: `daily_runner.py` 메시지 빌드 부분 (`_build_signal_message` 같은 함수)

**장점**: BT 영향 0, 즉시 가능, 고객 오해 방지

**단점**: 알고리즘 결함 자체는 안 고침 (cr 점프 그대로)

---

## 📋 사용자 추천 + 결정 대기

내(이전 세션 클로드) 추천:
- **단기**: 길 2 메시지 정직화 (즉시) → 메인 워크플로우 빠르게 재활성화
- **중장기**: 길 1 정확한 BT로 중앙값 검증 → production 흡수

**사용자가 결정해야 할 것**:
- A. 길 1로 정확한 BT 우선 진행, 결과 후 결정
- B. 길 2 메시지만 정직화로 빠르게 종결, 알고리즘은 후속 작업
- C. 길 1 + 길 2 병행

회사PC 클로드코드는 **사용자에게 어느 길로 갈지 묻고 진행** 권장.

---

## 🗂 분리 환경 (실험 디렉토리) 상태

**위치**: `C:\dev\claude-code\eps-momentum-us-multifactor`
**Branch**: `experiment/multifactor` (master 분리)
**Remote**: 없음 (push 불가)

### 회사PC에 분리 환경 없을 경우 setup
```bash
git clone --local C:\dev\claude-code\eps-momentum-us C:\dev\claude-code\eps-momentum-us-multifactor
cd C:\dev\claude-code\eps-momentum-us-multifactor
git remote remove origin
git checkout -b experiment/multifactor
# 그 후 multifactor_cache/loader.py 실행해 캐시 빌드 (1~2분)
PYTHONIOENCODING=utf-8 python multifactor_cache/loader.py
```

### 보존된 작업물 (분리 환경)
- `multifactor_cache/all_data.pkl` (1년치 가격 + NTM 캐시, 재실행 시 재생성)
- `multifactor_cache/phase4_results.pkl` (헨리 멀티팩터 검증 결과)
- `multifactor_cache/riskfolio_results.pkl` (비중 최적화 검증 결과)
- `multifactor_cache/segcap_results.pkl` (이번 cap/outlier BT 결과)
- `multifactor_phase4_sanity.py`
- `riskfolio_backtest.py`
- `segcap_backtest.py`
- `multifactor_cache/factors.py` (V/M/G 팩터 계산)
- `multifactor_cache/loader.py` (공통 데이터 캐시 로더)

---

## 🔗 관련 메모리 파일

- `~/.claude/projects/C--dev-claude-code-eps-momentum-us/memory/MEMORY.md` (인덱스)
- `feedback_iterative_insight.md` — 이전 단계 인사이트 → 다음 단계 반영 원칙
- `feedback_execution_principles.md` — Step 30분 + 캐시 한 번 로드
- `feedback_backtest_methodology.md` — multistart, 모든 metric, 차분 측정
- `user_risk_profile.md` — 수익 우선 (단 사용자가 "꼭 그런 건 아냐" 보정 의사 표시함)
- `project_henry_multifactor_validation_2026_04_26.md` — 직전 실험 (헨리 + riskfolio 둘 다 production 못 이김 입증)

---

## ⚠️ Production 안전 원칙 (반드시 준수)

1. **Production 디렉토리(`eps-momentum-us`) 코드 변경 시 BT 검증 후에만**
2. **DB 절대 직접 건드림 X** — 변형 BT는 `eps-momentum-us-multifactor/multifactor_cache/segcap_dbs/` 같은 분리 위치
3. **메인 워크플로우 재활성화 전에**:
   - production daily_runner.py 변경 사항 git diff로 review
   - test-private-only로 4/30 데이터 시뮬 송출 + 사용자 확인
   - 그 후 메인 enable
4. **사용자가 "수익 우선" 입장 트레이드오프 영역에선 신중**

---

## 🏁 회사PC 시작 가이드

1. 이 핸드오프 파일 읽음 (현재 위치)
2. 사용자에게 "길 1, 길 2, 병행 중 어느 쪽?" 물어봄
3. 분리 환경 setup 필요한지 확인 (위 setup 가이드)
4. 결정에 따라 진행
5. 완료 후 메인 워크플로우 재활성화 (`gh workflow enable "Daily EPS Momentum Screening"`)

**시간 임박 시 빠른 결정**: 길 2 (메시지만 정직화) → 30분 안에 완료 가능, 메인 워크플로우 재활성화. 길 1은 후속 세션.
