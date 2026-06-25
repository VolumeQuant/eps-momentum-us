# 기대성장(gap) × EPS-모멘텀 융합 연구 — 전수 기록 (2026-06-25)

> 출발점(사용자 직감): "PER 낮아도 forward 12m PER ≈ trailing PER이면 주가 지지부진, 둘이 벌어질수록 잘 오른다(SK하이닉스·삼성)." = **기대성장 = 선행EPS(NTM)÷후행EPS(TTM) = trailing_PE/forward_PE = 시장 기대 이익증가율.**
> 데이터: 2026-02-12~06-24 (91거래일), 유니버스 = EPS-screen eligible 137종목, PIT trailing EPS(yfinance 분기실적+45일), $1B 필터. 전부 faithful production-replay.
> ★ 결론 먼저: **gap은 진짜 알파지만 US에선 별도 harvest 불가. 2슬롯 유지가 답. KR 확신가중은 KR 유니버스 현상으로 US에 이식 안 됨.**

## 1. gap 신호 — 검증됨 (real)
- cross-sectional IC **+0.23** (PIT-clean, look-ahead 아님 — PIT EPS로 재검증 +0.232), momentum rank와 corr **−0.06**(직교), 5분위 단조(+3.5%→+13.9%), 양 시기 +.
- **이중확인(momentum-top ∩ gap-top) forward 20d: +18% vs 단일 +7~8% vs 무 +5%** (n=282~889, 대표본). ← 사용자 핵심 통찰 "겹치면 강한 알파" 확증.
- gap-factor 일평균 +0.53%.

## 2. KR 핸드오프 — 확신가중(Conviction Weighting), KR에선 작동
KR이 같은 알파로 도달: production 100% 불변, **보유 중 이중확인 종목만 비중 ×2~3, 미확인 ×1, 재정규화**(제외 금지).
- KR 7.4년: ×1 Calmar 3.92 → ×3 Calmar 5.52, CAGR 111→139%, MDD −28.4→−25.2%. **더 벌고 덜 깨짐.**
- ★KR 핵심 규칙: **제외(F)는 금지**(Calmar 4.24→2.03 반토막, 중소형 승자도 같이 빠짐) — 가중(G)만. look-ahead 백테스트는 상한, 배수 못 믿고 방향만, forward로 검증.

## 3. US harvest 시도 전수 (14가지) + 실패 이유
| # | 시도 | 결과 | 실패 이유 |
|---|---|---|---|
| 1 | gap 점수주입(가산) | −25~50p | 리비전 winner 밀어냄 |
| 2 | gap 점수주입(곱셈/conviction) | −25p | 동일 |
| 3 | sleeve를 momentum로 필터 | −20p | CIEN(트랩)이 momentum 등수 오히려 좋음→분리불가 |
| 4 | 등수 50/50 블렌드 | −15~20p | CIEN 보유 or winner 탈락 |
| 5 | 자본 블렌드(70/30) | Sharpe↓ | 상관 +0.83, 분산효과 없음 |
| 6 | 순수 교집합(2D 스윕) | +57% < sleeve67% | momentum 식은 gap승자(SNDK) 탈락 |
| 7 | 교집합 안정화(min-K) | +66% | sparse→gap sleeve로 희석, CIEN 복귀 |
| 8 | 2슬롯 이중확인 오버레이 | −40p, LOWO기각 | SNDK 밀어내고 MU 사서 손해 |
| 9 | 월간 융합(보유=둘중하나) | +66% | 월간이 SNDK 순간 놓침 |
| 10 | **daily 융합 슬롯3** | +116%→artifact | slot수 바꾸면 우위 뒤집힘(slot2 꼴찌), prod slot3=−90p 모순 |
| 11 | gap 가속도 트랩탐지(EXIT) | 실패 | CIEN이 −18% 직전까지 gap 확장(SNDK와 동일궤적), EPS건강 중 꺾임 0건 |
| 12 | gap-factor 타이밍 | 불가 | 과거5d→향후5d 상관 −0.35(평균회귀) |
| 13 | gap-breadth 레짐 | 죽음 | breadth 0.98~1.00(변동0), 상관 −0.07 |
| 14 | **확신가중(KR 이식)** | 2슬롯 +6p/MDD악화·sleeve flat~악화, LOWO음수 | US 유니버스가 KR과 다름(아래) |

## 4. 반복된 6가지 실패 메커니즘
- **A. winner-트랩 신호 공유**: 고gap 승자(SNDK)와 트랩(CIEN)이 gap레벨·가속도·min_seg 다 동일 → ex-ante 분리불가. CIEN −18%는 순수 idiosyncratic.
- **B. 엘리트 풀 내 역전**: top-ranked 안에선 신호 셀수록 향후수익 낮음(이미 엘리트 존).
- **C. cross-sectional IC ≠ 집중포트**: +0.23 IC는 분산 포트 속성, 2~3종목 책엔 특정종목 실현이 지배.
- **D. 둘다-강함 요구가 식은 winner 버림**: SNDK momentum 식어 교집합서 탈락(근데 계속 오름).
- **E. config/표본 artifact**: slot수/임계/리밸타이밍 바꾸면 우위 뒤집힘(91일·137종목·1fetch).
- **F. 좋은 아이디어는 이미 production에**: "winner momentum 식어도 보유" = 기존 PE_HOLD 룰(SNDK 잡은 비결).

## 5. 확신가중이 US로 이식 안 되는 이유 (핵심)
| | KR | US |
|---|---|---|
| 유니버스 | ~1900 broad | ~137 사전스크린(EPS상향) |
| 종목 성격 | 다양 | 테크/성장 쏠림 |
| 두 신호 상관 | **직교(+0.015)** | **+0.83 (높음)** |
| 확신가중 결과 | Calmar 3.92→5.52 | 2슬롯 MDD악화·sleeve 손해 |

→ KR은 **넓고 직교적**이라 이중확인이 진짜 차별화 → 확신가중 작동. US는 **좁고 상관 높아** 이미 비슷한 종목들이라 이중확인이 차별화 못 함 + 고gap=고변동에 비중 실으면 MDD↑. **같은 레시피, 다른 재료, 다른 결과.**

## 6. 최종 결론 / 액션
- **US 2슬롯 그대로 유지가 답.** 확신가중·융합 일체 배포 금지(14방향 다 실패/artifact).
- **gap의 US harvest는 이미 2슬롯이 함** — 현재 라이브 2슬롯이 LITE·NVDA(이중확인 종목)를 자연히 보유.
- **gap sleeve(K7·월간·$1B, +67%)는 독립 검증된 옵션 sleeve** — 수익은 2슬롯<, MDD 약간 낮음, 상관 0.83이라 분산효과 작음. 페이퍼/소액 옵션이지 메인 대체 아님. `gap_sleeve.py`로 모듈화(killswitch SLEEVE_DISABLE).
- **KR엔 확신가중 배포(거기선 검증), US엔 이식 금지.** US/KR 최적이 다른 건 시장 구조 차이.
- ★ 이 길 다시 파지 말 것 — 14방향 + 전문가 패널 2명 + KR 이식까지 전부 측정 완료.

> 재현: `research/rotation_cap_bt_2026_06_25.py`(faithful 엔진), scratchpad/*.py(개별 시도). 신호검증 IC/이중확인/확신가중 전부 PIT-clean.
