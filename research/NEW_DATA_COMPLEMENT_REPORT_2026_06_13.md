# 신규 데이터 발굴 + 보완 전략 (병렬 자율주행 2026-06-13)

> 두 트랙 동시 진행 후 본인이 통합·재검증. 스크립트: verify_target_revision.py / fetch_upgrades.py / ensemble_complement_bt.py.

## 종합 결론: 두 트랙 모두 robust 개선 없음. (전 실험과 일관)

## 트랙1 — yfinance 신규 데이터
- 미사용 소스 점검: upgrades_downgrades(히스토리有), info(targetMean/short/기관, 단면만), insider, options(단면), recommendations 등.
- **단면 신호(목표가 gap/공매도/기관보유/insider)는 히스토리 없어 forward IC 검증 불가 → 보류.**
- 유일 검증가능 = **목표가 개정폭(target_chg, upgrades_downgrades 60일)**. 1차(에이전트): IC +0.31, LOWO +0.26(winner5 제외) = 착시 아님처럼 보임.
- **재검증(본인)에서 정체 발각**:
  · 상위후보(part2≤10) IC +0.44 (선택단계도 유효해 보임) BUT
  · **corr(target_chg, 과거60일수익) = +0.755** → 76% 그냥 가격모멘텀(애널이 오른 뒤 목표가 올림=lagging).
  · **전략 BT(필터/틸트) = 0 효과**(+229.7% 동일) — saturation + part2_rank 모멘텀과 중복.
- → 높은 IC·LOWO통과는 "상승장 모멘텀 자기상관" 착시. **actionable 아님.** (LOWO만으론 부족, 교란검증 필수 교훈.)

## 트랙2 — 보완 병행 전략
- 후보(저베타/과대낙폭반등/저PER/매출성장모멘텀/IEF) 전부 **본전략과 상관 양수(+0.32~+0.82)** — 이 표본에 음의 헤지 없음.
- MDD 완화하나 수익 더 깎여 Calmar 1개 빼고 하락. 본전략 최악일엔 전 슬리브 동반하락(롱온리 한계).
- LOWO: SNDK 제외 시 합산도 붕괴 = 본전략 SNDK 의존 상속.
- N=1 상승장이라 진짜 약세장 방어는 검증불가 → 이미 있는 국면오버레이+80:20이 담당. 추가 슬리브는 수익만 깎음.

## 메타
신규 신호·보완 전략 둘 다, 이 프로젝트의 반복 결론으로 수렴: 겉보기 알파는 모멘텀/소수winner 교란. robust 독립 알파원 없음. 단순·집중 유지 + OOS 검증이 정답.
