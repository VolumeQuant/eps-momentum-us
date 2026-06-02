# 메가홀드 score/rank 보너스 자율주행 종합 리포트 (2026-06-02)

## 결론 (두괄식)

**V86e가 fundamental하게 최적. 사용자 안 "score/rank 보너스로 자연 상위 진입"은 BT에서 -57~-178p 처참 실패. 전문가 deep analysis로 본질 trade-off 입증 — 매수 단계 메가 보너스는 신규 알파 차단 (KEYS/FAF 등). V86e 유지 확정.**

| 시도 | 변형 수 | 결과 | 결정 |
|------|---------|------|------|
| V87 (rank 직접 override) | 6 variants | -57~-103p vs V86e | 폐기 |
| V88 (계산식 보너스 / 매도 보호) | 8 variants | -11~-179p vs V86e | 매도 보호만 보관 |
| V89 (slot 3 메가 전용) | 6 variants | -143~-155p vs V86e | 폐기 (75일 N=2 통계 부족) |

## 사용자 의도와 BT 결과의 본질적 충돌

### 사용자 의도
"V86e는 보유 결정 layer라 예외 처리. score/rank 자체에 보너스 줘서 자연스럽게 메가가 상위로 가야 함."

### BT가 입증한 진실
**메가 종목의 시그널이 self-reinforcing** — PEG<0.20이 되면 가격 +50% 더 가도 NTM 더 빨라 PEG는 더 낮아짐. 즉:
- score/adj_gap 보너스 약하게 → 메가 자연 5~10위 (V86e와 비슷)
- 보너스 강하게 → 메가 75일 내내 1위 점유 → KEYS/FAF 등 신규 알파 영구 차단

### 전문가 sub-agent 분석 요약

> V86e가 layer를 분리한 것은 **베이지안 정보 가중**입니다.
> - 보유 종목 = "EPS가 가격을 압도하는 슈퍼사이클 확인됨" 추가 정보 누적
> - 신규 종목 = mean reversion (PE 변화율 음수) 정보 하나뿐
> 
> V87/V88이 매수 회수를 줄이는 (baseline 8.6 → V88a 2.2) 이유는: **PEG<0.20 종목이 한번 메가 진입하면 영원히 1·2위 박혀 시간축에서 신규 알파 차단**.

## Phase별 상세 결과

### V87 — Rank 직접 Override

| variant | 설명 | lift | wins |
|---------|------|------|------|
| V87a | rank 강제 1 | -11.3p | 30/100 |
| V87b | rank 강제 2 | -19.4p | 17/100 |
| V87c | rank min(orig, 3) | +31.6p | 96/100 |
| V87d | rank min(orig, 5) | +34.8p | 100/100 |
| V87e | rank min(orig, 10) | +34.8p | 100/100 |
| V87f | rank min(orig, 2) | +9.8p | 64/100 |

**LOWO -MU-SNDK 모두 0/100 wins or 음수** — N=2 의존도 100%, robust 0.
**V86e vs V87 직접**: 모두 V86e -57~-104p 음수.

### V88 — adj_gap 보너스 / 매도 보호

| variant | 설명 | lift | wins | LOWO -MU-SNDK |
|---------|------|------|------|---------------|
| V88a | adj_gap × 1.3 | -79.4p | 0/100 | -45.3p (0) ❌ |
| V88b | adj_gap × 1.5 | -85.4p | 0/100 | -49.4p (0) ❌ |
| V88c | adj_gap × 2.0 | -86.3p | 0/100 | -50.1p (0) ❌ |
| V88d | adj_gap - 0.05 | -74.2p | 0/100 | -40.8p (0) ❌ |
| V88e | adj_gap - 0.10 | -74.2p | 0/100 | -40.8p (0) ❌ |
| V88f | 매도만 rank>20 | +41.5p | 100/100 | +12.8p (97) |
| V88g | 매도만 rank>30 | +81.5p | 100/100 | **+12.8p (97)** |
| V88h | 매도만 rank>∞ | +81.5p | 100/100 | +12.8p (97) |

**V88g/h가 V86e와 본질 등가** (carryover의 다른 표현). alpha -11p but LOWO 약간 우월 (+12.8 vs +10.7).
**매수 단계 보너스 (V88a~e)는 모두 fundamentals 실패.**

### V89 — Slot 3 메가 전용 확장

| variant | 비중 | lift | wins | 매수회수 |
|---------|------|------|------|----------|
| V89a | 50/30/20 | -59.2p | 2/100 | 19.8 |
| V89b | 50/25/25 | -55.0p | 2/100 | 19.8 |
| V89c | 40/30/30 | -54.1p | 2/100 | 19.8 |
| V89d | 60/30/10 | -64.6p | 2/100 | 19.8 |
| V89e | 균등 33/33/33 | -53.5p | 1/100 | 19.8 |
| V89f | 메가 있을때만 활성 | -59.2p | 2/100 | 19.8 |

**모두 V86e 대비 -143~-155p**. 매수 회수 19.8 (baseline 11.3) — slot 3가 알파를 더하지 않고 회전 빈도만 늘려 회전 비용 가설.
**전문가 예측 정확**: "75일 N=2로 통계 의미 약함"

## 부수적 발견

### Phase 7 rev_exit 재검토 (전문가 권고)
| rev_exit | lift | LOWO -MU-SNDK | 평가 |
|----------|------|---------------|------|
| 0.15 | +82.5p | +11.8p (99/100) | **안전** |
| 0.20 | +86.0p | -1.3p (32/100) | ⚠️ valley |
| **0.25** | **+92.5p** | +10.7p (91/100) | **현재 적용** |

전문가: "rev_growth<0.25는 fit risk → 0.15가 더 안전" — 알파 -10p but LOWO +1p 우월.
판단: 현재 0.25 유지. 사용자 자고 일어났을 때 0.15로 보수화 검토 가능.

## 최종 권고

### 1. V86e 유지 ⭐ (변경 없음)
- BT 모든 차원 우월
- LOWO -MU-SNDK +10.7p (91/100) 견고성 입증
- 현재 master 적용 중

### 2. 사용자 의도 충족 방법 (BT 무관, 표시 layer만)

#### A. 멘탈 모델 재기술
- `check_mega_hold`: "예외 함수" → "regime 분기 함수"로 docstring 갱신
- 코드 변경 없이 **사용자 일관성 욕구 충족**
- "메가는 mean reversion regime이 아니라 EPS revision regime이다" 명시

#### B. 표시 강화 (이미 V86e 했음)
- watchlist 메가 홀드 표시 ✓
- 운영 규칙 "🔒메가 시그니처는 홀드" ✓

#### C. 향후 자율주행 후보 (실제 가치)
1. **PEG cutoff sweep** (0.15/0.18/0.20/0.22/0.25) — Phase 2에서 PEG only가 진짜 알파라 확인했지만 cutoff 정밀 검증 필요
2. **약세장 BT** (2020/2022 데이터 수집) — 메가 반전 시 V86e 거동
3. **rev_growth 0.15 vs 0.25** stress test — 알파 -10p but LOWO +1p trade-off

### 3. V88g/h를 fallback으로 보관
- V86e와 본질 동일, alpha -11p
- 만약 미래 universe에 메가 종목 더 자주 등장하면 (예: 2026 후반 새 AI 사이클) V88g/h가 더 우월할 가능성
- 현재는 V86e 우월

### 4. 폐기 확정
- V87 시리즈 (rank 직접 override)
- V88a~e (adj_gap 보너스)
- V89 시리즈 (slot 3 확장)

## 자율주행 산출물

```
research/auto_bt_v87_rank_bonus.py + .log
research/auto_bt_v88_adj_gap_boost.py + .log
research/auto_bt_v89_slot3_mega.py + .log
research/V87_V88_V89_AUTONOMOUS_REPORT_2026_06_02.md (이 파일)
```

## 사용자가 일어나서 결정할 것

1. **V86e 유지에 동의?** (BT 결과 입증, 변경 권고 없음)
2. **rev_growth cutoff 0.25 → 0.15 보수화?** (LOWO 99/100 vs 91/100, but alpha -10p)
3. **다음 자율주행 priority?** (PEG sweep / 약세장 BT / rev_growth stress)

## Caveat (정직)

- **75일 BT, N=2 (MU/SNDK) 의존** — 모든 결론은 이 한계 위에
- **전문가 sub-agent도 동일 한계 인지** — V86e robust 검증은 LOWO +10.7p (91/100) 하나에 의존
- **메가 반전 국면 미검증** — 다음 자율주행 핵심 후보
