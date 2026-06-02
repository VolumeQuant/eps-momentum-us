# V92 4 Phase 자율주행 종합 + "v84 이후 흔들림" 진단 (2026-06-03)

## 결론 (두괄식)

**v84 → V86e+ 흔들림 우려 정당. but BT가 각 단계 진짜 진보 입증 (patch on patch 아님). V93 후보 1개 발견 (eps_quality min→mean, LOWO +4p) but DB 재계산 필요. V86e+ freeze 권고.**

## 사용자 우려 진단 (Phase D)

**"v84 이후 자꾸 흔들리네"** — patch on patch 의심.

| 단계 | alpha vs v84 | LOWO -MU-SNDK | 평가 |
|------|------|-------|------|
| v84 (baseline) | 0p | 0p | 시작점 |
| v86 (NTM AND PEG<0.20) | **+81.5p** | +12.8p | **큰 진보** |
| V86e (PEG<0.20 only) | +81.5p | +12.8p | NTM 빼도 동일 (단순화) |
| V86e + rev_exit 0.25 | +92.5p | +10.7p | alpha +11p / LOWO -2p (trade-off) |
| V86e+ (PEG<0.22) | +92.5p | **+14.7p** | LOWO +4p 회복 |

→ **각 단계 진짜 진보**. 단순 patch on patch 아니라 BT robust 검증된 누적 개선.

## 4 후보 검증 결과

### Phase A — eps_quality 재설계 ★
| seg_metric | lift | LOWO -MU-SNDK |
|-----------|------|---------------|
| min (현재) | +92.5p | +14.7p (95/100) |
| **mean** | +92.9p | **+18.9p (95/100)** |
| **weighted** | +92.9p | **+18.9p (95/100)** |

→ alpha +0.4p, LOWO **+4.2p** 더 robust. 가속도 정보 보존 효과.

### Phase B — rev_growth cutoff stress
- 0.20 valley **+1.3p (57/100)** 재확인 (피해야)
- 0.25/0.30 plateau +92.5p / LOWO +14.7p
- **현재 0.25 그대로**

### Phase C — dd_30 메가 면제
| dd | exempt | lift | LOWO |
|----|--------|------|------|
| 0.25 | NO (현재) | +92.5p | +14.7p |
| 0.30 | NO | +93.8p | +15.0p |
| **0.25** | **YES** | **+93.3p** | +14.7p |
| 0.30 | YES | +93.7p | +15.0p |

→ 메가 면제 +0.8p, dd 0.30 +1.3p. 마이너 개선.

### Phase E — conviction asymmetry (사용자 의견)
| variant | lift | wins | LOWO |
|---------|------|------|------|
| E1 (메가 adj_gap>0 → 0) | **-38.6p** | 9/100 | -63p (0/100) |
| E2 (메가 adj_gap>0 → -1) | -38.6p | 9/100 | -63p |
| E3 (메가 adj_gap>0 → -3) | -38.6p | 9/100 | -63p |
| E4 (모든 adj_gap>0 → cap 5) | -38.6p | 9/100 | -63p |

→ **사용자 직관 정확**. conviction 비대칭은 "안 좋은 종목 거르기" 의도. 무력화 시 신규 매수 logic 망가짐.

## 종합 — 가장 robust한 conf

| seg | rev | dd_exempt | lift | LOWO |
|-----|-----|-----------|------|------|
| **mean** | 0.25 | YES | **+93.7p** | **+18.9p** ★ |
| weighted | 0.25 | YES | +93.7p | +18.9p (동일) |
| min | 0.25 | YES | +93.3p | +14.7p |
| **min** | **0.25** | **NO (현재 V86e+)** | **+92.5p** | **+14.7p** |

V93 후보 = mean + dd_exempt YES (+1.2p, +4.2p LOWO 개선)

## 권고

### 옵션 1: V86e+ 그대로 freeze ⭐
- 마지막 변경 안 함. 사용자 "흔들린다" 우려 해소
- V86e+ 자체 BT robust 입증됨 (+92.5p / LOWO +14.7p)
- 다음 변경은 약세장 데이터 누적 후 (수개월 이후)

### 옵션 2: V93 적용 (eps_quality mean + dd_exempt YES)
- alpha +1.2p (마이너), LOWO +4.2p (의미 있는 robust)
- 단점: **DB 재계산 필요** (eps_quality가 adj_gap 곱셈 → ranking 변경)
- "v84 이후 흔들림" 우려 키울 가능성
- 작업량: v83/v84 마이그레이션 패턴

### 옵션 3: 절충 — dd_mega_exempt만 추가 (DB 재계산 X)
- alpha +0.8p, LOWO 동일 (+14.7p)
- 코드 1줄 추가
- 메가 종목 단기 폭락 후 진입 가능 (슈퍼사이클 회복)

## 내 권고: 옵션 1 (V86e+ freeze)

이유:
1. 사용자 "흔들린다" 우려 정당 → 변경 빈도 최소화
2. V86e+ 자체 BT 충분히 robust
3. eps_quality 변경은 DB 재계산 + 큰 변경 — marginal 개선에 비해 risk 큼
4. **시스템 안정성 우선** (다음 cron 자동 작동, 변경 없음)
5. V86e+가 BT 모든 차원 우월. 더 좋은 게 marginal 개선뿐

## 산출물

- `research/auto_bt_v92_4phase_audit.py` + `.log` (4 phase 검증)
- `research/auto_bt_v92_phaseE_conviction.py` + `.log` (Phase E 실험)
- 이 보고서

## Caveat (정직)

- 75일 BT, N=2 (MU/SNDK) 한계 동일
- V92 발견들도 같은 한계 위
- 약세장 미검증 — 메가 반전 시 거동 모름
- **사용자 자가 발견 시 추가 자율주행 가능**
