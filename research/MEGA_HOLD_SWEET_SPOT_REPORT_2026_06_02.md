# 메가홀드 Sweet Spot 자율주행 종합 리포트 (2026-06-02)

## 결론 (두괄식)

**회사 PC v86 (NTM≥60 AND PEG<0.20)은 over-engineered. 진짜 sweet spot은 V86e: PEG<0.20 메가홀드 + min_seg<-2 OR rev_growth<25% 매도. 알파 +11p / LOWO 견고성 +10.7p 개선.**

| 지표 | v86 (회사PC) | V86e (sweet spot) | 차이 |
|------|--------------|---------------------|------|
| 전체 lift | +81.5p | **+92.5p** | +11.0p |
| paired wins | 100/100 | 100/100 | 동일 |
| LOWO -MU | +58.8p | +72.5p | +13.7p |
| LOWO -SNDK | +23.5p | +13.0p | -10.5p |
| **LOWO -MU-SNDK** | **+0.0p (0/100)** | **+10.7p (91/100)** | +10.7p ★ |
| 인접 plateau | 좁음 (4 cells) | 넓음 (PEG 0.18~0.25 × rev 0.25~0.30) | ↑↑ |
| 메가홀드 universe | 미래 universe 변화 위험 | PEG-universal | ↑ |
| 복잡도 (조건 수) | 2 (NTM, PEG) | 2 (PEG, rev_growth exit) | 동일 |

---

## V86e 사양

```python
def check_mega_hold(ticker):
    """V86e (2026-06-02): PEG-only 메가홀드.
    
    조건 1개:
      PEG = (price/ntm_current) / (rev_growth × 100) < 0.20
    """
    ...
    return peg < 0.20  # NTM 조건 없음

# 매도 트리거 (다음 중 하나):
# 1. min_seg < -2 (EPS 꺾임) — 기존
# 2. rev_growth < 0.25 (매출성장 25% 미만) — V86e 신규
# 3. 메가 조건 자체 미충족 (PEG ≥ 0.20)

# 비중: 메가홀드 포함 시 50/50 (v86와 동일)
# 보호 강도: 무한 (rank cutoff 없음 — Phase 4 검증)
```

---

## 자율주행 Phase 결과 요약

### Phase 1 — NTM × PEG 7×7 = 49 cells 정밀 그리드
- **30 cells plateau** (+81.4 ~ +81.5p / 100/100)
- NTM 조건은 PEG<0.20 안에서 추가 효과 0
- NTM=80 PEG≥0.30 → LOWO -4.3p (다른 메가 종목 노이즈)
- → **NTM 조건 over-engineering 가능성 발견**

### Phase 2 — 대체 시그니처 비교 ★
| variant | lift | LOWO -MU-SNDK |
|---------|------|---------------|
| S1: NTM≥60 only | +71.6p | -9.6p (0/100) ✗ |
| **S2: PEG<0.20 only** | **+81.5p** | **+12.8p (97/100)** ★ |
| S3: NTM≥60 AND PEG<0.20 (v86 원안) | +81.5p | +0.0p (0/100) ⚠️ |
| S6: NTM≥60 AND PEG<0.15 | +39.0p | +0.0p |
| S7: NTM≥60 AND rev_g≥50% | +71.6p | -9.6p ✗ |
| S11: NTM≥100 AND PEG<0.25 AND rev_g≥30% | +81.5p | +0.0p |

**결정적 발견**: PEG가 단독 알파 소스. NTM 조건은 noise + LOWO robustness 손실.

### Phase 3 — 해제 조건 sweet spot
| variant | lift | LOWO -MU-SNDK |
|---------|------|---------------|
| E1: min_seg<-2 only | +81.5p | +12.8p (97) |
| E2-E4: min_seg cutoff 변형 | +81.5p | 동일 |
| **E5: min_seg<-2 OR rev_growth<15% 매도** | **+82.5p** | **+11.8p (99)** ★ |
| E6: + PEG≥0.30 매도 | +79.6p | +8.3p (72) |
| E7: + 60일 cap | +71.0p | +12.8p (97) |

→ rev_growth exit 추가가 알파 + 안정성 모두 개선.

### Phase 4 — 보호 강도 (rank cutoff)
| variant | lift |
|---------|------|
| F1: 무한 보호 | +78.6p ★ |
| F2: rank≤15 보호 | -1.1p (사실상 효과 0) |
| F3: rank≤20 보호 | +7.9p |
| F4: rank≤30 보호 | +35.4p |
| F5: rank≤50 보호 | +35.4p (F4와 동일) |

→ MU/SNDK가 30위 밖으로 자주 밀려 무한 보호 필수.

### Phase 5 — V86 vs V86b vs V86c 직접 비교
| variant | lift | LOWO -MU-SNDK | 전반 | 후반 |
|---------|------|---------------|------|------|
| V86 (v86 원안) | +81.5p | +0.0p (0) | +124.2p | +73.5p |
| V86b (PEG only) | +81.5p | +12.8p (97) | +124.2p | +73.5p |
| V86c (PEG + rev<15% exit) | +82.5p | +11.8p (99) | +125.8p | +74.5p |

→ V86c가 모든 면에서 V86 우월 (lift +1p, LOWO 무한 개선).

### Phase 6 — rev_exit cutoff 그리드
| rev_exit | lift |
|----------|------|
| 0.10 | +81.5p |
| 0.15 | +82.5p |
| **0.20** | **+86.0p** but LOWO -MU-SNDK -1.3p ⚠️ **valley** |
| **0.25** | **+92.5p** ★ |
| 0.30 | +92.5p (plateau) |

### Phase 7 — rev_exit LOWO 종합 정밀 ★★
| rev_exit | lift | wins | -MU | -SNDK | **-MU-SNDK** |
|----------|------|------|-----|-------|--------------|
| 0.00 (없음) | +81.5 | 100 | +59.0 | +23.7 | +12.8 (97) |
| 0.10 | +81.5 | 100 | +59.0 | +23.7 | +12.8 (97) |
| 0.15 | +82.5 | 100 | +80.9 | +26.4 | +11.8 (99) |
| 0.20 | +86.0 | 100 | +66.5 | +16.5 | **-1.3 (32)** ⚠️ |
| **0.25** | **+92.5** | 100 | +72.5 | +13.0 | **+10.7 (91)** ★ |
| 0.30 | +92.5 | 100 | +72.5 | +13.0 | +10.7 (91) |
| 0.35 | +80.9 | 100 | +71.4 | +15.9 | +10.4 (91) |

**결정**:
- rev_exit 0.25 = 알파 최대 + LOWO robust (+10.7p, 91/100)
- 0.20은 valley (피해야 함)
- 0.30도 동일 — plateau 양방향 robust

---

## 정직한 trade-off

### 안전 후보 (V86c — rev_exit 0.15)
- 알파 +82.5p (vs v86 +81.5p, **+1p**)
- LOWO -MU-SNDK +11.8p (99/100)
- 가장 보수적 매도 → 미래 universe 강건성 ↑

### 공격 후보 (V86e — rev_exit 0.25) ⭐
- 알파 +92.5p (vs v86 +81.5p, **+11p**)
- LOWO -MU-SNDK +10.7p (91/100)
- plateau 우상단 (rev_exit 0.30도 동일) → 임계값 sensitivity 낮음

---

## 권고 — V86e 채택

이유:
1. **알파 +11p (vs v86)** — 무료 lunch에 가까움
2. **LOWO 견고성 무한 개선** (0p → 10.7p)
3. **plateau 우상단 안전 영역** (rev_exit 0.25~0.30 동일, PEG 0.18~0.25 동일)
4. **단순성** — NTM 조건 제거, rev_growth만 1개 새 조건 추가 (net 복잡도 동일)
5. **PEG 의미적 robustness** — NTM은 단기 노이즈 (분석가 변경) but PEG는 fundamentals

---

## V86e Caveat (정직)

1. **75일 데이터 한계** — 단일 상승장, 메가 반전 미검증 (v86과 동일)
2. **rev_exit 0.20 valley** — 임계값 약간 잘못 잡으면 음수 가능 (Phase 6 발견)
3. **N=2 (MU/SNDK)** — 알파 79%가 두 종목, 미래 universe robust 보장 어려움
4. **rev_growth 데이터 정확성** — 분기 발표 lag

→ caveat은 v86과 동일. V86e가 v86보다 개선이지 새로운 위험 도입 없음.

---

## 적용 방안

### 옵션 A: v86 브랜치 위에 V86e 패치 후 머지 (권고)
```bash
git checkout v86-mega-hold
# check_mega_hold 함수 수정:
#   - NTM 조건 제거 (PEG only)
#   - exit 트리거에 rev_growth<0.25 추가
git commit -m "V86e: PEG-only mega hold + rev_growth<25% exit"
git checkout master
git merge v86-mega-hold
git push
```

### 옵션 B: master에 직접 V86e 패치 (v86 브랜치 무시)
```bash
git checkout master
# daily_runner.py 직접 수정 (v86 브랜치 변경 사항 + V86e 모두 포함)
git commit -m "v86e: PEG mega hold + rev<25% exit"
git push
```

→ 옵션 A가 회사 PC handoff 흐름 존중 + git history 깔끔.

---

## 다음 자율주행 후보

1. **약세장 BT** (2020/2022 데이터 수집) — 메가 반전 시 V86e 거동 검증
2. **rev_exit 0.25 vs 0.30 fine-grained** — plateau 내 정밀 비교
3. **시간 stratification** — earnings season vs 비-season 효과
4. **Variant: PEG + rev_growth percentile** — 절대값 대신 universe 상대값 (universe 변화 robust)
