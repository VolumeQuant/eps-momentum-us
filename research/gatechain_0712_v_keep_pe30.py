# -*- coding: utf-8 -*-
"""적대적 검증 (2026-07-12): 권고 keep-pe-30 재실행.

주장 검증 대상:
  - pe행(gap1.5) LOWO 고원: pe30/35/40 exSM = 74.5/82.4/90.5
  - MDD: pe30 -17.7 < pe35 -19.2 < pe40 -20.9 < inf -25.1
  - pe25: raw +121.6 국소피크지만 exSM +40.6 붕괴
반박 렌즈:
  1) 수치 재현되나
  2) exSM만이 아니라 exS/exM 단일 제외에서도 결론 유지되나
  3) 위상별 분포: pe30의 MDD 우위가 특정 위상 몰빵인가
  4) paired 차분(위상별 pe30-대안)이 노이즈 대비 유의한가
읽기 전용 — DB 쓰기 없음.
"""
import sys, os, json
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.dirname(HERE)
sys.path.insert(0, BASE)
sys.path.insert(0, HERE)
import vm_canonical_bt as vc

AD, FULL, DVDB, TC, TE_SPARSE = vc._load()
TE_FULL = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm_full.json'), encoding='utf-8'))
TE_FULL.pop('_meta', None)


def bt(te, pe_max=30, gap_thr=1.5, N=5, R=5, start=2, dv_min=1000.0, phase=0, exclude=frozenset()):
    """agenda2.bt와 동일(agroup 미사용) — canonical 규약 복제 + TE 주입."""
    hold = []; rets = []
    for i in range(start, len(AD)):
        d, pv = AD[i], AD[i - 1]
        px = FULL.get(d, {}); ppx = FULL.get(pv, {})
        drr = 0.0
        for t in hold:
            cu = px.get(t, {}).get('px'); pp = ppx.get(t, {}).get('px')
            if cu and pp and pp > 0:
                drr += (1.0 / N) * (cu - pp) / pp
        rets.append(drr)
        if i % R == phase:
            cand = []
            for tk, v in FULL.get(d, {}).items():
                if tk in exclude or not vc._industry_ok(tk, TC):
                    continue
                dv = DVDB.get(d, {}).get(tk)
                if dv is None or dv < dv_min:
                    continue
                if vc._ms(v) < 0:
                    continue
                if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1:
                    continue
                if v['px'] / v['nc'] > pe_max:
                    continue
                if gap_thr:
                    te_v = vc._pit_te(te, tk, d)
                    g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
                    if g is not None and g < gap_thr:
                        continue
                cand.append((tk, vc._rev90(v)))
            cand.sort(key=lambda x: -x[1])
            hold = [t for t, _ in cand[:N]]
    r = np.array(rets)
    nav = np.cumprod(1 + r); peak = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / peak - 1).min()) * 100


PES = [20, 25, 30, 35, 40, 60, 10 ** 9]
EXES = [('raw', frozenset()), ('exS', frozenset({'SNDK'})), ('exM', frozenset({'MU'})),
        ('exSM', frozenset({'SNDK', 'MU'}))]

res = {}  # (pe, exlbl) -> list of (ret, mdd) per phase
for pe in PES:
    for exl, ex in EXES:
        res[(pe, exl)] = [bt(TE_FULL, pe_max=pe, phase=p, exclude=ex) for p in range(5)]

def lbl(pe):
    return 'inf' if pe > 1e6 else str(pe)

print('═══ 1. pe행 @gap1.5/N5/dv1000, 위상평균 (재현 체크) ═══')
print(f"{'pe':>5} | {'raw ret/mdd':>16} | {'exS':>14} | {'exM':>14} | {'exSM':>14}")
for pe in PES:
    line = f'{lbl(pe):>5} |'
    for exl, _ in EXES:
        ph = res[(pe, exl)]
        rr = np.mean([x[0] for x in ph]); mm = np.mean([x[1] for x in ph])
        line += f'  {rr:+7.1f}/{mm:+6.1f} |'
    print(line)

print('\n═══ 2. 위상별 상세 (raw): pe별 (ret, mdd) ═══')
for pe in PES:
    ph = res[(pe, 'raw')]
    print(f"  pe{lbl(pe):>4}: " + '  '.join(f'p{p}({r:+6.1f},{m:+6.1f})' for p, (r, m) in enumerate(ph)))

print('\n═══ 3. paired 차분 (pe30 − 대안), 위상별 ret / mdd ═══')
base = res[(30, 'raw')]
for pe in PES:
    if pe == 30:
        continue
    alt = res[(pe, 'raw')]
    dr_ = [base[p][0] - alt[p][0] for p in range(5)]
    dm = [base[p][1] - alt[p][1] for p in range(5)]
    print(f"  pe30−pe{lbl(pe):>4}: ret diff 위상별 {[round(x,1) for x in dr_]} 평균 {np.mean(dr_):+.1f}"
          f" | mdd diff {[round(x,1) for x in dm]} 평균 {np.mean(dm):+.1f}"
          f" | mdd우위 위상수 {sum(1 for x in dm if x > 0)}/5")

print('\n═══ 4. exSM paired 차분 (pe30 − 대안) ═══')
base = res[(30, 'exSM')]
for pe in PES:
    if pe == 30:
        continue
    alt = res[(pe, 'exSM')]
    dr_ = [base[p][0] - alt[p][0] for p in range(5)]
    dm = [base[p][1] - alt[p][1] for p in range(5)]
    print(f"  pe30−pe{lbl(pe):>4}: ret {[round(x,1) for x in dr_]} 평균 {np.mean(dr_):+.1f}"
          f" | mdd {[round(x,1) for x in dm]} 평균 {np.mean(dm):+.1f}")

print('\n═══ 5. pe25 붕괴 원인: exS/exM 단일 제외 분해 ═══')
for exl, _ in EXES:
    ph = res[(25, exl)]
    print(f"  pe25 {exl}: {np.mean([x[0] for x in ph]):+.1f} / {np.mean([x[1] for x in ph]):+.1f}"
          f" | 위상별 {[(round(r,1), round(m,1)) for r, m in ph]}")

print('\n완료.')
