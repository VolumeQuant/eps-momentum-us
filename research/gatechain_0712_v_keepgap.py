# -*- coding: utf-8 -*-
"""적대적 검증: keep-gap-1.5 권고 (2026-07-12).

검증 항목:
1) 베이스라인 재현: pe30/gap1.5/N5/dv1000, end=2026-07-08 위상평균 +103.2/-17.7, exSM +67.6?
2) gap 스윕 plateau: 0(무게이트)/1.0/1.25/1.5/1.75/2.0/2.25/2.5 — 전기간+7/08, 위상별+평균
3) LOWO: exclude SNDK / MU / 둘다 — 각 임계에서
4) paired 차분: gap1.5 vs 1.25 vs 무게이트 — 위상별 차분 분포(노이즈 대비)
"""
import sys, os
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np
from vm_canonical_bt import canonical_bt

EXCL = {
    'full': frozenset(),
    'exSNDK': frozenset({'SNDK'}),
    'exMU': frozenset({'MU'}),
    'exSM': frozenset({'SNDK', 'MU'}),
}
GAPS = [0, 1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5]
R = 5

def report(gap_thr, end_date, exclude):
    pp = {p: canonical_bt(pe_max=30, gap_thr=gap_thr, N=5, R=R, phase=p,
                          end_date=end_date, dv_min=1000.0, exclude=exclude)
          for p in range(R)}
    rets = [pp[p][0] for p in range(R)]
    mdds = [pp[p][1] for p in range(R)]
    return rets, mdds

for end_date, label in [('2026-07-08', 'end=2026-07-08'), (None, '전기간(~7/10)')]:
    print(f'\n================ {label} ================')
    for exname, exset in EXCL.items():
        print(f'\n--- {exname} ---')
        base_rets = None
        for g in GAPS:
            rets, mdds = report(g, end_date, exset)
            avg_r, avg_m = np.mean(rets), np.mean(mdds)
            line = f'gap>={g:<5}: avg {avg_r:+7.1f}% / MDD {avg_m:+6.1f}%  phases: ' + \
                   ' '.join(f'{r:+6.1f}' for r in rets)
            if g == 1.5:
                base_rets = rets
            print(line)
        # paired diff vs 1.5
        print('  paired diff (gap - gap1.5) per phase:')
        for g in GAPS:
            if g == 1.5: continue
            rets, _ = report(g, end_date, exset)
            d = [rets[p] - base_rets[p] for p in range(R)]
            print(f'    gap{g:<5}: mean {np.mean(d):+6.1f}p  phases: ' +
                  ' '.join(f'{x:+6.1f}' for x in d))
