# -*- coding: utf-8 -*-
"""적대적 검증 (2026-07-12): keep-rev90-single 권고의 미출력 주장 검증.

주장: "[2]의 rev90상위 그룹 rev30 tercile spread(+8.95% fwd20)는 LOWO에서 소멸."
검증: tercile 분해를 exclude={}, {SNDK}, {MU}, {SNDK,MU}로 반복 + 티커 기여 분해.
"""
import sys, os
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.dirname(HERE)
sys.path.insert(0, BASE); sys.path.insert(0, HERE)
import vm_canonical_bt as vb
import gatechain_0712_a1_multiwindow as a1

AD, FULL = a1.AD, a1.FULL
START, R = a1.START, a1.R
LOWO_SETS = a1.LOWO_SETS
lbl = {frozenset(): 'full', frozenset({'SNDK'}): '-SNDK', frozenset({'MU'}): '-MU',
       frozenset({'SNDK', 'MU'}): '-both'}


def tercile(exclude, hz):
    day_terc = {0: [], 1: [], 2: []}; day_spread = []
    hi_members = {}
    for i in range(START, len(AD) - hz):
        d = AD[i]; ps = a1.passers(d, exclude=exclude)
        rows = []
        for tk in ps:
            f = a1.fwd_ret(tk, i, hz)
            r30 = a1.rev(FULL[d][tk], 'n30')
            if f is None or r30 is None: continue
            rows.append((tk, a1.rev0(FULL[d][tk], 'n90'), r30, f))
        if len(rows) < 15: continue
        r90s = np.array([r[1] for r in rows])
        cut = np.percentile(r90s, 70)
        grp = [r for r in rows if r[1] >= cut]
        if len(grp) < 6: continue
        grp.sort(key=lambda x: x[2])
        k = len(grp) // 3
        lo, mid, hi = grp[:k], grp[k:2 * k], grp[2 * k:]
        m = [np.mean([g[3] for g in b]) for b in (lo, mid, hi)]
        for j in range(3): day_terc[j].append(m[j])
        day_spread.append(m[2] - m[0])
        for g in hi: hi_members[g[0]] = hi_members.get(g[0], 0) + 1
    sp = np.array(day_spread)
    return day_terc, sp, hi_members


for hz in (5, 20):
    print(f'=== fwd{hz}d: rev90상위30% 내 rev30 tercile spread(high-low), LOWO별 ===')
    for ex in LOWO_SETS:
        terc, sp, him = tercile(ex, hz)
        top_hi = sorted(him.items(), key=lambda x: -x[1])[:6]
        print(f'  {lbl[ex]:6s}: low {np.mean(terc[0]):+6.2f}  mid {np.mean(terc[1]):+6.2f}  high {np.mean(terc[2]):+6.2f}'
              f'  spread {sp.mean():+6.2f}% (std {sp.std():.2f}, n일 {len(sp)}, 양수일 {np.mean(sp>0)*100:.0f}%,'
              f' t~{sp.mean()/sp.std()*np.sqrt(len(sp)):+.1f}⚠중첩)')
        print(f'          high-tercile 최다 멤버: ' + ', '.join(f'{t}({c})' for t, c in top_hi))
