# -*- coding: utf-8 -*-
"""적대적 검증 (2026-07-12): 권고 keep-rev90-single 반박 시도.

[V1] 횡단면 버킷 증거(rev90高 그룹 내 rev30 tercile fwd20 spread +8.95%)가
     LOWO(SNDK/MU 제외)에서 정말 소멸하는지 직접 재현.
[V2] 권고가 스윕하지 않은 '온건 블렌드'(w*rev90+(1-w)*rev30, w=0.95~0.6)가
     LOWO 전승하는 robust 승자인지 — 있으면 권고 반박.
[V3] 위상별 diff 분포 — baseline 우위가 특정 위상 몰빵인지.
"""
import sys, os
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE); sys.path.insert(0, os.path.dirname(HERE))
import gatechain_0712_a1_multiwindow as g

LOWO = g.LOWO_SETS
LBL = {frozenset(): 'full', frozenset({'SNDK'}): '-SNDK', frozenset({'MU'}): '-MU',
       frozenset({'SNDK', 'MU'}): '-both'}


def bucket_test(drop=frozenset(), hz=20):
    day_spread = []
    terc = {0: [], 1: [], 2: []}
    for i in range(g.START, len(g.AD) - hz):
        d = g.AD[i]
        rows = []
        for tk in g.passers(d):
            if tk in drop: continue
            f = g.fwd_ret(tk, i, hz)
            r30 = g.rev(g.FULL[d][tk], 'n30')
            if f is None or r30 is None: continue
            rows.append((g.rev0(g.FULL[d][tk], 'n90'), r30, f))
        if len(rows) < 15: continue
        r90s = np.array([r[0] for r in rows])
        cut = np.percentile(r90s, 70)
        grp = [r for r in rows if r[0] >= cut]
        if len(grp) < 6: continue
        grp.sort(key=lambda x: x[1])
        k = len(grp) // 3
        lo, mid, hi = grp[:k], grp[k:2 * k], grp[2 * k:]
        m = [np.mean([x[2] for x in b]) for b in (lo, mid, hi)]
        for j in range(3): terc[j].append(m[j])
        day_spread.append(m[2] - m[0])
    sp = np.array(day_spread)
    return (np.mean(terc[0]), np.mean(terc[1]), np.mean(terc[2]),
            sp.mean(), sp.std(), len(sp), float(np.mean(sp > 0)) * 100)


def main():
    print('=' * 78)
    print('[V1] 버킷(rev90상위30% 내 rev30 tercile, fwd20) — LOWO 재현')
    for drop in LOWO:
        lo, mid, hi, sm, ss, n, pos = bucket_test(drop, 20)
        print(f'  {LBL[drop]:6s}: low {lo:+.2f}  mid {mid:+.2f}  high {hi:+.2f}'
              f'  | spread {sm:+.2f}% (std {ss:.2f}, n일 {n}, 양수일 {pos:.0f}%)')
    print('  (fwd5 병기)')
    for drop in LOWO:
        lo, mid, hi, sm, ss, n, pos = bucket_test(drop, 5)
        print(f'  {LBL[drop]:6s}: low {lo:+.2f}  mid {mid:+.2f}  high {hi:+.2f}'
              f'  | spread {sm:+.2f}% (std {ss:.2f}, n일 {n}, 양수일 {pos:.0f}%)')

    print('\n' + '=' * 78)
    print('[V2] 온건 블렌드 w*rev90+(1-w)*rev30 — 권고가 스윕 안 한 공백지대')
    for w in (0.95, 0.9, 0.8, 0.7, 0.6):
        g.RANKS[f'mix{w}'] = (lambda w_: lambda v: w_ * g.rev0(v, 'n90')
                              + (1 - w_) * g.rev0(v, 'n30'))(w)
    for w in (0.9, 0.8):
        g.RANKS[f'mix60_{w}'] = (lambda w_: lambda v: w_ * g.rev0(v, 'n90')
                                 + (1 - w_) * g.rev0(v, 'n60'))(w)
    base = {ex: g.report('rev90', None, ex) for ex in LOWO}
    print('  baseline rev90 : ' + '  '.join(
        f'{LBL[ex]} {base[ex][0]:+6.1f}%/{base[ex][1]:+5.1f}' for ex in LOWO))
    names = [f'mix{w}' for w in (0.95, 0.9, 0.8, 0.7, 0.6)] + ['mix60_0.9', 'mix60_0.8']
    for nm in names:
        cells = []
        for ex in LOWO:
            t, m, per = g.report(nm, None, ex)
            diffs = [per[p][0] - base[ex][2][p][0] for p in range(g.R)]
            cells.append((t, m, float(np.mean(diffs)), float(np.min(diffs)),
                          float(np.max(diffs)), sum(1 for x in diffs if x > 0)))
        print(f'  {nm:14s}: ' + '  '.join(
            f'{LBL[ex]} {c[0]:+6.1f}%/{c[1]:+5.1f} (Δ{c[2]:+5.1f} [{c[3]:+.0f},{c[4]:+.0f}] 승{c[5]}/5)'
            for ex, c in zip(LOWO, cells)))

    print('\n' + '=' * 78)
    print('[V3] 위상별 paired diff (full, 대표 변형) — 우위가 위상 몰빵인가')
    for nm in ('rev60', 'blend_v8010', 'blend_eq', 'mix0.9', 'mix0.8'):
        _, _, per = g.report(nm, None, frozenset())
        diffs = [per[p][0] - base[frozenset()][2][p][0] for p in range(g.R)]
        print(f'  {nm:12s}: ' + '  '.join(f'위상{p} {diffs[p]:+6.1f}' for p in range(g.R)))


if __name__ == '__main__':
    main()
