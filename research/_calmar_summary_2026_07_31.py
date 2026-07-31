# -*- coding: utf-8 -*-
"""최종 요약표 — 2x2(지표 × 게이트) Calmar 비교 + 검증 결과 (2026-07-31)"""
import sys, os
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
from _factcheck_2026_07_31 import bt
from _pure_eps_followup_2026_07_31 import agg as agg_cost

ad, *_ = C._load()
YRS = (len(ad) - 2) / 252.0
G_NOW = dict(dv=1, pe=1, gap=1, seg=1)
G_SLIM = dict(dv=1, pe=1)

def calmar(t, m):
    cagr = ((1 + t / 100) ** (1 / YRS) - 1) * 100
    return cagr, (cagr / abs(m) if m else 0)

def rp(**kw):
    o = [bt(phase=p, **kw) for p in range(5)]
    return float(np.mean([x[0] for x in o])), float(np.mean([x[1] for x in o]))

CFG = [
    ('A  현행 (전망상향폭 + 조건4개)', 'rev90',   G_NOW),
    ('B  괴리율 + 조건4개 (지표만 교체)', 'adj_gap', G_NOW),
    ('C  전망상향폭 + 조건2개 (게이트만 정리)', 'rev90',   G_SLIM),
    ('D★ 괴리율 + 조건2개 (둘 다)', 'adj_gap', G_SLIM),
]

if __name__ == '__main__':
    print('측정기간 %s ~ %s (%.2f년, %d영업일) | 슬롯5 · 5일리밸 · 위상평균'
          % (ad[2], ad[-1], YRS, len(ad) - 2))
    print()
    print('%-36s %8s %9s %8s %8s' % ('구성', '총수익%', '연환산%', 'MDD%', 'Calmar'))
    print('-' * 74)
    res = {}
    for lbl, met, g in CFG:
        t, m = rp(metric=met, gates=g); cg, cal = calmar(t, m)
        res[lbl[0]] = (t, m, cal)
        print('%-36s %+8.1f %+9.1f %+8.1f %8.2f' % (lbl, t, m, cg, cal))

    print('\n[거래비용 반영 후 Calmar]')
    print('%-36s %8s %8s %8s' % ('구성', '0bp', '10bp', '20bp'))
    print('-' * 64)
    for lbl, met, g in [CFG[0], CFG[3]]:
        gg = dict(use_dv=bool(g.get('dv')), use_pe=bool(g.get('pe')),
                  use_gap=bool(g.get('gap')), use_seg=bool(g.get('seg')))
        row = []
        for c in (0, 10, 20):
            t, m, _ = agg_cost(metric=met, cost_bp=c, **gg)
            row.append(calmar(t, m)[1])
        print('%-36s %8.2f %8.2f %8.2f' % (lbl, *row))

    print('\n[검증: 최악 조건에서의 Calmar]')
    print('%-24s %10s %10s %8s' % ('', 'A 현행', 'D★ 제안', '판정'))
    print('-' * 56)
    tests = [('전체', dict()), ('~4/15', dict(end_date='2026-04-15')),
             ('~5/15', dict(end_date='2026-05-15')), ('~6/15', dict(end_date='2026-06-15')),
             ('~7/15', dict(end_date='2026-07-15')),
             ('ex-SNDK', dict(exclude=frozenset(['SNDK']))),
             ('ex-MU', dict(exclude=frozenset(['MU']))),
             ('ex-SNDK/MU', dict(exclude=frozenset(['SNDK', 'MU']))),
             ('ex-SNDK/MU/STX', dict(exclude=frozenset(['SNDK', 'MU', 'STX'])))]
    win = 0
    for lbl, kw in tests:
        a = calmar(*rp(metric='rev90', gates=G_NOW, **kw))[1]
        d = calmar(*rp(metric='adj_gap', gates=G_SLIM, **kw))[1]
        if d > a: win += 1
        print('%-24s %10.2f %10.2f %8s' % (lbl, a, d, 'D 승' if d > a else 'A 승'))
    print('-' * 56)
    print('%-24s %10s %10s %8s' % ('', '', '', '%d/%d' % (win, len(tests))))
