# -*- coding: utf-8 -*-
"""구엔진 +304% 결과의 정체 규명: 보유이력 추적 + LOWO."""
import sys, os
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
from _old_engine_bt_2026_07_31 import bt_daily, ranks_for, load

ALLG = dict(dv=1, pe=1, gap=1, seg=1)

def trace(src='ag', E=3, X=10, S=2, gates=ALLG, end_date=None, exclude=frozenset()):
    ad, FULL, DVDB, TC, _ = C._load(); TE = C._load_te('full')
    if end_date: ad = tuple(d for d in ad if d <= end_date)
    hold = []; segs = {}; out = []
    for i in range(2, len(ad)):
        d = ad[i]
        rk = ranks_for(d, src, FULL, DVDB, TC, TE, gates)
        keep = [t for t in hold if rk.get(t, 9999) <= X]
        for t in set(hold) - set(keep):
            e = segs.pop(t); px = FULL.get(d, {}).get(t, {}).get('px')
            out.append((t, e[0], d, e[1], px, (px / e[1] - 1) * 100 if px and e[1] else None))
        if len(keep) < S:
            pool = sorted([(r, t) for t, r in rk.items()
                           if r <= E and t not in keep and t not in exclude])
            for _, t in pool[:S - len(keep)]:
                keep.append(t); segs[t] = (d, FULL.get(d, {}).get(t, {}).get('px'))
        hold = keep
    for t, (d0, p0) in segs.items():
        px = FULL.get(ad[-1], {}).get(t, {}).get('px')
        out.append((t, d0, '보유중', p0, px, (px / p0 - 1) * 100 if px and p0 else None))
    return out

if __name__ == '__main__':
    end = sys.argv[1] if len(sys.argv) > 1 else None
    tr = trace(end_date=end)
    print('=== 구엔진(괴리율+전게이트 E3/X10/S2) 거래 내역 %d건 ===' % len(tr))
    print('%-6s %-11s %-11s %8s %8s %9s' % ('종목', '진입', '청산', '진입가', '청산가', '수익%'))
    tot = {}
    for t, d0, d1, p0, p1, r in sorted(tr, key=lambda x: x[1]):
        print('%-6s %-11s %-11s %8s %8s %9s'
              % (t, d0, d1, '%.1f' % p0 if p0 else '-', '%.1f' % p1 if p1 else '-',
                 '%+.1f' % r if r is not None else '-'))
        if r is not None: tot[t] = tot.get(t, 0) + r
    print('\n종목별 누적 기여(단순합):')
    for t, v in sorted(tot.items(), key=lambda x: -x[1]):
        print('  %-6s %+8.1f%%' % (t, v))

    print('\n=== LOWO — 한 종목 운인가 ===')
    print('%-22s %10s %10s' % ('제외', '수익%', 'MDD%'))
    base = bt_daily(src='ag', gates=ALLG, end_date=end)
    print('%-22s %+10.1f %+10.1f' % ('없음', base[0], base[1]))
    for t in [x[0] for x in sorted(tot.items(), key=lambda x: -x[1])[:6]]:
        r = bt_daily(src='ag', gates=ALLG, end_date=end, exclude=frozenset([t]))
        print('%-22s %+10.1f %+10.1f' % ('ex-' + t, r[0], r[1]))
    top3 = frozenset(x[0] for x in sorted(tot.items(), key=lambda x: -x[1])[:3])
    r = bt_daily(src='ag', gates=ALLG, end_date=end, exclude=top3)
    print('%-22s %+10.1f %+10.1f' % ('ex-상위3 동시', r[0], r[1]))
