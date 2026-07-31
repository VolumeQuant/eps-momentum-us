# -*- coding: utf-8 -*-
"""MDD 동일 현상 규명 (2026-07-31) — 다른 종목을 고르는 두 전략의 6월까지 MDD가
   −16.4%로 소수점까지 같았던 이유. 보유 겹침 / MDD 발생 시점 / 낙폭 원천 추적."""
import sys, os
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
from _prod_faithful_bt_2026_07_31 import load

FULL, AG, FUND = load()
TE = C._load_te('full')
AD, _, _, TC, _ = C._load()


def path(mode, R=5, phase=0, end=None, N=5):
    ad = tuple(d for d in AD if (not end or d <= end))
    hold = []; rets = []; holds = {}
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]
        px, ppx = FULL.get(d, {}), FULL.get(pv, {})
        r = 0.0
        for t in hold:
            cu = px.get(t, {}).get('px'); pp = ppx.get(t, {}).get('px')
            if cu and pp and pp > 0: r += (1.0 / N) * (cu - pp) / pp
        if i % R == phase:
            cand = []
            for tk, v in FULL.get(d, {}).items():
                if not C._industry_ok(tk, TC): continue
                p, nc = v['px'], v['nc']
                if v['dv'] is None or v['dv'] < 1000.0: continue
                if nc <= 0 or (v['n90'] or 0) <= 0.1: continue
                if p / nc > 30.0 or p < 10 or (v['na'] or 0) < 3: continue
                if (nc - (v['n90'] or 0)) / abs(v['n90'] or 1) <= 0: continue
                om, fcf, roe = FUND.get(tk, (None, None, None))
                if om is not None and om < 0.05: continue
                if fcf is not None and roe is not None and fcf < 0 and roe < 0: continue
                if mode == 'gap':
                    a = AG.get(d, {}).get(tk)
                    if a is None: continue
                    cand.append((a, tk))
                else:
                    ms = min(C._ms(dict(nc=nc, n7=v['n7'], n30=v['n30'],
                                        n60=v['n60'], n90=v['n90'])), 0)
                    if ms < 0: continue
                    te = C._pit_te(TE, tk, d); g = (nc / te) if (te and te > 0) else None
                    if g is not None and g < 1.5: continue
                    cand.append((-(nc - v['n90']) / abs(v['n90']) * 100, tk))
            cand.sort(); hold = [t for _, t in cand[:N]]; holds[d] = list(hold)
        rets.append(r)
    nav = np.cumprod(1 + np.array(rets)); pk = np.maximum.accumulate(nav)
    return ad[2:], nav, nav / pk - 1, holds


if __name__ == '__main__':
    print('=== ① MDD가 언제 발생했나 (위상0 단일경로) ===')
    for end, lbl in [('2026-06-15', '~6/15'), ('2026-07-15', '~7/15'), (None, '전체')]:
        out = []
        for mode in ('rev90', 'gap'):
            dts, nav, dd, _ = path(mode, end=end)
            i = int(np.argmin(dd))
            pk_i = int(np.argmax(nav[:i + 1]))
            out.append('%s: %.1f%% (%s→%s)' % (mode, dd[i] * 100, dts[pk_i][5:], dts[i][5:]))
        print('  %-8s %s | %s' % (lbl, out[0], out[1]))

    print('\n=== ② 위상 5개 각각의 MDD (평균이 우연히 같은 건지) ===')
    for end, lbl in [('2026-06-15', '~6/15'), (None, '전체')]:
        for mode in ('rev90', 'gap'):
            ms = []
            for p in range(5):
                _, _, dd, _ = path(mode, phase=p, end=end)
                ms.append(dd.min() * 100)
            print('  %-8s %-6s %s  평균 %.2f' % (lbl, mode,
                  ' '.join('%6.1f' % m for m in ms), np.mean(ms)))

    print('\n=== ③ 보유 종목 겹침 ===')
    _, _, _, ha = path('rev90'); _, _, _, hb = path('gap')
    ov = [(d, len(set(ha[d]) & set(hb[d]))) for d in sorted(ha) if d in hb]
    print('  ' + '  '.join('%s:%d' % (d[5:], n) for d, n in ov))
    print('  평균 겹침 %.2f/5' % np.mean([n for _, n in ov]))
    pre = [n for d, n in ov if d < '2026-06-15']; post = [n for d, n in ov if d >= '2026-06-15']
    print('  6/15 이전 평균 %.2f/5   |   6/15 이후 평균 %.2f/5' % (np.mean(pre), np.mean(post)))
