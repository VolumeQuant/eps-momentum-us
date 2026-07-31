# -*- coding: utf-8 -*-
"""괴리율 vs rev90 견고성 전수 (2026-07-31) — 실행지연 lag=1 정직 기준.
사용자 요구: "랜덤 진입일·구간별·견고성·인접안정성 다 해봤냐"
픽을 (날짜×지표)로 선계산 후 NAV만 재계산 → 랜덤 시작일 수백 회도 빠르게.
"""
import sys, os, random
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
from _prod_faithful_bt_2026_07_31 import load

FULL, AG, FUND = load(); TE = C._load_te('full'); AD, _, _, TC, _ = C._load()
PX = {d: {t: v['px'] for t, v in FULL.get(d, {}).items() if v['px']} for d in AD}


def rank_at(d, mode, N=5, pe=30.0, dv=1000.0, exclude=frozenset()):
    cand = []
    for tk, v in FULL.get(d, {}).items():
        if tk in exclude or not C._industry_ok(tk, TC): continue
        p, nc = v['px'], v['nc']
        if v['dv'] is None or v['dv'] < dv: continue
        if nc <= 0 or (v['n90'] or 0) <= 0.1: continue
        if p / nc > pe or p < 10 or (v['na'] or 0) < 3: continue
        if (nc - (v['n90'] or 0)) / abs(v['n90'] or 1) <= 0: continue
        om, fcf, roe = FUND.get(tk, (None, None, None))
        if om is not None and om < 0.05: continue
        if fcf is not None and roe is not None and fcf < 0 and roe < 0: continue
        if mode == 'gap':
            a = AG.get(d, {}).get(tk)
            if a is None: continue
            cand.append((a, tk))
        else:
            ms = min(C._ms(dict(nc=nc, n7=v['n7'], n30=v['n30'], n60=v['n60'], n90=v['n90'])), 0)
            if ms < 0: continue
            te = C._pit_te(TE, tk, d); g = (nc / te) if (te and te > 0) else None
            if g is not None and g < 1.5: continue
            cand.append((-(nc - v['n90']) / abs(v['n90']) * 100, tk))
    cand.sort()
    return [t for _, t in cand[:N]]


_CACHE = {}
def picks(mode, N=5, pe=30.0, dv=1000.0, exclude=frozenset()):
    k = (mode, N, pe, dv, exclude)
    if k not in _CACHE:
        _CACHE[k] = {d: rank_at(d, mode, N, pe, dv, exclude) for d in AD}
    return _CACHE[k]


def run(mode, start=2, end=None, R=5, phase=None, N=5, lag=1, **kw):
    P = picks(mode, N=N, **kw)
    lo, hi = start, (end if end else len(AD))
    ph = phase if phase is not None else lo % R
    hold = []; pend = None; rets = []
    for i in range(lo, hi):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        r = sum((px[t] / ppx[t] - 1) / N for t in hold if t in px and t in ppx and ppx[t] > 0)
        rets.append(r)
        if pend is not None and pend[0] == i: hold = pend[1]; pend = None
        if i % R == ph:
            new = P[d]
            if lag == 0: hold = new
            else: pend = (i + lag, new)
    if not rets: return 0.0, 0.0
    a = np.array(rets); nav = np.cumprod(1 + a); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100


def avg(mode, R=5, **kw):
    o = [run(mode, R=R, phase=p, **kw) for p in range(R)]
    return np.mean([x[0] for x in o]), np.mean([x[1] for x in o])


if __name__ == '__main__':
    YRS = (len(AD) - 2) / 252.0
    def cal(t, m): return (((1 + t/100) ** (1/YRS) - 1) * 100) / abs(m) if m else 0

    print('=== ① 랜덤 진입일 paired 비교 (200회, 같은 시작일에 두 전략) ===')
    random.seed(42)
    wins_r = wins_m = wins_c = 0; drs = []; dms = []
    for _ in range(200):
        s = random.randint(2, len(AD) - 30)
        e = min(s + random.randint(25, 80), len(AD))
        a = run('rev90', start=s, end=e); b = run('gap', start=s, end=e)
        if b[0] > a[0]: wins_r += 1
        if b[1] > a[1]: wins_m += 1
        drs.append(b[0] - a[0]); dms.append(b[1] - a[1])
    print('  괴리율 승률 — 수익 %.0f%% | MDD %.0f%%' % (wins_r/2, wins_m/2))
    print('  평균 차이 — 수익 %+.2f%%p | MDD %+.2f%%p' % (np.mean(drs), np.mean(dms)))
    print('  수익차 중앙값 %+.2f%%p, MDD차 중앙값 %+.2f%%p' % (np.median(drs), np.median(dms)))

    print('\n=== ② 구간 3분할 (walk-forward) ===')
    n = len(AD); b1, b2 = 2 + (n-2)//3, 2 + 2*(n-2)//3
    print('%-22s %20s %20s %6s' % ('구간', 'A rev90', 'D 괴리율', '승'))
    for lbl, s, e in [('전반 %s~%s' % (AD[2][5:], AD[b1][5:]), 2, b1),
                      ('중반 %s~%s' % (AD[b1][5:], AD[b2][5:]), b1, b2),
                      ('후반 %s~%s' % (AD[b2][5:], AD[-1][5:]), b2, n)]:
        a = avg('rev90', start=s, end=e); d = avg('gap', start=s, end=e)
        print('%-22s %+8.1f/%+7.1f(%5.2f) %+8.1f/%+7.1f(%5.2f) %6s'
              % (lbl, a[0], a[1], cal(*a), d[0], d[1], cal(*d), 'D' if cal(*d) > cal(*a) else 'A'))

    print('\n=== ③ 인접 안정성 (파라미터 ±) ===')
    print('%-22s %20s %20s %6s' % ('설정', 'A rev90', 'D 괴리율', '승'))
    grid = [('기준 N5/R5/PE30/dv1B', {}),
            ('N=4', dict(N=4)), ('N=6', dict(N=6)),
            ('R=3', dict(R=3)), ('R=7', dict(R=7)),
            ('PE<=25', dict(pe=25.0)), ('PE<=35', dict(pe=35.0)),
            ('dv>=$0.75B', dict(dv=750.0)), ('dv>=$1.5B', dict(dv=1500.0))]
    cas = []
    for lbl, kw in grid:
        a = avg('rev90', **kw); d = avg('gap', **kw)
        ca, cd = cal(*a), cal(*d); cas.append(cd)
        print('%-22s %+8.1f/%+7.1f(%5.2f) %+8.1f/%+7.1f(%5.2f) %6s'
              % (lbl, a[0], a[1], ca, d[0], d[1], cd, 'D' if cd > ca else 'A'))
    print('  → D Calmar 인접 CV = %.3f' % (np.std(cas)/np.mean(cas)))
