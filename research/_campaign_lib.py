# -*- coding: utf-8 -*-
"""품질기준 캠페인 공용 하네스 (2026-08-01 밤).

production 패리티 체인(2026-08-01 최종: dv$300M·PER30·ms-2·ru3·na>=6·가치함정·안전필터·dedup)
+ 검증 의무 배터리(전기간·LOWO 3종·6/15~·랜덤 paired). 모든 틱은 이걸 import해서 측정한다.
⚠️ production 게이트가 또 바뀌면 이 파일의 chain()을 같은 커밋에서 갱신할 것 —
   8/1 '하네스 DV 박제' 사고(REDESIGN_DEBATE 추기 3)의 재발 방지 규약.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import _revup30_gate_2026_07_31 as G
import vm_canonical_bt as C
import unified_vm_track as U

AD, PX, FULL, TC, RU, FUND, AG = G.AD, G.PX, G.FULL, G.TC, G.RU, G.FUND, G.AG
IDX = {d: i for i, d in enumerate(AD)}
YRS = (len(AD) - 2) / 252.0
N = 5
STRESS = '2026-06-15'


def trap(d, tk):
    """가치함정: rev30<=+2% AND 20일 주가 하락 (production TRAP_* 패리티)."""
    i = IDX[d]
    if i < 20:
        return False
    v = FULL[d].get(tk)
    p20 = PX.get(AD[i - 20], {}).get(tk)
    if not v or not p20:
        return False
    n30 = v.get('n30')
    r30 = (v['nc'] - n30) / abs(n30) * 100 if n30 and abs(n30) > 0.01 else 0
    return r30 <= 2.0 and v['px'] < p20


def chain(d, dv_min=300.0, na_min=6, use_trap=True):
    """현행 production 게이트 전체를 통과한 후보를 adj_gap 오름차순으로 반환."""
    cand = []
    for tk, v in FULL.get(d, {}).items():
        if not C._industry_ok(tk, TC):
            continue
        p, nc = v['px'], v['nc']
        if v['dv'] is None or v['dv'] < dv_min:
            continue
        if nc <= 0 or (v['n90'] or 0) <= 0.1:
            continue
        if p / nc > 30 or p < 10 or (v['na'] or 0) < na_min:
            continue
        if (nc - (v['n90'] or 0)) / abs(v['n90'] or 1) <= 0:
            continue
        om, fcf, roe = FUND.get(tk, (None, None, None))
        if om is not None and om < 0.05:
            continue
        if fcf is not None and roe is not None and fcf < 0 and roe < 0:
            continue
        if min(C._ms(dict(nc=nc, n7=v['n7'], n30=v['n30'], n60=v['n60'], n90=v['n90'])), 0) < -2.0:
            continue
        u30, _ = RU.get(d, {}).get(tk, (0, 0))
        if u30 < 3:
            continue
        if use_trap and trap(d, tk):
            continue
        a = AG.get(d, {}).get(tk)
        if a is None:
            continue
        cand.append((a, tk))
    cand.sort()
    seen, out = set(), []
    for a, tk in cand:
        k = U.DUAL_CLASS.get(tk, tk)
        if k in seen:
            continue
        seen.add(k)
        out.append(tk)
    return out


def base_picks():
    return {d: chain(d) for d in AD}


def run(P, phase, ex=frozenset(), start=2, end=None, R=5):
    hold = []; pend = None; rets = []
    hi = end if end else len(AD)
    for i in range(start, hi):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        rets.append(sum((px[t] / ppx[t] - 1) / N for t in hold
                        if t in px and t in ppx and ppx[t] > 0))
        if pend is not None and pend[0] <= i:
            hold = pend[1]; pend = None
        if i % R == phase:
            pend = (i + 1, [t for t in P[d] if t not in ex][:N])
    r = np.array(rets)
    if not len(r):
        return None
    nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return (nav[-1] - 1) * 100, (nav / pk - 1).min() * 100


def cal(r, m):
    return (((1 + r / 100) ** (1 / YRS) - 1) * 100) / abs(m) if m else 0


def avg(P, ex=frozenset(), **kw):
    o = [run(P, p, ex, **kw) for p in range(5)]
    o = [x for x in o if x]
    return np.mean([x[0] for x in o]), np.mean([x[1] for x in o])


def battery(P, BASE, label=''):
    """의무 배터리: 전기간·6/15~·LOWO 3종 — (dict 반환, 출력은 호출측)."""
    si = next(i for i, d in enumerate(AD) if d >= STRESS)
    out = {'label': label}
    r, m = avg(P); out['full'] = (r, m, cal(r, m))
    sr, sm = avg(P, start=si); out['stress'] = (sr, sm)
    out['lowo'] = {}
    for ex in [('SNDK',), ('MU',), ('SNDK', 'MU', 'STX')]:
        e = frozenset(ex)
        br, bm = avg(BASE, e); nr, nm = avg(P, e)
        out['lowo']['ex-' + '/'.join(ex)] = (cal(br, bm), cal(nr, nm))
    return out


def paired(P, BASE, n_iter=150, seed=7):
    import random
    random.seed(seed)
    w = [0, 0]; n = 0
    for _ in range(n_iter):
        s = random.randint(2, len(AD) - 30); e = min(s + random.randint(25, 80), len(AD))
        b = run(BASE, s % 5, start=s, end=e); x = run(P, s % 5, start=s, end=e)
        if b and x:
            n += 1
            if x[0] > b[0]: w[0] += 1
            if x[1] > b[1]: w[1] += 1
    return (w[0] / n * 100 if n else 0, w[1] / n * 100 if n else 0, n)
