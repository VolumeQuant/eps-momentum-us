# -*- coding: utf-8 -*-
"""실행 지연 감사 (2026-07-31) — 전략 BT가 '종가로 순위 매기고 그 종가에 매수'로 계산돼 있다.
실제로는 종가를 본 뒤에나 주문하므로 최소 1일 늦게 체결된다.
★특히 괴리율(adj_gap)은 가격 기반 지표라 rev90(가격 무관)보다 이 optimism을 더 크게 먹을 수 있음.
lag=0(현행 BT) vs lag=1(정직) 비교로 오늘의 결론이 살아남는지 확인.
"""
import sys, os
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
from _prod_faithful_bt_2026_07_31 import load

FULL, AG, FUND = load(); TE = C._load_te('full'); AD, _, _, TC, _ = C._load()


def rank_at(d, mode):
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
            ms = min(C._ms(dict(nc=nc, n7=v['n7'], n30=v['n30'], n60=v['n60'], n90=v['n90'])), 0)
            if ms < 0: continue
            te = C._pit_te(TE, tk, d); g = (nc / te) if (te and te > 0) else None
            if g is not None and g < 1.5: continue
            cand.append((-(nc - v['n90']) / abs(v['n90']) * 100, tk))
    cand.sort()
    return [t for _, t in cand[:5]]


def bt(mode, lag=0, R=5, phase=0, exclude=frozenset(), end=None):
    ad = tuple(d for d in AD if (not end or d <= end))
    hold = []; pend = None; rets = []
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]
        px, ppx = FULL.get(d, {}), FULL.get(pv, {})
        r = 0.0
        for t in hold:
            cu = px.get(t, {}).get('px'); pp = ppx.get(t, {}).get('px')
            if cu and pp and pp > 0: r += 0.2 * (cu - pp) / pp
        rets.append(r)
        if pend is not None and pend[0] == i:      # 지연 체결
            hold = pend[1]; pend = None
        if i % R == phase:
            new = [t for t in rank_at(d, mode) if t not in exclude]
            if lag == 0: hold = new
            else: pend = (i + lag, new)
    a = np.array(rets); nav = np.cumprod(1 + a); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100


def rep(mode, lag, **kw):
    o = [bt(mode, lag, phase=p, **kw) for p in range(5)]
    return np.mean([x[0] for x in o]), np.mean([x[1] for x in o])


if __name__ == '__main__':
    YRS = (len(AD) - 2) / 252.0
    def cal(t, m): return (((1 + t/100) ** (1/YRS) - 1) * 100) / abs(m) if m else 0
    print('전략 BT 실행지연 감사 (production 게이트, R5, 위상평균, %.2f년)' % YRS)
    print('%-34s %9s %9s %8s' % ('', '수익%', 'MDD%', 'Calmar'))
    print('-' * 64)
    for mode, lbl in [('rev90', 'A 현행(전망상향폭)'), ('gap', 'D 괴리율')]:
        for lag in (0, 1, 2):
            t, m = rep(mode, lag)
            tag = ' ←현행 BT' if lag == 0 else (' ←정직' if lag == 1 else '')
            print('%-34s %+9.1f %+9.1f %8.2f%s' % ('%s  lag=%d' % (lbl, lag), t, m, cal(t, m), tag))
        print()
    print('LOWO (lag=1 정직 기준)')
    print('%-22s %20s %20s' % ('제외', 'A 현행', 'D 괴리율'))
    for ex in [(), ('SNDK',), ('MU',), ('SNDK', 'MU'), ('SNDK', 'MU', 'STX')]:
        e = frozenset(ex)
        a = rep('rev90', 1, exclude=e); b = rep('gap', 1, exclude=e)
        print('%-22s %+8.1f/%+7.1f(%5.2f) %+8.1f/%+7.1f(%5.2f)'
              % ('ex-' + ('/'.join(ex) if ex else '없음'), a[0], a[1], cal(*a), b[0], b[1], cal(*b)))
