# -*- coding: utf-8 -*-
"""production 정확 게이트로 재측정 (2026-07-31) — 커밋 전 최종 점검.
연구 하네스(vm_canonical_bt)는 A군 안전필터(동전주 p<10·애널<3·rev90<=0·OM<5%·FCF&ROE 동시음수)를
빠뜨려 production보다 느슨했음. unified_vm_track.us_candidates와 동일한 체인으로 다시 잰다.
(v83.3 시뮬레이터-프로덕션 불일치 사고 교훈: BT 전 production 코드 대조 필수)
"""
import sys, os, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
import daily_runner as dr

_M = {}
def load():
    if 'x' in _M: return _M['x']
    conn = sqlite3.connect(dr.DB_PATH)
    full, ag, fund = {}, {}, {}
    for tk, d, px, nc, n7, n30, n60, n90, na, dv, a, om, fcf, roe in conn.execute(
            'SELECT ticker,date,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,num_analysts,'
            'dollar_volume_30d,adj_gap,operating_margin,free_cashflow,roe FROM ntm_screening '
            'WHERE price IS NOT NULL AND ntm_current>0'):
        full.setdefault(d, {})[tk] = dict(px=px, nc=nc, n7=n7, n30=n30, n60=n60, n90=n90,
                                          na=na, dv=dv)
        if a is not None: ag.setdefault(d, {})[tk] = float(a)
        e = fund.setdefault(tk, [None, None, None])
        if om is not None: e[0] = om
        if fcf is not None: e[1] = fcf
        if roe is not None: e[2] = roe
    conn.close(); _M['x'] = (full, ag, fund); return _M['x']


def bt(mode='rev90', N=5, R=5, phase=0, end_date=None, exclude=frozenset(), cost_bp=0.0):
    """mode: 'rev90'=현행(게이트4) / 'gap'=제안(괴리율, gap·min_seg 해제)"""
    ad, FULLc, DVDB, TC, _ = C._load()
    TE = C._load_te('full'); FULL, AG, FUND = load()
    if end_date: ad = tuple(d for d in ad if d <= end_date)
    GAPM = (mode == 'gap')
    hold = []; rets = []; turn = 0
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]
        px, ppx = FULL.get(d, {}), FULL.get(pv, {})
        drr = 0.0
        for t in hold:
            cu = px.get(t, {}).get('px'); pp = ppx.get(t, {}).get('px')
            if cu and pp and pp > 0: drr += (1.0 / N) * (cu - pp) / pp
        if i % R == phase:
            cand = []
            for tk, v in FULL.get(d, {}).items():
                if tk in exclude or not C._industry_ok(tk, TC): continue
                p, nc = v['px'], v['nc']
                # ── unified_vm_track.us_candidates 게이트 체인 (동일 순서) ──
                if v['dv'] is None or v['dv'] < 1000.0: continue
                if not GAPM and min(C._ms(dict(nc=nc, n7=v['n7'], n30=v['n30'],
                                               n60=v['n60'], n90=v['n90'])), 0) < 0: continue
                if nc <= 0 or (v['n90'] or 0) <= 0.1: continue
                if p / nc > 30.0: continue
                te_v = C._pit_te(TE, tk, d)
                g = (nc / te_v) if (te_v and te_v > 0) else None
                if not GAPM and g is not None and g < 1.5: continue
                # A군 안전필터 (production 고유 — 연구 하네스에 없던 부분)
                if p < 10 or (v['na'] or 0) < 3: continue
                if (nc - (v['n90'] or 0)) / abs(v['n90'] or 1) <= 0: continue
                om, fcf, roe = FUND.get(tk, (None, None, None))
                if om is not None and om < 0.05: continue
                if fcf is not None and roe is not None and fcf < 0 and roe < 0: continue
                if GAPM:
                    a = AG.get(d, {}).get(tk)
                    if a is None: continue
                    cand.append((a, tk))
                else:
                    cand.append((-(nc - v['n90']) / abs(v['n90']) * 100, tk))
            cand.sort(); new = [t for _, t in cand[:N]]
            ch = len(set(new) - set(hold)); turn += ch
            if cost_bp and hold: drr -= (ch / N) * 2 * cost_bp / 10000.0
            hold = new
        rets.append(drr)
    r = np.array(rets); nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100, turn


def rep(**kw):
    o = [bt(phase=p, **kw) for p in range(5)]
    return (float(np.mean([x[0] for x in o])), float(np.mean([x[1] for x in o])),
            float(np.mean([x[2] for x in o])))

YRS = None
if __name__ == '__main__':
    ad, *_ = C._load(); YRS = (len(ad) - 2) / 252.0
    def cal(t, m):
        return (((1 + t / 100) ** (1 / YRS) - 1) * 100) / abs(m) if m else 0
    print('production 정확 게이트 · %.2f년 · 위상평균\n' % YRS)
    print('%-26s %9s %9s %8s %9s' % ('', '수익%', 'MDD%', 'Calmar', '총교체'))
    print('-' * 66)
    base = {}
    for lbl, mode in [('A 현행 (rev90)', 'rev90'), ('D★ 제안 (괴리율)', 'gap')]:
        t, m, tv = rep(mode=mode); base[mode] = (t, m)
        print('%-26s %+9.1f %+9.1f %8.2f %9.1f' % (lbl, t, m, cal(t, m), tv))
    print('\n[비용 20bp 반영]')
    for lbl, mode in [('A 현행', 'rev90'), ('D★ 제안', 'gap')]:
        t, m, _ = rep(mode=mode, cost_bp=20)
        print('%-26s %+9.1f %+9.1f %8.2f' % (lbl, t, m, cal(t, m)))
    print('\n[LOWO · 기간]')
    print('%-22s %20s %20s %7s' % ('', 'A 현행', 'D★ 제안', '판정'))
    tests = [('전체', {}), ('~5/15', dict(end_date='2026-05-15')),
             ('~6/15', dict(end_date='2026-06-15')), ('~7/15', dict(end_date='2026-07-15')),
             ('ex-SNDK', dict(exclude=frozenset(['SNDK']))),
             ('ex-MU', dict(exclude=frozenset(['MU']))),
             ('ex-SNDK/MU', dict(exclude=frozenset(['SNDK', 'MU']))),
             ('ex-SNDK/MU/STX', dict(exclude=frozenset(['SNDK', 'MU', 'STX'])))]
    w = 0
    for lbl, kw in tests:
        a = rep(mode='rev90', **kw); b = rep(mode='gap', **kw)
        ca, cb = cal(a[0], a[1]), cal(b[0], b[1])
        if cb > ca: w += 1
        print('%-22s %+8.1f/%+6.1f(%5.2f) %+8.1f/%+6.1f(%5.2f) %7s'
              % (lbl, a[0], a[1], ca, b[0], b[1], cb, 'D' if cb > ca else 'A'))
    print('-' * 72); print('D 우세: %d/%d' % (w, len(tests)))
