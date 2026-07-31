# -*- coding: utf-8 -*-
"""괴리율(adj_gap) vs rev90 후속 검증 (2026-07-31)
 ① 약세 프록시: 하락일 손실률(downside capture)·최악 구간·조정창 분해 (진짜 약세장 데이터 없음)
 ② 회전율 + 거래비용 반영
 ③ 게이트 4종 전수 조합(16셀) — 전부 빼는 게 맞나, 일부만 빼는 게 맞나
"""
import sys, os, sqlite3, itertools
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
import daily_runner as dr
from _pure_eps_vs_vm_2026_07_31 import load_ag


def run(metric='rev90', use_dv=True, use_pe=True, use_gap=True, use_seg=True,
        N=5, R=5, phase=0, start=2, end_date=None, exclude=frozenset(), cost_bp=0.0):
    """반환 dict: rets, dates, holds(리밸별), turnover, tot, mdd"""
    ad, FULL, DVDB, TC, _ = C._load()
    TE = C._load_te('full'); AG = load_ag()
    if end_date: ad = tuple(d for d in ad if d <= end_date)
    hold = []; rets = []; dates = []; holds = []; turn = []
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]
        px = FULL.get(d, {}); ppx = FULL.get(pv, {})
        drr = 0.0
        for t in hold:
            cu = px.get(t, {}).get('px'); pp = ppx.get(t, {}).get('px')
            if cu and pp and pp > 0: drr += (1.0 / N) * (cu - pp) / pp
        if i % R == phase:
            cand = []
            for tk, v in FULL.get(d, {}).items():
                if tk in exclude or not C._industry_ok(tk, TC): continue
                if use_dv:
                    dv = DVDB.get(d, {}).get(tk)
                    if dv is None or dv < 1000.0: continue
                if use_seg and C._ms(v) < 0: continue
                if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
                if use_pe and v['px'] / v['nc'] > 30.0: continue
                if use_gap:
                    te_v = C._pit_te(TE, tk, d)
                    g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
                    if g is not None and g < 1.5: continue
                if metric == 'rev90':
                    key = -C._rev90(v)
                else:
                    a = AG.get(d, {}).get(tk)
                    if a is None: continue
                    key = a
                cand.append((key, tk))
            cand.sort(); new = [t for _, t in cand[:N]]
            ch = len(set(new) - set(hold))
            turn.append(ch); holds.append((d, new))
            if cost_bp and hold:
                drr -= (ch / N) * 2 * cost_bp / 10000.0  # 매도+매수 왕복
            hold = new
        rets.append(drr); dates.append(d)
    r = np.array(rets); nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return dict(rets=r, dates=dates, holds=holds, turnover=turn,
                tot=float(nav[-1] - 1) * 100, mdd=float((nav / pk - 1).min()) * 100)


def agg(**kw):
    R = kw.get('R', 5)
    out = [run(phase=p, **kw) for p in range(R)]
    return (float(np.mean([o['tot'] for o in out])), float(np.mean([o['mdd'] for o in out])), out)


def market_series(end_date=None):
    """유니버스 동일가중 일별 수익 = 시장 프록시 (API 0회, PIT)."""
    ad, FULL, *_ = C._load()
    if end_date: ad = tuple(d for d in ad if d <= end_date)
    out = {}
    for i in range(1, len(ad)):
        d, pv = ad[i], ad[i - 1]
        cur, prv = FULL.get(d, {}), FULL.get(pv, {})
        rs = [(cur[t]['px'] - prv[t]['px']) / prv[t]['px']
              for t in cur if t in prv and prv[t]['px'] and prv[t]['px'] > 0 and cur[t]['px']]
        if rs: out[d] = float(np.mean(rs))
    return out


A = dict(metric='rev90', use_dv=True, use_pe=True, use_gap=True, use_seg=True)
B = dict(metric='adj_gap', use_dv=False, use_pe=False, use_gap=False, use_seg=True)

if __name__ == '__main__':
    end = sys.argv[1] if len(sys.argv) > 1 else None
    mkt = market_series(end)

    print('=== ① 약세 프록시 (진짜 약세장 데이터 없음 → 하락일 거동으로 대리) ===')
    for lbl, kw in [('현행VM', A), ('괴리율無게이트', B)]:
        _, _, outs = agg(end_date=end, **kw)
        dn_c, up_c, w5 = [], [], []
        for o in outs:
            m = np.array([mkt.get(d, 0.0) for d in o['dates']]); r = o['rets']
            dn = m < 0; up = m > 0
            dn_c.append(r[dn].sum() / m[dn].sum()); up_c.append(r[up].sum() / m[up].sum())
            nav = np.cumprod(1 + r)
            w5.append(min((nav[i + 5] / nav[i] - 1) for i in range(len(nav) - 5)) * 100)
        print('%-14s 하락일 손실배수 %.2f배  상승일 이익배수 %.2f배  최악5일 %+.1f%%'
              % (lbl, np.mean(dn_c), np.mean(up_c), np.mean(w5)))

    print('\n=== ② 회전율 + 거래비용 ===')
    print('%-16s %8s %28s' % ('', '리밸당교체', '비용 0bp / 10bp / 20bp (수익%)'))
    for lbl, kw in [('현행VM', A), ('괴리율無게이트', B)]:
        _, _, outs = agg(end_date=end, **kw)
        tv = np.mean([np.mean(o['turnover'][1:]) for o in outs])
        cs = []
        for c in (0, 10, 20):
            t, m, _ = agg(end_date=end, cost_bp=c, **kw); cs.append((t, m))
        print('%-16s %6.2f/5   %s' % (lbl, tv,
              '  '.join('%+7.1f(MDD%+6.1f)' % (t, m) for t, m in cs)))

    print('\n=== ③ 게이트 16조합 전수 (괴리율 기준, N=5) ===')
    print('%-4s %-4s %-4s %-5s %9s %9s %7s' % ('dv', 'PER', 'gap', 'mseg', '수익%', 'MDD%', '수익/MDD'))
    res = []
    for dv, pe, gp, sg in itertools.product([True, False], repeat=4):
        t, m, _ = agg(metric='adj_gap', use_dv=dv, use_pe=pe, use_gap=gp, use_seg=sg, end_date=end)
        res.append((t / abs(m) if m else 0, t, m, dv, pe, gp, sg))
    for ratio, t, m, dv, pe, gp, sg in sorted(res, reverse=True):
        print('%-4s %-4s %-4s %-5s %+9.1f %+9.1f %7.2f'
              % (*['O' if x else '.' for x in (dv, pe, gp, sg)], t, m, ratio))
