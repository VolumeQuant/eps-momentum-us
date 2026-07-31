# -*- coding: utf-8 -*-
"""구시스템 엔진 재현 BT (2026-07-31) — TER/AEIS/KEYS 시절 방식.
   매일 순위 평가 → 진입 rank<=E, 이탈 rank>X, 슬롯 S, 균등비중.
   현행 VM(5일 고정 리밸·topN 통째 교체)과 구조 자체가 다름.
순위 소스:
  p2   = DB part2_rank (3일 가중, 구시스템 실제 매매 기준)
  cr   = DB composite_rank (당일 순위)
  ag   = adj_gap 재계산 순위(게이트 토글 가능)
  rev90= 현행 지표(비교용)
"""
import sys, os, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
import daily_runner as dr

_C = {}
def load():
    if 'x' in _C: return _C['x']
    conn = sqlite3.connect(dr.DB_PATH)
    ag, cr, p2, ms = {}, {}, {}, {}
    for tk, d, a, c_, p, s1, s2, s3, s4 in conn.execute(
            'SELECT ticker,date,adj_gap,composite_rank,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d '
            'FROM ntm_screening'):
        if a is not None: ag.setdefault(d, {})[tk] = float(a)
        if c_ is not None: cr.setdefault(d, {})[tk] = float(c_)
        if p is not None: p2.setdefault(d, {})[tk] = float(p)
    conn.close(); _C['x'] = (ag, cr, p2); return _C['x']


def ranks_for(d, src, FULL, DVDB, TC, TE, gates):
    """그날 순위 dict {ticker: rank(1부터)}"""
    AG, CR, P2 = load()
    if src == 'p2': return P2.get(d, {})
    if src == 'cr': return CR.get(d, {})
    rows = []
    for tk, v in FULL.get(d, {}).items():
        if not C._industry_ok(tk, TC): continue
        if gates.get('dv'):
            dv = DVDB.get(d, {}).get(tk)
            if dv is None or dv < 1000.0: continue
        if gates.get('seg') and C._ms(v) < 0: continue
        if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
        if gates.get('pe') and v['px'] / v['nc'] > 30.0: continue
        if gates.get('gap'):
            te_v = C._pit_te(TE, tk, d)
            g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
            if g is not None and g < 1.5: continue
        if src == 'ag':
            a = AG.get(d, {}).get(tk)
            if a is None: continue
            rows.append((a, tk))
        else:
            rows.append((-C._rev90(v), tk))
    rows.sort()
    return {tk: i + 1 for i, (_, tk) in enumerate(rows)}


def bt_daily(src='p2', E=3, X=10, S=2, gates=None, end_date=None,
             exclude=frozenset(), cost_bp=0.0, start=2):
    gates = gates or {}
    ad, FULL, DVDB, TC, _ = C._load()
    TE = C._load_te('full')
    if end_date: ad = tuple(d for d in ad if d <= end_date)
    hold = []; rets = []; nchg = 0
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]
        px = FULL.get(d, {}); ppx = FULL.get(pv, {})
        drr = 0.0
        for t in hold:
            cu = px.get(t, {}).get('px'); pp = ppx.get(t, {}).get('px')
            if cu and pp and pp > 0: drr += (1.0 / S) * (cu - pp) / pp
        rk = ranks_for(d, src, FULL, DVDB, TC, TE, gates)
        ch = 0
        keep = [t for t in hold if rk.get(t, 9999) <= X]
        ch += len(hold) - len(keep)
        if len(keep) < S:
            pool = sorted([(r, t) for t, r in rk.items()
                           if r <= E and t not in keep and t not in exclude])
            for _, t in pool[:S - len(keep)]:
                keep.append(t); ch += 1
        if cost_bp and ch: drr -= (ch / S) * cost_bp / 10000.0
        hold = keep; nchg += ch
        rets.append(drr)
    r = np.array(rets); nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return (float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100,
            nchg / max(1, len(rets)) * 21)  # 월평균 매매건수


if __name__ == '__main__':
    end = sys.argv[1] if len(sys.argv) > 1 else None
    ALLG = dict(dv=1, pe=1, gap=1, seg=1); NOG = dict(seg=1)
    print('%-42s %9s %9s %8s' % ('구성', '수익%', 'MDD%', '월매매'))
    print('-' * 72)
    import vm_canonical_bt as CC
    v = CC.canonical_report(pe_max=30, gap_thr=1.5, N=5, end_date=end)
    print('%-42s %+9.1f %+9.1f %8s' % ('[현행 VM] rev90 5일리밸 top5', v['avg_ret'], v['avg_mdd'], '~1.9'))
    for lbl, kw in [
        ('[구엔진] DB part2_rank E3/X10/S2', dict(src='p2')),
        ('[구엔진] DB 당일순위(cr) E3/X10/S2', dict(src='cr')),
        ('[구엔진] 괴리율 무게이트 E3/X10/S2', dict(src='ag', gates=NOG)),
        ('[구엔진] 괴리율 +dv+PER E3/X10/S2', dict(src='ag', gates=dict(dv=1, pe=1, seg=1))),
        ('[구엔진] 괴리율 전게이트 E3/X10/S2', dict(src='ag', gates=ALLG)),
        ('[구엔진] rev90 무게이트 E3/X10/S2', dict(src='rev90', gates=NOG)),
        ('[구엔진] rev90 전게이트 E3/X10/S2', dict(src='rev90', gates=ALLG)),
    ]:
        t, m, mc = bt_daily(end_date=end, **kw)
        print('%-42s %+9.1f %+9.1f %8.1f' % (lbl, t, m, mc))
