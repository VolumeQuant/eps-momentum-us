# -*- coding: utf-8 -*-
"""검증 보조: 106일 패널에서 full 게이트(dv+PER+gap) 통과자 중 gap missing=pass 비율 (PIT)."""
import sys, os, json, sqlite3
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr

TE = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm_full.json'), encoding='utf-8'))
TC = json.load(open(os.path.join(BASE, 'ticker_info_cache.json'), encoding='utf-8'))
BAD = dr.COMMODITY_INDUSTRIES | dr.OFF_STRATEGY_INDUSTRIES
BAD_TK = set(dr.COMMODITY_TICKERS)

def ind_ok(tk):
    if tk in BAD_TK:
        return False
    v = TC.get(tk)
    ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v)
    return not (isinstance(ind, str) and ind in BAD)

def seg(a, b):
    try:
        return (a - b) / abs(b)
    except Exception:
        return -9

def pit_te(tk, d):
    rec = TE.get(tk)
    if not rec:
        return None
    best = None
    for rd, v in rec:
        if rd <= d:
            best = v
    return best

conn = sqlite3.connect(os.path.join(BASE, 'eps_momentum_data.db'))
c = conn.cursor()
dates = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date')]
print(f'dates: {len(dates)} ({dates[0]}~{dates[-1]})')
tot_pass = tot_miss = 0
monthly = {}
for d in dates:
    for tk, p, nc, n7, n30, n60, n90, dv in c.execute(
            'SELECT ticker,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d '
            'FROM ntm_screening WHERE date=? AND price IS NOT NULL AND ntm_current>0', (d,)):
        if not ind_ok(tk):
            continue
        if dv is None or dv < 1000:
            continue
        if min(seg(nc, n7), seg(n7, n30), seg(n30, n60), seg(n60, n90)) < 0:
            continue
        if nc <= 0 or (n90 or 0) <= 0.1:
            continue
        if p / nc > 30:
            continue
        te = pit_te(tk, d)
        g = (nc / te) if (te and te > 0) else None
        if g is not None and g < 1.5:
            continue
        tot_pass += 1
        m = d[:7]
        e = monthly.setdefault(m, [0, 0])
        e[0] += 1
        if g is None:
            tot_miss += 1
            e[1] += 1
conn.close()
print(f'게이트(dv+PER+gap) 통과 종목-일: {tot_pass:,} | missing=pass: {tot_miss:,} ({tot_miss/tot_pass*100:.1f}%)')
for m, (a, b) in sorted(monthly.items()):
    print(f'  {m}: 통과 {a:,} 중 missing {b:,} ({b/a*100:.1f}%)')
