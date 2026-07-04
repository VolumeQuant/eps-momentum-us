# -*- coding: utf-8 -*-
"""성과지표 총비교 — 현행 carry 2슬롯 vs 재설계 최종안(PER<=30+gap>=2.5+모멘텀 top5 R5) vs SPY/QQQ.
같은 하네스·같은 91일 창에서 일별 수익률 시계열 → CAGR/vol/Sharpe/Sortino/MDD/Calmar/최악일.
주의: 91일 단일 강세장 in-sample — 연율화 지표는 창이 짧아 절대값이 부풀며, '서로 비교'용으로만."""
import sys, os, json, sqlite3
import numpy as np
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr

conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
FULL = {}
for tk, d, p2, px, nc, n7, n30, n60, n90, ru in c.execute(
        'SELECT ticker,date,part2_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,rev_up30 '
        'FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0'):
    FULL.setdefault(d, {})[tk] = dict(p2=p2, px=px, nc=nc, n7=n7, n30=n30, n60=n60, n90=n90, ru=ru)
conn.close()
AP = {d: {t: v['px'] for t, v in FULL.get(d, {}).items()} for d in ad}
DVF = pd.read_parquet(os.path.join(BASE, 'research', 'dv_full_2026_07_04.parquet'))
DVF.index = pd.to_datetime(DVF.index).strftime('%Y-%m-%d')
DV = {d: {t: (None if pd.isna(DVF.loc[d, t]) else float(DVF.loc[d, t])) for t in DVF.columns} for d in DVF.index if d in set(ad)}
TC = json.load(open(os.path.join(BASE, 'ticker_info_cache.json'), encoding='utf-8'))
BAD_IND = dr.COMMODITY_INDUSTRIES | dr.OFF_STRATEGY_INDUSTRIES
BAD_TK = set(dr.COMMODITY_TICKERS)
def industry_ok(tk):
    if tk in BAD_TK: return False
    v = TC.get(tk)
    ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v)
    return not (isinstance(ind, str) and ind in BAD_IND)
TE = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm.json'), encoding='utf-8'))
def pit_te(tk, d):
    r = TE.get(tk); v = None
    if not r: return None
    for rd, e in r:
        if rd <= d: v = e
        else: break
    return v
def gap_of(tk, v, d):
    te = pit_te(tk, d)
    return (v['nc'] / te) if (te and te > 0 and v['nc'] and v['nc'] > 0) else None
def ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)
def rev90(v):
    return (v['nc'] - v['n90']) / abs(v['n90']) * 100 if (v['n90'] and abs(v['n90']) > 0.01) else 0

def pick(d, N, pe_max, gap_thr):
    cand = []
    for tk, v in FULL.get(d, {}).items():
        if not industry_ok(tk): continue
        dv = DV.get(d, {}).get(tk)
        if dv is None or dv < 1000: continue
        if ms(v) < 0: continue
        if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
        if v['px'] / v['nc'] > pe_max: continue
        if gap_thr:
            g = gap_of(tk, v, d)
            if g is not None and g < gap_thr: continue
        cand.append((tk, rev90(v)))
    cand.sort(key=lambda x: -x[1])
    return [t for t, _ in cand[:N]]

def daily_vm(N=5, R=5, pe_max=30, gap_thr=2.5, phase=0, start=2):
    hold = []; rets = []
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(hold); drr = 0.0
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp
        rets.append(drr)
        if i % R == phase % R:
            hold = pick(d, N, pe_max, gap_thr)
    return np.array(rets)

def daily_carry(start=2):
    PH = dr.PE_HOLD
    pf = {}; rets = []
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]
        data = {t: v for t, v in FULL.get(d, {}).items() if v.get('p2') is not None}
        px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(pf); drr = 0.0
        for t in pf:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp
        rets.append(drr)
        for t in list(pf):
            it = data.get(t); full_row = FULL.get(d, {}).get(t)
            if it is not None and ms(it) < -2:
                del pf[t]; continue
            if it is None:
                if full_row and full_row['nc'] and full_row['nc'] > 0 and full_row['px'] and \
                        (full_row['px'] / full_row['nc']) >= PH:
                    del pf[t]
                continue
            rk = it['p2']
            if rk is not None and rk <= dr.EXIT_RANK: continue
            _pe = (it['px'] / it['nc']) if (it['px'] and it['nc'] and it['nc'] > 0) else 999
            if _pe >= PH: del pf[t]
        if len(pf) < 2:
            def eok(tk, v):
                if v['p2'] is None or v['p2'] > 5 or ms(v) < 0: return False
                dvv = DV.get(d, {}).get(tk)
                if dvv is None or dvv < 1000: return False
                g = gap_of(tk, v, d)
                return not (g is not None and g < 2.5)
            cand = sorted([(t, v['p2']) for t, v in data.items() if t not in pf and eok(t, v)], key=lambda x: x[1])
            for t, _ in cand:
                if len(pf) >= 2: break
                pf[t] = 1
    return np.array(rets)

def bench_daily(sym):
    import yfinance as yf
    h = yf.download(sym, start=ad[1], end='2026-07-04', progress=False, threads=False)['Close']
    h = h.squeeze()
    h.index = h.index.strftime('%Y-%m-%d')
    h = h[h.index.isin(ad[2:])]
    return h.pct_change().dropna().values

def metrics(rets, label):
    rets = np.asarray(rets, dtype=float)
    nav = np.cumprod(1 + rets); total = nav[-1] - 1
    n = len(rets)
    cagr = (1 + total) ** (252 / n) - 1
    vol = rets.std(ddof=1) * np.sqrt(252)
    sharpe = (rets.mean() / rets.std(ddof=1)) * np.sqrt(252) if rets.std(ddof=1) > 0 else 0
    dn = rets[rets < 0]
    sortino = (rets.mean() / dn.std(ddof=1)) * np.sqrt(252) if len(dn) > 1 and dn.std(ddof=1) > 0 else float('inf')
    peak = np.maximum.accumulate(nav); mdd = (nav / peak - 1).min()
    calmar = cagr / abs(mdd) if mdd < 0 else float('inf')
    win = (rets > 0).mean()
    return dict(label=label, total=total * 100, cagr=cagr * 100, vol=vol * 100, sharpe=sharpe,
                sortino=sortino, mdd=mdd * 100, calmar=calmar, worst=rets.min() * 100, win=win * 100, n=n)

rows = []
rows.append(metrics(daily_carry(), '현행 carry 2슬롯'))
vm_all = [metrics(daily_vm(phase=p), f'재설계 phase{p}') for p in range(5)]
m = {k: float(np.mean([r[k] for r in vm_all])) for k in vm_all[0] if k not in ('label',)}
m['label'] = '재설계 최종안(위상평균)'
rows.append(m)
rows.append(metrics(daily_vm(gap_thr=None), '재설계 gap없음(참고)'))
try:
    rows.append(metrics(bench_daily('SPY'), 'SPY'))
    rows.append(metrics(bench_daily('QQQ'), 'QQQ'))
except Exception as e:
    print('bench fetch fail:', e)

print(f'=== 성과지표 총비교 (동일 91일 창 {ad[2]}~{ad[-1]}, 연율화=252/91 외삽 주의) ===')
print(f'{"":24}{"총수익":>8}{"CAGR연율":>10}{"변동성":>8}{"Sharpe":>8}{"Sortino":>9}{"MDD":>7}{"Calmar":>8}{"최악일":>8}{"승률":>7}')
for r in rows:
    print(f"{r['label']:<24}{r['total']:>+7.0f}%{r['cagr']:>+9.0f}%{r['vol']:>7.0f}%{r['sharpe']:>8.2f}{r['sortino']:>9.2f}{r['mdd']:>+6.0f}%{r['calmar']:>8.1f}{r['worst']:>+7.1f}%{r['win']:>6.0f}%")
