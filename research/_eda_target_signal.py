# -*- coding: utf-8 -*-
import sqlite3, json
from datetime import datetime, timedelta
import numpy as np

DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
tgt = json.load(open(r'C:\dev\claude code\eps-momentum-us\research\_targets_cache.json'))
conn = sqlite3.connect(DB); cur = conn.cursor()

dts = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
didx = {d:i for i,d in enumerate(dts)}
# price per ticker per date (trading dates of window)
px = {}
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    px.setdefault(tk,{})[d]=p
# eps revision signal: ntm_current/ntm_90d-1
epsrev = {}
for tk,d,nc,n90 in cur.execute('SELECT ticker,date,ntm_current,ntm_90d FROM ntm_screening'):
    if n90 and abs(n90)>1e-6:
        epsrev.setdefault(tk,{})[d]=(nc/n90-1)

def dparse(s): return datetime.strptime(s,'%Y-%m-%d')

def tgt_signal(tk, d, lookback=30):
    """trailing-lookback일 순 목표가 상향 강도 (sum of pct target changes) + raise-lower count"""
    d0 = dparse(d); lo = d0 - timedelta(days=lookback)
    mag = 0.0; nraise = 0; nlower = 0
    for r in tgt.get(tk, []):
        rd = dparse(r['date'])
        if lo < rd <= d0:
            pta = (r['pta'] or '').lower()
            if r['prior']>0 and r['cur']>0 and 'rais' in pta:
                mag += (r['cur']-r['prior'])/r['prior']; nraise+=1
            elif r['prior']>0 and r['cur']>0 and 'lower' in pta:
                mag += (r['cur']-r['prior'])/r['prior']; nlower+=1
    return mag, nraise-nlower

# Build panel: for each ticker-date with price, compute signals + forward 5d return
rows=[]
for tk in px:
    for d in px[tk]:
        i = didx.get(d)
        if i is None: continue
        # forward 5d return
        if i+5 < len(dts):
            d5 = dts[i+5]
            p0 = px[tk].get(d); p5 = px[tk].get(d5)
            if p0 and p5:
                fwd5 = p5/p0-1
            else: continue
        else: continue
        mag, net = tgt_signal(tk, d, 30)
        er = epsrev.get(tk,{}).get(d)
        rows.append((d, tk, mag, net, er, fwd5))

print('패널 행수:', len(rows))
mags = np.array([r[2] for r in rows])
nets = np.array([r[3] for r in rows])
fwd  = np.array([r[5] for r in rows])
ers  = np.array([r[4] if r[4] is not None else np.nan for r in rows])

def ic(x,y):
    m = ~(np.isnan(x)|np.isnan(y))
    from scipy.stats import spearmanr
    return spearmanr(x[m],y[m])

print('\n=== IC (Spearman, 신호 vs 5일 forward 수익) ===')
print('목표가 변동강도(mag30) vs fwd5 :', ic(mags,fwd))
print('목표가 순상향수(net30)  vs fwd5 :', ic(nets,fwd))
print('EPS변동(ntm/90d)        vs fwd5 :', ic(ers,fwd))

print('\n=== 목표가신호 vs EPS신호 상관 (독립성) ===')
print('mag30 vs epsrev:', ic(mags,ers))

print('\n=== mag30 분위별 평균 fwd5 (예측력 형태) ===')
order = np.argsort(mags)
n=len(order); q=5
for k in range(q):
    idx = order[k*n//q:(k+1)*n//q]
    print(f'  Q{k+1}: mag30 [{mags[idx].min():+.3f}~{mags[idx].max():+.3f}]  avg fwd5={fwd[idx].mean()*100:+.2f}%  n={len(idx)}')
