# -*- coding: utf-8 -*-
"""2슬롯 비중 검증: 1:1(균등) vs 집중(100/0) vs 70/30/80/20 vs 동적(순위갭).
v119 보유 리플레이 후 매일 2종목(순위순)에 비중 적용. ⚠️ 라이브 ~4개월·강세장(메가) 짧은표본."""
import sqlite3, math, sys
from collections import defaultdict
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
con = sqlite3.connect('eps_momentum_data.db'); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
data = {}
for d in dates:
    data[d] = {}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,dollar_volume_30d FROM ntm_screening WHERE date=?', (d,)):
        tk = r[0]; nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9]); segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a-b)/abs(b)*100)) if b and abs(b) > 0.01 else 0)
        data[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], min_seg=min(segs), high30=r[9], dv=r[10], ntm=nc)
pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk] = p
con.close()
def verified(t, i):
    for j in (i, i-1, i-2):
        if j < 0: return False
        x = data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr'] > 30: return False
    return True

def replay():
    """v119 → 각 날짜 보유 ticker 리스트(순위순)."""
    held = {}; hist = []
    for i in range(len(dates)):
        d = dates[i]; dd = data[d]
        for tk in list(held):
            info = dd.get(tk)
            if info and info.get('min_seg', 0) < -2: del held[tk]; continue
            if info is None: continue
            p2 = info.get('p2')
            if not (p2 is None or p2 > 10): continue
            _pe = info['price']/info['ntm'] if info.get('ntm', 0) > 0 else 999
            if _pe >= 15: del held[tk]
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                if tk in held: continue
                if info.get('min_seg', 0) < 0 or not info['price'] or not verified(tk, i): continue
                if info.get('high30') and info['price']/info['high30']-1 < -0.25: continue
                if (info.get('dv') or 0) < 1000: continue
                p2 = info.get('p2')
                if p2 is None or p2 > 5: continue
                cands.append((p2, tk))
            cands.sort()
            for _, tk in cands[:2-len(held)]: held[tk] = 1
        # 순위순 정렬(part2_rank 낮은=슬롯1)
        order = sorted(held.keys(), key=lambda t: data[d].get(t, {}).get('p2') or 99)
        hist.append((d, order))
    return hist

hist = replay()
def sim(weight_fn):
    rets = []
    for i in range(1, len(dates)):
        d = dates[i]; dp = dates[i-1]; order = hist[i-1][1]
        if not order: rets.append(0.0); continue
        w = weight_fn(order, dp)
        r = 0.0
        for tk, wt in zip(order, w):
            pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
            if pp and pn: r += wt*(pn/pp-1)
        rets.append(r)
    return np.array(rets)
def metrics(r):
    eq = np.cumprod(1+r); tot = (eq[-1]-1)*100; yrs = len(r)/252
    sh = (r.mean()*252)/(r.std()*math.sqrt(252)) if r.std() > 0 else 0
    mdd = ((eq/np.maximum.accumulate(eq)-1).min())*100
    return tot, mdd, sh
def w_equal(o, dp): return [1/len(o)]*len(o)
def w_conc(o, dp): return [1.0]+[0.0]*(len(o)-1)
def w_7030(o, dp): return [0.7, 0.3] if len(o) == 2 else [1.0]
def w_8020(o, dp): return [0.8, 0.2] if len(o) == 2 else [1.0]
def w_dyn(o, dp):  # 순위갭 동적: 슬롯1 part2=1·슬롯2 갭 크면 집중
    if len(o) < 2: return [1.0]
    p1 = data[dp].get(o[0], {}).get('p2') or 1; p2 = data[dp].get(o[1], {}).get('p2') or 1
    return [1.0, 0.0] if (p2-p1) >= 2 else [0.5, 0.5]

print(f'=== 2슬롯 비중 비교 ({dates[0]}~{dates[-1]}, {len(dates)}일, ⚠️강세장·메가) ===')
print(f'{"비중":<14}{"누적%":>9}{"MDD%":>8}{"Sharpe":>8}')
for nm, fn in [('균등 50/50', w_equal), ('70/30', w_7030), ('80/20', w_8020), ('집중 100/0', w_conc), ('동적(순위갭2)', w_dyn)]:
    t, m, s = metrics(sim(fn))
    print(f'{nm:<14}{t:>+8.1f}{m:>+8.1f}{s:>8.2f}')
print('\n해석: 집중/동적이 균등보다 크게 우월하면 1:1은 손해(승자 비중↑가 맞음). 비슷하면 1:1도 무방.')
print('⚠️ 4개월·메가(SNDK/MU) 구간이라 집중이 유리하게 나올 편향 — 장기검증은 v84(2step_t15 +11.45%p) 참조.')
