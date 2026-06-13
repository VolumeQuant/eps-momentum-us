# -*- coding: utf-8 -*-
"""융합실험 1: C2(buy-dip) 우위 × 동적비중 (자율주행 2026-06-13).
인사이트: C2(EPS↑+가격↓)가 C1보다 전진수익 +3pp(robust). 진입부스트는 0효과(2슬롯 포화).
→ 대안: 이미 뽑힌 2종목 중 하나가 C2면 그쪽에 비중을 더 준다(진입 아닌 배분 레버).
v119 base sim(stored part2_rank), 매일 C2 상태로 비중 재산정. baseline=50/50."""
import sqlite3, random, statistics as st
from collections import defaultdict
import sys; sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'; N=100; SAMP=3; MINH=10
WINNERS={'MU','SNDK','STX','LITE','TTMI','NVDA','TER','BE'}
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
didx={d:i for i,d in enumerate(dates)}
data={}
for d in dates:
    data[d]={}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,dollar_volume_30d,eps_chg_weighted FROM ntm_screening WHERE date=?',(d,)):
        tk=r[0];nc,n7,n30,n60,n90=(float(x) if x else 0 for x in r[4:9]);segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
        data[d][tk]=dict(p2=r[1],cr=r[2],price=r[3],min_seg=min(segs),high30=r[9],dv=r[10],ntm=nc,epsw=r[11] or 0)
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p
con.close()
def price_chg(tk,d,lb=30):
    i=didx.get(d)
    if i is None or i-lb<0: return None
    p0=pf[dates[i-lb]].get(tk); p1=pf[d].get(tk)
    return (p1/p0-1)*100 if (p0 and p1) else None
def is_c2(tk,d,info):
    p30=price_chg(tk,d,30)
    return (info.get('epsw',0)>0 and p30 is not None and p30<0)
def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True
def sim(w_c2=0.5,exclude=(),start=0,track=False):
    """w_c2: 2종목 중 한쪽이 C2면 C2쪽 비중(상대는 1-w_c2). 둘 다 같은 부류면 50/50."""
    held=set();prev=None;val=1.0;peak=1.0;mdd=0;series=[]
    def weights(hs,d):
        hs=list(hs)
        if len(hs)<2: return {hs[0]:1.0} if hs else {}
        a,b=hs[0],hs[1]
        ca=is_c2(a,d,data[d].get(a,{})); cb=is_c2(b,d,data[d].get(b,{}))
        if ca and not cb: return {a:w_c2,b:1-w_c2}
        if cb and not ca: return {b:w_c2,a:1-w_c2}
        return {a:0.5,b:0.5}
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0;wmap=weights(prev,dp)
            for tk in prev:
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp);w=wmap.get(tk,0)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);peak=max(peak,val);mdd=min(mdd,(val/peak-1)*100)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk)
            if info and info.get('min_seg',0)<-2: held.discard(tk);continue
            if info is None: continue
            p2=info.get('p2')
            if not (p2 is None or p2>10): continue
            _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
            if _pe>=15: held.discard(tk)
        if len(held)<2:
            cands=[]
            for tk,info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg',0)<0 or not info['price'] or not verified(tk,i): continue
                if info.get('high30') and info['price']/info['high30']-1<-0.25: continue
                if (info.get('dv') or 0)<1000: continue
                p2=info.get('p2')
                if p2 is None or p2>5: continue
                cands.append((p2,tk))
            cands.sort()
            for _,tk in cands[:2-len(held)]: held.add(tk)
        if track: series.append(frozenset(held))
        prev=set(held)
    if track: return (val-1)*100,mdd,series
    return (val-1)*100,mdd
elig=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def run(w,exclude=()):
    cs,ms=[],[]
    for ch in seeds:
        for s in ch: c,m=sim(w,exclude,s);cs.append(c);ms.append(m)
    return cs,ms
print('=== C2 우대 비중 BT (2종목 중 C2쪽에 w_c2 배분) ===')
print(f'{"C2비중":<14}{"전기간":>9}{"전MDD":>8}{"paired":>9}{"LOWO최악":>10}')
for w in [0.5,0.6,0.7,0.8,1.0]:
    c,m=sim(w);cs,ms=run(w);pavg=st.mean(cs)
    worst=pavg
    for ww in WINNERS: worst=min(worst,st.mean(run(w,(ww,))[0]))
    lbl='50/50(baseline)' if w==0.5 else f'C2 {int(w*100)}/{int((1-w)*100)}'
    print(f'{lbl:<14}{c:>+8.1f}%{m:>+7.1f}%{pavg:>+8.1f}%{worst:>+9.1f}%',flush=True)
print('\n해석: C2 우대비중이 baseline(50/50)보다 수익·LOWO 개선이면 = C2 우위를 배분으로 monetize 성공.')
