# -*- coding: utf-8 -*-
"""파라미터 전수조사 (자율주행 2026-06-13) — 진입순위 × 이탈순위 × 비중방식.
stored part2_rank 충실 BT(v119 base: $1B, min_seg≥0, dd_30_25, PE<15 hold).
목표: 현행(진입5/이탈10/50-50) 대비 robust 개선 config 탐색. LOWO 필수."""
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
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,dollar_volume_30d FROM ntm_screening WHERE date=?',(d,)):
        tk=r[0];nc,n7,n30,n60,n90=(float(x) if x else 0 for x in r[4:9]);segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
        data[d][tk]=dict(p2=r[1],cr=r[2],price=r[3],min_seg=min(segs),high30=r[9],dv=r[10],ntm=nc)
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p
con.close()
def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True
def sim(entry_r=5,exit_r=10,slots=2,wmode='5050',pe_hold=15,exclude=(),start=0):
    held={};order=[];prev=None;val=1.0;peak=1.0;mdd=0
    def wmap(hs):
        hs=[t for t in order if t in hs]
        if len(hs)<2: return {hs[0]:1.0} if hs else {}
        if wmode=='5050': return {hs[0]:0.5,hs[1]:0.5}
        if wmode=='top1': return {hs[0]:1.0,hs[1]:0.0}   # rank1 집중
        if wmode=='7030': return {hs[0]:0.7,hs[1]:0.3}
        return {hs[0]:0.5,hs[1]:0.5}
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0;wm=wmap(set(prev))
            for tk in prev:
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp);w=wm.get(tk,0)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);peak=max(peak,val);mdd=min(mdd,(val/peak-1)*100)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk)
            if info and info.get('min_seg',0)<-2: held.pop(tk);order.remove(tk) if tk in order else None;continue
            if info is None: continue
            p2=info.get('p2')
            if not (p2 is None or p2>exit_r): continue
            _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
            if _pe>=pe_hold:
                held.pop(tk); order.remove(tk) if tk in order else None
        if len(held)<slots:
            cands=[]
            for tk,info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg',0)<0 or not info['price'] or not verified(tk,i): continue
                if info.get('high30') and info['price']/info['high30']-1<-0.25: continue
                if (info.get('dv') or 0)<1000: continue
                p2=info.get('p2')
                if p2 is None or p2>entry_r: continue
                cands.append((p2,tk))
            cands.sort()
            for _,tk in cands[:slots-len(held)]:
                held[tk]=1; order.append(tk)
        prev=dict(held)
    return (val-1)*100,mdd
elig=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def run(**kw):
    cs,ms=[],[]
    for ch in seeds:
        for s in ch: c,m=sim(start=s,**kw);cs.append(c);ms.append(m)
    return cs,ms
def lowo(**kw):
    base=st.mean(run(**kw)[0]); worst=base
    for w in WINNERS:
        kw2=dict(kw); kw2['exclude']=(w,)
        worst=min(worst,st.mean(run(**kw2)[0]))
    return worst
print('=== 진입순위 × 이탈순위 (50/50, slots2, PE15) ===')
print(f'{"진입/이탈":<12}{"전기간":>9}{"전MDD":>8}{"paired":>9}{"LOWO":>9}')
for er in [3,5,8]:
    for xr in [8,10,12,15]:
        c,m=sim(entry_r=er,exit_r=xr);pavg=st.mean(run(entry_r=er,exit_r=xr)[0]);lw=lowo(entry_r=er,exit_r=xr)
        star=' ←현행' if (er==5 and xr==10) else ''
        print(f'진입{er}/이탈{xr:<5}{c:>+8.1f}%{m:>+7.1f}%{pavg:>+8.1f}%{lw:>+8.1f}%{star}',flush=True)
print('\n=== 비중방식 (진입5/이탈10) ===')
print(f'{"비중":<14}{"전기간":>9}{"전MDD":>8}{"paired":>9}{"LOWO":>9}')
for wm,lbl in [('5050','50/50 ←현행'),('top1','rank1 집중100'),('7030','70/30')]:
    c,m=sim(wmode=wm);pavg=st.mean(run(wmode=wm)[0]);lw=lowo(wmode=wm)
    print(f'{lbl:<14}{c:>+8.1f}%{m:>+7.1f}%{pavg:>+8.1f}%{lw:>+8.1f}%',flush=True)
print('\n해석: 현행(진입5/이탈10/50-50) 대비 paired+LOWO 동시 개선 config만 후보. 한쪽만 좋으면 과적합.')
