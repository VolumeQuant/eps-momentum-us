# -*- coding: utf-8 -*-
"""Part C: C2(buy-dip) 우위 LOWO 검증 + C2 진입부스트 BT (자율주행 2026-06-13).
유니버스가 C1/C2뿐이므로 '차등'은 사실상 C2 우대. v83 C2 boost가 MU 착시로 제거됐던 것 재검증.
BT: v119 base(sim)에 진입순위를 part2 - boost*is_C2 로 재정렬. LOWO로 단일종목 착시 확인."""
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
    return (info['epsw']>0 and p30 is not None and p30<0)
def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True

# === C2 vs C1 전진수익 LOWO (단일종목 착시 확인) ===
def fwd(tk,d,k):
    i=didx.get(d)
    if i is None or i+k>=len(dates): return None
    p0=pf[d].get(tk); p1=pf[dates[i+k]].get(tk)
    return (p1/p0-1)*100 if (p0 and p1) else None
print('=== C2(buy-dip) vs C1 전진20일 수익 — LOWO ===')
def c1c2_means(exclude=()):
    c1=[];c2=[]
    for d in dates:
        for tk,info in data[d].items():
            if tk in exclude or info['p2'] is None: continue
            f=fwd(tk,d,20)
            if f is None or info['epsw']==0: continue
            p30=price_chg(tk,d,30)
            if p30 is None: continue
            (c2 if (info['epsw']>0 and p30<0) else (c1 if info['epsw']>0 and p30>0 else [])).append(f) if info['epsw']>0 else None
    return (st.mean(c1) if c1 else 0, st.mean(c2) if c2 else 0, len(c1), len(c2))
m1,m2,n1,n2=c1c2_means()
print(f'  전체: C1 {m1:+.1f}%(n{n1}) vs C2 {m2:+.1f}%(n{n2}) → C2-C1 = {m2-m1:+.1f}p')
for w in ['MU','SNDK','NVDA','BE']:
    a1,a2,_,_=c1c2_means((w,))
    print(f'  -{w:<5} 제외: C1 {a1:+.1f}% vs C2 {a2:+.1f}% → C2-C1 = {a2-a1:+.1f}p')

# === C2 진입 부스트 BT (v119 base) ===
def sim(c2_boost=0.0,exclude=(),start=0,slots=2,track=False):
    held={};prev=None;val=1.0;peak=1.0;mdd=0;series=[]
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,(w,) in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);peak=max(peak,val);mdd=min(mdd,(val/peak-1)*100)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk)
            if info and info.get('min_seg',0)<-2: del held[tk];continue
            if info is None: continue
            p2=info.get('p2')
            if not (p2 is None or p2>10): continue
            _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
            if _pe>=15: del held[tk]
        if len(held)<slots:
            cands=[]
            for tk,info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg',0)<0 or not info['price'] or not verified(tk,i): continue
                if info.get('high30') and info['price']/info['high30']-1<-0.25: continue
                if (info.get('dv') or 0)<1000: continue
                p2=info.get('p2')
                if p2 is None or p2>5: continue
                eff=p2 - (c2_boost if is_c2(tk,d,info) else 0)  # C2면 순위 끌어올림
                cands.append((eff,p2,tk))
            cands.sort();pick=cands[:slots-len(held)]
            for _,_,tk in pick: held[tk]=(1.0/slots,)
        if track: series.append(frozenset(held.keys()))
        prev=dict(held)
    if track: return (val-1)*100,mdd,series
    return (val-1)*100,mdd
elig=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def run(boost,exclude=()):
    cs,ms=[],[]
    for ch in seeds:
        for s in ch:
            c,m=sim(boost,exclude,s);cs.append(c);ms.append(m)
    return cs,ms
print('\n=== C2 진입부스트 BT (v119 base, boost=순위 끌어올림) ===')
print(f'{"부스트":<14}{"전기간":>9}{"전MDD":>8}{"paired":>9}{"LOWO최악":>10}')
for b in [0.0,1.0,2.0,3.0,5.0]:
    c,m=sim(b);cs,ms=run(b);pavg=st.mean(cs)
    worst=pavg
    for w in WINNERS: worst=min(worst,st.mean(run(b,(w,))[0]))
    lbl='없음(baseline)' if b==0 else f'C2 +{b:.0f}'
    print(f'{lbl:<14}{c:>+8.1f}%{m:>+7.1f}%{pavg:>+8.1f}%{worst:>+9.1f}%',flush=True)
print('\n해석: C2 boost가 baseline보다 수익·LOWO 개선이면 채택. v83때처럼 단일종목 착시(LOWO 붕괴)면 기각.')
