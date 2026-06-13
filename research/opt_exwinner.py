# -*- coding: utf-8 -*-
"""SNDK/MU 제외 기준 최적화 + 고PER(PE_HOLD) 재검증 (자율주행 2026-06-13).
목적함수: 슈퍼winner(SNDK,MU) 없이도 좋은 config (과적합 회피). PE_HOLD·이탈순위 스윕."""
import sqlite3, random, statistics as st, math
from collections import defaultdict
import numpy as np
import sys; sys.stdout.reconfigure(encoding='utf-8')
con=sqlite3.connect('eps_momentum_data.db');cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
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
def sim(pe_hold=15,exit_r=10,entry_r=5,exclude=(),start=0):
    held=set();prev=None;val=1.0;peak=1.0;mdd=0
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];r=0
            for tk in held:
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: r+=(1.0/len(held))*(pn/pp-1)
            val*=(1+r);peak=max(peak,val);mdd=min(mdd,(val/peak-1)*100)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk)
            if info and info.get('min_seg',0)<-2: held.discard(tk);continue
            if info is None: continue
            p2=info.get('p2')
            if not (p2 is None or p2>exit_r): continue
            _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
            if _pe>=pe_hold: held.discard(tk)
        if len(held)<2:
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
            for _,tk in cands[:2-len(held)]: held.add(tk)
        prev=set(held)
    return (val-1)*100,mdd
elig=list(range(2,len(dates)-10));seeds=[random.Random(s).sample(elig,3) for s in range(100)]
def paired(exclude=(),**kw):
    cs,ms=[],[]
    for ch in seeds:
        for s in ch: c,m=sim(exclude=exclude,start=s,**kw);cs.append(c);ms.append(m)
    return st.mean(cs),st.mean(ms)
EX=('SNDK','MU')
print('=== SNDK/MU 제외 시 성과 (현행 PE15/이탈10) ===')
cf,mf=sim();ce,me=sim(exclude=EX)
pf_,pm=paired();pe_,pme=paired(exclude=EX)
print(f'  전체:        전기간{cf:+.1f}% MDD{mf:+.1f}% | paired{pf_:+.1f}%')
print(f'  SNDK+MU제외:  전기간{ce:+.1f}% MDD{me:+.1f}% | paired{pe_:+.1f}%  ← 슈퍼winner 없이도 이만큼')
print('\n=== 고PER(PE_HOLD) 재검증 — SNDK/MU 제외 기준 최적화 ===')
print(f'{"PE_HOLD":<10}{"제외전기간":>11}{"제외MDD":>9}{"제외paired":>11}{"(참고)전체paired":>15}')
best=None
for pe in [10,12,15,18,20,25,30,40,999]:
    ce,me=sim(pe_hold=pe,exclude=EX);pe_ex,_=paired(pe_hold=pe,exclude=EX);pe_full,_=paired(pe_hold=pe)
    lbl=f'PE<{pe}'+(' (현행)' if pe==15 else '')+(' (순위만)' if pe==999 else '')
    print(f'{lbl:<10}{ce:>+10.1f}%{me:>+8.1f}%{pe_ex:>+10.1f}%{pe_full:>+14.1f}%',flush=True)
    if best is None or pe_ex>best[1]: best=(pe,pe_ex)
print(f'  → SNDK/MU제외 paired 최고: PE<{best[0]} ({best[1]:+.1f}%)')
print('\n=== 이탈순위 재검증 — SNDK/MU 제외 기준 (PE15 고정) ===')
print(f'{"이탈":<8}{"제외전기간":>11}{"제외MDD":>9}{"제외paired":>11}')
for xr in [8,10,12,15]:
    ce,me=sim(exit_r=xr,exclude=EX);pe_ex,_=paired(exit_r=xr,exclude=EX)
    print(f'이탈{xr:<5}{ce:>+10.1f}%{me:>+8.1f}%{pe_ex:>+10.1f}%',flush=True)
print('\n해석: SNDK/MU 제외 paired를 최대화하는 PE/이탈이 현행(15/10)과 다르면 = 더 robust한 config 후보.')
