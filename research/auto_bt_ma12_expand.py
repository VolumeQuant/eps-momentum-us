# -*- coding: utf-8 -*-
"""MA12-hold 하 미탐색 파라미터 확장 그리드 (reexam 엔진 재사용)
핵심: 원 reexam은 entry=slots+1로 하드코딩 → entry_r 독립 파라미터화.
Phase A: exit_r {8,10,12,15} sweep   (s2/5050/entry3)   ← 이탈선 미탐색(8,15)
Phase B: entry_r {2,3,4} sweep       (s2/5050/exit10)   ← 진입선 미탐색(2,4)
Phase C: slots {1,2,3}               (5050/exit10/entry=slots+1) ← slot1 미탐색
각 config LOWO(-MU-SNDK-STX) 병기. 현 운영 = s2/5050/exit10/entry3 = ★
usage: python auto_bt_ma12_expand.py [N=100] [SAMP=3]
"""
import sys, sqlite3, random, statistics
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
import os
DB=os.path.join(os.path.dirname(__file__),'..','eps_momentum_data.db')
N=int(sys.argv[1]) if len(sys.argv)>1 else 100
SAMP=int(sys.argv[2]) if len(sys.argv)>2 else 3
MINH=10
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
data={}
for d in dates:
    data[d]={}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30 FROM ntm_screening WHERE date=?',(d,)):
        tk=r[0];nc,n7,n30,n60,n90=(float(x) if x else 0 for x in r[5:10]);segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
        data[d][tk]=dict(p2=r[1],cr=r[2],price=r[3],score=r[4] or 0,min_seg=min(segs) if segs else 0,high30=r[10])
all_dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE price IS NOT NULL ORDER BY date')]
didx={d:i for i,d in enumerate(all_dates)};px=defaultdict(dict);pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): px[tk][d]=p;pf[d][tk]=p
con.close()
def ma(tk,d,n=12):
    i=didx.get(d)
    if i is None or i-n+1<0: return None
    v=[px[tk].get(all_dates[j]) for j in range(i-n+1,i+1)];v=[x for x in v if x]
    return sum(v)/len(v) if len(v)>=max(2,n//2) else None
def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True
def sim(slots,weight,exit_r,entry_r,exclude=(),start=0):
    held={};prev=None;val=1.0;peak=1.0;mdd=0.0
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,w in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);peak=max(peak,val);mdd=max(mdd,(peak-val)/peak)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk)
            if info is not None and info.get('min_seg') is not None and info['min_seg']<-2: del held[tk];continue
            p2=info.get('p2') if info else None
            if info is None or p2 is None or p2>exit_r:
                m=ma(tk,d,12)
                if m is not None and (px[tk].get(d) or 0)>m: continue  # MA12 위면 보유
                del held[tk]
        if len(held)<slots:
            c=[]
            for tk,info in dd.items():
                if info['p2'] is None or info['p2']>entry_r or tk in held or tk in exclude: continue
                if info.get('min_seg') is not None and info['min_seg']<0: continue
                if not info['price'] or not verified(tk,i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30']-1<-0.25: continue
                c.append((info['p2'],info['score'],tk))
            c.sort();pick=c[:slots-len(held)]
            for _,_,tk in pick: held[tk]=0
        if held:
            ks=list(held.keys());n=len(ks)
            if weight=='5050' or n==1:
                for k in ks: held[k]=1.0/n
            elif weight=='2step':
                sc=sorted(ks,key=lambda k:-(data[d].get(k,{}).get('score',0)))
                s1=data[d].get(sc[0],{}).get('score',0);s2=data[d].get(sc[1],{}).get('score',0) if n>1 else 0
                gap=(s1-s2)/s1*100 if s1>0 else 0
                if n>=2 and gap>=15:
                    for k in ks: held[k]=0.0
                    held[sc[0]]=1.0
                else:
                    for k in ks: held[k]=1.0/n
        prev=dict(held)
    return (val-1)*100,mdd*100
elig=list(range(len(dates)-MINH));seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def run(slots,weight,exit_r,entry_r,exclude=()):
    cums=[];mdds=[]
    for ch in seeds:
        for s in ch:
            cu,md=sim(slots,weight,exit_r,entry_r,exclude,s);cums.append(cu);mdds.append(md)
    return cums,mdds
def med(x):return statistics.median(x)
CUR=(2,'5050',10,3)  # 현 운영
def show(cfgs,title):
    print('\n'+'='*94);print(title);print('='*94)
    print(f'{"config":<30}{"avg":>9}{"med":>9}{"MDD중앙":>9}{"Calmar":>8}{"LOWO(-MU-SNDK-STX)":>20}')
    print('-'*94)
    for slots,weight,exit_r,entry_r in cfgs:
        c,m=run(slots,weight,exit_r,entry_r)
        cl,_=run(slots,weight,exit_r,entry_r,('MU','SNDK','STX'))
        avg=statistics.mean(c);mm=med(m);cal=avg/mm if mm>0 else 0
        star=' ★현운영' if (slots,weight,exit_r,entry_r)==CUR else ''
        name=f's{slots}/{weight}/exit{exit_r}/entry{entry_r}'
        print(f'{name:<30}{avg:>+8.1f}%{med(c):>+8.1f}%{mm:>8.1f}%{cal:>8.2f}{statistics.mean(cl):>+18.1f}%{star}')
print(f'paired N={N} x SAMP={SAMP} = {N*SAMP} runs/config | dates={len(dates)}')
# Phase A: exit sweep (8,10,12,15) at s2/5050/entry3
show([(2,'5050',8,3),(2,'5050',10,3),(2,'5050',12,3),(2,'5050',15,3)],
     'Phase A: 이탈선 sweep (s2/5050/entry3) — 미탐색 exit 8,15')
# Phase B: entry sweep (2,3,4) at s2/5050/exit10
show([(2,'5050',10,2),(2,'5050',10,3),(2,'5050',10,4)],
     'Phase B: 진입선 sweep (s2/5050/exit10) — 미탐색 entry 2,4')
# Phase C: slot sweep (1,2,3) entry=slots+1
show([(1,'5050',10,2),(2,'5050',10,3),(3,'5050',10,4)],
     'Phase C: 슬롯 sweep (5050/exit10, entry=slots+1) — 미탐색 slot1')
