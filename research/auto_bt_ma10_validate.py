# -*- coding: utf-8 -*-
"""MA10-hold 확정 검증: leave-STX-out (+전체 winner) + walk-forward (시계열 OOS)
질문: MA10-hold(+33p)가 진짜 broad인가, MU/SNDK/STX 3종목 의존인가?
1) leave-winner-out 확장: -STX, -MU-SNDK-STX, -top5 → 다 빼도 양수면 broad.
2) walk-forward: 시작일 구간 5블록 순차 → 각 블록(OOS 성격)에서 baseline 대비 lift.
"""
import sys, sqlite3, random, statistics
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db';N=100;SAMP=3;MINH=10
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
def ma(tk,d,n=10):
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
def sim(use_ma,exclude=(),start=0,end=None):
    held={};prev=None;val=1.0;peak=1.0;mdd=0.0
    hi=end if end is not None else len(dates)
    for i in range(start,hi):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0;w=1.0/len(prev) if prev else 0
            for tk in prev:
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);peak=max(peak,val);mdd=max(mdd,(peak-val)/peak)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk)
            if info is not None and info.get('min_seg') is not None and info['min_seg']<-2: del held[tk];continue
            p2=info.get('p2') if info else None
            if info is None or p2 is None or p2>10:
                if use_ma:
                    m=ma(tk,d,10)
                    if m is not None and (px[tk].get(d) or 0)>m: continue
                del held[tk]
        if len(held)<2:
            c=[]
            for tk,info in dd.items():
                if info['p2'] is None or info['p2']>3 or tk in held or tk in exclude: continue
                if info.get('min_seg') is not None and info['min_seg']<0: continue
                if not info['price'] or not verified(tk,i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30']-1<-0.25: continue
                c.append((info['p2'],info['score'],tk))
            c.sort()
            for _,_,tk in c[:2-len(held)]: held[tk]=dd[tk]['price']
        prev=dict(held)
    return (val-1)*100
elig=list(range(len(dates)-MINH))
seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def paired(use_ma,exclude=(),seed_set=None):
    ss=seed_set or seeds;sav=[]
    for ch in ss: sav.append(statistics.mean([sim(use_ma,exclude,s) for s in ch]))
    return sav
print('='*80);print('MA10-hold 확정 검증');print('='*80)
print('\n[1] leave-winner-out 확장 (다 빼도 양수면 broad, 0이면 소수의존)')
for exn,ex in [('전체',()),('-STX',('STX',)),('-MU-SNDK',('MU','SNDK')),
               ('-MU-SNDK-STX',('MU','SNDK','STX')),('-top5',('MU','SNDK','STX','LITE','TTMI'))]:
    b=paired(False,ex);m=paired(True,ex)
    lf=[y-x for x,y in zip(b,m)];w=sum(1 for l in lf if l>0)
    print(f'  [{exn:<14}] baseline {statistics.mean(b):>+6.0f}% → ma10 {statistics.mean(m):>+6.0f}% | lift {statistics.mean(lf):>+6.1f}p ({w:>3}/100)')
print('\n[2] walk-forward (시작일 5블록 순차 — 각 블록 OOS 성격)')
nb=5;blk=len(elig)//nb
for b_i in range(nb):
    lo=b_i*blk;hi=(b_i+1)*blk if b_i<nb-1 else len(elig)
    ss=[random.Random(s+b_i*100).sample(range(lo,hi),min(SAMP,hi-lo)) for s in range(N)]
    bb=paired(False,(),ss);mm=paired(True,(),ss)
    lf=[y-x for x,y in zip(bb,mm)];w=sum(1 for l in lf if l>0)
    print(f'  블록{b_i+1} (시작 {dates[lo]}~{dates[min(hi-1,len(dates)-1)]}): baseline {statistics.mean(bb):>+6.0f}% → ma10 {statistics.mean(mm):>+6.0f}% | lift {statistics.mean(lf):>+6.1f}p ({w:>3}/100)')
print('\n[3] 판정')
b=paired(False,('MU','SNDK','STX'));m=paired(True,('MU','SNDK','STX'))
lf3=statistics.mean([y-x for x,y in zip(b,m)])
verdict='✅ broad (3종목 넘어 일반화)' if lf3>2 else '⚠️ 3종목 의존 (그 이상 효과 미미)'
print(f'  -MU-SNDK-STX lift = {lf3:+.1f}p → {verdict}')
