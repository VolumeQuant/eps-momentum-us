# -*- coding: utf-8 -*-
"""fwd_PE를 '약하게' 거는 검증 — ①보유 임계 완화(15→18→20→25→30→무한) ②진입 soft tilt(배제말고 우선순위만).
v119 base. DB dv 사용. baseline=보유PE<15(현행)."""
import sqlite3, random, statistics as st
from collections import defaultdict
import sys; sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'; N=100; SAMP=3; MINH=10
WINNERS={'MU','SNDK','STX','LITE','TTMI','NVDA','TER','BE'}
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
data={}
for d in dates:
    data[d]={}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,dollar_volume_30d,rev_growth FROM ntm_screening WHERE date=?',(d,)):
        tk=r[0];nc,n7,n30,n60,n90=(float(x) if x else 0 for x in r[4:9]);segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
        data[d][tk]=dict(p2=r[1],cr=r[2],price=r[3],min_seg=min(segs),high30=r[9],dv=r[10],ntm=nc,rg=r[11] or 0)
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p
con.close()
def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True
def sim(pe_hold=15, tilt_k=0.0, exclude=(), start=0, slots=2, track=False):
    """pe_hold: 10위밖일 때 fwd_PE>=pe_hold면 매도(낮을수록 빨리 매도=강하게). 999=순위만으로 매도.
    tilt_k: 진입 우선순위에 PE 페널티 가중(배제X, 0=순위만). key=p2 + tilt_k*max(0,PE-20)/10."""
    held={};prev=None;val=1.0;peak=1.0;mdd=0;series=[]
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,(ed,ep,w) in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);peak=max(peak,val);mdd=min(mdd,(val/peak-1)*100)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk);ed,ep,w=held[tk]
            if info and info.get('min_seg',0)<-2: del held[tk];continue
            if info is None: continue
            p2=info.get('p2')
            if not (p2 is None or p2>10): continue
            _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
            if _pe>=pe_hold: del held[tk]
        if len(held)<slots:
            cands=[]
            for tk,info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg',0)<0 or not info['price'] or not verified(tk,i): continue
                if info.get('high30') and info['price']/info['high30']-1<-0.25: continue
                if (info.get('dv') or 0)<1000: continue
                p2=info.get('p2')
                if p2 is None or p2>5: continue
                _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
                key=p2 + tilt_k*max(0.0,_pe-20)/10.0   # soft tilt: 배제X, 고PE 후순위
                cands.append((key,p2,tk))
            cands.sort();pick=cands[:slots-len(held)]
            for _,_,tk in pick: held[tk]=(d,dd[tk]['price'],1.0/slots)
        if track: series.append(frozenset(held.keys()))
        prev=dict(held)
    if track: return (val-1)*100,mdd,series
    return (val-1)*100,mdd
elig=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def run(pe_hold,tilt_k=0.0,exclude=()):
    cs,ms=[],[]
    for ch in seeds:
        for s in ch:
            c,m=sim(pe_hold,tilt_k,exclude,s);cs.append(c);ms.append(m)
    return cs,ms
def report(label,pe_hold,tilt_k=0.0):
    c,m=sim(pe_hold,tilt_k)
    cs,ms=run(pe_hold,tilt_k)
    pavg=st.mean(cs);pmdd=st.mean(ms)
    worst=pavg
    for w in WINNERS: worst=min(worst,st.mean(run(pe_hold,tilt_k,(w,))[0]))
    print(f'{label:<24}{c:>+8.1f}%{m:>+7.1f}%{pavg:>+9.1f}%{pmdd:>+7.1f}%{worst:>+9.1f}%',flush=True)

print(f'BT {dates[0]}~{dates[-1]} ({len(dates)}일), v119\n')
print(f'{"설정":<24}{"전기간":>9}{"전MDD":>8}{"paired":>10}{"pMDD":>8}{"LOWO":>9}')
print('-- ① 보유 임계 완화 (높을수록 winner 더 오래 보유 = 약하게) --')
for pe in [15,18,20,22,23,24,25,26,28,30,35,40,999]:
    report(f'보유 PE<{pe}'+(' (현행)' if pe==15 else '')+(' (순위만매도)' if pe==999 else ''), pe, 0.0)
# 어떤 종목이 PE15 vs PE25 차이를 만드나 (deterministic 경로)
_,_,s15=sim(15,0.0,track=True);_,_,s25=sim(25,0.0,track=True)
from collections import Counter
extra=Counter()
for a,b in zip(s15,s25):
    for tk in (b-a): extra[tk]+=1
print(f'  → PE<25에서 더(오래) 보유된 종목: {dict(extra.most_common(8))}')
print('-- ② 진입 soft tilt (고PE 후순위, 배제X) — 보유PE<15 고정 --')
for k in [0.0,0.5,1.0,2.0,5.0]:
    report(f'tilt_k={k}'+(' (현행=tilt無)' if k==0 else ''), 15, k)
print('\n해석: 보유완화는 v119문서 "PE<20=동일"과 일치하는지 확인. tilt는 배제 안하니 Task A하드캡보다 부드러움 — 개선이면 채택.')
