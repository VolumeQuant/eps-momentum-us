# -*- coding: utf-8 -*-
"""이탈선 H10 vs H12 정밀 분해 — 사용자 도전(기간에 조정 있었는데 H12가 최고면 채택해야?).
1) 종목별 기여 분해 (one-stock 착시 여부)
2) walk-forward 서브구간별 (3월 조정구간 포함 — 조정에서도 H12가 이기나)
production-replay, PE_HOLD=30, entry Top5, slots2."""
import sqlite3, statistics as st
from collections import defaultdict
import sys; sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'; PE_HOLD=30.0
con=sqlite3.connect(DB);cur=con.cursor()
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

def sim(H,start=0,end=None,attrib=None):
    if end is None: end=len(dates)
    held={};prev=None;val=1.0;peak=1.0;mdd=0
    for i in range(start,end):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,w in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn:
                    r=w*(pn/pp-1); ret+=r
                    if attrib is not None: attrib[tk]+=r*val  # 절대기여(복리근사)
            val*=(1+ret);peak=max(peak,val);mdd=min(mdd,(val/peak-1)*100)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk)
            if info and info.get('min_seg',0)<-2: del held[tk];continue
            if info is None: continue
            p2=info.get('p2')
            if not (p2 is None or p2>H): continue
            _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
            if _pe>=PE_HOLD: del held[tk]
        if len(held)<2:
            cands=[]
            for tk,info in dd.items():
                if tk in held: continue
                if info.get('min_seg',0)<0 or not info['price'] or not verified(tk,i): continue
                if info.get('high30') and info['price']/info['high30']-1<-0.25: continue
                if (info.get('dv') or 0)<1000: continue
                p2=info.get('p2')
                if p2 is None or p2>5: continue
                cands.append((p2,tk))
            cands.sort()
            for _,tk in cands[:2-len(held)]: held[tk]=0.5
        prev=dict(held)
    return (val-1)*100,mdd

print(f'BT {dates[0]}~{dates[-1]} ({len(dates)}일)\n')
print('=== (1) 전기간 H10 vs H12 + 종목별 기여 분해 ===')
for H in (10,12):
    a=defaultdict(float); full,mdd=sim(H,attrib=a)
    top=sorted(a.items(),key=lambda x:-x[1])[:6]
    print(f'H{H}: 전기간 {full:+.1f}% MDD {mdd:.1f}%')
    print('   종목기여 상위: '+', '.join(f'{t}{v*100:+.0f}' for t,v in top))
# 차이 기여
a10=defaultdict(float); sim(10,attrib=a10)
a12=defaultdict(float); sim(12,attrib=a12)
diff=sorted(((t,(a12[t]-a10.get(t,0))*100) for t in set(a12)|set(a10)),key=lambda x:-abs(x[1]))
print('\n   H12-H10 기여차 (큰 순): '+', '.join(f'{t}{v:+.0f}' for t,v in diff[:8] if abs(v)>1))

print('\n=== (2) walk-forward 서브구간 (3월조정 포함) ===')
# 3구간: 2-3월(조정), 4-5월(반등), 6월
def widx(mm): return [i for i,d in enumerate(dates) if d[5:7] in mm]
segs=[('2-3월(조정·MDD-8.7%)',['02','03']),('4-5월(반등)',['04','05']),('6월(이란딥)',['06'])]
print(f'{"구간":<22}{"H10":>9}{"H12":>9}{"차이":>8}')
for nm,mm in segs:
    idx=widx(mm)
    if not idx: continue
    s,e=idx[0],idx[-1]+1
    f10,_=sim(10,s,e); f12,_=sim(12,s,e)
    print(f'{nm:<22}{f10:>+8.1f}%{f12:>+8.1f}%{f12-f10:>+7.1f}%p',flush=True)

print('\n해석: H12 이득이 (a)여러 종목·(b)조정구간 포함 전 구간에서 +면 robust→채택검토.')
print('한 종목(BE 등)·반등구간에만 몰리면 = 착시/V자의존 → 기각 유지.')
