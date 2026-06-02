# -*- coding: utf-8 -*-
"""슬롯 2→3 정밀 검증
- 가중치 방식: slot2_2step(현재), slot3_equal, slot3_top(50/30/20), slot3_2step
- 100×3 paired BT, MDD 분포(median/p90/max), Calmar, leave-winner-out, 부분기간
- entry pool = part2_rank ≤ slots+1, pick top slots (공정 비교)
"""
import sys, sqlite3, random, statistics, math
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB = ROOT / 'eps_momentum_data.db'
N_SEEDS=100; SAMPLES=3; MIN_HOLD=10

con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
data={}
for d in dates:
    data[d]={}
    for r in cur.execute('''SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30 FROM ntm_screening WHERE date=?''',(d,)):
        tk=r[0];nc,n7,n30,n60,n90=(float(x) if x else 0 for x in r[5:10]);segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
        data[d][tk]=dict(p2=r[1],cr=r[2],price=r[3],score=r[4] or 0,min_seg=min(segs) if segs else 0,high30=r[10])
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p

def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True

def weights(scheme, picked):
    n=len(picked)
    if n==0: return []
    if scheme=='slot2_2step':
        if n==1: return [1.0]
        s1,s2=picked[0][1],picked[1][1]
        return [1.0,0.0] if (s1-s2)>=15 else [0.5,0.5]
    if scheme=='equal':
        return [1.0/n]*n
    if scheme=='top532':
        base=[0.5,0.3,0.2][:n]; s=sum(base); return [x/s for x in base]
    if scheme=='slot3_2step':
        if n==1: return [1.0]
        s1,s2=picked[0][1],picked[1][1]
        if (s1-s2)>=15:  # 1위 강하면 top-heavy
            base=[0.5,0.3,0.2][:n]
        else:
            base=[1.0/n]*n
        s=sum(base); return [x/s for x in base]
    return [1.0/n]*n

def sim(scheme,slots,exclude=(),start=0):
    held={};prev=None;val=1.0;peak=1.0;mdd=0.0;rets=[];trades=[]
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,(ed,ep,w) in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);rets.append(ret);peak=max(peak,val);mdd=max(mdd,(peak-val)/peak)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk);ep=held[tk][1];p2=info.get('p2') if info else None
            if info is None or p2 is None or p2>10:
                sp=(info.get('price') if info else None) or pf[d].get(tk,ep);trades.append(dict(tk=tk,ret=(sp/ep-1)*100));del held[tk]
            elif info.get('min_seg') is not None and info['min_seg']<-2:
                sp=info['price'] or ep;trades.append(dict(tk=tk,ret=(sp/ep-1)*100));del held[tk]
        if len(held)<slots:
            cands=[]
            for tk,info in dd.items():
                if info['p2'] is None or info['p2']>slots+1: continue
                if tk in held or tk in exclude: continue
                if info.get('min_seg') is not None and info['min_seg']<0: continue
                if not info['price']: continue
                if not verified(tk,i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30']-1<-0.25: continue
                cands.append((info['p2'],info['score'],tk))
            cands.sort(key=lambda x:x[0])
            need=slots-len(held); pick=cands[:need]
            w=weights(scheme,pick)
            for idx,(_,_,tk) in enumerate(pick):
                if w[idx]>0: held[tk]=(d,dd[tk]['price'],w[idx])
        prev=dict(held)
    return dict(cum=(val-1)*100,mdd=mdd*100,trades=trades,rets=rets)

elig=dates[:-MIN_HOLD];seeds=[]
for s in range(N_SEEDS): random.seed(s);seeds.append(random.sample(range(len(elig)),SAMPLES))

def run(scheme,slots,exclude=()):
    cums=[];mdds=[];savg=[]
    for ch in seeds:
        sr=[]
        for s in ch:
            r=sim(scheme,slots,exclude=exclude,start=s);cums.append(r['cum']);mdds.append(r['mdd']);sr.append(r['cum'])
        savg.append(sum(sr)/len(sr))
    return cums,mdds,savg

print('='*100)
print('슬롯 2→3 정밀 검증 (100×3 paired, entry≤slots+1)')
print('='*100)
variants=[('slot2_2step',2),('equal',3),('top532',3),('slot3_2step',3),('equal',4),('equal',5)]
base_cums,base_mdds,base_savg=run('slot2_2step',2)
def pct(xs,p): xs=sorted(xs);return xs[min(len(xs)-1,int(len(xs)*p))]
print(f'\n{"variant":<22}{"avg":>9}{"med":>9}{"MDD중앙":>9}{"MDD_p90":>9}{"MDD최대":>9}{"Calmar":>8}{"lift":>9}{"wins":>8}')
print('-'*100)
results={}
for sch,sl in variants:
    cums,mdds,savg=run(sch,sl);results[(sch,sl)]=(cums,mdds,savg)
    avg=statistics.mean(cums);med=statistics.median(cums)
    mdd_med=statistics.median(mdds);mdd_p90=pct(mdds,0.9);mdd_max=max(mdds)
    cal=avg/mdd_med if mdd_med>0 else 0
    lifts=[b-a for a,b in zip(base_savg,savg)];wins=sum(1 for l in lifts if l>0)
    name=f'{sch}/s{sl}'
    mk=' ★' if (sch=='slot2_2step') else '  '
    lift_s='' if sch=='slot2_2step' and sl==2 else f'{sum(lifts)/len(lifts):>+7.1f}p{wins:>6}/100'
    print(f'{mk}{name:<20}{avg:>+8.1f}%{med:>+8.1f}%{mdd_med:>8.1f}%{mdd_p90:>8.1f}%{mdd_max:>8.1f}%{cal:>8.1f}{lift_s}')

# leave-winner-out for slot3 equal
print('\n--- leave-winner-out: slot3_equal vs slot2 (MU/SNDK 제외 견고성) ---')
for ex_name,ex in [('전체',()),('-MU',('MU',)),('-SNDK',('SNDK',)),('-MU-SNDK',('MU','SNDK'))]:
    _,_,b=run('slot2_2step',2,exclude=ex)
    _,_,n=run('equal',3,exclude=ex)
    lifts=[y-x for x,y in zip(b,n)];wins=sum(1 for l in lifts if l>0)
    print(f'  [{ex_name:<9}] slot3-slot2 lift {sum(lifts)/len(lifts):>+7.1f}p  ({wins:>3}/100)')

# 부분기간: 전반(start 앞 절반) vs 후반
print('\n--- 부분기간 견고성: 시작일 전반부 vs 후반부 (slot3_equal vs slot2) ---')
half=len(elig)//2
for label,lo,hi in [('전반부 시작',0,half),('후반부 시작',half,len(elig))]:
    sub=[random.Random(s).sample(range(lo,hi),min(SAMPLES,hi-lo)) for s in range(N_SEEDS)]
    def runsub(sch,sl):
        avg=[]
        for ch in sub:
            sr=[sim(sch,sl,start=s)['cum'] for s in ch];avg.append(sum(sr)/len(sr))
        return avg
    b=runsub('slot2_2step',2);n=runsub('equal',3)
    lifts=[y-x for x,y in zip(b,n)];wins=sum(1 for l in lifts if l>0)
    print(f'  [{label}] slot2 avg {statistics.mean(b):+.1f}% / slot3 avg {statistics.mean(n):+.1f}% / lift {sum(lifts)/len(lifts):+.1f}p ({wins}/100)')

con.close()
