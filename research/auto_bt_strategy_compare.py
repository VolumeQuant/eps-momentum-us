# -*- coding: utf-8 -*-
"""전략 head-to-head: v84(baseline) vs v86e+(메가홀드) vs v110(각분야1등) vs MA10-hold(신규)
같은 엔진·진입필터·2슬롯·50/50, 차이는 entry/hold 규칙만. paired 100×3 + LOWO.
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
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth FROM ntm_screening WHERE date=?',(d,)):
        tk=r[0];nc,n7,n30,n60,n90=(float(x) if x else 0 for x in r[5:10]);segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
        rg=r[11];price=r[3]
        peg=(price/nc)/(rg*100) if (price and nc and nc>0 and rg and rg>0) else None
        mega_score=((nc/n90-1)*100 + rg*100 + 50/peg) if (peg and peg<0.18 and rg and rg>=0.25 and n90>0) else None
        data[d][tk]=dict(p2=r[1],cr=r[2],price=price,score=r[4] or 0,min_seg=min(segs) if segs else 0,
                         high30=r[10],peg=peg,rg=rg,mega_score=mega_score)
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
def is_mega(info): return info is not None and info.get('peg') is not None and info['peg']<0.18 and info.get('rg') and info['rg']>=0.25
def hold_override(strat,tk,d,info):
    # rank>10 매도 회피 조건
    if strat in ('megahold','v110'):
        return is_mega(info) and not (info.get('rg') and info['rg']<0.25)
    if strat=='ma10':
        m=ma(tk,d,10); return m is not None and (px[tk].get(d) or 0)>m
    return False  # baseline
def sim(strat,exclude=(),start=0):
    held={};prev=None;val=1.0;peak=1.0;mdd=0.0
    for i in range(start,len(dates)):
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
                if hold_override(strat,tk,d,info): continue
                del held[tk]
        if len(held)<2:
            if strat=='v110':
                # slot1 part2 Top1 + slot2 mega_score Top1
                if not any(True for _ in held):  # 빈 슬롯
                    pass
                cands=[(info['p2'],info['score'],tk) for tk,info in dd.items()
                       if info['p2'] and info['p2']<=3 and tk not in held and tk not in exclude
                       and (info.get('min_seg') is None or info['min_seg']>=0) and info['price'] and verified(tk,dates.index(d))
                       and not(info.get('high30') and info['price'] and info['price']/info['high30']-1<-0.25)]
                cands.sort()
                # slot1: part2 top1
                if len(held)<2 and cands:
                    held[cands[0][2]]=cands[0][2] and dd[cands[0][2]]['price']
                # slot2: mega_score top1
                if len(held)<2:
                    megs=[(info['mega_score'],tk) for tk,info in dd.items()
                          if info.get('mega_score') is not None and tk not in held and tk not in exclude
                          and (info.get('min_seg') is None or info['min_seg']>=0) and info['price'] and verified(tk,dates.index(d))]
                    megs.sort(key=lambda x:-x[0])
                    if megs: held[megs[0][1]]=dd[megs[0][1]]['price']
            else:
                c=[]
                for tk,info in dd.items():
                    if info['p2'] is None or info['p2']>3 or tk in held or tk in exclude: continue
                    if info.get('min_seg') is not None and info['min_seg']<0: continue
                    if not info['price'] or not verified(tk,dates.index(d)): continue
                    if info.get('high30') and info['price'] and info['price']/info['high30']-1<-0.25: continue
                    c.append((info['p2'],info['score'],tk))
                c.sort()
                for _,_,tk in c[:2-len(held)]: held[tk]=dd[tk]['price']
        prev=dict(held)
    return (val-1)*100,mdd*100
elig=list(range(len(dates)-MINH));seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def run(strat,exclude=()):
    cums=[];mdds=[]
    for ch in seeds:
        for s in ch:
            cu,md=sim(strat,exclude,s);cums.append(cu);mdds.append(md)
    return cums,mdds
print('='*92);print('전략 비교 (paired 100×3, 동일 엔진/2슬롯/50-50)');print('='*92)
strats=[('v84 baseline',' baseline'),('v86e+ 메가홀드','megahold'),('v110 각분야1등','v110'),('MA10-hold (신규)','ma10')]
print(f'\n{"전략":<18}{"avg":>9}{"med":>9}{"MDD중앙":>9}{"MDD최대":>9}{"Calmar":>8}')
print('-'*72)
def med(x):return statistics.median(x)
results={}
for name,s in strats:
    cums,mdds=run(s.strip())
    avg=statistics.mean(cums);mm=med(mdds);cal=avg/mm if mm>0 else 0
    results[s.strip()]=cums
    print(f'{name:<18}{avg:>+8.1f}%{med(cums):>+8.1f}%{mm:>8.1f}%{max(mdds):>8.1f}%{cal:>8.2f}')
print('\n--- LOWO: -MU-SNDK-STX 제외 시 각 전략 avg (큰winner 빼도 견고한가) ---')
for name,s in strats:
    cums,_=run(s.strip(),('MU','SNDK','STX'))
    print(f'  {name:<18} {statistics.mean(cums):>+7.1f}%')
