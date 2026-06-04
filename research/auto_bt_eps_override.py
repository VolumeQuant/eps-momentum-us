# -*- coding: utf-8 -*-
"""사용자 검증 요청: "MA12 위면 EPS 꺾여도 안 파는게 낫나?"
현행 v111  : 매도 = min_seg<-2(EPS꺾임, 항상) OR (rank>10 AND 가격<MA12)
대안 A     : 매도 = (rank>10 AND 가격<MA12)              ← EPS꺾임 무시(MA12 위면 보유)
대안 B     : 매도 = (가격<MA12)                          ← 순수 MA12, 순위/EPS 무관
진입/슬롯/비중 동일(part2≤slots+1, min_seg≥0 진입필터, 2슬롯, 50/50).
paired 100×3 + LOWO(-MU-SNDK-STX) + walk-forward 5블록.
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
def sim(variant,exclude=(),start=0):
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
            cp=px[tk].get(d)
            ms=info.get('min_seg') if info else None
            # 현행만 EPS꺾임 항상매도
            if variant=='v111' and ms is not None and ms<-2: del held[tk];continue
            p2=info.get('p2') if info else None
            if variant=='altB':
                # 순수 MA12: 순위 무관, 가격<MA12면 매도
                if cp is None: continue
                m=ma(tk,d,12)
                if m is not None and cp>m: continue
                del held[tk]
            else:
                # v111/altA: rank>10 일 때만 MA12 체크
                if p2 is None or p2>10:
                    if cp is None: continue
                    m=ma(tk,d,12)
                    if m is not None and cp>m: continue
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
            for _,_,tk in c[:2-len(held)]: held[tk]=1
        prev=dict(held)
    return (val-1)*100,mdd*100
elig=list(range(len(dates)-MINH));seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def paired(variant,exclude=()):
    cums=[];mdds=[];sav=[]
    for ch in seeds:
        sr=[]
        for s in ch:
            cu,md=sim(variant,exclude,s);cums.append(cu);mdds.append(md);sr.append(cu)
        sav.append(sum(sr)/len(sr))
    return cums,mdds,sav
def med(x):return statistics.median(x)
print('='*78);print('EPS꺾임 매도 vs MA12-override 검증 (paired 100×3, 2슬롯/50-50)');print('='*78)
base_c,base_m,base_sav=paired('v111')
print(f'\n{"변형":<32}{"avg":>9}{"med":>9}{"MDD중앙":>9}{"vs현행 lift":>12}{"wins":>7}')
print('-'*78)
print(f'{"v111 현행(EPS꺾임 항상매도)":<32}{statistics.mean(base_c):>+8.1f}%{med(base_c):>+8.1f}%{med(base_m):>8.1f}%{"(기준)":>12}')
for name,v in [('altA: MA12위면 EPS꺾임무시','altA'),('altB: 순수MA12(순위/EPS무관)','altB')]:
    c,m,sav=paired(v)
    lifts=[a-b for b,a in zip(base_sav,sav)];wins=sum(1 for l in lifts if l>0)
    print(f'{name:<32}{statistics.mean(c):>+8.1f}%{med(c):>+8.1f}%{med(m):>8.1f}%{statistics.mean(lifts):>+11.1f}p{wins:>6}')
print('\n--- LOWO: -MU-SNDK-STX 제외 (큰winner 빼도 견고한가) ---')
bl,_,blsav=paired('v111',('MU','SNDK','STX'))
print(f'  v111 현행         {statistics.mean(bl):>+7.1f}%  (기준)')
for name,v in [('altA','altA'),('altB','altB')]:
    c,_,sav=paired(v,('MU','SNDK','STX'))
    lf=statistics.mean([a-b for b,a in zip(blsav,sav)])
    print(f'  {name:<16}{statistics.mean(c):>+7.1f}%  lift {lf:+.1f}p')
print('\n--- walk-forward (시작일 5블록 순차, OOS 성격) ---')
nb=5;blk=len(elig)//nb
for v in ['altA','altB']:
    line=f'  {v:<6}'
    for b_i in range(nb):
        lo=b_i*blk;hi=(b_i+1)*blk if b_i<nb-1 else len(elig)
        ss=[random.Random(s+b_i*100).sample(range(lo,hi),min(SAMP,hi-lo)) for s in range(N)]
        bb=[statistics.mean([sim('v111',(),x)[0] for x in ch]) for ch in ss]
        nn=[statistics.mean([sim(v,(),x)[0] for x in ch]) for ch in ss]
        lf=statistics.mean([a-b for b,a in zip(bb,nn)]);line+=f' B{b_i+1}:{lf:+.0f}'
    print(line)
