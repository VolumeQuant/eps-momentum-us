# -*- coding: utf-8 -*-
"""'일찍 안 팔기' robust 연구 — 가격추세 기반 보유 (PEG 아님)
배경: 모든 winner(MU/SNDK/LITE/STX/TTMI/FORM/MOD)가 순위 10위 밖 빠진 뒤에도 +60~190% 더 감.
     PEG-hold는 MU/SNDK만(2종목 착시). 공통점=가격 상승 지속.
가설: 보유 종목이 가격 추세 유지(>MA / 신고가 / 수익중)면 순위 무관 보유, 추세깨지면 매도.
검증: baseline(rank>10 매도) vs 추세보유 변형들. paired 100×3 + LOWO(MU/SNDK 제외) + 부분기간.
robust 기준: baseline 우월 + LOWO 비음수(broad winner 잡음) + 부분기간 둘다 비음수.
min_seg<-2(EPS 꺾임)는 항상 매도. 보유 종목은 슬롯 점유(신규 차단) — 현실 trade-off 반영.
"""
import sys, sqlite3, random, statistics
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'; N=100; SAMP=3; MINH=10
con=sqlite3.connect(DB); cur=con.cursor()
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
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    px[tk][d]=p; pf[d][tk]=p
con.close()
def ma(tk,d,n):
    i=didx.get(d)
    if i is None or i-n+1<0: return None
    vals=[px[tk].get(all_dates[j]) for j in range(i-n+1,i+1)]; vals=[v for v in vals if v]
    return sum(vals)/len(vals) if len(vals)>=max(2,n//2) else None
def high20(tk,d):
    i=didx.get(d)
    if i is None: return None
    vals=[px[tk].get(all_dates[j]) for j in range(max(0,i-19),i+1)]; vals=[v for v in vals if v]
    return max(vals) if vals else None
def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True
def hold_ok(rule,tk,d,ep):
    cp=px[tk].get(d)
    if cp is None: return False
    if rule=='ma20': m=ma(tk,d,20); return m is not None and cp>m
    if rule=='ma10': m=ma(tk,d,10); return m is not None and cp>m
    if rule=='winning': return cp>ep  # 수익중
    if rule=='near_high': h=high20(tk,d); return h is not None and cp/h-1>=-0.10  # 신고가 -10% 이내
    if rule=='ma20_win': m=ma(tk,d,20); return (m is not None and cp>m) and cp>ep
    return False
def sim(rule,exclude=(),start=0):
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
            info=dd.get(tk);ep=held[tk]
            if info is not None and info.get('min_seg') is not None and info['min_seg']<-2:
                del held[tk];continue
            p2=info.get('p2') if info else None
            if info is None or p2 is None or p2>10:
                if rule!='baseline' and hold_ok(rule,tk,d,ep): continue  # 추세 유지 → 보유
                del held[tk]
        if len(held)<2:
            c=[]
            for tk,info in dd.items():
                if info['p2'] is None or info['p2']>3 or tk in held or tk in exclude: continue
                if info.get('min_seg') is not None and info['min_seg']<0: continue
                if not info['price'] or not verified(tk,i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30']-1<-0.25: continue
                c.append((info['p2'],info['score'],tk))
            c.sort();pk=c[:2-len(held)]
            for _,_,tk in pk: held[tk]=dd[tk]['price']
        prev=dict(held)
    return (val-1)*100,mdd*100
elig=list(range(len(dates)-MINH));seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def paired(rule,exclude=()):
    cums=[];mdds=[];sav=[]
    for ch in seeds:
        sr=[]
        for s in ch:
            cu,md=sim(rule,exclude,s);cums.append(cu);mdds.append(md);sr.append(cu)
        sav.append(sum(sr)/len(sr))
    return cums,mdds,sav
print('='*88);print("'일찍 안 팔기' 가격추세 보유 — paired 100×3 + LOWO + 부분기간");print('='*88)
bc,bm,bsav=paired('baseline')
def med(x):return statistics.median(x)
print(f'\n{"rule":<14}{"avg":>9}{"med":>9}{"MDD중앙":>9}{"MDD최대":>9}{"lift":>9}{"wins":>7}')
print('-'*70)
print(f'{"baseline":<14}{statistics.mean(bc):>+8.1f}%{med(bc):>+8.1f}%{med(bm):>8.1f}%{max(bm):>8.1f}%')
rules=['ma20','ma10','winning','near_high','ma20_win']
res={}
for r in rules:
    cums,mdds,sav=paired(r)
    lifts=[b-a for a,b in zip(bsav,sav)];wins=sum(1 for l in lifts if l>0)
    res[r]=dict(sav=sav,lift=statistics.mean(lifts),wins=wins)
    print(f'{r:<14}{statistics.mean(cums):>+8.1f}%{med(cums):>+8.1f}%{med(mdds):>8.1f}%{max(mdds):>8.1f}%{statistics.mean(lifts):>+8.1f}p{wins:>6}')
# 상위 후보 LOWO + 부분기간
print('\n--- LOWO (MU/SNDK 제외 시 broad winner 잡나) + 부분기간 ---')
cands=sorted([r for r in rules if res[r]['lift']>0 and res[r]['wins']>=60],key=lambda r:-res[r]['lift'])[:3]
print('검증 대상:',cands if cands else '없음 (baseline 못 이김)')
for r in cands:
    line=f'  {r:<12} LOWO['
    for exn,ex in [('전체',()),('-MU',('MU',)),('-SNDK',('SNDK',)),('-둘',('MU','SNDK'))]:
        _,_,b=paired('baseline',ex);_,_,n=paired(r,ex)
        lf=statistics.mean([y-x for x,y in zip(b,n)]);line+=f'{exn}{lf:+.0f} '
    line+='] '
    half=len(elig)//2
    for lbl,lo,hi in [('전반',0,half),('후반',half,len(elig))]:
        sub=[random.Random(s+11).sample(range(lo,hi),min(SAMP,hi-lo)) for s in range(N)]
        bb=[statistics.mean([sim('baseline',(),x)[0] for x in ch]) for ch in sub]
        nn=[statistics.mean([sim(r,(),x)[0] for x in ch]) for ch in sub]
        lf=statistics.mean([y-x for x,y in zip(bb,nn)]);line+=f'{lbl}{lf:+.0f} '
    print(line)
