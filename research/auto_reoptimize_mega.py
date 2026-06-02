# -*- coding: utf-8 -*-
"""메가홀드 ON 상태에서 진입/이탈/슬롯 재최적화 (v86 기준 최적 전략 탐색)
⚠️ 75일·2종목 과적합 위험 → LOWO + 부분기간 통과한 것만 채택. 안되면 v86 그대로.
그리드: slots {2,3} × exit_rank {8,10,12}, 전부 메가홀드 ON.
robustness gate: paired wins ≥70 + LOWO(MU/SNDK제외) 비음수 + 부분기간 둘다 비음수.
"""
import sys, sqlite3, random, statistics
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'
N=100;SAMP=3;MINH=10
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
data={}
for d in dates:
    data[d]={}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth FROM ntm_screening WHERE date=?',(d,)):
        tk=r[0];nc,n7,n30,n60,n90=(float(x) if x else 0 for x in r[5:10]);segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
        ntm=(nc/n90-1)*100 if n90 and n90>0 else None
        rg=r[11];fpe=(r[3]/nc) if (r[3] and nc>0) else None;peg=(fpe/(rg*100)) if (fpe and rg and rg>0) else None
        data[d][tk]=dict(p2=r[1],cr=r[2],price=r[3],score=r[4] or 0,min_seg=min(segs) if segs else 0,high30=r[10],ntm=ntm,peg=peg)
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p
con.close()
def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True
def ismega(info):
    if info is None or info.get('ntm') is None or info.get('peg') is None: return False
    return info['ntm']>=60 and info['peg']<0.2
def sim(slots,exitr,mega,excl=(),start=0):
    held={};prev=None;val=1.0;peak=1.0;mdd=0.0
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,(ep,w) in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);peak=max(peak,val);mdd=max(mdd,(peak-val)/peak)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk);ep=held[tk][0]
            if info is not None and info.get('min_seg') is not None and info['min_seg']<-2:
                del held[tk];continue
            p2=info.get('p2') if info else None
            if info is None or p2 is None or p2>exitr:
                if mega and ismega(info): continue
                del held[tk]
        if len(held)<slots:
            c=[]
            for tk,info in dd.items():
                if info['p2'] is None or info['p2']>slots+1 or tk in held or tk in excl: continue
                if info.get('min_seg') is not None and info['min_seg']<0: continue
                if not info['price'] or not verified(tk,i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30']-1<-0.25: continue
                c.append((info['p2'],info['score'],tk))
            c.sort();pk=c[:slots-len(held)]
            hasmega=any(ismega(dd.get(tk)) for tk in held)
            if len(held)==0 and len(pk)>=2 and slots==2 and not hasmega:
                w=[1.0,0.0] if (pk[0][1]-pk[1][1])>=15 else [0.5,0.5]
                for si,(_,_,tk) in enumerate(pk[:2]):
                    if w[si]>0: held[tk]=(dd[tk]['price'],w[si])
            else:
                tot=len(held)+len(pk)
                eqw=1.0/min(tot,slots) if tot>0 else 0
                for _,_,tk in pk: held[tk]=(dd[tk]['price'],eqw)
                # 기존 보유 가중치도 균등 재조정
                for tk in held: held[tk]=(held[tk][0],eqw)
        prev=dict(held)
    return (val-1)*100,mdd*100
elig=list(range(len(dates)-MINH));seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def paired(slots,exitr,mega,excl=()):
    cums=[];mdds=[];sav=[]
    for ch in seeds:
        sr=[]
        for s in ch:
            cu,md=sim(slots,exitr,mega,excl,s);cums.append(cu);mdds.append(md);sr.append(cu)
        sav.append(sum(sr)/len(sr))
    return cums,mdds,sav
# 기준 = v86 (slot2/exit10/mega ON)
bc,bm,bsav=paired(2,10,True)
print('='*90);print('메가홀드 ON 재최적화 그리드 (기준 v86: slot2/exit10/mega)');print('='*90)
def med(x):return statistics.median(x)
print(f'{"config":<22}{"avg":>9}{"MDD중앙":>9}{"MDD최대":>9}{"Calmar":>8}{"lift":>9}{"wins":>7}')
print('-'*82)
cfgs=[(2,10,'v86기준'),(2,8,''),(2,12,''),(3,10,''),(3,8,''),(3,12,'')]
res={}
for slots,exitr,tag in cfgs:
    cums,mdds,sav=paired(slots,exitr,True)
    avg=statistics.mean(cums);mm=med(mdds);mx=max(mdds);cal=avg/mm if mm>0 else 0
    lifts=[b-a for a,b in zip(bsav,sav)];wins=sum(1 for l in lifts if l>0)
    res[(slots,exitr)]=dict(sav=sav,wins=wins,lift=statistics.mean(lifts))
    name=f's{slots}/exit{exitr}'+(f' ★{tag}' if tag else '')
    ls='' if (slots,exitr)==(2,10) else f'{statistics.mean(lifts):>+7.1f}p{wins:>6}'
    print(f'{name:<22}{avg:>+8.1f}%{mm:>8.1f}%{mx:>8.1f}%{cal:>8.1f}{ls}')
# 견고성 게이트: lift>0 & wins>=70 인 config만 LOWO+부분기간
print('\n--- 견고성 게이트 (lift>0 & wins≥70 config만 LOWO + 부분기간) ---')
cands=[(s,e) for (s,e),v in res.items() if (s,e)!=(2,10) and v['lift']>0 and v['wins']>=70]
if not cands:
    print('  ✗ v86(slot2/exit10) 대비 robust하게 우월한 config 없음 → v86 그대로가 최적')
else:
    for slots,exitr in cands:
        line=f'  s{slots}/exit{exitr}  LOWO['
        ok=True
        for exn,ex in [('전체',()),('-MU',('MU',)),('-SNDK',('SNDK',)),('-둘',('MU','SNDK'))]:
            _,_,b=paired(2,10,True,ex);_,_,n=paired(slots,exitr,True,ex)
            lf=statistics.mean([y-x for x,y in zip(b,n)]);line+=f'{exn}{lf:+.0f} '
            if lf<-2: ok=False
        line+='] '
        # 부분기간
        half=len(elig)//2
        for lbl,lo,hi in [('전반',0,half),('후반',half,len(elig))]:
            sub=[random.Random(s+999).sample(range(lo,hi),min(SAMP,hi-lo)) for s in range(N)]
            bb=[statistics.mean([sim(2,10,True,(),x)[0] for x in ch]) for ch in sub]
            nn=[statistics.mean([sim(slots,exitr,True,(),x)[0] for x in ch]) for ch in sub]
            lf=statistics.mean([y-x for x,y in zip(bb,nn)]);line+=f'{lbl}{lf:+.0f} '
            if lf<-2: ok=False
        line+=f' → {"✅ 채택가능" if ok else "✗ 견고성실패"}'
        print(line)
