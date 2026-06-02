# -*- coding: utf-8 -*-
"""B2+TS: 메가 홀드 + 트레일링 스탑
- 메가 홀드: NTM상향≥60 AND PEG<0.2 → rank>10 매도 스킵
- 트레일링 스탑: 모든 보유 종목, 고점 대비 -TS% → 탈출 (메가여도 적용, MDD 방어)
- 탈출 우선순위: TS → min_seg<-2 → rank>10(메가 아니면)
- baseline / TS단독 / B2단독 / B2+TS(8/12/15/20) 비교, MDD분포, Calmar, LOWO
목표: MU 상승 다 먹되(놓침 해결) 반전 시 빠져서 MDD 폭발 방지
"""
import sys, sqlite3, random, statistics
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
ROOT=Path(__file__).parent.parent;DB=ROOT/'eps_momentum_data.db'
N_SEEDS=100;SAMPLES=3;MIN_HOLD=10
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
data={}
for d in dates:
    data[d]={}
    for r in cur.execute('''SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth FROM ntm_screening WHERE date=?''',(d,)):
        tk=r[0];nc,n7,n30,n60,n90=(float(x) if x else 0 for x in r[5:10]);segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
        ntm_rev=(nc/n90-1)*100 if n90 and n90>0 else None
        rg=r[11];fpe=(r[3]/nc) if (r[3] and nc>0) else None;peg=(fpe/(rg*100)) if (fpe and rg and rg>0) else None
        data[d][tk]=dict(p2=r[1],cr=r[2],price=r[3],score=r[4] or 0,min_seg=min(segs) if segs else 0,high30=r[10],ntm_rev=ntm_rev,peg=peg)
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p

def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True
def is_mega(info,nt,pg):
    if info is None or info.get('ntm_rev') is None or info.get('peg') is None: return False
    return info['ntm_rev']>=nt and info['peg']<pg

def sim(override=False, ts=None, nt=60, pg=0.2, exclude=(), start=0):
    # held[tk]=(entry_date, entry_price, weight, peak_price)
    held={};prev=None;val=1.0;peak=1.0;mdd=0.0;rets=[];trades=[]
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,(ed,ep,w,pk) in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);rets.append(ret);peak=max(peak,val);mdd=max(mdd,(peak-val)/peak)
        dd=data[d]
        # peak 업데이트
        for tk in held:
            cp=pf[d].get(tk)
            if cp and cp>held[tk][3]: held[tk]=(held[tk][0],held[tk][1],held[tk][2],cp)
        for tk in list(held):
            info=dd.get(tk);ed,ep,w,pk=held[tk];cp=pf[d].get(tk,ep)
            # 1) 트레일링 스탑 (메가 포함 전 종목)
            if ts is not None and pk>0 and cp/pk-1 <= -ts:
                trades.append(dict(tk=tk,ret=(cp/ep-1)*100,reason='TS'));del held[tk];continue
            # 2) min_seg<-2
            if info is not None and info.get('min_seg') is not None and info['min_seg']<-2:
                sp=info['price'] or ep;trades.append(dict(tk=tk,ret=(sp/ep-1)*100,reason='minseg'));del held[tk];continue
            # 3) rank>10 — 메가 오버라이드면 홀드
            p2=info.get('p2') if info else None
            if info is None or p2 is None or p2>10:
                if override and is_mega(info,nt,pg): continue
                sp=(info.get('price') if info else None) or pf[d].get(tk,ep)
                trades.append(dict(tk=tk,ret=(sp/ep-1)*100,reason='rank'));del held[tk]
        if len(held)<2:
            cands=[]
            for tk,info in dd.items():
                if info['p2'] is None or info['p2']>3: continue
                if tk in held or tk in exclude: continue
                if info.get('min_seg') is not None and info['min_seg']<0: continue
                if not info['price']: continue
                if not verified(tk,i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30']-1<-0.25: continue
                cands.append((info['p2'],info['score'],tk))
            cands.sort(key=lambda x:x[0]);pick=cands[:2-len(held)]
            if len(held)==0 and len(pick)>=2:
                s1,s2=pick[0][1],pick[1][1];w=[1.0,0.0] if (s1-s2)>=15 else [0.5,0.5]
                for si,(_,_,tk) in enumerate(pick[:2]):
                    if w[si]>0: held[tk]=(d,dd[tk]['price'],w[si],dd[tk]['price'])
            else:
                for _,_,tk in pick: held[tk]=(d,dd[tk]['price'],0.5 if len(held)>=1 else 1.0,dd[tk]['price'])
        prev=dict(held)
    return dict(cum=(val-1)*100,mdd=mdd*100,trades=trades)

elig=dates[:-MIN_HOLD];seeds=[]
for s in range(N_SEEDS): random.seed(s);seeds.append(random.sample(range(len(elig)),SAMPLES))
def run(override,ts,exclude=()):
    cums=[];mdds=[];savg=[]
    for ch in seeds:
        sr=[]
        for s in ch:
            r=sim(override=override,ts=ts,exclude=exclude,start=s);cums.append(r['cum']);mdds.append(r['mdd']);sr.append(r['cum'])
        savg.append(sum(sr)/len(sr))
    return cums,mdds,savg

print('='*100)
print('B2+TS: 메가 홀드 + 트레일링 스탑 BT (100×3 paired)')
print('='*100)
_,_,base_savg=run(False,None)
variants=[('baseline',False,None),('TS15만(홀드X)',False,0.15),('B2 홀드(TS X)',True,None),
          ('B2+TS20',True,0.20),('B2+TS15',True,0.15),('B2+TS12',True,0.12),('B2+TS8',True,0.08)]
print(f'\n{"variant":<18}{"avg":>9}{"med":>9}{"MDD중앙":>9}{"MDD최대":>9}{"Calmar":>8}{"lift":>9}{"wins":>8}')
print('-'*88)
for name,ov,ts in variants:
    cums,mdds,savg=run(ov,ts)
    avg=statistics.mean(cums);med=statistics.median(cums);mdm=statistics.median(mdds);mdx=max(mdds)
    cal=avg/mdm if mdm>0 else 0
    lifts=[b-a for a,b in zip(base_savg,savg)];wins=sum(1 for l in lifts if l>0)
    mk=' ★' if name=='baseline' else '  '
    ls='' if name=='baseline' else f'{sum(lifts)/len(lifts):>+7.1f}p{wins:>6}/100'
    print(f'{mk}{name:<16}{avg:>+8.1f}%{med:>+8.1f}%{mdm:>8.1f}%{mdx:>8.1f}%{cal:>8.1f}{ls}')

print('\n--- leave-winner-out: B2+TS15 vs baseline ---')
for exn,ex in [('전체',()),('-MU',('MU',)),('-SNDK',('SNDK',)),('-MU-SNDK',('MU','SNDK'))]:
    _,_,b=run(False,None,exclude=ex);_,_,n=run(True,0.15,exclude=ex)
    lifts=[y-x for x,y in zip(b,n)];wins=sum(1 for l in lifts if l>0)
    print(f'  [{exn:<9}] lift {sum(lifts)/len(lifts):>+7.1f}p ({wins:>3}/100)')

print('\n--- Full BT(start=0): MU/SNDK 거래 추적 ---')
for name,ov,ts in [('baseline',False,None),('B2 홀드',True,None),('B2+TS15',True,0.15),('B2+TS12',True,0.12)]:
    r=sim(override=ov,ts=ts,start=0)
    mu=[t for t in r['trades'] if t['tk']=='MU'];sn=[t for t in r['trades'] if t['tk']=='SNDK']
    cumv=r['cum'];mddv=r['mdd']
    mus=f'{mu[0]["ret"]:+.0f}%({mu[0]["reason"]})' if mu else '홀드중'
    sns=f'{sn[0]["ret"]:+.0f}%({sn[0]["reason"]})' if sn else '홀드중'
    print(f'  [{name:<10}] cum {cumv:+.1f}% MDD {mddv:.1f}%  MU={mus}  SNDK={sns}')
con.close()
