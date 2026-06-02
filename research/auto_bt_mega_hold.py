# -*- coding: utf-8 -*-
"""B2: 메가 홀드 오버라이드 BT
가설: 보유 종목이 메가 시그니처(NTM EPS상향≥thr AND PEG<pthr) 유지 시
      part2_rank>10(회전 매도)여도 안 팔고 홀드 → MU/SNDK를 더 오래 먹는다.
- min_seg<-2(어닝 악화) 매도는 유지 (시그니처 페이드 안전장치)
- 슬롯 점유 비용(메가 홀드하면 신규 진입 막힘) 그대로 반영
- baseline(v85 slot2) vs 오버라이드 여러 임계, paired 100×3, LOWO, MDD, 홀드연장
⚠️ 메모리 경고: 어닝 후 회전 자체가 알파 채널일 수 있음(과거 NTM lookback 정정 0/6 패). 검증 후 판단.
"""
import sys, sqlite3, random, statistics
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
ROOT=Path(__file__).parent.parent
DB=ROOT/'eps_momentum_data.db'
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
        rg=r[11]; fpe=(r[3]/nc) if (r[3] and nc>0) else None
        peg=(fpe/(rg*100)) if (fpe and rg and rg>0) else None
        data[d][tk]=dict(p2=r[1],cr=r[2],price=r[3],score=r[4] or 0,min_seg=min(segs) if segs else 0,
                         high30=r[10],ntm_rev=ntm_rev,peg=peg)
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p

def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True

def is_mega(info, ntm_thr, peg_thr):
    if info is None: return False
    if info.get('ntm_rev') is None or info.get('peg') is None: return False
    return info['ntm_rev']>=ntm_thr and info['peg']<peg_thr

def sim(ntm_thr=None, peg_thr=None, exclude=(), start=0):
    """ntm_thr=None → baseline(오버라이드 없음)"""
    held={};prev=None;val=1.0;peak=1.0;mdd=0.0;rets=[];trades=[];holds=[]
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
            # min_seg<-2 (어닝 악화) = 항상 매도
            if info is not None and info.get('min_seg') is not None and info['min_seg']<-2:
                sp=info['price'] or ep;trades.append(dict(tk=tk,ret=(sp/ep-1)*100,bd=held[tk][0],sd=d,reason='minseg'));holds.append((tk,(dates.index(d)-dates.index(held[tk][0])) if held[tk][0] in dates else 0));del held[tk];continue
            # rank>10 또는 데이터없음 = 매도 후보 — 단 메가 오버라이드면 홀드
            if info is None or p2 is None or p2>10:
                if ntm_thr is not None and is_mega(info, ntm_thr, peg_thr):
                    continue  # 메가 홀드: 매도 스킵
                sp=(info.get('price') if info else None) or pf[d].get(tk,ep)
                trades.append(dict(tk=tk,ret=(sp/ep-1)*100,bd=held[tk][0],sd=d,reason='rank'))
                del held[tk]
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
                    if w[si]>0: held[tk]=(d,dd[tk]['price'],w[si])
            else:
                for _,_,tk in pick: held[tk]=(d,dd[tk]['price'],0.5 if len(held)>=1 else 1.0)
        prev=dict(held)
    return dict(cum=(val-1)*100,mdd=mdd*100,trades=trades,rets=rets)

elig=dates[:-MIN_HOLD];seeds=[]
for s in range(N_SEEDS): random.seed(s);seeds.append(random.sample(range(len(elig)),SAMPLES))
def run(ntm,peg,exclude=()):
    cums=[];mdds=[];savg=[]
    for ch in seeds:
        sr=[]
        for s in ch:
            r=sim(ntm,peg,exclude=exclude,start=s);cums.append(r['cum']);mdds.append(r['mdd']);sr.append(r['cum'])
        savg.append(sum(sr)/len(sr))
    return cums,mdds,savg

print('='*100)
print('B2: 메가 홀드 오버라이드 BT (NTM상향≥thr AND PEG<pthr → 순위이탈 매도 스킵)')
print('='*100)
def pct(xs,p): xs=sorted(xs);return xs[min(len(xs)-1,int(len(xs)*p))]
variants=[('baseline',None,None),('NTM60_PEG0.2',60,0.2),('NTM60_PEG0.3',60,0.3),
          ('NTM80_PEG0.2',80,0.2),('NTM100_PEG0.2',100,0.2),('NTM60_only',60,99)]
_,_,base_savg=run(None,None)
print(f'\n{"variant":<18}{"avg":>9}{"med":>9}{"MDD중앙":>9}{"MDD최대":>9}{"lift":>9}{"wins":>8}')
print('-'*80)
for name,nt,pg in variants:
    cums,mdds,savg=run(nt,pg)
    avg=statistics.mean(cums);med=statistics.median(cums);mdd_med=statistics.median(mdds);mdd_max=max(mdds)
    lifts=[b-a for a,b in zip(base_savg,savg)];wins=sum(1 for l in lifts if l>0)
    mk=' ★' if name=='baseline' else '  '
    ls='' if name=='baseline' else f'{sum(lifts)/len(lifts):>+7.1f}p{wins:>6}/100'
    print(f'{mk}{name:<16}{avg:>+8.1f}%{med:>+8.1f}%{mdd_med:>8.1f}%{mdd_max:>8.1f}%{ls}')

# LOWO for NTM60_PEG0.2
print('\n--- leave-winner-out: NTM60_PEG0.2 vs baseline ---')
for exn,ex in [('전체',()),('-MU',('MU',)),('-SNDK',('SNDK',)),('-MU-SNDK',('MU','SNDK'))]:
    _,_,b=run(None,None,exclude=ex);_,_,n=run(60,0.2,exclude=ex)
    lifts=[y-x for x,y in zip(b,n)];wins=sum(1 for l in lifts if l>0)
    print(f'  [{exn:<9}] lift {sum(lifts)/len(lifts):>+7.1f}p ({wins:>3}/100)')

# full BT: 어떤 trade가 홀드 연장됐나
print('\n--- Full BT(start=0): baseline vs NTM60_PEG0.2 거래 비교 ---')
for name,nt,pg in [('baseline',None,None),('NTM60_PEG0.2',60,0.2)]:
    r=sim(nt,pg,start=0)
    byt=defaultdict(lambda:[0,0.0])
    for t in r['trades']: byt[t['tk']][0]+=1;byt[t['tk']][1]+=t['ret']
    mu=byt.get('MU'); sndk=byt.get('SNDK')
    cumv=r['cum']; mddv=r['mdd']; ntr=len(r['trades'])
    print(f'  [{name:<14}] cum {cumv:+.1f}% MDD {mddv:.1f}% 거래{ntr}건  MU={mu}  SNDK={sndk}')
con.close()
