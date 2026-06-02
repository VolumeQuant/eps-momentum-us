# -*- coding: utf-8 -*-
"""자율 Stage2: 밸류/성장/PEG 진입필터 paired BT + leave-winner-out
사용자 시스템 목적: 압도적 성장 + 저평가 종목 매수.
EDA 결과: rev_growth corr +0.41, fwd_pe corr -0.25. winner=PEG 0.1, loser=PEG ~1.0.

필터 (진입 게이트, 나머지 v84 룰 동일):
  pe<=N      : forward PE = price/ntm_current <= N
  rg>=X      : rev_growth >= X
  peg<=Y     : PEG = fwd_pe/(rev_growth*100) <= Y  (rev_growth<=0이면 탈락)
  조합       : pe<=30 & rg>=0.30  등
데이터 부족(ntm/rg 없음)시 = 보수적 통과(pass-through) 아님, 정보없으면 탈락(엄격).
  -> 단 baseline none은 전부 통과. 비교 위해 필터별로만 적용.
"""
import sys, sqlite3, random, statistics, math
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB = ROOT / 'eps_momentum_data.db'
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10

def load():
    con = sqlite3.connect(DB); cur = con.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
    data = {}
    for d in dates:
        data[d] = {}
        for r in cur.execute('''SELECT ticker,part2_rank,composite_rank,price,score,
                ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth
                FROM ntm_screening WHERE date=?''', (d,)):
            tk = r[0]
            nc,n7,n30,n60,n90 = (float(x) if x else 0 for x in r[5:10])
            segs=[]
            for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
                segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
            fwd_pe = (r[3]/nc) if (r[3] and nc>0) else None
            rg = r[11]
            peg = (fwd_pe/(rg*100)) if (fwd_pe and rg and rg>0) else None
            data[d][tk] = dict(p2=r[1],cr=r[2],price=r[3],score=r[4] or 0,
                min_seg=min(segs) if segs else 0, high30=r[10],
                fwd_pe=fwd_pe, rg=rg, peg=peg)
    all_dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE price IS NOT NULL ORDER BY date')]
    pf=defaultdict(dict)
    for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
        pf[d][tk]=p
    con.close()
    return dates,data,pf

def passes(info, filt):
    if filt=='none': return True
    pe=info['fwd_pe']; rg=info['rg']; peg=info['peg']
    if filt=='pe<=30&rg>=0.30':
        return pe is not None and pe<=30 and rg is not None and rg>=0.30
    if filt=='pe<=35&rg>=0.25':
        return pe is not None and pe<=35 and rg is not None and rg>=0.25
    if filt=='peg<=1.0&rg>=0.30':
        return peg is not None and peg<=1.0 and rg is not None and rg>=0.30
    if filt.startswith('pe<='):
        return pe is not None and pe <= float(filt[4:])
    if filt.startswith('rg>='):
        return rg is not None and rg >= float(filt[4:])
    if filt.startswith('peg<='):
        return peg is not None and peg <= float(filt[5:])
    return True

def verified(t,i,dates,data):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True

def sim(dates,data,pf,filt='none',slots=2,entry=3,exit_=10,start=0,excl=()):
    held={}; prev=None; val=1.0; peak=1.0; mdd=0.0; rets=[]; trades=[]
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1]; ret=0
            for tk,(ed,ep,si,w) in prev.items():
                pp=pf[dp].get(tk); pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret); rets.append(ret); peak=max(peak,val); mdd=max(mdd,(peak-val)/peak)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk); ep=held[tk][1]; p2=info.get('p2') if info else None
            if info is None or p2 is None or p2>exit_:
                sp=(info.get('price') if info else None) or pf[d].get(tk,ep)
                trades.append(dict(tk=tk,ret=(sp/ep-1)*100)); del held[tk]
            elif info.get('min_seg') is not None and info['min_seg']<-2:
                sp=info['price'] or ep; trades.append(dict(tk=tk,ret=(sp/ep-1)*100)); del held[tk]
        if len(held)<slots:
            cands=[]
            for tk,info in dd.items():
                if info['p2'] is None or info['p2']>entry: continue
                if tk in held or tk in excl: continue
                if info.get('min_seg') is not None and info['min_seg']<0: continue
                if not info['price']: continue
                if not verified(tk,i,dates,data): continue
                if info.get('high30') and info['price'] and info['price']/info['high30']-1<-0.25: continue
                if not passes(info,filt): continue
                cands.append((info['p2'],info['score'],tk))
            cands.sort(key=lambda x:x[0]); picked=cands[:slots]
            if len(picked)==1:
                _,_,tk=picked[0]; held[tk]=(d,dd[tk]['price'],0,1.0)
            elif len(picked)>=2:
                s1,s2=picked[0][1],picked[1][1]
                w=[1.0,0.0] if (s1-s2)>=15 else [0.5,0.5]
                for si,(_,_,tk) in enumerate(picked[:2]):
                    if w[si]>0: held[tk]=(d,dd[tk]['price'],si,w[si])
        prev=dict(held)
    return dict(cum=(val-1)*100,mdd=mdd*100,trades=trades,rets=rets)

def paired_avgs(dates,data,pf,filt,seed_starts,excl=()):
    avgs=[]
    for chosen in seed_starts:
        sr=[sim(dates,data,pf,filt=filt,start=s,excl=excl)['cum'] for s in chosen]
        avgs.append(sum(sr)/len(sr))
    return avgs

def main():
    dates,data,pf=load()
    print('='*100); print('Stage2: 밸류/성장/PEG 진입필터 paired BT (v84 sim, 100×3)'); print('='*100)
    print(f'dates {len(dates)} ({dates[0]}~{dates[-1]})')
    elig=dates[:-MIN_HOLD]; seed_starts=[]
    for s in range(N_SEEDS):
        random.seed(s); seed_starts.append(random.sample(range(len(elig)),SAMPLES))
    variants=['none','pe<=30','pe<=25','pe<=20','rg>=0.30','rg>=0.40','rg>=0.50',
              'peg<=1.0','peg<=0.8','pe<=30&rg>=0.30','pe<=35&rg>=0.25','peg<=1.0&rg>=0.30']
    print(f'\n{"filter":<20}{"avg":>9}{"med":>9}{"mdd":>9}{"sharpe":>8}{"cal":>7}{"lift":>9}{"wins":>9}')
    print('-'*82)
    base=paired_avgs(dates,data,pf,'none',seed_starts)
    results={}
    for filt in variants:
        avgs=paired_avgs(dates,data,pf,filt,seed_starts)
        # full stats from all runs
        allr=[]; allm=[]
        for chosen in seed_starts:
            for s in chosen:
                r=sim(dates,data,pf,filt=filt,start=s); allr.append(r['cum']); allm.append(r['mdd'])
        avg=sum(allr)/len(allr); med=sorted(allr)[len(allr)//2]; mdd=max(allm)
        std=statistics.pstdev(allr); sh=avg/std if std>0 else 0; cal=avg/mdd if mdd>0 else 0
        lifts=[b-a for a,b in zip(base,avgs)]; wins=sum(1 for l in lifts if l>0)
        results[filt]=dict(avgs=avgs,lift=sum(lifts)/len(lifts),wins=wins)
        mk=' ★' if filt=='none' else '  '
        liftstr='' if filt=='none' else f'{sum(lifts)/len(lifts):>+7.1f}p{wins:>6}/100'
        print(f'{mk}{filt:<18}{avg:>+8.1f}%{med:>+8.1f}%{mdd:>+8.1f}%{sh:>8.2f}{cal:>7.2f}{liftstr}')

    # 상위 후보 leave-winner-out
    print('\n--- leave-winner-out (MU/SNDK 제외 시 edge 유지?) ---')
    # pick top by wins among filters that changed something
    cand=[f for f in variants if f!='none' and results[f]['wins']>=60]
    cand=sorted(cand,key=lambda f:-results[f]['lift'])[:4]
    print('검증 대상:', cand)
    for filt in cand:
        line=f'  {filt:<18} '
        for ex_name,ex in [('전체',()),('-MU',('MU',)),('-SNDK',('SNDK',)),('-MU-SNDK',('MU','SNDK'))]:
            b=paired_avgs(dates,data,pf,'none',seed_starts,excl=ex)
            n=paired_avgs(dates,data,pf,filt,seed_starts,excl=ex)
            lifts=[y-x for x,y in zip(b,n)]; wins=sum(1 for l in lifts if l>0)
            line+=f'{ex_name}:{sum(lifts)/len(lifts):>+6.1f}p({wins:>3})  '
        print(line)

    # full-74 trade 비교 + 물린종목 차단 확인
    print('\n--- Full BT(start=0) + KEYS/AEIS/WMG 매수차단 확인 ---')
    for filt in ['none','pe<=30','peg<=1.0&rg>=0.30','rg>=0.40','pe<=30&rg>=0.30']:
        r=sim(dates,data,pf,filt=filt,start=0)
        bought=defaultdict(list)
        for t in r['trades']: bought[t['tk']].append(round(t['ret'],1))
        wins=sum(1 for t in r['trades'] if t['ret']>0); n=len(r['trades'])
        tgt={k:bought.get(k,[]) for k in ['KEYS','AEIS','WMG','MU','SNDK']}
        print(f'  [{filt:<18}] cum{r["cum"]:>+7.1f}% MDD{r["mdd"]:>5.1f}% 거래{n:>2} 승{wins}/{n}  MU={tgt["MU"]} SNDK={tgt["SNDK"]} | KEYS={tgt["KEYS"]} AEIS={tgt["AEIS"]} WMG={tgt["WMG"]}')

if __name__=='__main__':
    main()
