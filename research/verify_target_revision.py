# -*- coding: utf-8 -*-
"""목표가 개정폭(target_chg) 신호 엄격검증 (자율주행 2026-06-13).
에이전트 발견(IC+0.31, LOWO+0.26) 재확인 + 결정적 추가검증:
① 상위후보(part2≤10) 내에서도 IC 유효한가(선택단계 유효성) — 아니면 전략개선 불가(C2/옛목표가 함정)
② 가격모멘텀 교란 — target_chg가 과거수익과 상관 높으면 그냥 추세추종(lagging)
③ 전략 BT: target_chg 진입필터/틸트가 실제 성과 개선하나 (LOWO)."""
import json, sqlite3, statistics as st
from collections import defaultdict
import numpy as np, pandas as pd
from scipy.stats import spearmanr
import sys; sys.stdout.reconfigure(encoding='utf-8')

ud_raw=json.load(open('research/_tmp_ud_data.json'))
ud={}
for rec in ud_raw:
    tk=rec.get('ticker')
    if 'ud' in rec:
        df=pd.DataFrame(rec['ud']);
        if len(df):
            df['GradeDate']=pd.to_datetime(df['GradeDate'])
            ud[tk]=df
px=pd.read_parquet('research/_price_hist_cache.parquet'); px.index=pd.to_datetime(px.index).tz_localize(None)
print(f'ud 종목 {len(ud)}, 가격 종목 {px.shape[1]}')

con=sqlite3.connect('eps_momentum_data.db');cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
p2map=defaultdict(dict)
for d,tk,p2 in cur.execute('SELECT date,ticker,part2_rank FROM ntm_screening WHERE part2_rank IS NOT NULL'):
    p2map[d][tk]=p2
con.close()

def tgt_chg(tk,dt,lb=60):
    if tk not in ud: return None
    df=ud[tk]; dd=pd.Timestamp(dt)
    w=df[(df.GradeDate<=dd)&(df.GradeDate>dd-pd.Timedelta(days=lb))&(df.priorPriceTarget>0)&(df.currentPriceTarget>0)]
    if len(w)==0: return None
    return float((w.currentPriceTarget/w.priorPriceTarget-1).mean())
def ret(tk,dt,k):
    if tk not in px.columns: return None
    s=px[tk].dropna();dd=pd.Timestamp(dt)
    fut=s[s.index>dd]; cur_=s[s.index<=dd]
    if k>0: return (fut.iloc[k-1]/cur_.iloc[-1]-1)*100 if len(fut)>=k and len(cur_) else None
    past=s[s.index<=dd]
    return (past.iloc[-1]/past.iloc[len(past)+k-1]-1)*100 if len(past)>=-k+1 else None

# 신호일: 2주 간격
sig_dates=dates[::10]
broad=defaultdict(list); top=defaultdict(list); confound=[]
for d in sig_dates:
    for tk in ud:
        sig=tgt_chg(tk,d,60); f20=ret(tk,d,20)
        if sig is None or f20 is None: continue
        broad[d].append((sig,f20,tk))
        past60=ret(tk,d,-60)
        if past60 is not None: confound.append((sig,past60))
        p2=p2map.get(d,{}).get(tk)
        if p2 is not None and p2<=10: top[d].append((sig,f20,tk))
def avg_ic(bucket):
    ics=[]
    for d,v in bucket.items():
        if len(v)>=6:
            ic=spearmanr([x[0] for x in v],[x[1] for x in v])[0]
            if not np.isnan(ic): ics.append(ic)
    return (np.mean(ics) if ics else float('nan')), len(ics), sum(len(v) for v in bucket.values())
ib,nb,tb=avg_ic(broad); it,nt,tt=avg_ic(top)
print(f'\n① IC 비교 (forward 20일):')
print(f'  넓은 유니버스: IC {ib:+.3f} (날짜{nb}, 관측{tb})')
print(f'  상위후보(part2≤10): IC {it:+.3f} (날짜{nt}, 관측{tt})  ← 선택단계 유효성')
print(f'\n② 가격모멘텀 교란: corr(target_chg, 과거60일수익) = {spearmanr([c[0] for c in confound],[c[1] for c in confound])[0]:+.3f}')
print(f'   (높으면 = 이미 오른 종목 목표가 올린 것 = lagging 추세추종)')

# ③ 전략 BT: target_chg 진입필터 (목표가 상향중인 것만 매수)
con=sqlite3.connect('eps_momentum_data.db');cur=con.cursor()
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
import random
def sim(tgt_mode=None,exclude=(),start=0):
    held=set();prev=None;val=1.0;peak=1.0;mdd=0
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];r=0
            for tk in prev:
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: r+=(1.0/len(prev))*(pn/pp-1)
            val*=(1+r);peak=max(peak,val);mdd=min(mdd,(val/peak-1)*100)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk)
            if info and info.get('min_seg',0)<-2: held.discard(tk);continue
            if info is None: continue
            p2=info.get('p2')
            if not (p2 is None or p2>10): continue
            _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
            if _pe>=15: held.discard(tk)
        if len(held)<2:
            cands=[]
            for tk,info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg',0)<0 or not info['price'] or not verified(tk,i): continue
                if info.get('high30') and info['price']/info['high30']-1<-0.25: continue
                if (info.get('dv') or 0)<1000: continue
                p2=info.get('p2')
                if p2 is None or p2>5: continue
                tc=tgt_chg(tk,d,60)
                if tgt_mode=='filter' and (tc is None or tc<=0): continue  # 목표가 상향중만
                key=p2 - (1.0 if (tgt_mode=='tilt' and tc and tc>0.03) else 0)
                cands.append((key,p2,tk))
            cands.sort()
            for _,_,tk in cands[:2-len(held)]: held.add(tk)
        prev=set(held)
    return (val-1)*100,mdd
elig=list(range(2,len(dates)-10));seeds=[random.Random(s).sample(elig,3) for s in range(60)]
def run(**kw):
    return [sim(start=s,**kw)[0] for ch in seeds for s in ch]
print('\n③ 전략 BT (target_chg 진입필터/틸트):')
print(f'{"설정":<18}{"전기간":>9}{"paired":>9}{"LOWO":>9}')
WIN={'MU','SNDK','STX','LITE','NVDA','TER','BE'}
for lbl,kw in [('baseline',{}),('목표가상향 필터',{'tgt_mode':'filter'}),('목표가 틸트',{'tgt_mode':'tilt'})]:
    c,_=sim(**kw);b=st.mean(run(**kw));worst=min([b]+[st.mean(run(exclude=(w,),**kw)) for w in WIN])
    print(f'{lbl:<18}{c:>+8.1f}%{b:>+8.1f}%{worst:>+8.1f}%',flush=True)
print('\n해석: 상위후보 IC가 0에 가깝거나 BT개선 없으면 = IC는 있어도 전략엔 무용(saturation/이미 상위가 비슷).')
