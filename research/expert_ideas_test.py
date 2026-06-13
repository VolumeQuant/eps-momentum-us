# -*- coding: utf-8 -*-
"""전문가 3인 아이디어 검증 (자율주행 2026-06-13).
A. Novy-Marx 품질(gross/operating margin·ROE) 틸트·필터 — IC + BT.
B. Ilmanen/AQR 변동성 타겟팅 오버레이 — MDD/Calmar 개선?
C. Lopez de Prado Deflated Sharpe — 시행횟수 보정 reality check."""
import sqlite3, random, statistics as st, math
from collections import defaultdict
import numpy as np
from scipy.stats import spearmanr
import sys; sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'; N=100; SAMP=3; MINH=10
WINNERS={'MU','SNDK','STX','LITE','TTMI','NVDA','TER','BE'}
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
didx={d:i for i,d in enumerate(dates)}
data={}
for d in dates:
    data[d]={}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,dollar_volume_30d,operating_margin,gross_margin,roe FROM ntm_screening WHERE date=?',(d,)):
        tk=r[0];nc,n7,n30,n60,n90=(float(x) if x else 0 for x in r[4:9]);segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
        data[d][tk]=dict(p2=r[1],cr=r[2],price=r[3],min_seg=min(segs),high30=r[9],dv=r[10],ntm=nc,
                         opm=r[11],gpm=r[12],roe=r[13])
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p
con.close()
def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True
def fwd(tk,d,k=20):
    i=didx.get(d)
    if i is None or i+k>=len(dates): return None
    p0=pf[d].get(tk); p1=pf[dates[i+k]].get(tk)
    return (p1/p0-1)*100 if (p0 and p1) else None

# === A. 품질 IC ===
print('=== A. 품질 팩터 예측력 (랭킹유니버스, 전진20일 IC) ===')
for qn,qk in [('영업마진','opm'),('매출총이익률','gpm'),('ROE','roe')]:
    by_d=defaultdict(list)
    for d in dates:
        for tk,info in data[d].items():
            if info['p2'] is None or info.get(qk) is None: continue
            f=fwd(tk,d,20)
            if f is not None: by_d[d].append((info[qk],f))
    ics=[spearmanr([x[0] for x in v],[x[1] for x in v])[0] for v in by_d.values() if len(v)>=8]
    ics=[x for x in ics if not np.isnan(x)]
    print(f'  {qn:<10} 평균IC {np.mean(ics):+.3f} (양수=고품질→고수익)')

# === sim: 품질필터/틸트 옵션 + 일별수익 반환 ===
def sim(qfilter=None,qtilt=False,exclude=(),start=0,ret_series=False):
    held=set();prev=None;val=1.0;peak=1.0;mdd=0;rets=[]
    for i in range(start,len(dates)):
        d=dates[i];r_t=0.0
        if prev and i>start:
            dp=dates[i-1]
            for tk in prev:
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: r_t+=(1.0/len(prev))*(pn/pp-1)
            val*=(1+r_t);peak=max(peak,val);mdd=min(mdd,(val/peak-1)*100)
        rets.append(r_t)
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
                if qfilter is not None and (info.get('opm') is None or info['opm']<qfilter): continue  # 품질 필터
                key=p2 - (0.5 if (qtilt and info.get('opm') and info['opm']>0.2) else 0)  # 고마진 우대 틸트
                cands.append((key,tk))
            cands.sort()
            for _,tk in cands[:2-len(held)]: held.add(tk)
        prev=set(held)
    if ret_series: return rets
    return (val-1)*100,mdd
elig=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def run(**kw):
    cs=[]
    for ch in seeds:
        for s in ch: cs.append(sim(start=s,**kw)[0])
    return cs
def lowo(**kw):
    base=st.mean(run(**kw));worst=base
    for w in WINNERS:
        kw2=dict(kw);kw2['exclude']=(w,);worst=min(worst,st.mean(run(**kw2)))
    return base,worst
print('\n=== A. 품질 필터/틸트 BT ===')
print(f'{"설정":<22}{"전기간":>9}{"전MDD":>8}{"paired":>9}{"LOWO":>9}')
for lbl,kw in [('baseline',{}),('영업마진>10% 필터',{'qfilter':0.10}),('영업마진>20% 필터',{'qfilter':0.20}),('고마진 틸트',{'qtilt':True})]:
    c,m=sim(**kw);b,lw=lowo(**kw)
    print(f'{lbl:<22}{c:>+8.1f}%{m:>+7.1f}%{b:>+8.1f}%{lw:>+8.1f}%',flush=True)

# === B. 변동성 타겟팅 오버레이 ===
print('\n=== B. 변동성 타겟팅 오버레이 (deterministic 경로) ===')
rets=np.array(sim(ret_series=True))
def metrics(r):
    eq=np.cumprod(1+r);tot=(eq[-1]-1)*100
    mdd=((eq/np.maximum.accumulate(eq)-1).min())*100
    sd=r.std()*math.sqrt(252);sh=(r.mean()*252)/sd if sd>0 else 0
    return tot,mdd,sh
tot0,mdd0,sh0=metrics(rets)
print(f'  {"무조정(현행)":<24} 총{tot0:>+7.0f}% MDD{mdd0:>+6.1f}% Sharpe{sh0:>5.2f}')
realized=np.array([rets[max(0,i-20):i].std()*math.sqrt(252) if i>=5 else 0.3 for i in range(len(rets))])
tgt=np.median(realized[realized>0])  # 타겟=중앙 변동성
for cap in [1.0,1.5,2.0]:
    expo=np.clip(tgt/np.where(realized>0,realized,tgt),0,cap)
    expo=np.roll(expo,1);expo[0]=1.0  # lookahead 방지(어제 vol로 오늘 노출)
    vr=rets*expo
    tot,mdd,sh=metrics(vr)
    print(f'  변동성타겟 cap{cap}        총{tot:>+7.0f}% MDD{mdd:>+6.1f}% Sharpe{sh:>5.2f}',flush=True)
print(f'  (타겟변동성={tgt*100:.0f}%)')

# === C. Deflated Sharpe (시행횟수 보정) ===
print('\n=== C. Deflated Sharpe (Lopez de Prado, 과적합 reality check) ===')
sr=sh0/math.sqrt(252)  # 일간 Sharpe
n=len(rets)
from scipy.stats import norm, skew, kurtosis
sk=skew(rets);ku=kurtosis(rets,fisher=False)
for n_trials in [1,20,50,100]:
    # 기대 최대 SR (n_trials개 중) under null — Bailey&LdP 근사
    e_max=(0.5772/math.sqrt(n_trials) if False else 0)  # placeholder
    z=norm.ppf(1-1.0/n_trials) if n_trials>1 else 0
    sr0=z/math.sqrt(n)  # null하 시행 중 기대 최대 일간SR
    # DSR: 관측 SR이 sr0보다 유의한가
    se=math.sqrt((1 - sk*sr + (ku-1)/4*sr**2)/(n-1))
    dsr=norm.cdf((sr-sr0)/se) if se>0 else 0
    print(f'  시행 {n_trials:>3}회 가정 → DSR(진짜일 확률) {dsr*100:>5.1f}%  (일간SR관측 {sr:.3f}, null기대 {sr0:.3f})')
print('해석: DSR이 95%+면 통계적으로 진짜. 시행 많을수록 문턱↑. 짧은 표본(84일)이라 보수적으로 봐야.')
