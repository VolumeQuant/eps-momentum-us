# -*- coding: utf-8 -*-
"""시스템 종합 성과·매매내역·잔고·위험지표 (보고용). v119 충실 replay(누적 +229.7% 일치 검증)."""
import sqlite3, math
from collections import defaultdict
import numpy as np, pandas as pd
import sys; sys.stdout.reconfigure(encoding='utf-8')
con=sqlite3.connect('eps_momentum_data.db');cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
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
held={};entry={};rets=[];trades=[];eqdates=[]
for i in range(len(dates)):
    d=dates[i]
    if i>0:
        dp=dates[i-1];r=0
        for tk in held:
            pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
            if pp and pn: r+=(1.0/len(held))*(pn/pp-1)
        rets.append(r);eqdates.append(d)
    dd=data[d]
    for tk in list(held):
        info=dd.get(tk);reason=None
        if info and info.get('min_seg',0)<-2: reason='EPS꺾임'
        elif info is not None:
            p2=info.get('p2')
            if not (p2 is None or p2>10): reason=None
            else:
                _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
                reason=f'순위이탈+고PER({_pe:.0f})' if _pe>=15 else None
        if reason:
            ep=entry[tk];xp=pf[d].get(tk,ep[1])
            trades.append((ep[0],d,tk,ep[1],xp,(xp/ep[1]-1)*100,dates.index(d)-dates.index(ep[0]),reason))
            del held[tk];del entry[tk]
    if len(held)<2:
        cands=[]
        for tk,info in dd.items():
            if tk in held: continue
            if info.get('min_seg',0)<0 or not info['price'] or not verified(tk,i): continue
            if info.get('high30') and info['price']/info['high30']-1<-0.25: continue
            if (info.get('dv') or 0)<1000: continue
            p2=info.get('p2')
            if p2 is None or p2>5: continue
            cands.append((p2,tk))
        cands.sort()
        for _,tk in cands[:2-len(held)]:
            held[tk]=1;entry[tk]=(d,dd[tk]['price'])
rets=np.array(rets)
# SPY 벤치마크
import yfinance as yf
spy=yf.download('SPY',start=dates[0],end='2026-06-13',auto_adjust=True,progress=False)['Close']
spy=spy.iloc[:,0] if hasattr(spy,'columns') else spy
spy_r=spy.pct_change().dropna().values

def metrics(r,n_days_actual):
    eq=np.cumprod(1+r);tot=(eq[-1]-1)*100
    yrs=n_days_actual/252
    cagr=((eq[-1])**(1/yrs)-1)*100
    vol=r.std()*math.sqrt(252)*100
    sh=(r.mean()*252)/(r.std()*math.sqrt(252)) if r.std()>0 else 0
    dn=r[r<0];sor=(r.mean()*252)/(dn.std()*math.sqrt(252)) if len(dn) and dn.std()>0 else 0
    mdd=((eq/np.maximum.accumulate(eq)-1).min())*100
    cal=cagr/abs(mdd) if mdd<0 else 0
    wr=(r>0).mean()*100
    return dict(tot=tot,cagr=cagr,vol=vol,sh=sh,sor=sor,mdd=mdd,cal=cal,wr=wr,best=r.max()*100,worst=r.min()*100)
ms=metrics(rets,len(rets)); mspy=metrics(spy_r,len(spy_r))
print('=== 성과 지표 (시뮬, 2026-02-17~06-12, 81거래일, 50/50 v119) ===')
print(f'{"지표":<16}{"시스템":>12}{"SPY":>12}')
for k,lbl in [('tot','누적수익%'),('cagr','CAGR%(연율)'),('vol','변동성%(연율)'),('sh','Sharpe'),('sor','Sortino'),('mdd','MDD%'),('cal','Calmar'),('wr','일승률%'),('best','최고일%'),('worst','최악일%')]:
    print(f'{lbl:<16}{ms[k]:>+11.2f}{mspy[k]:>+12.2f}')
print(f'\n=== 매매내역 (종료된 거래 {len(trades)}건) ===')
for ed,xd,tk,ep,xp,ret,days,rs in trades:
    print(f'  {tk:<6} {ed}~{xd} ({days}일) {ep:.0f}->{xp:.0f} {ret:+.1f}% [{rs}]')
print(f'\n=== 현재 보유 (시뮬) ===')
last=dates[-1]
for tk in held:
    ed,ep=entry[tk];cp=pf[last].get(tk,ep)
    print(f'  {tk:<6} 진입 {ed} @{ep:.0f} → 현재 @{cp:.0f}  미실현 {(cp/ep-1)*100:+.1f}% ({dates.index(last)-dates.index(ed)}일 보유)')
print(f'\n승/패(종료): {sum(1 for t in trades if t[5]>0)}/{sum(1 for t in trades if t[5]<=0)}')
