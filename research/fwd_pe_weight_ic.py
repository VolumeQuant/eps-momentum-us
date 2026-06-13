# -*- coding: utf-8 -*-
"""fwd_pe_chg 가중치 재검증 — IC(예측력) 스윕 (자율주행 2026-06-13).
fwd_pe_chg = 7/30/60/90일 PE변화의 가중합. 현행 w=[.30,.10,.10,.50].
ntm_* 는 DB(추정치 스냅샷), 가격은 yfinance(90일 lookback DB보다 길어야).
검증: 재구성 fwd_pe_chg로 adj_gap 역산→stored와 일치하는지. 그 후 날짜별 IC로 가중치 비교."""
import sqlite3, json, os
from collections import defaultdict
import numpy as np, pandas as pd
from scipy.stats import spearmanr
import sys; sys.stdout.reconfigure(encoding='utf-8')

tks=json.load(open('/tmp/rank_universe.json'))
CACHE='research/_price_hist_cache.parquet'
import yfinance as yf
if os.path.exists(CACHE):
    px=pd.read_parquet(CACHE)
    print(f'가격 캐시 로드: {px.shape}')
else:
    print(f'{len(tks)}종목 1년 가격 fetch (threads=2)...')
    px=yf.download(tks, period='1y', auto_adjust=True, progress=False, threads=2)['Close']
    px.to_parquet(CACHE)
    print(f'fetch+캐시 완료: {px.shape}')
px.index=pd.to_datetime(px.index).tz_localize(None)

# DB 관측: ntm_*, adj_gap, ranks
con=sqlite3.connect('eps_momentum_data.db'); cur=con.cursor()
obs=cur.execute('SELECT date,ticker,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,adj_gap,composite_rank,part2_rank FROM ntm_screening WHERE composite_rank<=30').fetchall()
con.close()

def price_on(tk,dt,nearest=False):
    """dt 종가. nearest=True면 production처럼 |날짜차| 최소 거래일(전후무관), 아니면 dt 이하 최근."""
    if tk not in px.columns: return None
    s=px[tk].dropna()
    if len(s)==0: return None
    if nearest:
        i=int(np.abs((s.index-dt).days).argmin())
        return float(s.iloc[i])
    s2=s[s.index<=dt]
    return float(s2.iloc[-1]) if len(s2) else None
def price_fwd(tk,dt,k=20):
    if tk not in px.columns: return None
    s=px[tk].dropna(); s=s[s.index>dt]
    return float(s.iloc[k-1]) if len(s)>=k else None

WEIGHTS={
 '현행 [.30/.10/.10/.50]':[.30,.10,.10,.50],
 '구 v80 [.40/.30/.20/.10]':[.40,.30,.20,.10],
 '균등 [.25x4]':[.25,.25,.25,.25],
 '90단독 [0/0/0/1]':[0,0,0,1.0],
 '7단독 [1/0/0/0]':[1.0,0,0,0],
 '30단독':[0,1.0,0,0],
 '60단독':[0,0,1.0,0],
 '단기강조 [.50/.30/.10/.10]':[.50,.30,.10,.10],
 '장기점증 [.10/.20/.30/.40]':[.10,.20,.30,.40],
 '7+90 [.50/0/0/.50]':[.50,0,0,.50],
 '90강화 [.20/.05/.05/.70]':[.20,.05,.05,.70],
}
keys=['7d','30d','60d','90d']; days=[7,30,60,90]

# 관측별 fwd_pe_chg 컴포넌트 사전계산 (각 horizon의 pe_chg_period)
recs=[]  # (date, ticker, [pe_chg_7,30,60,90], adj_gap_stored, fwd20ret)
import datetime as dt_
miss=0
for d,tk,nc,n7,n30,n60,n90,ag,cr,p2 in obs:
    if not nc or nc<=0: continue
    dd=pd.Timestamp(d)
    p_now=price_on(tk,dd)
    if p_now is None: continue
    fwd_pe_now=p_now/nc
    ntm_map={'7d':n7,'30d':n30,'60d':n60,'90d':n90}
    comps={}
    ok=True
    for key,dy in zip(keys,days):
        nv=ntm_map[key]; p_then=price_on(tk,dd-pd.Timedelta(days=dy),nearest=True)
        if nv and nv>0 and p_then and p_then>0:
            fpe_then=p_then/nv
            comps[key]=(fwd_pe_now-fpe_then)/fpe_then*100
        else:
            comps[key]=None
    f20=price_fwd(tk,dd,20)
    fret=(f20/p_now-1)*100 if f20 else None
    recs.append((d,tk,comps,ag,fret))
print(f'관측 {len(recs)} (가격매칭 성공)')

def fwd_pe_chg(comps,w):
    s=0.0;tw=0.0
    for key,wi in zip(keys,w):
        if comps[key] is not None and wi>0:
            s+=wi*comps[key]; tw+=wi
    return s/tw if tw>0 else None

# === 검증: 현행 가중치 재구성 fwd_pe_chg로 adj_gap 역산 multiplier 분포 ===
cur_w=WEIGHTS['현행 [.30/.10/.10/.50]']
mults=[]
for d,tk,comps,ag,fret in recs:
    fpc=fwd_pe_chg(comps,cur_w)
    if fpc and abs(fpc)>0.1 and ag is not None:
        mults.append(ag/fpc)  # = (1+dir_factor)*eps_q, 정상범위 [0.49,1.69]
mults=np.array(mults)
inrange=((mults>=0.45)&(mults<=1.75)).mean()*100
print(f'\n[재구성 검증] adj_gap/fwd_pe_chg_recon 배수: 중앙값 {np.median(mults):.2f}, [0.45~1.75] 비율 {inrange:.0f}% (높을수록 재구성 충실)')

# === 날짜별 IC 스윕 ===
by_date=defaultdict(list)
for d,tk,comps,ag,fret in recs:
    if fret is not None: by_date[d].append((tk,comps,fret))
print(f'\n=== 가중치별 IC (날짜별 Spearman 평균; 음수=PE압축→고수익=좋은신호) ===')
print(f'{"가중치":<28}{"평균IC":>9}{"|IC|":>7}{"승률(IC<0)":>10}')
results=[]
for name,w in WEIGHTS.items():
    ics=[]
    for d,lst in by_date.items():
        sig=[];ret=[]
        for tk,comps,fret in lst:
            fpc=fwd_pe_chg(comps,w)
            if fpc is not None: sig.append(fpc); ret.append(fret)
        if len(sig)>=8:
            ic=spearmanr(sig,ret)[0]
            if not np.isnan(ic): ics.append(ic)
    if ics:
        avg=np.mean(ics); neg=sum(1 for x in ics if x<0)/len(ics)*100
        results.append((name,avg,abs(avg),neg))
results.sort(key=lambda x:x[1])  # 가장 음수(좋은) 먼저
for name,avg,absic,neg in results:
    print(f'{name:<28}{avg:>+8.3f}{absic:>7.3f}{neg:>9.0f}%')
print('\n해석: 평균IC 가장 음수인 가중치 = PE압축 신호가 forward수익을 가장 잘 예측. 현행보다 나으면 후보.')
