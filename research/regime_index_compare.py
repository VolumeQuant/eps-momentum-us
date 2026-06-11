# -*- coding: utf-8 -*-
"""국면 오버레이 신호 지수 비교: S&P500 vs NASDAQ100 vs DOW30
규칙 동일(MA200 15일확인 OR VIX>36 2일확인, 회복 15일), 신호 지수만 교체.
평가: QQQ 프록시 보유/defense=IEF. defense% · 휘프소(flips) · 4대약세장 포착 · CAGR/MDD/Cal.
재사용: regime_eda_market.py의 confirm/eval/bear/flips 로직.
"""
import sys; from pathlib import Path
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
START='2000-01-01'
KNOWN_BEARS={'dotcom':('2000-09-01','2002-10-15'),'GFC08':('2007-10-09','2009-03-09'),
             'COVID20':('2020-02-19','2020-03-23'),'rate22':('2022-01-03','2022-10-12')}
def fetch(t):
    import yfinance as yf
    df=yf.download(t,start=START,auto_adjust=True,progress=False)
    if df is None or df.empty: return None
    cl=df['Close']
    if isinstance(cl,pd.DataFrame): cl=cl.iloc[:,0]
    cl.index=pd.to_datetime(cl.index).tz_localize(None); return cl.dropna()
def confirm(raw,n):
    reg=pd.Series(False,index=raw.index); st=False; sd=sb=0
    for i,d in enumerate(raw.values):
        if d: sd+=1; sb=0
        else: sb+=1; sd=0
        if not st and sd>=n: st=True
        elif st and sb>=n: st=False
        reg.iloc[i]=st
    return reg
def eval_ov(proxy,reg,dret_px=None):
    px=proxy.reindex(reg.index).ffill().dropna()
    r=reg.reindex(px.index).ffill().fillna(False)
    pos=(~r).shift(1).fillna(False); pret=px.pct_change().fillna(0)
    if dret_px is not None:
        dret=dret_px.reindex(px.index).ffill().pct_change().fillna(0)
        strat=np.where(pos,pret,dret)
    else: strat=np.where(pos,pret,0.0)
    nav=(1+pd.Series(strat,index=px.index)).cumprod()
    yrs=(px.index[-1]-px.index[0]).days/365.25
    cagr=nav.iloc[-1]**(1/yrs)-1; peak=nav.cummax(); mdd=((nav-peak)/peak).min()
    return {'cagr':cagr*100,'mdd':mdd*100,'cal':cagr/abs(mdd) if mdd<0 else float('nan'),'nav':nav.iloc[-1]}
def bears(reg):
    return {k:(reg.loc[s:e].mean()*100 if len(reg.loc[s:e]) else float('nan')) for k,(s,e) in KNOWN_BEARS.items()}
def flips(reg): return int((reg!=reg.shift(1)).sum())

print('데이터 수집(yfinance)...')
idx={'SP500':fetch('^GSPC'),'NDX100':fetch('^NDX'),'DOW30':fetch('^DJI')}
qqq=fetch('QQQ'); vix=fetch('^VIX'); ief=fetch('IEF')
for k,v in idx.items(): print(f'  {k}: {v.index[0].date()}~{v.index[-1].date()} ({len(v)})')
print(f'  QQQ {qqq.index[0].date()}~ ({len(qqq)}) | VIX {len(vix)} | IEF {ief.index[0].date()}~ ({len(ief)})')

# VIX 규칙(공통): VIX>36, 2일 확인
vix_raw=(vix>36); vix_reg=confirm(vix_raw,2)

print('\n'+'='*104)
print('신호 지수별 비교 — proxy=QQQ, defense=IEF, 규칙: MA200(15d확인) [±VIX>36(2d)]')
print('='*104)
hdr=f'{"신호지수":<22}{"def%":>6}{"flips":>6} | {"dotcom":>7}{"GFC08":>7}{"COVID":>7}{"rate22":>7} | {"CAGR":>7}{"MDD":>7}{"Cal":>6}{"NAVx":>7}'
# baseline: QQQ buy&hold
bh=eval_ov(qqq,pd.Series(False,index=qqq.index))
print(f'\n[QQQ buy&hold]        {"":>6}{"-":>6} | {"":>7}{"":>7}{"":>7}{"":>7} | {bh["cagr"]:>+6.1f}%{bh["mdd"]:>+6.1f}%{bh["cal"]:>6.2f}{bh["nav"]:>6.1f}x')
for mode,use_vix in [('MA200만',False),('MA200+VIX',True)]:
    print(f'\n--- {mode} ---'); print(hdr); print('-'*104)
    for name,s in idx.items():
        ma=s.rolling(200).mean(); raw=(s<ma).fillna(False)
        reg=confirm(raw,15)
        if use_vix:
            reg=(reg.reindex(vix_reg.index.union(reg.index)).fillna(False) | vix_reg.reindex(reg.index.union(vix_reg.index)).fillna(False))
            reg=reg.reindex(s.index).fillna(False)
        ov=eval_ov(qqq,reg,ief); bc=bears(reg); fl=flips(reg); dpct=reg.mean()*100
        print(f'{name:<22}{dpct:>6.1f}{fl:>6} | {bc["dotcom"]:>6.0f}%{bc["GFC08"]:>6.0f}%{bc["COVID20"]:>6.0f}%{bc["rate22"]:>6.0f}% | {ov["cagr"]:>+6.1f}%{ov["mdd"]:>+6.1f}%{ov["cal"]:>6.2f}{ov["nav"]:>6.1f}x')
print('\n해석: 약세장 포착%↑ + flips(휘프소)↓ + MDD개선 + CAGR손실작음 = 우수. QQQ buy&hold가 비교 기준.')
