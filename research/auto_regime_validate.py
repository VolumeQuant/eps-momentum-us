# -*- coding: utf-8 -*-
"""Task B: 약세장 국면 오버레이 엄격 검증 (26년).
production 규칙 정확 복제: SPX<MA200(15일확인) OR VIX>36(2일확인) → defense(IEF).
검증: ①4대 약세장 포착 ②오버레이 효과(MDD/Calmar/수익) ③휘프소 ④파라미터 견고성 ⑤현재국면.
lookahead 방지: 당일까지 데이터로 판단 → 익일 포지션 적용."""
import sys; sys.stdout.reconfigure(encoding='utf-8')
import yfinance as yf, pandas as pd, numpy as np

def confirm_daily(raw, n):
    """raw bool 시퀀스 → 각 시점의 hysteresis 상태 시퀀스 (production _confirm_regime 일별판)."""
    out=[]; state=False; sd=sb=0
    for d in raw:
        if d: sd+=1; sb=0
        else: sb+=1; sd=0
        if not state and sd>=n: state=True
        elif state and sb>=n: state=False
        out.append(state)
    return out

print("데이터 fetch (^GSPC, ^VIX, IEF)...", flush=True)
spx=yf.download('^GSPC',start='1999-01-01',auto_adjust=True,progress=False)['Close']
vix=yf.download('^VIX',start='1999-01-01',auto_adjust=True,progress=False)['Close']
ief=yf.download('IEF',start='1999-01-01',auto_adjust=True,progress=False)['Close']
for s in (spx,vix,ief):
    if hasattr(s,'columns'): pass
spx=spx.iloc[:,0] if hasattr(spx,'columns') else spx
vix=vix.iloc[:,0] if hasattr(vix,'columns') else vix
ief=ief.iloc[:,0] if hasattr(ief,'columns') else ief
df=pd.DataFrame({'spx':spx,'vix':vix,'ief':ief}).dropna(subset=['spx'])
df.index=pd.to_datetime(df.index)
print(f"기간: {df.index[0].date()} ~ {df.index[-1].date()} ({len(df)}일), VIX유효 {df['vix'].notna().sum()}, IEF유효 {df['ief'].notna().sum()}")

def regime_series(ma_confirm=15, vix_thr=36, vix_confirm=2, ma_period=200):
    ma=df['spx'].rolling(ma_period).mean()
    below=(df['spx']<ma)
    ma_def=pd.Series(confirm_daily(below.fillna(False).tolist(), ma_confirm), index=df.index)
    vraw=(df['vix']>vix_thr).fillna(False)
    vix_def=pd.Series(confirm_daily(vraw.tolist(), vix_confirm), index=df.index)
    defense=(ma_def | vix_def)
    defense[ma.isna()]=False  # MA200 미형성 구간 boost
    return defense, ma_def, vix_def

def overlay_sim(defense):
    """익일 적용: 오늘 defense면 내일 IEF, 아니면 SPY. IEF 없으면 현금(0)."""
    sret=df['spx'].pct_change().fillna(0)
    iret=df['ief'].pct_change().fillna(0)
    pos_def=defense.shift(1).fillna(False)  # lookahead 방지
    strat=np.where(pos_def & df['ief'].notna().shift(1).fillna(False), iret,
                   np.where(pos_def, 0.0, sret))
    strat=pd.Series(strat,index=df.index)
    return strat

def metrics(ret):
    eq=(1+ret).cumprod()
    years=(df.index[-1]-df.index[0]).days/365.25
    cagr=eq.iloc[-1]**(1/years)-1
    mdd=((eq/eq.cummax()-1)).min()
    calmar=cagr/abs(mdd) if mdd<0 else float('nan')
    return eq.iloc[-1]-1, cagr, mdd, calmar

# === 1. 오버레이 vs 매수보유 (기본 파라미터) ===
defense,ma_def,vix_def=regime_series()
strat=overlay_sim(defense)
bh=df['spx'].pct_change().fillna(0)
print("\n=== 1) 오버레이 효과 (1999~2026, IEF/현금 방어) ===")
for nm,r in [('매수보유(SPY)',bh),('국면오버레이',strat)]:
    tot,cagr,mdd,cal=metrics(r)
    print(f"  {nm:<14} 총{tot*100:>8.0f}%  CAGR{cagr*100:>6.1f}%  MDD{mdd*100:>7.1f}%  Calmar{cal:>5.2f}")
print(f"  방어일수: {defense.sum()}/{len(defense)} ({defense.mean()*100:.1f}%)")

# === 2. 4대 약세장 포착 ===
print("\n=== 2) 4대 약세장 포착 (방어구간이 폭락을 막았나) ===")
bears={'닷컴(00-02)':('2000-03-01','2002-10-31'),'금융위기(07-09)':('2007-10-01','2009-03-31'),
       'COVID(20)':('2020-02-15','2020-04-30'),'2022약세장':('2022-01-01','2022-10-31')}
for nm,(s,e) in bears.items():
    m=(df.index>=s)&(df.index<=e)
    spx_dd=((df['spx'][m]/df['spx'][m].cummax()-1)).min()*100
    dfrac=defense[m].mean()*100
    # 방어 진입이 폭락 저점보다 먼저였나
    print(f"  {nm:<14} SPX낙폭{spx_dd:>6.0f}%  방어커버{dfrac:>5.0f}% of 구간")

# === 3. 휘프소 (false alarm) ===
print("\n=== 3) 전환/휘프소 ===")
trans=int((defense!=defense.shift(1)).sum())
# 방어 에피소드별: 시작~끝, 그동안 SPX 수익(방어가 옳았으면 음수)
epi=[]; ind=None
for i,(dt,v) in enumerate(defense.items()):
    if v and ind is None: ind=dt
    if not v and ind is not None:
        seg=df['spx'][(df.index>=ind)&(df.index<dt)]
        if len(seg)>1: epi.append((ind.date(),dt.date(),len(seg),(seg.iloc[-1]/seg.iloc[0]-1)*100))
        ind=None
good=sum(1 for *_,r in epi if r<2); bad=sum(1 for *_,r in epi if r>=2)
print(f"  전환 횟수: {trans}, 방어 에피소드: {len(epi)}개 (옳음<+2%: {good}, 휘프소>=+2%: {bad})")
print("  주요 방어구간 (길이>20일):")
for s,e,n,r in sorted(epi,key=lambda x:-x[2])[:8]:
    print(f"    {s} ~ {e} ({n}일) 구간 SPX {r:+.0f}%")

# === 4. 파라미터 견고성 ===
print("\n=== 4) 파라미터 견고성 (Calmar / MDD) ===")
print(f"  {'MA확인':>6}{'VIX임계':>7}{'총수익':>9}{'MDD':>8}{'Calmar':>7}")
for mc in [10,15,20]:
    for vt in [30,36,40]:
        de,_,_=regime_series(ma_confirm=mc,vix_thr=vt)
        tot,cagr,mdd,cal=metrics(overlay_sim(de))
        star=' ←현행' if (mc==15 and vt==36) else ''
        print(f"  {mc:>6}{vt:>7}{tot*100:>8.0f}%{mdd*100:>7.1f}%{cal:>7.2f}{star}")

# === 5. 현재 국면 ===
print("\n=== 5) 현재 국면 (최근) ===")
cur=defense.iloc[-1]; spx_now=df['spx'].iloc[-1]; ma_now=df['spx'].rolling(200).mean().iloc[-1]
vix_now=df['vix'].iloc[-1]
print(f"  {df.index[-1].date()}: {'🛡️방어' if cur else '🟢정상(boost)'} | SPX {spx_now:.0f} vs MA200 {ma_now:.0f} ({(spx_now/ma_now-1)*100:+.1f}%) | VIX {vix_now:.1f}")
print(f"  MA방어={ma_def.iloc[-1]}, VIX방어={vix_def.iloc[-1]}")
