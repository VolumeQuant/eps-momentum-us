# -*- coding: utf-8 -*-
"""PE_gap 강건성: ①look-ahead 점검(시기분할) ②모멘텀/성장 직교성 ③PIT trailing EPS 재구성"""
import sqlite3, pandas as pd, pickle, numpy as np, yfinance as yf
from scipy.stats import spearmanr
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\pxU.pkl')
info = pickle.load(open(SP+r'\info.pkl','rb'))
teps_now={tk:info[tk].get('trailingEps') for tk in info}
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date")]
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def fwd(tk,d,n):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    p0=PX[tk].iloc[i]
    if pd.isna(p0): return None
    j=min(i+n,len(PX)-1); v=PX[tk].iloc[j]
    return (float(v)/float(p0)-1)*100 if pd.notna(v) else None
def mom(tk,d,n):  # 과거 n일 모멘텀
    i=pi.get(d)
    if i is None or tk not in PX.columns or i<n: return None
    p0=PX[tk].iloc[i-n]; p1=PX[tk].iloc[i]
    return (float(p1)/float(p0)-1)*100 if (pd.notna(p0) and pd.notna(p1)) else None

# PIT trailing EPS 재구성 (yfinance 분기 diluted EPS, 보고지연 +45일 가정)
print("PIT trailing EPS 재구성 중 (yfinance 분기실적)...")
pit_eps={}  # tk -> list[(report_date, ttm_eps)]
tks=list(PX.columns)
for tk in tks:
    try:
        qi=yf.Ticker(tk).quarterly_income_stmt
        if qi is None or qi.empty: continue
        row=None
        for k in ['Diluted EPS','Basic EPS']:
            if k in qi.index: row=qi.loc[k]; break
        if row is None: continue
        q=row.dropna().sort_index()  # index=quarter end dates asc
        rec=[]
        qe=list(q.items())
        for j in range(3,len(qe)):
            ttm=sum(qe[j-k][1] for k in range(4))
            rdate=(qe[j][0]+pd.Timedelta(days=45)).strftime('%Y-%m-%d')  # 보고지연
            rec.append((rdate,float(ttm)))
        if rec: pit_eps[tk]=rec
    except Exception: continue
print(f"  PIT EPS 확보: {len(pit_eps)}/{len(tks)}종목")
def pit_teps(tk,d):
    rec=pit_eps.get(tk)
    if not rec: return None
    val=None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val

obs=[]
for d in all_dates:
    for tk,p2,nc in cur.execute("SELECT ticker,part2_rank,ntm_current FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL",(d,)):
        if nc is None or nc<=0: continue
        tn=teps_now.get(tk); tp=pit_teps(tk,d)
        f20=fwd(tk,d,20); m20=mom(tk,d,20)
        if f20 is None: continue
        rec={'date':d,'tk':tk,'p2':p2,'f20':f20,'m20':m20}
        rec['gap_now']=nc/tn if (tn and tn>0.5) else None
        rec['gap_pit']=nc/tp if (tp and tp>0.5) else None
        obs.append(rec)
df=pd.DataFrame(obs)

def avgIC(d,fac,tgt='f20'):
    ics=[]
    for _,g in d.dropna(subset=[fac,tgt]).groupby('date'):
        if len(g)>=8:
            ic=spearmanr(g[fac],g[tgt]).correlation
            if pd.notna(ic): ics.append(ic)
    return np.mean(ics),len(ics)

print("\n=== ① look-ahead 점검: gap_now(현재EPS앵커) vs gap_pit(PIT EPS) ===")
for fac in ['gap_now','gap_pit']:
    ic,n=avgIC(df,fac); print(f"  {fac}: 평균IC {ic:+.3f} (n={n}일)")
# 시기분할 (gap_now)
mid=all_dates[len(all_dates)//2]
for lo,hi,lbl in [(all_dates[0],mid,'전반'),(mid,all_dates[-1],'후반')]:
    s=df[(df.date>=lo)&(df.date<hi)]
    ic,n=avgIC(s,'gap_now'); print(f"  gap_now {lbl}: IC {ic:+.3f} (n={n})")

print("\n=== ② 모멘텀/성장 직교성 ===")
d2=df.dropna(subset=['gap_now','m20'])
print(f"  gap vs 과거20일모멘텀 상관: {spearmanr(d2['gap_now'],d2['m20']).correlation:+.3f}")
print(f"  과거모멘텀(m20) 자체 IC: {avgIC(df,'m20')[0]:+.3f}")
# 모멘텀 통제: m20 5분위 내에서 gap이 여전히 예측하나
df2=df.dropna(subset=['gap_now','m20','f20']).copy()
df2['mq']=pd.qcut(df2['m20'],3,labels=['모멘텀低','중','高'])
print("  모멘텀 통제 후 gap IC:")
for mq,g in df2.groupby('mq',observed=True):
    print(f"    {mq}: gap IC {spearmanr(g['gap_now'],g['f20']).correlation:+.3f} (n={len(g)})")

print("\n=== ③ gap_pit 분위수 (PIT clean) ===")
dp=df.dropna(subset=['gap_pit']).copy()
dp['q']=pd.qcut(dp['gap_pit'],5,labels=['Q1低','Q2','Q3','Q4','Q5高'])
for q,g in dp.groupby('q',observed=True):
    print(f"  {q}: 향후20d 평균 {g['f20'].mean():+5.1f}% 중앙 {g['f20'].median():+5.1f}% (n={len(g)})")
