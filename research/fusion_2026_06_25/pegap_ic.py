# -*- coding: utf-8 -*-
"""PE_gap = forwardEPS(ntm_current)/trailingEPS 예측력 검증 (IC + 분위수).
   trailing EPS = yfinance 현재값 앵커(분기 느림→근사, 한계 명시). 수익률=pxU 가격."""
import sqlite3, pandas as pd, pickle, numpy as np
from scipy.stats import spearmanr
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
PX = pd.read_pickle(r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad\pxU.pkl')
info = pickle.load(open(r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad\info.pkl','rb'))
teps={tk:(info[tk].get('trailingEps')) for tk in info}
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date")]
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def fwd(tk,d,n):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    p0=px(tk,d)
    if not p0: return None
    j=min(i+n,len(PX)-1)
    v=PX[tk].iloc[j]
    return (float(v)/p0-1)*100 if pd.notna(v) else None

# 관측치 수집: (date, tk, gap, part2_rank, fwd20)
obs=[]
for d in all_dates:
    rows=cur.execute("SELECT ticker,part2_rank,ntm_current FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL",(d,)).fetchall()
    for tk,p2,nc in rows:
        te=teps.get(tk)
        if te is None or te<0.5 or nc is None or nc<=0: continue  # turnaround/저베이스 제외
        gap=nc/te  # forward/trailing = 기대 성장배수
        f20=fwd(tk,d,20)
        if f20 is None: continue
        obs.append({'date':d,'tk':tk,'gap':gap,'lg':np.log(gap),'p2':p2,'f20':f20})
df=pd.DataFrame(obs)
print(f"관측치 {len(df)}건, {df.date.nunique()}일, {df.tk.nunique()}종목")
print(f"gap 분포: min {df.gap.min():.2f} / 중앙 {df.gap.median():.2f} / 평균 {df.gap.mean():.2f} / max {df.gap.max():.2f}")

# 1) 일별 cross-sectional IC (Spearman: gap vs fwd20)
ics=[]
for d,g in df.groupby('date'):
    if len(g)>=8:
        ic=spearmanr(g['gap'],g['f20']).correlation
        if pd.notna(ic): ics.append(ic)
print(f"\n=== [1] 일별 cross-sectional IC (gap vs 향후20일 수익) ===")
print(f"  평균 IC {np.mean(ics):+.3f}, 중앙 {np.median(ics):+.3f}, 양수비율 {sum(1 for i in ics if i>0)}/{len(ics)}")

# 2) gap 5분위 → 평균 향후20일 수익 (pooled)
df['q']=pd.qcut(df['gap'],5,labels=['Q1(저gap)','Q2','Q3','Q4','Q5(고gap)'])
print(f"\n=== [2] gap 5분위별 평균 향후20일 수익 (pooled {len(df)}건) ===")
g=df.groupby('q',observed=True)['f20'].agg(['mean','median','count'])
for q,r in g.iterrows():
    print(f"  {q:10}: 평균 {r['mean']:+5.1f}% / 중앙 {r['median']:+5.1f}% (n={int(r['count'])})")

# 3) 사용자 가설 직접: gap≈1(성장기대無) vs gap 큰 것
print(f"\n=== [3] 사용자 가설 직접 — gap 구간별 ===")
for lo,hi,lbl in [(0,1.1,'gap≈1 (성장기대無)'),(1.1,1.5,'gap 1.1~1.5'),(1.5,2.5,'gap 1.5~2.5'),(2.5,99,'gap 2.5+ (고성장기대)')]:
    s=df[(df.gap>=lo)&(df.gap<hi)]
    if len(s): print(f"  {lbl:22}: 평균 향후20d {s['f20'].mean():+5.1f}% (n={len(s)})")

# 4) 현 시스템(part2_rank)과 중복인가? — gap과 rank 상관 + rank 통제 후 gap IC
print(f"\n=== [4] 현 시스템과 직교성 ===")
print(f"  gap vs part2_rank 상관(Spearman): {spearmanr(df['gap'],df['p2']).correlation:+.3f} (음수=고gap이 상위랭크)")
# 같은 rank 버킷 내에서 gap이 추가 예측력 있나
top=df[df.p2<=10]
if len(top)>=20:
    print(f"  top10 내에서 gap IC: {spearmanr(top['gap'],top['f20']).correlation:+.3f} (n={len(top)})")
