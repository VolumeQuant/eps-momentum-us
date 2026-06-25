# -*- coding: utf-8 -*-
"""전문가 아이디어 IC 검증: accel(NTM 2차도함수)·gap_slope·breadth·price-NTM divergence
   각각 향후20d 예측력 + part2_rank/gap과 직교성"""
import sqlite3, pandas as pd, pickle, numpy as np
from scipy.stats import spearmanr
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\pxU.pkl'); pit_eps=pickle.load(open(SP+r'\pit_eps.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date")]
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def pxv(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def fwd(tk,d,n=20):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    p0=pxv(tk,d)
    if not p0: return None
    j=min(i+n,len(PX)-1); v=PX[tk].iloc[j]
    return (float(v)/p0-1)*100 if pd.notna(v) else None
def pmom(tk,d,n=30):
    i=pi.get(d)
    if i is None or tk not in PX.columns or i<n: return None
    p0=PX[tk].iloc[i-n]; p1=PX[tk].iloc[i]
    return (float(p1)/float(p0)-1)*100 if (pd.notna(p0) and pd.notna(p1)) else None
def pit_teps(tk,d):
    rec=pit_eps.get(tk); val=None
    if not rec: return None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val
def seg(a,b): return (a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0.0

obs=[]
for d in all_dates:
    for tk,p2,nc,n7,n30,n60,n90,u,dn,na in cur.execute(
        "SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,rev_up30,rev_down30,num_analysts FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL",(d,)):
        if not nc or nc<=0: continue
        f=fwd(tk,d)
        if f is None: continue
        s1,s2,s3,s4=seg(nc,n7),seg(n7,n30),seg(n30,n60),seg(n60,n90)
        accel=1.5*s1+0.5*s2-0.5*s3-1.5*s4   # NTM 속도의 추세 = 가속도
        gslope=seg(nc,n30)                   # NTM 30d 모멘텀(=gap slope, TTM 불변시)
        te=pit_teps(tk,d); gap=nc/te if (te and te>0.5) else np.nan
        br=((u or 0)-(dn or 0))/na if na else np.nan   # breadth
        pm=pmom(tk,d); div=(pm-seg(nc,n30)) if pm is not None else np.nan  # price-NTM divergence(클수록 derating)
        obs.append({'date':d,'tk':tk,'p2':p2,'f20':f,'accel':accel,'gslope':gslope,'gap':gap,'breadth':br,'diverg':div})
df=pd.DataFrame(obs)
print(f"관측 {len(df)}건, {df.date.nunique()}일")

def avgIC(fac, sign=1, df_=None):
    d_=df if df_ is None else df_
    ics=[]
    for _,g in d_.dropna(subset=[fac,'f20']).groupby('date'):
        if len(g)>=8:
            ic=spearmanr(g[fac]*sign,g['f20']).correlation
            if pd.notna(ic): ics.append(ic)
    return np.mean(ics) if ics else np.nan, len(ics)

print("\n=== 아이디어별 향후20d IC + part2_rank/gap 직교성 ===")
print(f"{'신호':12} {'IC':>7} {'양수일':>7} {'vs rank corr':>12} {'vs gap corr':>11}")
for fac,sign,desc in [('gap',1,'gap level(기준)'),('accel',1,'NTM가속도'),('gslope',1,'NTM30d모멘텀'),
                       ('breadth',1,'revision breadth'),('diverg',-1,'price-NTM diverg(역)')]:
    ic,n=avgIC(fac,sign)
    cr=spearmanr(df[fac]*sign,df['p2']).correlation
    cg=spearmanr(df.dropna(subset=[fac,'gap'])[fac]*sign, df.dropna(subset=[fac,'gap'])['gap']).correlation
    print(f"{desc:12} {ic:>+7.3f} {n:>7} {cr:>+12.3f} {cg:>+11.3f}")

print("\n=== 사이징 관점: 2종목 중 신호 높은 쪽 vs 낮은 쪽 향후수익 (pairwise, top5내) ===")
# 같은 날 top5 내에서 신호 1등 vs 꼴찌의 평균 fwd
for fac,sign in [('gap',1),('accel',1),('breadth',1),('gslope',1)]:
    hi,lo=[],[]
    for _,g in df.dropna(subset=[fac]).groupby('date'):
        g5=g[g.p2<=5]
        if len(g5)>=3:
            g5=g5.sort_values(fac,ascending=(sign<0))
            hi.append(g5.iloc[-1]['f20']); lo.append(g5.iloc[0]['f20'])
    if hi: print(f"  {fac:9}: top5내 신호1등 {np.mean(hi):+.1f}% vs 꼴찌 {np.mean(lo):+.1f}% (Δ{np.mean(hi)-np.mean(lo):+.1f}%p, n={len(hi)})")
