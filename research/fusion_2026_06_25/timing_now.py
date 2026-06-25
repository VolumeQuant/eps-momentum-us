# -*- coding: utf-8 -*-
"""지금 데이터로 끝낸다 — 약세장/episode 안 셈. 매일(신호→향후수익) 91일 전수.
   ①gap-factor 모멘텀(자기 추세가 미래 예측?) ②gap-breadth→유니버스 향후수익 ③gap-궤적 사이징"""
import sqlite3, pandas as pd, pickle, numpy as np
from scipy.stats import spearmanr
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb')); DV=pd.read_pickle(SP+r'\dv137.pkl')
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
D={}
for d in all_dates:
    D[d]={r[0]:{'nc':r[1],'n30':r[2]} for r in cur.execute("SELECT ticker,ntm_current,ntm_30d FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def ret1(tk,d):  # 당일 1d return (전일→당일)
    i=pi.get(d)
    if i is None or i==0 or tk not in PX.columns: return None
    a,b=PX[tk].iloc[i-1],PX[tk].iloc[i]
    return float(b)/float(a)-1 if (pd.notna(a) and pd.notna(b)) else None
def pit_te(tk,d):
    rec=pit.get(tk); val=None
    if not rec: return None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val
def gap(tk,d,which='nc'):
    v=D.get(d,{}).get(tk)
    if not v or not v[which] or v[which]<=0: return None
    te=pit_te(tk,d)
    if not te or te<0.5: return None
    g=v[which]/te; return g if g<=10 else None

# 일별: gap-factor 1d수익(Q5-Q1), gap-breadth(가속 비율), 유니버스 평균 1d수익
days=[]
for d in all_dates:
    cands=[(t,gap(t,d)) for t in D.get(d,{}) if gap(t,d) is not None]
    cands=[(t,g) for t,g in cands if t in PX.columns]
    if len(cands)<10: continue
    cands.sort(key=lambda x:-x[1])
    n=len(cands); q=n//5
    top=[t for t,_ in cands[:q]]; bot=[t for t,_ in cands[-q:]]
    rt=np.nanmean([ret1(t,d) for t in top if ret1(t,d) is not None])
    rb=np.nanmean([ret1(t,d) for t in bot if ret1(t,d) is not None])
    # breadth: gap_now > gap_30d 비율
    acc=[1 if (gap(t,d) and gap(t,d,'n30') and gap(t,d)>gap(t,d,'n30')) else 0 for t,_ in cands]
    uni=np.nanmean([ret1(t,d) for t,_ in cands if ret1(t,d) is not None])
    days.append({'d':d,'factor':(rt-rb) if (rt==rt and rb==rb) else np.nan,'breadth':np.mean(acc),'uni':uni})
df=pd.DataFrame(days).dropna()
print(f"일별 관측 {len(df)}일\n")
# ① gap-factor 자기모멘텀: 과거5일 factor누적 → 향후5일 factor
df['f_past5']=df['factor'].rolling(5).sum().shift(1)
df['f_fwd5']=df['factor'].rolling(5).sum().shift(-5)
sub=df.dropna(subset=['f_past5','f_fwd5'])
print("=== ① gap-factor 자기 모멘텀 (타이밍 가능?) ===")
print(f"  과거5일 factor → 향후5일 factor 상관: {spearmanr(sub['f_past5'],sub['f_fwd5']).correlation:+.2f} (n={len(sub)})")
print(f"  gap-factor 평균 일수익 {df['factor'].mean()*100:+.2f}% (양수면 gap이 매일 +)")
# ② gap-breadth → 향후 유니버스 수익
df['uni_fwd5']=df['uni'].rolling(5).sum().shift(-5)
sub2=df.dropna(subset=['breadth','uni_fwd5'])
print("\n=== ② gap-breadth → 향후5일 유니버스수익 (레짐 타이밍?) ===")
print(f"  breadth(가속비율) 범위 {df['breadth'].min():.2f}~{df['breadth'].max():.2f}, 평균 {df['breadth'].mean():.2f}")
print(f"  breadth → 향후5일 유니버스수익 상관: {spearmanr(sub2['breadth'],sub2['uni_fwd5']).correlation:+.2f} (n={len(sub2)})")
# breadth 낮은날(하위33%) vs 높은날 향후수익
lo=sub2[sub2.breadth<=sub2.breadth.quantile(0.33)]; hi=sub2[sub2.breadth>=sub2.breadth.quantile(0.67)]
print(f"  breadth 낮은날 향후5d 유니버스 {lo['uni_fwd5'].mean()*100:+.1f}% | 높은날 {hi['uni_fwd5'].mean()*100:+.1f}%")
print("\n=== ③ gap-궤적 사이징 (2슬롯 가중) — 별도 스크립트 필요시. 우선 ①②로 타이밍 가부 판정 ===")
