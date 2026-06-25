# -*- coding: utf-8 -*-
"""이중확인(momentum AND gap) 종목이 진짜 강한 알파인가 — forward return으로 측정"""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
D={}
for d in all_dates:
    D[d]={r[0]:{'nc':r[1],'cr':r[2]} for r in cur.execute("SELECT ticker,ntm_current,composite_rank FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def fwd(tk,d,n=20):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    p0=px(tk,d)
    if not p0: return None
    j=min(i+n,len(PX)-1); v=PX[tk].iloc[j]
    return (float(v)/p0-1)*100 if pd.notna(v) else None
def pit_te(tk,d):
    rec=pit.get(tk); val=None
    if not rec: return None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val
def gap(tk,d):
    v=D.get(d,{}).get(tk)
    if not v or not v['nc'] or v['nc']<=0: return None
    te=pit_te(tk,d)
    if not te or te<0.5: return None
    g=v['nc']/te; return g if g<=10 else None

# 매일: 각 종목의 momentum 등수(cr), gap 등수 매기고 forward 측정
rows=[]
for d in all_dates:
    cands=[(t,gap(t,d),D[d][t]['cr']) for t in D.get(d,{}) if gap(t,d) is not None and t in PX.columns]
    if len(cands)<10: continue
    by_gap=sorted(cands,key=lambda x:-x[1]); grank={t:i+1 for i,(t,_,_) in enumerate(by_gap)}
    n=len(cands)
    for t,g,cr in cands:
        f=fwd(t,d)
        if f is None: continue
        rows.append({'d':d,'t':t,'grank':grank[t],'mrank':cr,'f20':f,'gap':g})
df=pd.DataFrame(rows)
N=len(df)
print(f"관측 {N}건\n")
# 이중확인 vs 단일 vs 무
for M in [10,15,20]:
    print(f"=== momentum top{M} AND gap top{M} (이중확인) vs 각 단일 ===")
    both=df[(df.mrank<=M)&(df.grank<=M)]
    onlym=df[(df.mrank<=M)&(df.grank>M)]
    onlyg=df[(df.mrank>M)&(df.grank<=M)]
    neither=df[(df.mrank>M)&(df.grank>M)]
    for lbl,sub in [('이중확인(both)',both),('momentum만',onlym),('gap만',onlyg),('둘다아님',neither)]:
        if len(sub): print(f"  {lbl:14}: 향후20d 평균 {sub['f20'].mean():+5.1f}% / 중앙 {sub['f20'].median():+5.1f}% (n={len(sub)})")
    print()
# 이중확인 종목 실제 면면 (어떤 종목들인지)
M=20
both=df[(df.mrank<=M)&(df.grank<=M)]
print(f"=== 이중확인(top20 AND top20) 자주 등장 종목 ===")
print(both['t'].value_counts().head(12).to_dict())
