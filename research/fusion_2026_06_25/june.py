# -*- coding: utf-8 -*-
"""6월 거동 + MU·SNDK 없을때 sleeve 현재잔고 + LITE·NVDA 비교"""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb')); DV=pd.read_pickle(SP+r'\dv137.pkl')
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
elig={}
for d in all_dates:
    elig[d]={r[0]:r[1] for r in cur.execute("SELECT ticker,ntm_current FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
dvi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(DV.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def dvol(tk,d):
    i=dvi.get(d)
    if i is None or tk not in DV.columns: return None
    v=DV[tk].iloc[i]; return float(v) if pd.notna(v) else None
def pit_te(tk,d):
    rec=pit.get(tk); val=None
    if not rec: return None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val
def gap(tk,d):
    nc=elig.get(d,{}).get(tk)
    if not nc or nc<=0: return None
    te=pit_te(tk,d)
    if not te or te<0.5: return None
    g=nc/te; return g if g<=10 else None
jun=[d for d in all_dates if d>='2026-06-01']
j0,j1=jun[0],jun[-1]
def ret(tk,a,b):
    pa,pb=px(tk,a),px(tk,b)
    return (pb/pa-1)*100 if (pa and pb) else None

# sleeve 6월 보유 (5/01 리밸로 정해진 게 6월초까지, 6/01 리밸로 6월 보유)
def sleeve_hold_on(d,ban=()):
    ranked=sorted([(t,gap(t,d)) for t in elig.get(d,{}) if gap(t,d) is not None and t in PX.columns and t not in ban and (dvol(t,d) or 0)>=1000],key=lambda x:-x[1])
    return [t for t,_ in ranked[:7] if px(t,d)]

print(f"=== 6월 보유 종목 & 수익 ({j0} ~ {j1}) ===")
for ban,lbl in [((),'sleeve($1B) 전체'),(('MU','SNDK'),'sleeve($1B) MU·SNDK 제외')]:
    h=sleeve_hold_on(j0,ban)
    rs=[ret(t,j0,j1) for t in h if ret(t,j0,j1) is not None]
    print(f"\n[{lbl}] 6월보유: {', '.join(h)}")
    print(f"  6월 수익(동일가중): {np.mean(rs):+.1f}%")
    for t in h:
        r=ret(t,j0,j1); g=gap(t,j1)
        gs=f"{g:.1f}x" if g is not None else "n/a"
        print(f"    {t}: 6월 {r:+.1f}%  (gap {gs})" if r is not None else f"    {t}: -")

print(f"\n=== 고전중인 우리 2슬롯 라이브: LITE·NVDA 6월 ===")
for t in ['LITE','NVDA']:
    print(f"  {t}: 6월 {ret(t,j0,j1):+.1f}%")

print(f"\n=== 06-24 현재 sleeve 잔고 (MU·SNDK 제외, $1B) ===")
h=sleeve_hold_on(j1,('MU','SNDK'))
for t in h:
    g=gap(t,j1); gs=f"{g:.1f}x" if g is not None else "n/a"
    dv=dvol(t,j1); dvs=f"${dv:.0f}M" if dv else "n/a"
    r=ret(t,j0,j1); rs=f"{r:+.1f}%" if r is not None else "-"
    print(f"  {t}: gap {gs} · 거래대금 {dvs} · 6월 {rs}")
