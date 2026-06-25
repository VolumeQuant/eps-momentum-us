# -*- coding: utf-8 -*-
"""확신가중을 올바른 vehicle에: gap sleeve(7종목) 안에서 momentum도 확인된 종목 비중↑.
   = KR 3슬롯 확신가중의 US 정합 vehicle. M 스윕 + LOWO."""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb')); DV=pd.read_pickle(SP+r'\dv137.pkl')
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
D={}
for d in all_dates:
    D[d]={r[0]:{'nc':r[1],'cr':r[2]} for r in cur.execute("SELECT ticker,ntm_current,composite_rank FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
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
    v=D.get(d,{}).get(tk)
    if not v or not v['nc'] or v['nc']<=0: return None
    te=pit_te(tk,d)
    if not te or te<0.5: return None
    g=v['nc']/te; return g if g<=10 else None
def run(M,NC=30,K=7,ban=()):
    # gap sleeve K7 월간, 보유 중 momentum확인(composite_rank≤NC) 종목 ×M
    nav=1.0;peak=1.0;mdd=0.0;hold=[];lastpx={};rebm=None
    for d in all_dates:
        if hold and lastpx:
            w={t:(M if (D.get(d,{}).get(t,{}).get('cr') is not None and D[d][t]['cr']<=NC) else 1.0) for t in hold}
            tot=sum(w.values())
            dr=0.0
            for t in hold:
                cu,pp=px(t,d),lastpx.get(t)
                if cu and pp and pp>0: dr+=(w[t]/tot)*(cu-pp)/pp
            nav*=(1+dr);peak=max(peak,nav);mdd=min(mdd,nav/peak-1)
            for t in hold:
                if px(t,d): lastpx[t]=px(t,d)
        m=d[:7]
        if m!=rebm or not hold:
            ranked=sorted([(t,gap(t,d)) for t in D.get(d,{}) if gap(t,d) is not None and t in PX.columns and (dvol(t,d) or 0)>=1000 and t not in ban],key=lambda x:-x[1])
            new=[t for t,_ in ranked[:K] if px(t,d)]
            if new: hold=new;lastpx={t:px(t,d) for t in new};rebm=m
    return (nav-1)*100,mdd*100
print("=== gap sleeve(K7) 확신가중: momentum확인(cr≤NC) 종목 ×M ===")
print(f"{'M':>6} {'NC=20':>14} {'NC=30':>14} {'NC=50':>14}")
for M in [1,2,2.5,3,4]:
    row=[f"{run(M,NC)[0]:+.0f}%/{run(M,NC)[1]:.0f}" for NC in [20,30,50]]
    print(f"{M:>6} "+" ".join(f"{v:>14}" for v in row))
print("\n=== LOWO (M=3, NC=30) ===")
for w in ['SNDK','MU','LITE','COHR','CIEN']:
    b1=run(1,30,ban=(w,))[0];b2=run(3,30,ban=(w,))[0]
    print(f"  -{w}: ×1 {b1:+.0f}% → ×3 {b2:+.0f}% (Δ{b2-b1:+.0f}p)")
print("\n참고: 동일가중 sleeve(M1)=+67% / 2슬롯=+217%")
