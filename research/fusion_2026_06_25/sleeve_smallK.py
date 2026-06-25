# -*- coding: utf-8 -*-
"""gap sleeve 집중판(K=3~10) — 5종목 이하 현실성. 분산알파가 집중서 버티나 + WF + LOWO"""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
elig={}
for d in all_dates:
    elig[d]={r[0]:r[1] for r in cur.execute("SELECT ticker,ntm_current FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
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
def run(K,R=21,lo=None,hi=None,ban=()):
    rng=[d for d in all_dates if (lo is None or d>=lo) and (hi is None or d<=hi)]
    nav=1.0;peak=1.0;mdd=0.0;hold=[];lastpx={};rets=[]
    for k,d in enumerate(rng):
        if hold and lastpx:
            rs=[(px(t,d)/lastpx[t]-1) for t in hold if px(t,d) and lastpx.get(t)]
            if rs:
                r=np.mean(rs);nav*=(1+r);peak=max(peak,nav);mdd=min(mdd,nav/peak-1);rets.append(r)
                for t in hold:
                    if px(t,d):lastpx[t]=px(t,d)
        if k%R==0:
            ranked=sorted([(t,gap(t,d)) for t in elig.get(d,{}) if gap(t,d) is not None and t in PX.columns and t not in ban],key=lambda x:-x[1])
            newh=[t for t,_ in ranked[:K] if px(t,d)]
            if newh: hold=newh;lastpx={t:px(t,d) for t in newh}
    vol=np.std(rets)*np.sqrt(252) if len(rets)>2 else 1e-9
    sh=(((nav)**(252/len(rng))-1))/vol if vol>0 else 0
    return (nav-1)*100,mdd*100,sh
print("=== 집중판 K (R=21 월간, 동일가중) ===")
print(f"{'K':>3} {'누적%':>7} {'MDD%':>7} {'Sharpe':>7}")
for K in [3,5,7,10,15]:
    cum,mdd,sh=run(K); print(f"{K:>3} {cum:>7.0f} {mdd:>7.0f} {sh:>7.1f}")
print("\n=== walk-forward (전반/후반) ===")
mid='2026-04-21'
for K in [3,5,7,10,15]:
    a=run(K,lo=all_dates[0],hi=mid);b=run(K,lo=mid,hi=all_dates[-1])
    print(f"  K={K}: 전반 {a[0]:+.0f}%(MDD{a[1]:.0f},Sh{a[2]:.1f}) | 후반 {b[0]:+.0f}%(MDD{b[1]:.0f},Sh{b[2]:.1f})")
print("\n=== LOWO (K=5, winner 빼도 버티나) ===")
for w in ['SNDK','MU','COHR','LITE','AVGO']:
    full=run(5)[0];lo=run(5,ban=(w,))[0]
    print(f"  -{w}: {lo:+.0f}% (전체 {full:+.0f}%, Δ{lo-full:+.0f}p)")
print("\n=== K=5 매월 보유 종목 ===")
for d in ['2026-02-12','2026-03-02','2026-04-01','2026-05-01','2026-06-01']:
    if d in all_dates:
        ranked=sorted([(t,gap(t,d)) for t in elig.get(d,{}) if gap(t,d) is not None and t in PX.columns],key=lambda x:-x[1])
        print(f"  {d}: {', '.join(f'{t}({g:.1f})' for t,g in ranked[:5])}")
