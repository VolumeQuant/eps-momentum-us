# -*- coding: utf-8 -*-
"""gap sleeve E/X/S 최적화 (2슬롯 방식). S=5 슬롯.
   진입: gap순위 ≤ E면 매수(빈슬롯). 이탈: gap순위 > X면 매도(버퍼). 월간 평가."""
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
def run(E,X,S=5,R=21,lo=None,hi=None,ban=()):
    rng=[d for d in all_dates if (lo is None or d>=lo) and (hi is None or d<=hi)]
    nav=1.0;peak=1.0;mdd=0.0;hold=[];lastpx={};rets=[];turn=0;nreb=0
    for k,d in enumerate(rng):
        if hold and lastpx:
            rs=[(px(t,d)/lastpx[t]-1) for t in hold if px(t,d) and lastpx.get(t)]
            if rs:
                r=np.mean(rs);nav*=(1+r);peak=max(peak,nav);mdd=min(mdd,nav/peak-1);rets.append(r)
                for t in hold:
                    if px(t,d):lastpx[t]=px(t,d)
        if k%R==0:
            ranked=[(t,gap(t,d)) for t in elig.get(d,{}) if gap(t,d) is not None and t in PX.columns and t not in ban]
            ranked.sort(key=lambda x:-x[1])
            rank={t:i+1 for i,(t,_) in enumerate(ranked)}
            prev=set(hold)
            hold=[t for t in hold if rank.get(t,9999)<=X]        # 이탈: 순위>X면 매도
            for t,_ in ranked:                                    # 진입: 순위≤E, 빈슬롯 채움
                if len(hold)>=S: break
                if rank[t]<=E and t not in hold: hold.append(t)
            hold=[t for t in hold if px(t,d)]
            turn+=len(set(hold)-prev);nreb+=1
            lastpx={t:px(t,d) for t in hold}
    vol=np.std(rets)*np.sqrt(252) if len(rets)>2 else 1e-9
    sh=(((nav)**(252/len(rng))-1))/vol if vol>0 else 0
    return (nav-1)*100,mdd*100,sh,turn/max(nreb,1)
print("=== S=5 고정. E(진입) × X(이탈) 그리드 — 누적%/MDD%/Sharpe ===")
print(f"{'E\\X':>4}",*[f"{x:>15}" for x in [5,8,10,15,20]])
for E in [3,5,7]:
    row=[]
    for X in [5,8,10,15,20]:
        if X<E: row.append(" "*15); continue
        cum,mdd,sh,tn=run(E,X)
        row.append(f"{cum:>4.0f}/{mdd:>4.0f}/{sh:>3.1f}")
    print(f"{E:>4}",*[f"{v:>15}" for v in row])
print("\n=== 상위 후보 walk-forward (전반/후반) ===")
for E,X in [(5,5),(5,10),(5,15),(3,10),(7,15)]:
    a=run(E,X,lo=all_dates[0],hi='2026-04-21');b=run(E,X,lo='2026-04-21',hi=all_dates[-1])
    f=run(E,X)
    print(f"  E{E}/X{X}/S5: 전체 {f[0]:+.0f}%(MDD{f[1]:.0f},Sh{f[2]:.1f},회전{f[3]:.0f}) | 전반 {a[0]:+.0f}%(Sh{a[2]:.1f}) 후반 {b[0]:+.0f}%(Sh{b[2]:.1f})")
print("\n=== 최선 후보 LOWO ===")
for E,X in [(5,10),(5,15)]:
    full=run(E,X)[0]
    worst=min(run(E,X,ban=(w,))[0]-full for w in ['MU','SNDK','COHR','LITE','CIEN'])
    print(f"  E{E}/X{X}: worst-LOWO {worst:+.0f}p (전체 {full:+.0f}%)")
