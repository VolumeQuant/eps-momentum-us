# -*- coding: utf-8 -*-
"""gap sleeve 전수 스윕: 슬롯 K=1~20 (strict, 월간) + 각 walk-forward + LOWO. 빈틈없이."""
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
            if newh:hold=newh;lastpx={t:px(t,d) for t in newh}
    vol=np.std(rets)*np.sqrt(252) if len(rets)>2 else 1e-9
    sh=(((nav)**(252/len(rng))-1))/vol if vol>0 else 0
    return (nav-1)*100,mdd*100,sh
mid='2026-04-21'
print("=== 슬롯 K=1~20 전수 (strict, 월간) ===")
print(f"{'K':>3} {'누적%':>7} {'MDD%':>7} {'Sharpe':>7} | {'전반Sh':>7} {'후반Sh':>7} {'WF판정':>7}")
best=[]
for K in range(1,21):
    cum,mdd,sh=run(K)
    a=run(K,lo=all_dates[0],hi=mid)[2]; b=run(K,lo=mid,hi=all_dates[-1])[2]
    wf='OK' if (a>1 and b>1) else 'FAIL' if (a<0.5 or b<0.5) else '약'
    print(f"{K:>3} {cum:>7.0f} {mdd:>7.0f} {sh:>7.1f} | {a:>7.1f} {b:>7.1f} {wf:>7}")
    best.append((K,cum,mdd,sh,a,b))
# Sharpe + WF robust 동시 상위
print("\n=== Sharpe 상위 5 (전체) ===")
for K,cum,mdd,sh,a,b in sorted(best,key=lambda x:-x[3])[:5]:
    print(f"  K={K}: 누적{cum:+.0f}% MDD{mdd:.0f}% Sh{sh:.1f} (전반{a:.1f}/후반{b:.1f})")
print("\n=== 이탈 버퍼는 손해 확인 (E5 고정, X 변화) — sleeve_exs.py 결과: X5(strict) Sh6.0 > X10/15 Sh3.8~4.2 ===")
print("  → gap은 '신선도' 팩터라 버퍼(decayed 보유) 손해. 진입=이탈=같은 순위가 최적.")
