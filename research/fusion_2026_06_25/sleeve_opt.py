# -*- coding: utf-8 -*-
"""gap sleeve 파라미터 최적화: K(종목수)·R(리밸주기)·buffer(이탈)·weight(가중).
   91일 단일강세장 = 과적합주의 → plateau + walk-forward로 판정. MDD/Sharpe 공동 1차."""
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

def run(K=15,R=21,buf=1.0,weight='equal',lo=None,hi=None):
    rng=[d for d in all_dates if (lo is None or d>=lo) and (hi is None or d<=hi)]
    nav=1.0; peak=1.0; mdd=0.0; hold={}; lastpx={}; rets=[]; turn=0; nreb=0
    for k,d in enumerate(rng):
        if hold and lastpx:
            w=sum(hold.values())
            r=sum(hold[t]/w*(px(t,d)/lastpx[t]-1) for t in hold if px(t,d) and lastpx.get(t)) if w>0 else 0
            nav*=(1+r); peak=max(peak,nav); mdd=min(mdd,nav/peak-1); rets.append(r)
            for t in hold:
                if px(t,d): lastpx[t]=px(t,d)
        if k%R==0:
            ranked=sorted([(t,gap(t,d)) for t in elig.get(d,{}) if gap(t,d) is not None and t in PX.columns],key=lambda x:-x[1])
            rank={t:i+1 for i,(t,_) in enumerate(ranked)}
            topK=[t for t,_ in ranked[:K]]
            keep=[t for t in hold if rank.get(t,9999)<=K*buf]      # 버퍼: K*buf 안이면 유지
            new=[t for t in topK if t not in keep]
            newh=(keep+new)[:K]
            newh=[t for t in newh if px(t,d)]
            turn+=len(set(newh)-set(hold)); nreb+=1
            if weight=='gap':
                gv={t:(dict(ranked).get(t) or 1) for t in newh}
                hold={t:gv[t] for t in newh}
            else:
                hold={t:1.0 for t in newh}
            lastpx={t:px(t,d) for t in newh}
    vol=np.std(rets)*np.sqrt(252) if len(rets)>2 else 1e-9
    sharpe=( ((nav)**(252/len(rng))-1) )/vol if vol>0 else 0
    return {'cum':(nav-1)*100,'mdd':mdd*100,'vol':vol*100,'sharpe':sharpe,'turn':turn/max(nreb,1)}

print("=== K × R 그리드 (equal, buf1.0) — 누적 / MDD / Sharpe ===")
print(f"{'K\\R':>5}", *[f"{r:>16}" for r in [5,10,21,42]])
for K in [10,15,20,25,30]:
    row=[]
    for R in [5,10,21,42]:
        x=run(K,R)
        row.append(f"{x['cum']:>5.0f}/{x['mdd']:>4.0f}/{x['sharpe']:>3.1f}")
    print(f"{K:>5}", *[f"{v:>16}" for v in row])

print("\n=== 이탈 버퍼 (K=20,R=21) ===")
for buf in [1.0,1.5,2.0,3.0]:
    x=run(20,21,buf=buf); print(f"  buf={buf}: 누적{x['cum']:+.0f}% MDD{x['mdd']:.0f}% Sharpe{x['sharpe']:.2f} 회전{x['turn']:.0f}/리밸")

print("\n=== 가중 (K=20,R=21,buf1.5) ===")
for w in ['equal','gap']:
    x=run(20,21,1.5,w); print(f"  {w}: 누적{x['cum']:+.0f}% MDD{x['mdd']:.0f}% Sharpe{x['sharpe']:.2f}")

print("\n=== walk-forward 검증 (전반 2/12~4/21 vs 후반 4/21~6/24) ===")
mid='2026-04-21'
for K in [10,15,20,25]:
    a=run(K,21,lo=all_dates[0],hi=mid); b=run(K,21,lo=mid,hi=all_dates[-1])
    print(f"  K={K}: 전반 {a['cum']:+.0f}%(Sh{a['sharpe']:.1f}) | 후반 {b['cum']:+.0f}%(Sh{b['sharpe']:.1f})")
