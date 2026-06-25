# -*- coding: utf-8 -*-
"""리밸 주기 전수: 왜 월간? 매일(R1)~분기(R42) 비교. K=7. 누적/MDD/Sharpe/회전/WF."""
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
def run(K,R,lo=None,hi=None):
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
            ranked=sorted([(t,gap(t,d)) for t in elig.get(d,{}) if gap(t,d) is not None and t in PX.columns],key=lambda x:-x[1])
            newh=[t for t,_ in ranked[:K] if px(t,d)]
            if newh:
                turn+=len(set(newh)-set(hold));nreb+=1
                hold=newh;lastpx={t:px(t,d) for t in newh}
    vol=np.std(rets)*np.sqrt(252) if len(rets)>2 else 1e-9
    sh=(((nav)**(252/len(rng))-1))/vol if vol>0 else 0
    # 연환산 회전 (리밸당 교체 × 연리밸횟수 / K)
    yturn=turn/max(len(rng),1)*252/max(K,1)
    return (nav-1)*100,mdd*100,sh,yturn
print("=== K=7 리밸 주기 전수 (R=거래일) ===")
print(f"{'R(주기)':>10} {'누적%':>7} {'MDD%':>7} {'Sharpe':>7} {'연회전(배)':>10}")
for R,lbl in [(1,'매일'),(2,'2일'),(3,'3일'),(5,'주간'),(10,'2주'),(21,'월간'),(42,'분기')]:
    cum,mdd,sh,yt=run(7,R)
    print(f"{lbl:>10} {cum:>7.0f} {mdd:>7.0f} {sh:>7.1f} {yt:>10.1f}")
print("\n=== walk-forward (K=7) ===")
mid='2026-04-21'
for R,lbl in [(1,'매일'),(5,'주간'),(21,'월간')]:
    a=run(7,R,lo=all_dates[0],hi=mid);b=run(7,R,lo=mid,hi=all_dates[-1])
    print(f"  {lbl}: 전반 {a[0]:+.0f}%(Sh{a[2]:.1f}) | 후반 {b[0]:+.0f}%(Sh{b[2]:.1f})")
print("\n=== gap 신호 변화 속도: 일별 top7 교체율 ===")
prev=None;ch=[]
for d in all_dates:
    ranked=sorted([(t,gap(t,d)) for t in elig.get(d,{}) if gap(t,d) is not None and t in PX.columns],key=lambda x:-x[1])
    top=set(t for t,_ in ranked[:7])
    if prev is not None and top: ch.append(len(top-prev))
    if top: prev=top
print(f"  하루 평균 top7 교체 {np.mean(ch):.2f}종목/일 (모멘텀 part2와 비교용)")
