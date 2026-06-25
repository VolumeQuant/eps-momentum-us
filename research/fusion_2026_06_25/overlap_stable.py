# -*- coding: utf-8 -*-
"""#1 이중확인 안정화: 이중확인 우선 + 부족분 gap으로 채워 최소 K 유지. BT+WF+거래내역"""
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
def picks(d,N,K):
    cands=[(t,gap(t,d),D[d][t]['cr']) for t in D.get(d,{}) if gap(t,d) is not None and t in PX.columns and (dvol(t,d) or 0)>=1000]
    if not cands: return [],0
    by_gap=sorted(cands,key=lambda x:-x[1]); grank={t:i+1 for i,(t,_,_) in enumerate(by_gap)}
    double=sorted([t for t,g,cr in cands if cr<=N and grank[t]<=N], key=lambda t:D[d][t]['cr'])
    n_double=len(double)
    hold=double[:K]
    if len(hold)<K:                    # 부족분 gap-top으로 채움
        for t,_,_ in by_gap:
            if len(hold)>=K: break
            if t not in hold: hold.append(t)
    return [t for t in hold if px(t,d)],n_double
def run(N,K,lo=None,hi=None,verbose=False):
    rng=[d for d in all_dates if (lo is None or d>=lo) and (hi is None or d<=hi)]
    nav=1.0;peak=1.0;mdd=0.0;hold=[];lastpx={};rebm=None;rets=[];log=[]
    for d in rng:
        if hold and lastpx:
            rs=[(px(t,d)/lastpx[t]-1) for t in hold if px(t,d) and lastpx.get(t)]
            if rs:
                r=np.mean(rs);nav*=(1+r);peak=max(peak,nav);mdd=min(mdd,nav/peak-1);rets.append(r)
                for t in hold:
                    if px(t,d):lastpx[t]=px(t,d)
        m=d[:7]
        if m!=rebm or not hold:
            new,nd=picks(d,N,K)
            if new:
                if verbose: log.append((d,round((nav-1)*100,1),[t for t in hold if t not in new],[t for t in new if t not in hold],new[:],nd))
                hold=new;lastpx={t:px(t,d) for t in new};rebm=m
    vol=np.std(rets)*np.sqrt(252) if len(rets)>2 else 1e-9
    sh=((nav**(252/len(rng))-1))/vol if vol>0 else 0
    return (nav-1)*100,mdd*100,sh,hold,log
print("=== #1 안정화 이중확인 (이중확인 우선 + gap 채움, 최소 K) ===")
print(f"{'N(임계)×K(최소)':18} {'수익':>7} {'MDD':>7} {'Sharpe':>7}")
for N in [10,15,20]:
    for K in [5,7]:
        cum,mdd,sh,h,_=run(N,K)
        print(f"  top{N} × 최소{K:<2}      {cum:>+6.0f}% {mdd:>6.0f}% {sh:>7.1f}")
print("\n=== 최선(top20×5) walk-forward ===")
a=run(20,5,lo=all_dates[0],hi='2026-04-21');b=run(20,5,lo='2026-04-21',hi=all_dates[-1])
print(f"  전반 {a[0]:+.0f}%(Sh{a[2]:.1f}) | 후반 {b[0]:+.0f}%(Sh{b[2]:.1f})")
print("\n=== top20×5 거래내역 ===")
cum,mdd,sh,h,log=run(20,5,verbose=True)
for d,nv,s,b,hold,nd in log:
    print(f"{d} (NAV{nv:+.0f}%, 이중확인 {nd}개) OUT[{','.join(s) or '-'}] IN[{','.join(b) or '-'}] → {','.join(hold)}")
print(f"\n현재 잔고: {', '.join(h)}")
print(f"★ 수익률 {cum:+.1f}% · MDD {mdd:.1f}% · Sharpe {sh:.1f}")
print("\n비교: 2슬롯 +217%/-22% | gap sleeve +67%/-18% | SPY +7.7%")
