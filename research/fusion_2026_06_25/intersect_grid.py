# -*- coding: utf-8 -*-
"""융합 = 교집합 전부 보유. momentum top-N1 × gap top-N2 2D 스윕으로 스위트스팟."""
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
def intersect(d,N1,N2):
    cands=[(t,gap(t,d),D[d][t]['cr']) for t in D.get(d,{}) if gap(t,d) is not None and t in PX.columns and (dvol(t,d) or 0)>=1000]
    if not cands: return []
    by_gap=sorted(cands,key=lambda x:-x[1]); grank={t:i+1 for i,(t,_,_) in enumerate(by_gap)}
    inter=[(t,cr) for t,g,cr in cands if cr<=N1 and grank[t]<=N2]   # momentum top N1 ∩ gap top N2
    inter.sort(key=lambda x:x[1])
    return [t for t,_ in inter]
def run(N1,N2,lo=None,hi=None):
    rng=[d for d in all_dates if (lo is None or d>=lo) and (hi is None or d<=hi)]
    nav=1.0;peak=1.0;mdd=0.0;hold=[];lastpx={};rebm=None;rets=[];sizes=[]
    for d in rng:
        if hold and lastpx:
            rs=[(px(t,d)/lastpx[t]-1) for t in hold if px(t,d) and lastpx.get(t)]
            if rs:
                r=np.mean(rs);nav*=(1+r);peak=max(peak,nav);mdd=min(mdd,nav/peak-1);rets.append(r)
                for t in hold:
                    if px(t,d):lastpx[t]=px(t,d)
        m=d[:7]
        if m!=rebm or not hold:
            new=[t for t in intersect(d,N1,N2) if px(t,d)]
            if new: hold=new;lastpx={t:px(t,d) for t in new};sizes.append(len(new));rebm=m
    vol=np.std(rets)*np.sqrt(252) if len(rets)>2 else 1e-9
    sh=((nav**(252/len(rng))-1))/vol if vol>0 else 0
    return (nav-1)*100,mdd*100,sh,np.mean(sizes) if sizes else 0,min(sizes) if sizes else 0
Ns=[15,20,25,30,40,50]
print("=== 교집합 수익률 그리드 (행=momentum N1, 열=gap N2) ===")
print("    "+" ".join(f"g{n:>4}" for n in Ns))
for N1 in Ns:
    row=[]
    for N2 in Ns:
        cum,mdd,sh,avg,mn=run(N1,N2)
        row.append(f"{cum:>4.0f}")
    print(f"m{N1:>3} "+" ".join(f"{v:>5}" for v in row))
print("\n=== 상세 (수익/MDD/Sharpe/평균종목/최소종목) 주요 셀 ===")
for N1,N2 in [(20,20),(30,30),(40,40),(30,20),(20,30),(40,30),(30,40),(50,30),(30,50)]:
    cum,mdd,sh,avg,mn=run(N1,N2)
    print(f"  m{N1}×g{N2}: {cum:+.0f}% MDD{mdd:.0f}% Sh{sh:.1f} 평균{avg:.1f}종목(최소{mn})")
