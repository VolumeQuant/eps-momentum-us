# -*- coding: utf-8 -*-
"""이중확인(momentum top-N AND gap top-N) 전략 BT — 2슬롯·sleeve와 비교 + WF"""
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
def both_picks(d,N,K=None,dv1b=True):
    cands=[(t,gap(t,d),D[d][t]['cr']) for t in D.get(d,{}) if gap(t,d) is not None and t in PX.columns and (not dv1b or (dvol(t,d) or 0)>=1000)]
    if not cands: return []
    by_gap=sorted(cands,key=lambda x:-x[1]); grank={t:i+1 for i,(t,_,_) in enumerate(by_gap)}
    both=[(t,g,cr) for t,g,cr in cands if cr<=N and grank[t]<=N]
    both.sort(key=lambda x:x[2])   # momentum 등수순
    picks=[t for t,_,_ in both]
    return picks[:K] if K else picks
def run(N,K=None,R=21,lo=None,hi=None):
    rng=[d for d in all_dates if (lo is None or d>=lo) and (hi is None or d<=hi)]
    nav=1.0;peak=1.0;mdd=0.0;hold=[];lastpx={};rets=[];sizes=[]
    for k,d in enumerate(rng):
        if hold and lastpx:
            rs=[(px(t,d)/lastpx[t]-1) for t in hold if px(t,d) and lastpx.get(t)]
            if rs:
                r=np.mean(rs);nav*=(1+r);peak=max(peak,nav);mdd=min(mdd,nav/peak-1);rets.append(r)
                for t in hold:
                    if px(t,d):lastpx[t]=px(t,d)
        if k%R==0:
            new=both_picks(d,N,K)
            new=[t for t in new if px(t,d)]
            if new: hold=new;lastpx={t:px(t,d) for t in new};sizes.append(len(new))
            # 이중확인 0개면 직전 보유 유지(현금 대신, 드묾)
    vol=np.std(rets)*np.sqrt(252) if len(rets)>2 else 1e-9
    sh=((nav**(252/len(rng))-1))/vol if vol>0 else 0
    return (nav-1)*100,mdd*100,sh,np.mean(sizes) if sizes else 0
print(f"{'전략':28} {'수익':>7} {'MDD':>7} {'Sharpe':>7} {'평균종목':>7}")
print(f"{'(참고) 2슬롯':28} {'+217':>7} {'-22':>7} {'29.7':>7} {'2':>7}")
print(f"{'(참고) gap sleeve K7':28} {'+67':>7} {'-18':>7} {'5.2':>7} {'7':>7}")
for N in [10,15,20]:
    for K in [None,5,7]:
        cum,mdd,sh,sz=run(N,K)
        lbl=f"이중확인 top{N}" + (f" (최대{K})" if K else " (전부)")
        print(f"{lbl:28} {cum:>+6.0f}% {mdd:>6.0f}% {sh:>7.1f} {sz:>7.1f}")
print("\n=== walk-forward (이중확인 top15, 전부) ===")
a=run(15,None,lo=all_dates[0],hi='2026-04-21');b=run(15,None,lo='2026-04-21',hi=all_dates[-1])
print(f"  전반 {a[0]:+.0f}%(Sh{a[2]:.1f}) | 후반 {b[0]:+.0f}%(Sh{b[2]:.1f})")
print("\n=== 6/24 현재 이중확인 종목 (top15) ===")
print(', '.join(both_picks('2026-06-24',15)) or '없음')
