# -*- coding: utf-8 -*-
"""이중확인 전략 거래내역 — 2월부터 월별 보유·매매·수익"""
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
def both_picks(d,N):
    cands=[(t,gap(t,d),D[d][t]['cr']) for t in D.get(d,{}) if gap(t,d) is not None and t in PX.columns and (dvol(t,d) or 0)>=1000]
    if not cands: return []
    by_gap=sorted(cands,key=lambda x:-x[1]); grank={t:i+1 for i,(t,_,_) in enumerate(by_gap)}
    both=[(t,cr,gap(t,d)) for t,g,cr in cands if cr<=N and grank[t]<=N]
    both.sort(key=lambda x:x[1])
    return [(t,cr,g) for t,cr,g in both]
def run(N,verbose=False):
    nav=1.0;peak=1.0;mdd=0.0;hold=[];lastpx={};rebm=None;log=[]
    for d in all_dates:
        if hold and lastpx:
            rs=[(px(t,d)/lastpx[t]-1) for t in hold if px(t,d) and lastpx.get(t)]
            if rs:
                nav*=(1+np.mean(rs));peak=max(peak,nav);mdd=min(mdd,nav/peak-1)
                for t in hold:
                    if px(t,d):lastpx[t]=px(t,d)
        m=d[:7]
        if m!=rebm or not hold:
            picks=both_picks(d,N); new=[t for t,_,_ in picks if px(t,d)]
            if new:
                if verbose: log.append((d,round((nav-1)*100,1),[t for t in hold if t not in new],[t for t in new if t not in hold],picks))
                hold=new;lastpx={t:px(t,d) for t in new};rebm=m
    return (nav-1)*100,mdd*100,hold,log

print("=== 이중확인 전략 (top15, 월간, $1B) — 거래내역 ===")
cum,mdd,hold,log=run(15,verbose=True)
print(f"{'날짜':10} {'NAV%':>6}  매도→매수 / 보유(이중확인 종목)")
for d,nv,sells,buys,picks in log:
    names=', '.join(f"{t}(cr{cr},gap{g:.1f})" for t,cr,g in picks)
    print(f"{d:10} {nv:>+6.1f}  OUT[{','.join(sells) or '-'}] IN[{','.join(buys) or '-'}]")
    print(f"{'':18}→ 보유: {names}")
print(f"\n현재 잔고(06-24): {', '.join(hold)}")
print(f"★ 수익률 {cum:+.1f}% · MDD {mdd:.1f}%")
print("\n=== 다른 N 요약 ===")
for N in [10,20]:
    cm,md,h,_=run(N)
    print(f"  top{N}: {cm:+.0f}% · MDD{md:.0f}% · 현재[{', '.join(h)}]")
print("\n=== 비교 ===")
print("  2슬롯 +217%/MDD-22% | gap sleeve +67%/MDD-18% | SPY +7.7%")
