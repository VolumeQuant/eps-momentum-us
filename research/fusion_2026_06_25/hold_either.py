# -*- coding: utf-8 -*-
"""융합: 진입=이중확인(momentum AND gap 둘다 top), 보유=둘 중 하나만 top이어도 연장.
   SNDK처럼 momentum 식어도 gap 살아있으면 보유 유지. NE(진입)/NX(이탈) 스윕."""
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
def ranks(d):
    cands=[(t,gap(t,d),D[d][t]['cr']) for t in D.get(d,{}) if gap(t,d) is not None and t in PX.columns and (dvol(t,d) or 0)>=1000]
    by_gap=sorted(cands,key=lambda x:-x[1]); grank={t:i+1 for i,(t,_,_) in enumerate(by_gap)}
    mrank={t:cr for t,_,cr in cands}
    return grank,mrank
def run(NE,NX,lo=None,hi=None,verbose=False):
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
            grank,mrank=ranks(d)
            # 이탈: 보유 중 momentum>NX AND gap>NX (둘다 식음)이면 매도
            kept=[t for t in hold if (mrank.get(t,9999)<=NX or grank.get(t,9999)<=NX)]
            # 진입: 이중확인(둘다 ≤NE) 신규 추가
            doub=sorted([t for t in mrank if mrank[t]<=NE and grank.get(t,9999)<=NE], key=lambda t:mrank[t])
            for t in doub:
                if t not in kept: kept.append(t)
            new=[t for t in kept if px(t,d)]
            if new:
                if verbose: log.append((d,round((nav-1)*100,1),[t for t in hold if t not in new],[t for t in new if t not in hold],new[:]))
                hold=new;lastpx={t:px(t,d) for t in new};rebm=m
    vol=np.std(rets)*np.sqrt(252) if len(rets)>2 else 1e-9
    sh=((nav**(252/len(rng))-1))/vol if vol>0 else 0
    return (nav-1)*100,mdd*100,sh,hold,log
print("=== 진입 이중확인(NE) / 보유 둘중하나(NX) 스윕 ===")
print(f"{'NE/NX':12} {'수익':>7} {'MDD':>7} {'Sharpe':>7}")
for NE in [15,20]:
    for NX in [20,30,40,50]:
        cum,mdd,sh,h,_=run(NE,NX)
        print(f"  진입{NE}/보유{NX}    {cum:>+6.0f}% {mdd:>6.0f}% {sh:>7.1f}")
print("\n=== 최선 후보 walk-forward + SNDK 보유연장 확인 ===")
best=(20,40)
a=run(*best,lo=all_dates[0],hi='2026-04-21');b=run(*best,lo='2026-04-21',hi=all_dates[-1])
print(f"  진입{best[0]}/보유{best[1]}: 전반 {a[0]:+.0f}%(Sh{a[2]:.1f}) | 후반 {b[0]:+.0f}%(Sh{b[2]:.1f})")
cum,mdd,sh,h,log=run(*best,verbose=True)
print(f"\n  거래내역:")
for d,nv,s,bb,hold in log:
    print(f"   {d}(NAV{nv:+.0f}%) OUT[{','.join(s) or '-'}] IN[{','.join(bb) or '-'}] 보유{len(hold)}: {','.join(hold)}")
print(f"\n  현재잔고: {', '.join(h)}")
print(f"  ★ {cum:+.1f}% · MDD{mdd:.0f}% · Sharpe{sh:.1f}")
print("\n비교: 2슬롯 +217%/-22% | gap sleeve +67%/-18% | 교집합최선 +57%")
