# -*- coding: utf-8 -*-
"""매일 체크 융합: 진입=이중확인(momentum AND gap), 보유=둘 중 하나만 살아도 연장, 매일.
   2슬롯이 SNDK를 daily로 잡은 그 메커니즘을 융합에 적용. 슬롯/임계 스윕."""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb')); DV=pd.read_pickle(SP+r'\dv137.pkl')
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
D={}
for d in all_dates:
    D[d]={r[0]:{'nc':r[1],'cr':r[2],'n7':r[3],'n30':r[4],'n60':r[5],'n90':r[6]} for r in cur.execute("SELECT ticker,ntm_current,composite_rank,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
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
def seg(a,b): return (a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0.0
def minseg(v): return min(seg(v['nc'],v['n7']),seg(v['n7'],v['n30']),seg(v['n30'],v['n60']),seg(v['n60'],v['n90']))
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
def run(NE,NX,S,lo=None,hi=None,verbose=False):
    rng=[d for d in all_dates if (lo is None or d>=lo) and (hi is None or d<=hi)]
    nav=1.0;peak=1.0;mdd=0.0;pf={};lastpx={};rets=[];events=[]
    for d in rng[1:] if not lo else rng:
        # MtM
        if pf and lastpx:
            rs=[(px(t,d)/lastpx[t]-1) for t in pf if px(t,d) and lastpx.get(t)]
            if rs:
                r=np.mean(rs);nav*=(1+r);peak=max(peak,nav);mdd=min(mdd,nav/peak-1);rets.append(r)
        for t in pf:
            if px(t,d): lastpx[t]=px(t,d)
        grank,mrank=ranks(d)
        # 이탈 (매일): EPS꺾임 OR (momentum>NX AND gap>NX 둘다식음)
        for t in list(pf.keys()):
            v=D.get(d,{}).get(t)
            if v is None: continue
            ms=minseg(v)
            both_fade = (mrank.get(t,9999)>NX and grank.get(t,9999)>NX)
            if ms<-2 or both_fade:
                del pf[t]
                if verbose: events.append((d,'SELL',t))
        # 진입 (매일): 이중확인(둘다≤NE), 슬롯 여유분
        if len(pf)<S:
            doub=sorted([t for t in mrank if mrank[t]<=NE and grank.get(t,9999)<=NE and t not in pf], key=lambda t:mrank[t])
            for t in doub:
                if len(pf)>=S: break
                ip=px(t,d)
                if ip:
                    pf[t]=1; lastpx[t]=ip
                    if verbose: events.append((d,'BUY',t))
    vol=np.std(rets)*np.sqrt(252) if len(rets)>2 else 1e-9
    sh=((nav**(252/len(rng))-1))/vol if vol>0 else 0
    return (nav-1)*100,mdd*100,sh,sorted(pf.keys()),events
print("=== 매일 융합 (진입 이중확인NE / 보유 둘중하나NX / 슬롯S) ===")
print(f"{'NE/NX/S':16} {'수익':>7} {'MDD':>7} {'Sharpe':>7}  잔고")
for S in [3,5,7,99]:
    for NE in [10,15]:
        cum,mdd,sh,h,_=run(NE,30,S)
        print(f"  진입{NE}/보유30/슬롯{S if S<99 else '∞'}   {cum:>+6.0f}% {mdd:>6.0f}% {sh:>7.1f}  {','.join(h[:8])}{'...' if len(h)>8 else ''}")
print("\n=== SNDK 잡히나 확인 (진입15/보유30/슬롯7) ===")
cum,mdd,sh,h,ev=run(15,30,7,verbose=True)
sndk_ev=[e for e in ev if e[2]=='SNDK']
print(f"  SNDK 이벤트: {sndk_ev}")
print(f"  현재잔고({len(h)}): {','.join(h)}")
print(f"  ★ {cum:+.0f}% MDD{mdd:.0f}% Sharpe{sh:.1f}")
print("\n=== walk-forward (진입15/보유30/슬롯7) ===")
a=run(15,30,7,lo=all_dates[0],hi='2026-04-21');b=run(15,30,7,lo='2026-04-21',hi=all_dates[-1])
print(f"  전반 {a[0]:+.0f}%(Sh{a[2]:.1f}) | 후반 {b[0]:+.0f}%(Sh{b[2]:.1f})")
print("\n비교: 2슬롯 +217%/-22% | gap sleeve +67%/-18% | 월간융합 +66%")
