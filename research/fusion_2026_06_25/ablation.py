# -*- coding: utf-8 -*-
"""결정적 검증: +116%가 융합(gap기여) 덕인가 그냥 슬롯3인가.
   momentum단독 / gap단독 / 융합(이중확인) — 전부 daily·슬롯3·동일 exit구조로 격리."""
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
    return grank,{t:cr for t,_,cr in cands}
def run(mode,NE=10,NX=30,S=3,lo=None,hi=None,ban=()):
    rng=[d for d in all_dates if (lo is None or d>=lo) and (hi is None or d<=hi)]
    nav=1.0;peak=1.0;mdd=0.0;pf={};lastpx={};rets=[]
    for d in rng:
        if pf and lastpx:
            rs=[(px(t,d)/lastpx[t]-1) for t in pf if px(t,d) and lastpx.get(t)]
            if rs:
                r=np.mean(rs);nav*=(1+r);peak=max(peak,nav);mdd=min(mdd,nav/peak-1);rets.append(r)
        for t in pf:
            if px(t,d): lastpx[t]=px(t,d)
        grank,mrank=ranks(d)
        for t in list(pf.keys()):
            v=D.get(d,{}).get(t)
            if v is None: continue
            m_out=mrank.get(t,9999)>NX; g_out=grank.get(t,9999)>NX
            if minseg(v)<-2: del pf[t]; continue
            if mode=='mom' and m_out: del pf[t]
            elif mode=='gap' and g_out: del pf[t]
            elif mode=='fusion' and (m_out and g_out): del pf[t]   # 보유=둘중하나
        if len(pf)<S:
            if mode=='mom':
                cand=sorted([t for t in mrank if mrank[t]<=NE and t not in pf and t not in ban],key=lambda t:mrank[t])
            elif mode=='gap':
                cand=sorted([t for t in grank if grank[t]<=NE and t not in pf and t not in ban],key=lambda t:grank[t])
            else:  # fusion: 이중확인
                cand=sorted([t for t in mrank if mrank[t]<=NE and grank.get(t,9999)<=NE and t not in pf and t not in ban],key=lambda t:mrank[t])
            for t in cand:
                if len(pf)>=S: break
                ip=px(t,d)
                if ip: pf[t]=ip;lastpx[t]=ip
    vol=np.std(rets)*np.sqrt(252) if len(rets)>2 else 1e-9
    sh=((nav**(252/len(rng))-1))/vol if vol>0 else 0
    return (nav-1)*100,mdd*100,sh,sorted(pf.keys())
print("=== 격리: 전부 daily·슬롯3·보유NX30 (entry만 다름) ===")
print(f"{'모드':14} {'수익':>7} {'MDD':>7} {'Sharpe':>7}  잔고")
for mode,lbl in [('mom','momentum 단독'),('gap','gap 단독'),('fusion','융합(이중확인)')]:
    cum,mdd,sh,h=run(mode)
    print(f"{lbl:14} {cum:>+6.0f}% {mdd:>6.0f}% {sh:>7.1f}  {','.join(h)}")
print("\n=== 슬롯2~4 비교 (모드별) ===")
for S in [2,3,4]:
    r1=run('mom',S=S);r2=run('gap',S=S);r3=run('fusion',S=S)
    print(f"  슬롯{S}: momentum {r1[0]:+.0f}% | gap {r2[0]:+.0f}% | 융합 {r3[0]:+.0f}%")
print("\n=== walk-forward 모드별 (슬롯3) ===")
for mode,lbl in [('mom','momentum'),('gap','gap'),('fusion','융합')]:
    a=run(mode,lo=all_dates[0],hi='2026-04-21');b=run(mode,lo='2026-04-21',hi=all_dates[-1])
    print(f"  {lbl:10}: 전반 {a[0]:+.0f}%(Sh{a[2]:.1f}) | 후반 {b[0]:+.0f}%(Sh{b[2]:.1f})")
