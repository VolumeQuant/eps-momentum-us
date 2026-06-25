# -*- coding: utf-8 -*-
"""steelman: rank를 버리지 않고 gap을 혼합(z-blend)하면 2슬롯서 사는가 + LOWO"""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\pxU.pkl'); pit_eps=pickle.load(open(SP+r'\pit_eps.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
EXIT_RANK=12; PE_HOLD=30
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date")]
data={}
for d in all_dates:
    data[d]={r[0]:{'p2':r[1],'nc':r[2],'n7':r[3],'n30':r[4],'n60':r[5],'n90':r[6],'dv':r[7]}
             for r in cur.execute("SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL",(d,))}
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def minseg(v):
    return min((a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0 for a,b in [(v['nc'],v['n7']),(v['n7'],v['n30']),(v['n30'],v['n60']),(v['n60'],v['n90'])])
def pit_teps(tk,d):
    rec=pit_eps.get(tk); val=None
    if not rec: return None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val
def gap(tk,d):
    nc=data.get(d,{}).get(tk,{}).get('nc'); te=pit_teps(tk,d)
    return nc/te if (nc and nc>0 and te and te>0.5) else None

def run(lam=0.0, pool=5, ban=()):
    tradable=set(PX.columns)
    pf={}; nav=1.0; peak=1.0; mdd=0.0
    for k in range(2,len(all_dates)):
        d,pv=all_dates[k],all_dates[k-1]; dd=data.get(d,{})
        ms={tk:minseg(v) for tk,v in dd.items()}
        wrank={tk:v['p2'] for tk,v in dd.items() if v.get('p2')}
        elig=[(tk,v['p2']) for tk,v in dd.items() if ms.get(tk,0)>=-2 and v.get('p2')]
        dr=0.0
        for tk,info in pf.items():
            w=info['weight']/100; cu,pp=px(tk,d),px(tk,pv)
            if cu and pp and pp>0: dr+=w*(cu-pp)/pp*100
        nav*=(1+dr/100); peak=max(peak,nav); mdd=min(mdd,nav/peak-1)
        for tk in list(pf.keys()):
            cp=px(tk,d)
            if cp is None: continue
            it=dd.get(tk)
            if it is None: continue
            rk,nc,m=it['p2'],it['nc'],minseg(it)
            sell=False
            if m<-2: sell=True
            elif rk>EXIT_RANK:
                pe=(cp/nc) if (nc and nc>0) else 999
                if pe>=PE_HOLD: sell=True
            if sell: del pf[tk]
        if len(pf)<2:
            free=sorted([s for s in range(2) if s not in {info['slot_idx'] for info in pf.values()}])
            cands=[tk for tk,_ in elig if tk not in pf and tk in tradable and tk not in ban
                   and ms.get(tk,-9)>=0 and wrank.get(tk,999)<=pool and (dd.get(tk,{}).get('dv') or 0)>=1000]
            if cands:
                # z-blend: rank 작을수록 좋음→ -rank z, gap 클수록 좋음→ +gap z
                rks=np.array([wrank[t] for t in cands],float)
                gps=np.array([gap(t,d) or np.nan for t in cands],float)
                def z(a):
                    m=np.nanmean(a); s=np.nanstd(a)
                    return (a-m)/s if s>0 else np.zeros_like(a)
                gz=z(gps); gz=np.nan_to_num(gz,nan=0.0)
                score = (-z(rks)) + lam*gz  # 높을수록 좋음
                order=[cands[i] for i in np.argsort(-score)]
                for tk in order:
                    if len(pf)>=2: break
                    ip=px(tk,d); ix=free.pop(0) if free else len(pf)
                    if ip: pf[tk]={'entry_price':ip,'slot_idx':ix,'weight':0}
            n=len(pf)
            for info in pf.values(): info['weight']=100/n if n else 0
    return (nav-1)*100, mdd*100

base=run(0.0)[0]
print(f"SANITY baseline(lam0) {base:+.1f}%\n")
print("=== rank+gap z-blend (lam=gap 가중치), pool=8 ===")
for lam in [0.0,0.2,0.3,0.5,1.0]:
    cum,mdd=run(lam,pool=8)
    print(f"  lam={lam}: {cum:+.1f}% (Δ{cum-base:+.1f}p) MDD{mdd:.1f}%")
print("\n=== 최선 lam의 LOWO ===")
best_lam=0.3
winners=['MU','SNDK','STX','NVDA','LITE','AVGO']; worst=999
for w in winners:
    b=run(0.0,ban=(w,))[0]; r=run(best_lam,pool=8,ban=(w,))[0]
    print(f"  -{w:5}: base {b:>7.1f}% | blend {r:>7.1f}% | Δ{r-b:>+7.1f}p"); worst=min(worst,r-b)
print(f"  worst-LOWO(lam{best_lam}): {worst:+.1f}p → {'통과' if worst>0 else '기각'}")
