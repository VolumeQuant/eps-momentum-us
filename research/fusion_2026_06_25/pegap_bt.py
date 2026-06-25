# -*- coding: utf-8 -*-
"""PE_gap 종목선택 오버레이 BT — 전략에 넣어 LOWO까지 통과하나. PIT gap 사용(look-ahead clean)."""
import sqlite3, pandas as pd, pickle, numpy as np, yfinance as yf, os
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\pxU.pkl')
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

# PIT trailing EPS (캐시)
cache=SP+r'\pit_eps.pkl'
if os.path.exists(cache):
    pit_eps=pickle.load(open(cache,'rb'))
else:
    pit_eps={}
    for tk in PX.columns:
        try:
            qi=yf.Ticker(tk).quarterly_income_stmt
            if qi is None or qi.empty: continue
            row=None
            for k in ['Diluted EPS','Basic EPS']:
                if k in qi.index: row=qi.loc[k]; break
            if row is None: continue
            q=row.dropna().sort_index(); qe=list(q.items())
            rec=[((qe[j][0]+pd.Timedelta(days=45)).strftime('%Y-%m-%d'),float(sum(qe[j-k][1] for k in range(4)))) for j in range(3,len(qe))]
            if rec: pit_eps[tk]=rec
        except Exception: continue
    pickle.dump(pit_eps,open(cache,'wb'))
def pit_teps(tk,d):
    rec=pit_eps.get(tk); val=None
    if not rec: return None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val
def gap(tk,d):
    nc=data.get(d,{}).get(tk,{}).get('nc'); te=pit_teps(tk,d)
    if nc and nc>0 and te and te>0.5: return nc/te
    return None

def run(select='rank', pool=5, gap_min=None, ban=(), d_lo=None, d_hi=None):
    tradable=set(PX.columns)
    rng=[d for d in all_dates if (d_lo is None or d>=d_lo) and (d_hi is None or d<=d_hi)]
    pf={}; nav=1.0; peak=1.0; mdd=0.0
    for k in range(2,len(rng)):
        d,pv=rng[k],rng[k-1]; dd=data.get(d,{})
        ms={tk:minseg(v) for tk,v in dd.items()}
        wrank={tk:v['p2'] for tk,v in dd.items() if v.get('p2')}
        elig=sorted([(tk,v['p2']) for tk,v in dd.items() if ms.get(tk,0)>=-2 and v.get('p2')],key=lambda x:x[1])
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
            used={info['slot_idx'] for info in pf.values()}
            free=sorted([s for s in range(2) if s not in used])
            cands=[tk for tk,_ in elig if tk not in pf and tk in tradable and tk not in ban
                   and ms.get(tk,-9)>=0 and wrank.get(tk,999)<=pool and (dd.get(tk,{}).get('dv') or 0)>=1000]
            if gap_min is not None:
                cands=[tk for tk in cands if (gap(tk,d) or 0)>=gap_min]
            if select=='rank':
                cands.sort(key=lambda t:wrank.get(t,999))
            elif select=='gap':
                cands.sort(key=lambda t:-(gap(t,d) or 0))  # 고gap 우선
            for tk in cands:
                if len(pf)>=2: break
                ip=px(tk,d); ix=free.pop(0) if free else len(pf)
                if ip: pf[tk]={'entry_price':ip,'slot_idx':ix,'weight':0}
            n=len(pf)
            for info in pf.values(): info['weight']=100/n if n else 0
    return {'cum':(nav-1)*100,'mdd':mdd*100,'hold':sorted(pf.keys())}

base=run()
print(f"SANITY baseline {base['cum']:+.1f}% MDD{base['mdd']:.1f}% (목표~+216.9%) {base['hold']}")
print("\n=== gap 오버레이 변형 (진입선택만 변경, 청산룰 동일) ===")
def L(lbl,r): print(f"  {lbl:26} {r['cum']:>8.1f}% (Δ{r['cum']-base['cum']:>+7.1f}p) MDD{r['mdd']:>6.1f}% {r['hold']}")
L('baseline (rank, pool5)', base)
L('gap정렬 pool5', run(select='gap',pool=5))
L('gap정렬 pool8', run(select='gap',pool=8))
L('gap정렬 pool10', run(select='gap',pool=10))
L('rank + gap필터≥1.3', run(select='rank',pool=5,gap_min=1.3))
L('rank + gap필터≥1.5', run(select='rank',pool=5,gap_min=1.5))

print("\n=== LOWO (가장 좋은 변형 — winner 빼도 살아남나) ===")
best=('gap',8)
winners=['MU','SNDK','STX','NVDA','LITE','AVGO']
worst=999
for w in winners:
    b=run(ban=(w,))['cum']; r=run(select=best[0],pool=best[1],ban=(w,))['cum']
    print(f"  -{w:5}: base {b:>7.1f}% | gap-pool8 {r:>7.1f}% | Δ{r-b:>+7.1f}p")
    worst=min(worst,r-b)
print(f"  worst-LOWO: {worst:+.1f}p → {'통과' if worst>0 else '기각'}")

print("\n=== walk-forward 3블록 (gap pool8) ===")
for lo,hi,lbl in [(None,'2026-03-20','블록1'),('2026-03-20','2026-05-08','블록2'),('2026-05-08',None,'블록3')]:
    b=run(d_lo=lo,d_hi=hi)['cum']; r=run(select='gap',pool=8,d_lo=lo,d_hi=hi)['cum']
    print(f"  {lbl}: base {b:>7.1f}% | gap {r:>7.1f}% | Δ{r-b:>+7.1f}p")
