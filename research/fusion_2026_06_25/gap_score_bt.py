# -*- coding: utf-8 -*-
"""정공법: 점수에 W*gap_z 더해 재랭킹(KR 팩터 추가 방식). W 양/음 스윕 + LOWO.
   new_score = -z(part2_rank) + W*z(gap_pit) → 재랭킹 → 2슬롯 전략. W=0이면 baseline 정확."""
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
def seg(a,b): return (a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0.0
def minseg(v): return min(seg(v['nc'],v['n7']),seg(v['n7'],v['n30']),seg(v['n30'],v['n60']),seg(v['n60'],v['n90']))
def pit_teps(tk,d):
    rec=pit_eps.get(tk); val=None
    if not rec: return None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val
def gap(tk,d):
    v=data.get(d,{}).get(tk)
    if not v or not v['nc'] or v['nc']<=0: return np.nan
    te=pit_teps(tk,d)
    return v['nc']/te if (te and te>0.5) else np.nan

# 일별 new_rank 사전계산 (W별)
def newranks(W):
    nr={}
    for d in all_dates:
        dd=data.get(d,{})
        tks=[t for t in dd if dd[t].get('p2')]
        if not tks: nr[d]={}; continue
        p2=np.array([dd[t]['p2'] for t in tks],float)
        gp=np.array([gap(t,d) for t in tks],float)
        # log gap, winsorize, z
        lg=np.log(np.clip(gp,0.3,20))
        def z(a):
            m=np.nanmean(a); s=np.nanstd(a)
            return np.nan_to_num((a-m)/s,nan=0.0) if s>0 else np.zeros_like(a)
        score = -z(p2) + W*z(lg)   # 높을수록 좋음
        order=np.argsort(-score)
        nr[d]={tks[order[i]]:i+1 for i in range(len(tks))}  # new rank 1..N
    return nr

def run(W, ban=()):
    nr=newranks(W); tradable=set(PX.columns); pf={}; nav=1.0; peak=1.0; mdd=0.0
    for k in range(2,len(all_dates)):
        d,pv=all_dates[k],all_dates[k-1]; dd=data.get(d,{}); rk=nr[d]
        ms={tk:minseg(v) for tk,v in dd.items()}
        elig=sorted([(tk,rk[tk]) for tk in dd if ms.get(tk,0)>=-2 and tk in rk],key=lambda x:x[1])
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
            r=rk.get(tk); nc=it['nc']; m=minseg(it)
            sell=False
            if m<-2: sell=True
            elif (r is None or r>EXIT_RANK):
                pe=(cp/nc) if (nc and nc>0) else 999
                if pe>=PE_HOLD: sell=True
            if sell: del pf[tk]
        if len(pf)<2:
            free=sorted([s for s in range(2) if s not in {info['slot_idx'] for info in pf.values()}])
            cands=[tk for tk,_ in elig if tk not in pf and tk in tradable and tk not in ban
                   and ms.get(tk,-9)>=0 and rk.get(tk,999)<=5 and (dd.get(tk,{}).get('dv') or 0)>=1000]
            cands.sort(key=lambda t:rk.get(t,999))
            for tk in cands:
                if len(pf)>=2: break
                ip=px(tk,d); ix=free.pop(0) if free else len(pf)
                if ip: pf[tk]={'entry_price':ip,'slot_idx':ix,'weight':0}
            n=len(pf)
            for info in pf.values(): info['weight']=100/n if n else 0
    return (nav-1)*100, mdd*100, sorted(pf.keys())

base=run(0.0)
print(f"SANITY W=0 {base[0]:+.1f}% MDD{base[1]:.1f}% {base[2]} (목표 +216.7%)\n")
print("=== W 스윕 (음수=고gap 패널티/저gap 보너스, 양수=고gap 보너스) ===")
res={}
for W in [-1.0,-0.5,-0.3,-0.15,-0.05,0.0,0.05,0.15,0.3,0.5,1.0]:
    r=run(W); res[W]=r
    print(f"  W={W:>+5}: {r[0]:>8.1f}% (Δ{r[0]-base[0]:>+7.1f}p) MDD{r[1]:>6.1f}% {r[2]}")
bestW=max(res,key=lambda w:res[w][0])
print(f"\n최고 W={bestW} ({res[bestW][0]:+.1f}%). LOWO:")
worst=999
for w in ['MU','SNDK','STX','NVDA','LITE','AVGO']:
    b=run(0.0,ban=(w,))[0]; r=run(bestW,ban=(w,))[0]
    print(f"  -{w:5}: base {b:>7.1f}% | W{bestW} {r:>7.1f}% | Δ{r-b:>+7.1f}p"); worst=min(worst,r-b)
print(f"  worst-LOWO: {worst:+.1f}p → {'통과' if worst>0 else '기각'}")
