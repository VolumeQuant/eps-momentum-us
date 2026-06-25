# -*- coding: utf-8 -*-
"""충실판: production scoring 체인 그대로 재현 + 일별점수에 W*15*gap_z 추가(KR 팩터방식).
   _apply_conviction + _compute_w_gap_map(일별 z→3일가중) 정확 복제. W=0이면 baseline."""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\pxU.pkl'); pit_eps=pickle.load(open(SP+r'\pit_eps.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
EXIT_RANK=12; PE_HOLD=30; MISS=30
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
D={}
for d in all_dates:
    D[d]={}
    for r in cur.execute("SELECT ticker,adj_gap,rev_up30,num_analysts,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,rev_growth,part2_rank,dollar_volume_30d FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,)):
        D[d][r[0]]={'ag':r[1],'up':r[2],'na':r[3],'nc':r[4],'n7':r[5],'n30':r[6],'n60':r[7],'n90':r[8],'rg':r[9],'p2':r[10],'dv':r[11]}
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def seg(a,b): return (a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0.0
def minseg(v): return min(seg(v['nc'],v['n7']),seg(v['n7'],v['n30']),seg(v['n30'],v['n60']),seg(v['n60'],v['n90']))
def conviction(v):  # _apply_conviction 정확 복제
    ag=v['ag'] or 0; up=v['up']; na=v['na']; nc=v['nc']; n90=v['n90']; rg=v['rg']
    ratio=up/na if (na and na>0 and up is not None) else 0
    epsf=min(abs((nc-n90)/n90),3.0) if (n90 and abs(n90)>0.01 and nc is not None) else 0
    base=max(ratio,epsf)
    rb=min(min(rg,0.5)*0.6,0.3) if rg is not None else 0
    return ag*(1+base+rb)
def pit_teps(tk,d):
    rec=pit_eps.get(tk); val=None
    if not rec: return None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val
def gap(tk,d):
    v=D.get(d,{}).get(tk)
    if not v or not v['nc'] or v['nc']<=0: return np.nan
    te=pit_teps(tk,d)
    return v['nc']/te if (te and te>0.5) else np.nan

# 일별 combined 점수 (production 일별점수 + W*15*gap_z)
def day_scores(d, W):
    dd=D.get(d,{})
    tks=list(dd.keys())
    if not tks: return {}
    conv=np.array([conviction(dd[t]) for t in tks],float)
    m,s=np.mean(conv),np.std(conv)
    base={t:(max(30.0,65+(-(conv[i]-m)/s)*15) if s>0 else 65) for i,t in enumerate(tks)}
    if W!=0:
        lg=np.array([np.log(np.clip(gap(t,d),0.3,20)) if not np.isnan(gap(t,d)) else np.nan for t in tks])
        gm,gs=np.nanmean(lg),np.nanstd(lg)
        gz={t:(np.nan_to_num((lg[i]-gm)/gs,nan=0.0) if gs>0 else 0.0) for i,t in enumerate(tks)}
        return {t:base[t]+W*15*gz[t] for t in tks}
    return base

def run(W=0.0, ban=()):
    # 일별점수 캐시
    DS={d:day_scores(d,W) for d in all_dates}
    # w_gap → new_rank (3일가중, 빈날 penalty=원 part2 membership 기준)
    nr={}
    for k,d in enumerate(all_dates):
        win=[all_dates[k-2] if k>=2 else None, all_dates[k-1] if k>=1 else None, d]
        wts=[0.2,0.3,0.5]
        if win[0] is None and win[1] is None: wts=[0,0,1.0]
        elif win[0] is None: wts=[0,0.4,0.6]
        tks=set(DS[d].keys())
        wg={}
        for t in tks:
            s=0
            for i,dd_ in enumerate(win):
                if dd_ is None: continue
                if dd_!=d and (t not in D.get(dd_,{}) or D[dd_][t].get('p2') is None):
                    sc=MISS  # 과거 top30 밖 penalty (원 membership)
                else:
                    sc=DS.get(dd_,{}).get(t,MISS)
                s+=sc*wts[i]
            wg[t]=s
        order=sorted(wg,key=lambda t:-wg[t])
        nr[d]={order[i]:i+1 for i in range(len(order))}
    # 전략 (2슬롯, 충실 엔진)
    tradable=set(PX.columns); pf={}; nav=1.0; peak=1.0; mdd=0.0
    for k in range(2,len(all_dates)):
        d,pv=all_dates[k],all_dates[k-1]; dd=D.get(d,{}); rk=nr[d]
        ms={t:minseg(v) for t,v in dd.items()}
        elig=sorted([(t,rk[t]) for t in dd if ms.get(t,0)>=-2 and t in rk],key=lambda x:x[1])
        dr=0.0
        for t,info in pf.items():
            w=info['weight']/100; cu,pp=px(t,d),px(t,pv)
            if cu and pp and pp>0: dr+=w*(cu-pp)/pp*100
        nav*=(1+dr/100); peak=max(peak,nav); mdd=min(mdd,nav/peak-1)
        for t in list(pf.keys()):
            cp=px(t,d)
            if cp is None: continue
            v=dd.get(t)
            if v is None: continue
            r=rk.get(t); nc=v['nc']; m=minseg(v)
            sell=False
            if m<-2: sell=True
            elif (r is None or r>EXIT_RANK):
                pe=(cp/nc) if (nc and nc>0) else 999
                if pe>=PE_HOLD: sell=True
            if sell: del pf[t]
        if len(pf)<2:
            free=sorted([s for s in range(2) if s not in {info['slot_idx'] for info in pf.values()}])
            cands=[t for t,_ in elig if t not in pf and t in tradable and t not in ban
                   and ms.get(t,-9)>=0 and rk.get(t,999)<=5 and (dd.get(t,{}).get('dv') or 0)>=1000]
            cands.sort(key=lambda t:rk.get(t,999))
            for t in cands:
                if len(pf)>=2: break
                ip=px(t,d); ix=free.pop(0) if free else len(pf)
                if ip: pf[t]={'entry_price':ip,'slot_idx':ix,'weight':0}
            n=len(pf)
            for info in pf.values(): info['weight']=100/n if n else 0
    return (nav-1)*100, mdd*100, sorted(pf.keys())

base=run(0.0)
print(f"SANITY W=0: {base[0]:+.1f}% MDD{base[1]:.1f}% {base[2]} (목표 ~+216.7%)\n")
print("=== 충실판 W 스윕 (일별점수 + W*15*gap_z) ===")
res={}
for W in [-0.3,-0.1,-0.05,0.0,0.05,0.1,0.2,0.3,0.5]:
    r=run(W); res[W]=r
    print(f"  W={W:>+5}: {r[0]:>8.1f}% (Δ{r[0]-base[0]:>+7.1f}p) MDD{r[1]:>6.1f}% {r[2]}")
pos=[W for W in res if W>0 and res[W][0]>base[0]]
bestW=max(res,key=lambda w:res[w][0])
print(f"\n최고 W={bestW} ({res[bestW][0]:+.1f}%, MDD{res[bestW][1]:.1f}%). LOWO:")
worst=999
for w in ['MU','SNDK','STX','NVDA','LITE','AVGO']:
    b=run(0.0,ban=(w,))[0]; r=run(bestW,ban=(w,))[0]
    print(f"  -{w:5}: base {b:>7.1f}% | W{bestW} {r:>7.1f}% | Δ{r-b:>+7.1f}p"); worst=min(worst,r-b)
print(f"  worst-LOWO: {worst:+.1f}p → {'통과' if worst>0 else '기각'}")
