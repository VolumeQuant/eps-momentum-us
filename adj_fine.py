"""촘촘 인접 안정성 — 독립 스크립트 (import 없이 캐시 직접 구축)"""
import numpy as np, sys, time
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
from gridsearch_internal import load_all_data, is_case1
t0=time.time()
print("Loading...")
all_dates, p2_dates, raw, chg_data, _ = load_all_data()
# 캐시
MISS=30
bl_scores={}; bl_cgaps={}
for d in p2_dates:
    dr=raw.get(d,{})
    tks=[tk for tk in dr if dr[tk].get('comp_rank') is not None]
    cg={}
    for tk in tks:
        v=dr[tk]; ag=v['adj_gap'] or 0
        r2=(v['rev_up30']/v['num_analysts']) if v['num_analysts']>0 else 0
        ef=min(abs((v['ntm_cur']-v['ntm_90d'])/v['ntm_90d']),1.0) if v['ntm_90d'] and abs(v['ntm_90d'])>0.01 else 0
        bc=max(r2,ef); rb=0.3 if (v['rev_growth'] is not None and v['rev_growth']>=0.30) else 0
        cg[tk]=ag*(1+bc+rb)
    bl_cgaps[d]=cg
    vals=list(cg.values())
    if len(vals)>=2:
        m,s=np.mean(vals),np.std(vals)
        bl_scores[d]={tk:min(100,max(30,65+(-(v-m)/s)*15)) for tk,v in cg.items()} if s>0 else {tk:65 for tk in cg}
    else:
        bl_scores[d]={tk:65 for tk in cg}
print(f"Cache built: {time.time()-t0:.1f}s")

def mk_c1(period,nt,pt):
    s={}
    for d in p2_dates:
        dc=chg_data.get(d,{})
        s[d]={tk for tk in dc if is_case1(dc[tk],period,nt,pt)}
    return s

def sim(e,x,pos,period,nt,pt,st,ed):
    c1=mk_c1(period,nt,pt)
    port={};drs=[];trades=[];con=defaultdict(int)
    for di in range(len(p2_dates)):
        d=p2_dates[di]; dr_=raw.get(d,{}); dc=chg_data.get(d,{})
        d3=[p2_dates[max(0,di-2)],p2_dates[max(0,di-1)],p2_dates[di]]
        d3=[dd for dd in d3 if dd in bl_scores]
        wt=[0.2,0.3,0.5][-len(d3):]
        if len(d3)==2:wt=[0.4,0.6]
        elif len(d3)==1:wt=[1.0]
        wgap={}
        if pos=='P3_zscore':
            atk=set()
            for dd in d3: atk.update(bl_scores[dd].keys())
            for tk in atk:
                wg=0
                for i,dd in enumerate(d3):
                    sc=bl_scores[dd].get(tk,MISS)
                    if tk in c1.get(dd,set()): sc+=st
                    wg+=sc*wt[i]
                wgap[tk]=wg
        elif pos=='P1_adjgap':
            s3={}
            for dd in d3:
                c1d=c1.get(dd,set())
                if not c1d: s3[dd]=bl_scores[dd]; continue
                ncg={tk:(cg*(1+st) if tk in c1d else cg) for tk,cg in bl_cgaps[dd].items()}
                vs=list(ncg.values())
                if len(vs)>=2:
                    m,sv=np.mean(vs),np.std(vs)
                    s3[dd]={tk:min(100,max(30,65+(-(v-m)/sv)*15)) for tk,v in ncg.items()} if sv>0 else {tk:65 for tk in ncg}
                else: s3[dd]={tk:65 for tk in ncg}
            atk=set()
            for dd in d3: atk.update(s3.get(dd,{}).keys())
            for tk in atk:
                wgap[tk]=sum(s3.get(dd,{}).get(tk,MISS)*wt[i] for i,dd in enumerate(d3))
        tks=list(wgap.keys()); stk=sorted(tks,key=lambda t:wgap.get(t,0),reverse=True)
        rm={tk:i+1 for i,tk in enumerate(stk)}
        nc=defaultdict(int)
        for tk in tks:
            if rm.get(tk,999)<=30: nc[tk]=con.get(tk,0)+1
        con=nc; c1d=c1.get(d,set())
        for tk in list(port.keys()):
            rk=rm.get(tk);ms=dc.get(tk,{}).get('min_seg',0);ef=x+(ed if tk in c1d else 0)
            if (rk is None or rk>ef) or ms<-2:
                p=dr_.get(tk,{}).get('price')
                if p:trades.append((p-port[tk])/port[tk]*100)
                del port[tk]
        vc=3-len(port)
        if vc>0:
            for tk in stk:
                if vc<=0:break
                if tk in port:continue
                if rm.get(tk,999)>e:continue
                if con.get(tk,0)<3:continue
                ms=dc.get(tk,{}).get('min_seg',0)
                if ms<0:continue
                p=dr_.get(tk,{}).get('price')
                if p and p>0:port[tk]=p;vc-=1
        if port and di>0:
            prev=p2_dates[di-1];dr=0
            for tk in port:
                pn=dr_.get(tk,{}).get('price');pp=raw.get(prev,{}).get(tk,{}).get('price')
                if pn and pp and pp>0:dr+=(pn-pp)/pp*100
            dr/=len(port);drs.append(dr)
    if port:
        last=p2_dates[-1]
        for tk in list(port.keys()):
            p=raw.get(last,{}).get(tk,{}).get('price')
            if p:trades.append((p-port[tk])/port[tk]*100)
    cum=1.0;pk=1.0;mdd=0
    for dr in drs:cum*=(1+dr/100);pk=max(pk,cum);mdd=min(mdd,(cum-pk)/pk*100)
    n=len(trades);wr=(sum(1 for t in trades if t>0)/n*100) if n else 0
    da=np.array(drs) if drs else np.array([0])
    sh=(da.mean()/da.std()*np.sqrt(252)) if da.std()>0 else 0
    return round((cum-1)*100,2),round(sh,2)

print("\n[P1 7d] E2/X10/S3 — str 촘촘")
for st in [0.5,0.6,0.7,0.8,0.9,1.0,1.1,1.2,1.3,1.5]:
    r,s=sim(2,10,'P1_adjgap','7d',0.5,-1.0,st,5)
    m='→' if st==1.0 else ' '
    print(f"  {m} str={st:>3}: ret {r:+.1f}% Sharpe {s:.2f}")

print("\n[P1 7d] NTM 촘촘")
for nt in [0.3,0.4,0.5,0.6,0.7,0.8,1.0]:
    r,s=sim(2,10,'P1_adjgap','7d',nt,-1.0,1.0,5)
    m='→' if nt==0.5 else ' '
    print(f"  {m} N{nt:>3}: ret {r:+.1f}% Sharpe {s:.2f}")

print("\n[P1 7d] PX 촘촘")
for pt in [-0.5,-0.7,-0.8,-1.0,-1.2,-1.5,-2.0]:
    r,s=sim(2,10,'P1_adjgap','7d',0.5,pt,1.0,5)
    m='→' if pt==-1.0 else ' '
    print(f"  {m} P{pt:>4}: ret {r:+.1f}% Sharpe {s:.2f}")

print("\n[P1 7d] ED 촘촘")
for ed in [0,1,2,3,4,5,6,7]:
    r,s=sim(2,10,'P1_adjgap','7d',0.5,-1.0,1.0,ed)
    m='→' if ed==5 else ' '
    print(f"  {m} ED{ed}: ret {r:+.1f}% Sharpe {s:.2f}")

print("\n[P3 30d] E3/X8/S3 — str 촘촘")
for st in [4,5,6,7,8,9,10,11,12]:
    r,s=sim(3,8,'P3_zscore','30d',1.0,-1.0,st,5)
    m='→' if st==8 else ' '
    print(f"  {m} str={st:>2}: ret {r:+.1f}% Sharpe {s:.2f}")

print("\n[P3 30d] NTM 촘촘")
for nt in [0.5,0.7,0.8,1.0,1.2,1.5,2.0]:
    r,s=sim(3,8,'P3_zscore','30d',nt,-1.0,8,5)
    m='→' if nt==1.0 else ' '
    print(f"  {m} N{nt:>3}: ret {r:+.1f}% Sharpe {s:.2f}")

print("\n[P3 30d] PX 촘촘")
for pt in [-0.5,-0.7,-0.8,-1.0,-1.2,-1.5,-2.0]:
    r,s=sim(3,8,'P3_zscore','30d',1.0,pt,8,5)
    m='→' if pt==-1.0 else ' '
    print(f"  {m} P{pt:>4}: ret {r:+.1f}% Sharpe {s:.2f}")

print("\n[P3 30d] ED 촘촘")
for ed in [0,1,2,3,4,5,6,7]:
    r,s=sim(3,8,'P3_zscore','30d',1.0,-1.0,8,ed)
    m='→' if ed==5 else ' '
    print(f"  {m} ED{ed}: ret {r:+.1f}% Sharpe {s:.2f}")

print(f"\n총: {time.time()-t0:.1f}s")
