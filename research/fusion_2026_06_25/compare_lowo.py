# -*- coding: utf-8 -*-
"""MU·SNDK 빼도 비교 유지되나 — 2슬롯 vs sleeve($1B) LOWO"""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb')); DV=pd.read_pickle(SP+r'\dv137.pkl')
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
D={}
for d in all_dates:
    D[d]={r[0]:{'p2':r[1],'nc':r[2],'n7':r[3],'n30':r[4],'n60':r[5],'n90':r[6],'dv':r[7]}
          for r in cur.execute("SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
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

def sleeve(ban=(),K=7):
    nav=1.0;peak=1.0;mdd=0.0;hold=[];lastpx={};rebm=None
    for d in all_dates:
        if hold and lastpx:
            rs=[(px(t,d)/lastpx[t]-1) for t in hold if px(t,d) and lastpx.get(t)]
            if rs:
                nav*=(1+np.mean(rs));peak=max(peak,nav);mdd=min(mdd,nav/peak-1)
                for t in hold:
                    if px(t,d):lastpx[t]=px(t,d)
        m=d[:7]
        if m!=rebm or not hold:
            ranked=sorted([(t,gap(t,d)) for t in D.get(d,{}) if gap(t,d) is not None and t in PX.columns and t not in ban and (dvol(t,d) or 0)>=1000],key=lambda x:-x[1])
            new=[t for t,_ in ranked[:K] if px(t,d)]
            if new:hold=new;lastpx={t:px(t,d) for t in new};rebm=m
    return (nav-1)*100,mdd*100,hold

def twoslot(ban=()):
    EXIT_RANK=12;PE_HOLD=30;nav=1.0;peak=1.0;mdd=0.0;pf={}
    for k in range(2,len(all_dates)):
        d,pv=all_dates[k],all_dates[k-1];dd=D.get(d,{})
        ms={t:minseg(v) for t,v in dd.items()}
        wrank={t:v['p2'] for t,v in dd.items() if v.get('p2')}
        elig=sorted([(t,v['p2']) for t,v in dd.items() if ms.get(t,0)>=-2 and v.get('p2')],key=lambda x:x[1])
        dr=0.0
        for t,info in pf.items():
            w=info['w']/100;cu,pp=px(t,d),px(t,pv)
            if cu and pp and pp>0: dr+=w*(cu-pp)/pp*100
        nav*=(1+dr/100);peak=max(peak,nav);mdd=min(mdd,nav/peak-1)
        for t in list(pf.keys()):
            cp=px(t,d)
            if cp is None: continue
            it=dd.get(t)
            if it is None or it.get('p2') is None: continue
            rk=it['p2'];nc=it['nc'];mg=minseg(it);sell=False
            if mg<-2: sell=True
            elif rk>EXIT_RANK:
                pe=(cp/nc) if (nc and nc>0) else 999
                if pe>=PE_HOLD: sell=True
            if sell: del pf[t]
        if len(pf)<2:
            free=sorted([s for s in range(2) if s not in {i['slot'] for i in pf.values()}])
            cands=[t for t,_ in elig if t not in pf and t in PX.columns and t not in ban and ms.get(t,-9)>=0 and wrank.get(t,999)<=5 and (dd.get(t,{}).get('dv') or 0)>=1000]
            cands.sort(key=lambda t:wrank.get(t,999))
            for t in cands:
                if len(pf)>=2: break
                ip=px(t,d);ix=free.pop(0) if free else len(pf)
                if ip: pf[t]={'ep':ip,'slot':ix,'w':0}
            n=len(pf)
            for i in pf.values(): i['w']=100/n if n else 0
    last=all_dates[-1]
    hold={t:f"{(px(t,last)/pf[t]['ep']-1)*100:+.0f}%" for t in pf}
    return (nav-1)*100,mdd*100,hold

print(f"{'시나리오':16} {'시스템':14} {'수익률':>8} {'MDD':>7}  잔고")
for ban,lbl in [((),'전체'),(('MU','SNDK'),'MU·SNDK 제외')]:
    s=sleeve(ban); t=twoslot(ban)
    print(f"{lbl:16} {'2슬롯':14} {t[0]:>+7.0f}% {t[1]:>6.0f}%  {t[2]}")
    print(f"{'':16} {'sleeve($1B)':14} {s[0]:>+7.0f}% {s[1]:>6.0f}%  {', '.join(s[2])}")
    print()
