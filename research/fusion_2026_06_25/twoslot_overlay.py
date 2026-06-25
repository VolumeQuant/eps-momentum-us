# -*- coding: utf-8 -*-
"""#2 2슬롯에 이중확인 우선순위: momentum top후보 중 gap도 높은(이중확인) 종목 우선 매수. +LOWO"""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
D={}
for d in all_dates:
    D[d]={r[0]:{'p2':r[1],'nc':r[2],'n7':r[3],'n30':r[4],'n60':r[5],'n90':r[6],'dv':r[7],'cr':r[8]}
          for r in cur.execute("SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d,composite_rank FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
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
def run(overlay='none',ban=()):
    EXIT_RANK=12;PE_HOLD=30;nav=1.0;peak=1.0;mdd=0.0;pf={}
    for k in range(2,len(all_dates)):
        d,pv=all_dates[k],all_dates[k-1];dd=D.get(d,{})
        ms={t:minseg(v) for t,v in dd.items()}
        wrank={t:v['p2'] for t,v in dd.items() if v.get('p2')}
        elig=[(t,v['p2']) for t,v in dd.items() if ms.get(t,0)>=-2 and v.get('p2')]
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
            # 후보: momentum 진입권(part2_rank≤5) + $1B + min_seg
            cands=[t for t,_ in elig if t not in pf and t in PX.columns and t not in ban and ms.get(t,-9)>=0 and wrank.get(t,999)<=5 and (dd.get(t,{}).get('dv') or 0)>=1000]
            # gap 순위(전체 eligible 기준)
            gall=sorted([(t,gap(t,d)) for t in dd if gap(t,d) is not None],key=lambda x:-x[1])
            grank={t:i+1 for i,(t,_) in enumerate(gall)}
            if overlay=='double_first':       # 이중확인(gap top20) 우선, 그다음 part2순
                cands.sort(key=lambda t:(0 if grank.get(t,999)<=20 else 1, wrank.get(t,999)))
            elif overlay=='gap_first':         # 후보 중 gap 높은 순
                cands.sort(key=lambda t:-(gap(t,d) or 0))
            else:                              # baseline: part2순
                cands.sort(key=lambda t:wrank.get(t,999))
            for t in cands:
                if len(pf)>=2: break
                ip=px(t,d);ix=free.pop(0) if free else len(pf)
                if ip: pf[t]={'ep':ip,'slot':ix,'w':0}
            n=len(pf)
            for i in pf.values(): i['w']=100/n if n else 0
    return (nav-1)*100,mdd*100,sorted(pf.keys())
print("=== #2 2슬롯 이중확인 우선 오버레이 ===")
for ov,lbl in [('none','baseline (part2순)'),('double_first','이중확인 우선'),('gap_first','gap 높은순')]:
    cum,mdd,h=run(ov)
    print(f"  {lbl:18}: {cum:>+6.0f}% MDD{mdd:>5.0f}%  잔고{h}")
print("\n=== LOWO (이중확인 우선) ===")
worst=999
for w in ['SNDK','STX','MU','LITE','NVDA']:
    b=run('none',ban=(w,))[0];r=run('double_first',ban=(w,))[0]
    print(f"  -{w}: base {b:+.0f}% | 오버레이 {r:+.0f}% | Δ{r-b:+.0f}p");worst=min(worst,r-b)
print(f"  worst-LOWO {worst:+.0f}p → {'통과' if worst>0 else '기각'}")
