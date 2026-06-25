# -*- coding: utf-8 -*-
"""KR 핸드오프 이식: 확신가중. 선택/진입/이탈 production 불변, 보유 중 이중확인(gap高) 종목만 비중↑.
   제외 금지(미확인=×1 보유). M(확신배수) × S(슬롯) 스윕 + LOWO."""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
D={}
for d in all_dates:
    D[d]={r[0]:{'p2':r[1],'nc':r[2],'n7':r[3],'n30':r[4],'n60':r[5],'n90':r[6],'dv':r[7]}
          for r in cur.execute("SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
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
def grank_of(d):
    cands=[(t,gap(t,d)) for t in D.get(d,{}) if gap(t,d) is not None and t in PX.columns]
    cands.sort(key=lambda x:-x[1])
    return {t:i+1 for i,(t,_) in enumerate(cands)}
def run(M,NC,S=2,ban=()):
    EXIT_RANK=12;PE_HOLD=30;nav=1.0;peak=1.0;mdd=0.0;pf={}
    for k in range(2,len(all_dates)):
        d,pv=all_dates[k],all_dates[k-1];dd=D.get(d,{})
        ms={t:minseg(v) for t,v in dd.items()}
        wrank={t:v['p2'] for t,v in dd.items() if v.get('p2')}
        elig=sorted([(t,v['p2']) for t,v in dd.items() if ms.get(t,0)>=-2 and v.get('p2')],key=lambda x:x[1])
        gr=grank_of(d)
        # 확신가중: 보유 중 gap순위≤NC면 ×M, 아니면 ×1, 재정규화
        if pf:
            w={t:(M if gr.get(t,9999)<=NC else 1.0) for t in pf}
            tot=sum(w.values())
            dr=0.0
            for t in pf:
                cu,pp=px(t,d),px(t,pv)
                if cu and pp and pp>0: dr+=(w[t]/tot)*(cu-pp)/pp*100
            nav*=(1+dr/100);peak=max(peak,nav);mdd=min(mdd,nav/peak-1)
        # 이탈 (production 동일)
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
        # 진입 (production 동일: rank≤5, $1B, min_seg)
        if len(pf)<S:
            cands=[t for t,_ in elig if t not in pf and t in PX.columns and t not in ban and ms.get(t,-9)>=0 and wrank.get(t,999)<=5 and (dd.get(t,{}).get('dv') or 0)>=1000]
            cands.sort(key=lambda t:wrank.get(t,999))
            for t in cands:
                if len(pf)>=S: break
                if px(t,d): pf[t]=1
    return (nav-1)*100,mdd*100
print("=== 확신가중 (gap순위≤NC=이중확인 → ×M, 미확인 ×1) ===")
print("slot2 (production):")
print(f"{'M':>6} {'NC=15':>14} {'NC=20':>14} {'NC=30':>14}")
for M in [1,2,2.5,3]:
    row=[f"{run(M,NC,2)[0]:+.0f}%/{run(M,NC,2)[1]:.0f}" for NC in [15,20,30]]
    print(f"{M:>6} "+" ".join(f"{v:>14}" for v in row))
print("\nslot3 (KR식 3슬롯):")
print(f"{'M':>6} {'NC=15':>14} {'NC=20':>14} {'NC=30':>14}")
for M in [1,2,2.5,3]:
    row=[f"{run(M,NC,3)[0]:+.0f}%/{run(M,NC,3)[1]:.0f}" for NC in [15,20,30]]
    print(f"{M:>6} "+" ".join(f"{v:>14}" for v in row))
print("\n=== LOWO (slot3, M=2, NC=20) — winner 빼도 방향 유지? ===")
base=run(1,20,3)
for w in ['SNDK','STX','MU','LITE']:
    b1=run(1,20,3,ban=(w,))[0];b2=run(2,20,3,ban=(w,))[0]
    print(f"  -{w}: ×1 {b1:+.0f}% → ×2 {b2:+.0f}% (Δ{b2-b1:+.0f}p)")
