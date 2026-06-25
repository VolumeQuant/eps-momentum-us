# -*- coding: utf-8 -*-
"""gap × momentum 등수 50/50 블렌드 + 모멘텀등수 필터. CIEN vs SNDK 모멘텀 등수 진단."""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb')); DV=pd.read_pickle(SP+r'\dv137.pkl')
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
D={}
for d in all_dates:
    D[d]={r[0]:{'nc':r[1],'p2':r[2],'cr':r[3]} for r in cur.execute("SELECT ticker,ntm_current,part2_rank,composite_rank FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
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
def base_cands(d):  # $1B + gap 통과 종목
    out=[]
    for tk,v in D.get(d,{}).items():
        g=gap(tk,d)
        if g is None or tk not in PX.columns or (dvol(tk,d) or 0)<1000: continue
        out.append((tk,g,v['cr']))   # cr = composite_rank(모멘텀 일별등수, 작을수록 좋음)
    return out
def pick(d,mode,w=0.5,K=7):
    cands=base_cands(d)
    if not cands: return []
    # gap 등수 (작을수록 고gap)
    by_gap=sorted(cands,key=lambda x:-x[1]); grank={t:i+1 for i,(t,_,_) in enumerate(by_gap)}
    # momentum 등수 (composite_rank 작을수록 좋음, 후보 내 재랭킹)
    by_mom=sorted(cands,key=lambda x:(x[2] if x[2] is not None else 9999)); mrank={t:i+1 for i,(t,_,_) in enumerate(by_mom)}
    if mode=='gap':
        order=[t for t,_,_ in by_gap]
    elif mode=='blend':
        order=sorted(grank,key=lambda t:w*grank[t]+(1-w)*mrank[t])
    elif mode=='momfilt':   # 모멘텀 일별등수 top N 통과만 (러프 필터), 그중 gap순
        N=K*4
        ok=set(t for t,_,_ in by_mom[:N])
        order=[t for t,_,_ in by_gap if t in ok]
    return [t for t in order[:K] if px(t,d)]
def run(mode,w=0.5):
    nav=1.0;peak=1.0;mdd=0.0;hold=[];lastpx={};rebm=None;junh=None
    for d in all_dates:
        if hold and lastpx:
            rs=[(px(t,d)/lastpx[t]-1) for t in hold if px(t,d) and lastpx.get(t)]
            if rs:
                nav*=(1+np.mean(rs));peak=max(peak,nav);mdd=min(mdd,nav/peak-1)
                for t in hold:
                    if px(t,d):lastpx[t]=px(t,d)
        m=d[:7]
        if m!=rebm or not hold:
            new=pick(d,mode,w)
            if new:
                hold=new;lastpx={t:px(t,d) for t in new};rebm=m
                if d.startswith('2026-06'): junh=new[:]
    return (nav-1)*100,mdd*100,junh

print("=== CIEN vs 승자들 모멘텀 일별등수(composite_rank) 6/01 ===")
for t in ['CIEN','SNDK','MU','STX','COHR','AMD','ADI','ALAB']:
    v=D.get('2026-06-01',{}).get(t,{})
    g=gap(t,'2026-06-01')
    print(f"  {t}: gap {g:.1f}x · 모멘텀등수(cr) {v.get('cr')}" if g else f"  {t}: gap n/a · cr {v.get('cr')}")

print(f"\n=== 결과 (K=7 월간 $1B) ===")
print(f"{'모드':26} {'전체수익':>8} {'MDD':>7}  6월보유")
for mode,w,lbl in [('gap',0,'gap만'),('blend',0.5,'gap50/mom50 블렌드'),('blend',0.7,'gap70/mom30 블렌드'),('blend',0.3,'gap30/mom70 블렌드'),('momfilt',0,'러프필터(모멘텀상위만, gap순)')]:
    cum,mdd,junh=run(mode,w)
    print(f"{lbl:26} {cum:>+7.0f}% {mdd:>6.0f}%  {','.join(junh) if junh else '-'}")
