# -*- coding: utf-8 -*-
"""gap × momentum 교집합 sleeve: 고gap 중 '추정치대비 진짜 저평가'(momentum)만.
   momentum 신호 = part2_rank(EPS모멘텀 순위) / adj_gap<0(PE압축=싸짐). CIEN 함정 걸러지나?"""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb')); DV=pd.read_pickle(SP+r'\dv137.pkl')
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
D={}
for d in all_dates:
    D[d]={r[0]:{'nc':r[1],'p2':r[2],'cr':r[3],'ag':r[4]} for r in cur.execute("SELECT ticker,ntm_current,part2_rank,composite_rank,adj_gap FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
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
def cand_list(d,mode):
    """그날 후보(gap내림차순). mode로 momentum 필터."""
    out=[]
    for tk,v in D.get(d,{}).items():
        g=gap(tk,d)
        if g is None or tk not in PX.columns: continue
        if (dvol(tk,d) or 0)<1000: continue
        if mode=='p2_30' and (v['p2'] is None or v['p2']>30): continue
        if mode=='p2_15' and (v['p2'] is None or v['p2']>15): continue
        if mode=='cheap' and (v['ag'] is None or v['ag']>=0): continue   # adj_gap<0=PE압축=싸짐
        if mode=='cr_30' and (v['cr'] is None or v['cr']>30): continue    # 모멘텀 일별순위 상위
        if mode.startswith('trap'):                                        # 극단 비싼 함정만 제거
            thr=float(mode.split('_')[1])
            if v['ag'] is not None and v['ag']>thr: continue
        out.append((tk,g))
    out.sort(key=lambda x:-x[1])
    return out
def run(mode,K=7):
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
            new=[t for t,_ in cand_list(d,mode)[:K] if px(t,d)]
            if new:
                hold=new;lastpx={t:px(t,d) for t in new};rebm=m
                if d.startswith('2026-06'): junh=new[:]
    return (nav-1)*100,mdd*100,hold,junh
print(f"{'모드':24} {'전체수익':>8} {'MDD':>7} {'6월보유':>40}")
for mode,lbl in [('none','gap만 (현 sleeve)'),('trap_5','gap, 함정제거 adj_gap>5'),('trap_10','gap, 함정제거 adj_gap>10'),('trap_15','gap, 함정제거 adj_gap>15'),('cheap','gap × adj_gap<0'),('p2_15','gap × 모멘텀top15')]:
    cum,mdd,hold,junh=run(mode)
    print(f"{lbl:26} {cum:>+7.0f}% {mdd:>6.0f}%  {','.join(junh) if junh else '-'}")
# walk-forward 최선 trap
print("\n=== trap_10 walk-forward + LOWO ===")
def runwf(mode,lo,hi):
    nav=1.0;peak=1.0;mdd=0.0;hold=[];lastpx={};rebm=None
    for d in [x for x in all_dates if lo<=x<=hi]:
        if hold and lastpx:
            rs=[(px(t,d)/lastpx[t]-1) for t in hold if px(t,d) and lastpx.get(t)]
            if rs:
                nav*=(1+np.mean(rs))
                for t in hold:
                    if px(t,d):lastpx[t]=px(t,d)
        m=d[:7]
        if m!=rebm or not hold:
            new=[t for t,_ in cand_list(d,mode)[:7] if px(t,d)]
            if new:hold=new;lastpx={t:px(t,d) for t in new};rebm=m
    return (nav-1)*100
for mode in ['none','trap_10']:
    a=runwf(mode,all_dates[0],'2026-04-21');b=runwf(mode,'2026-04-21',all_dates[-1])
    print(f"  {mode}: 전반 {a:+.0f}% | 후반 {b:+.0f}%")
# CIEN 진단: 6/01 momentum 신호
print("\n=== CIEN 6/01 진단 (왜 gap만으론 잡혔나) ===")
v=D.get('2026-06-01',{}).get('CIEN',{})
print(f"  gap {gap('CIEN','2026-06-01'):.1f}x | part2_rank {v.get('p2')} | composite_rank {v.get('cr')} | adj_gap {v.get('ag'):.1f}")
print(f"  → adj_gap {'양수=PE팽창=안싸다(momentum이 걸러냄)' if (v.get('ag') or 0)>=0 else '음수=싸다'}")
