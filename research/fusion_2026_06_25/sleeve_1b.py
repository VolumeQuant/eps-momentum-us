# -*- coding: utf-8 -*-
"""sleeve에 $1B 거래대금 필터 적용 — 있고/없고 비교 (K=7 월간)"""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb')); DV=pd.read_pickle(SP+r'\dv137.pkl')
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
elig={}
for d in all_dates:
    elig[d]={r[0]:r[1] for r in cur.execute("SELECT ticker,ntm_current FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
dvi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(DV.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def dv(tk,d):
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
    nc=elig.get(d,{}).get(tk)
    if not nc or nc<=0: return None
    te=pit_te(tk,d)
    if not te or te<0.5: return None
    g=nc/te; return g if g<=10 else None
def run(K=7,dvmin=0,log=False):
    nav=1.0;peak=1.0;mdd=0.0;hold=[];lastpx={};rebm=None;trades=[]
    for d in all_dates:
        if hold and lastpx:
            rs=[(px(t,d)/lastpx[t]-1) for t in hold if px(t,d) and lastpx.get(t)]
            if rs:
                nav*=(1+np.mean(rs));peak=max(peak,nav);mdd=min(mdd,nav/peak-1)
                for t in hold:
                    if px(t,d):lastpx[t]=px(t,d)
        m=d[:7]
        if m!=rebm or not hold:
            ranked=[(t,gap(t,d)) for t in elig.get(d,{}) if gap(t,d) is not None and t in PX.columns and (dvmin<=0 or (dv(t,d) or 0)>=dvmin)]
            ranked.sort(key=lambda x:-x[1])
            new=[t for t,_ in ranked[:K] if px(t,d)]
            if new:
                if log: trades.append((d,round((nav-1)*100,1),[t for t in hold if t not in new],[t for t in new if t not in hold]))
                hold=new;lastpx={t:px(t,d) for t in new};rebm=m
    return (nav-1)*100,mdd*100,hold,trades
print("=== sleeve K=7 월간: $1B 필터 유무 ===")
for dvmin,lbl in [(0,'필터 없음'),(1000,'$1B+ 필터')]:
    cum,mdd,hold,tr=run(7,dvmin,log=True)
    print(f"\n[{lbl}] 수익률 {cum:+.1f}% · MDD {mdd:.1f}%")
    print(f"  현재 잔고: {', '.join(hold)}")
    if dvmin>0:
        for d,nv,s,b in tr:
            print(f"  {d} (NAV{nv:+.0f}%) OUT[{','.join(s) or '-'}] IN[{','.join(b) or '-'}]")
# 제외되는 종목 확인 (06-24 기준 gap 상위인데 $1B 미달)
print("\n=== 06-24 gap 상위 10 중 $1B 미달로 빠지는 종목 ===")
d='2026-06-24'
ranked=sorted([(t,gap(t,d),dv(t,d)) for t in elig.get(d,{}) if gap(t,d) is not None and t in PX.columns],key=lambda x:-x[1])[:10]
for t,g,v in ranked:
    mark=' ❌$1B미달' if (v or 0)<1000 else ''
    print(f"  {t}: gap {g:.1f}x, 거래대금 ${v:.0f}M{mark}" if v else f"  {t}: gap {g:.1f}x, 거래대금 데이터없음")
