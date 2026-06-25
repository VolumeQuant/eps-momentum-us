# -*- coding: utf-8 -*-
"""gap sleeve를 2월 시스템 시작일부터 돌렸으면? — 모듈과 동일 로직(월간리밸·daily MtM·상위15)"""
import sqlite3, pandas as pd, pickle, numpy as np, yfinance as yf
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
elig={}
for d in all_dates:
    elig[d]={r[0]:r[1] for r in cur.execute("SELECT ticker,ntm_current FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
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

K=15
nav=1.0; peak=1.0; mdd=0.0; hold=[]; lastpx={}; rebal_month=None; navser={}
for d in all_dates:
    # MtM (보유 기준)
    if hold and lastpx:
        rs=[(px(t,d)/lastpx[t]-1) for t in hold if px(t,d) and lastpx.get(t)]
        if rs:
            nav*=(1+np.mean(rs)); peak=max(peak,nav); mdd=min(mdd,nav/peak-1)
            for t in hold:
                if px(t,d): lastpx[t]=px(t,d)
    navser[d]=nav
    # 월간 리밸
    mth=d[:7]
    if mth!=rebal_month or not hold:
        cands=[(t,gap(t,d)) for t in elig.get(d,{}) if gap(t,d) is not None and t in PX.columns]
        cands.sort(key=lambda x:-x[1])
        newh=[t for t,_ in cands[:K] if px(t,d)]
        if newh:
            hold=newh; lastpx={t:px(t,d) for t in hold}; rebal_month=mth

# SPY + 동일가중 유니버스
spy=yf.download('^GSPC',start='2026-02-01',end='2026-06-25',auto_adjust=False,progress=False)['Close']
spy=spy.iloc[:,0] if hasattr(spy,'columns') else spy
spyv={dd.strftime('%Y-%m-%d'):float(v) for dd,v in spy.items()}
sd=[d for d in all_dates if d in spyv]
spy_ret=(spyv[sd[-1]]/spyv[sd[0]]-1)*100

print(f"=== gap sleeve 2월 시작일부터 ({all_dates[0]} ~ {all_dates[-1]}, {len(all_dates)}거래일) ===")
print(f"  gap sleeve(상위15·월간리밸): 누적 {(nav-1)*100:+.1f}%  MDD {mdd*100:.1f}%")
print(f"  SPY 동기간: {spy_ret:+.1f}%")
print(f"\n  비교 — 2슬롯 EPS-모멘텀(BT): +193~245% (집중) / 우리 라이브 트랙은 6/11 epoch부터라 별개")
# 월별 진행
print("\n  월말 NAV 진행:")
for m in ['2026-02','2026-03','2026-04','2026-05','2026-06']:
    ds=[d for d in all_dates if d.startswith(m)]
    if ds: print(f"    {m}: {(navser[ds[-1]]-1)*100:+.1f}%")
