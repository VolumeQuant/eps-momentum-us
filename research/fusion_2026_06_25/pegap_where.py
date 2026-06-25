# -*- coding: utf-8 -*-
"""gap이 어디서 수확되나: ①SNDK 반례 확인 ②2슬롯 혼합(blend) ③분산 슬리브(top-N)"""
import sqlite3, pandas as pd, pickle, numpy as np, os
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\pxU.pkl'); pit_eps=pickle.load(open(SP+r'\pit_eps.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date")]
data={}
for d in all_dates:
    data[d]={r[0]:{'p2':r[1],'nc':r[2]} for r in cur.execute("SELECT ticker,part2_rank,ntm_current FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL",(d,))}
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def pit_teps(tk,d):
    rec=pit_eps.get(tk); val=None
    if not rec: return None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val
def gap(tk,d):
    nc=data.get(d,{}).get(tk,{}).get('nc'); te=pit_teps(tk,d)
    return nc/te if (nc and nc>0 and te and te>0.5) else None

# ① SNDK/STX 반례 — 우리 winner들의 gap은?
print("=== ① 우리 2슬롯 winner들의 gap (저gap이면 gap전략과 상충) ===")
for tk in ['SNDK','STX','MU','NVDA','LITE','AVGO']:
    gs=[gap(tk,d) for d in all_dates if gap(tk,d)]
    if gs: print(f"  {tk:5}: gap 평균 {np.mean(gs):.2f} (min {min(gs):.2f}~max {max(gs):.2f})  최종수익 큰 winner인가")

# ③ 분산 슬리브: 매일 top-N by gap vs by rank vs bottom-N, 동일가중 일배분 (팩터 수확 입증용, 비용무시)
def sleeve(select, N, d_lo=None, d_hi=None):
    rng=[d for d in all_dates if (d_lo is None or d>=d_lo) and (d_hi is None or d<=d_hi)]
    nav=1.0; peak=1.0; mdd=0.0
    for k in range(1,len(rng)):
        d,pv=rng[k],rng[k-1]; dd=data.get(pv,{})  # 전일 종가에 보유 결정
        elig=[tk for tk in dd if gap(tk,pv) is not None and tk in PX.columns]
        if select=='gap_top': elig.sort(key=lambda t:-gap(t,pv))
        elif select=='gap_bot': elig.sort(key=lambda t:gap(t,pv))
        elif select=='rank': elig.sort(key=lambda t:dd[t]['p2'])
        hold=elig[:N]
        if not hold: continue
        r=np.mean([ (px(tk,d)/px(tk,pv)-1) for tk in hold if px(tk,d) and px(tk,pv)])*100 if hold else 0
        nav*=(1+r/100); peak=max(peak,nav); mdd=min(mdd,nav/peak-1)
    return (nav-1)*100, mdd*100
print("\n=== ③ 분산 슬리브 (top-N 동일가중, 일리밸, 비용무시 — 팩터 수확 입증) ===")
for N in [10,15]:
    gt=sleeve('gap_top',N); rk=sleeve('rank',N); gb=sleeve('gap_bot',N)
    print(f"  N={N}: gap상위 {gt[0]:+.0f}%(MDD{gt[1]:.0f}) | rank상위 {rk[0]:+.0f}%(MDD{rk[1]:.0f}) | gap하위 {gb[0]:+.0f}%(MDD{gb[1]:.0f})")
    print(f"        → gap상위-gap하위 스프레드 {gt[0]-gb[0]:+.0f}%p (팩터 수확)")

# 시기 분할로 분산 슬리브 robust?
print("\n  분산슬리브(N=10) 시기분할 gap상위-gap하위 스프레드:")
mid=all_dates[len(all_dates)//2]
for lo,hi,l in [(all_dates[0],mid,'전반'),(mid,all_dates[-1],'후반')]:
    gt=sleeve('gap_top',10,lo,hi); gb=sleeve('gap_bot',10,lo,hi)
    print(f"    {l}: {gt[0]-gb[0]:+.0f}%p")
