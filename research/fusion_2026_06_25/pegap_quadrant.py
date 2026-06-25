# -*- coding: utf-8 -*-
"""사용자 관찰2: 'trailing PE<20 낮고 + gap≈1(forward 비슷) = 지지부진 우하향' 검증"""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\pxU.pkl'); pit_eps=pickle.load(open(SP+r'\pit_eps.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date")]
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def pxv(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def fwd(tk,d,n):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    p0=pxv(tk,d)
    if not p0: return None
    j=min(i+n,len(PX)-1); v=PX[tk].iloc[j]
    return (float(v)/p0-1)*100 if pd.notna(v) else None
def pit_teps(tk,d):
    rec=pit_eps.get(tk); val=None
    if not rec: return None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val
obs=[]
for d in all_dates:
    for tk,nc in cur.execute("SELECT ticker,ntm_current FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL",(d,)):
        if not nc or nc<=0: continue
        te=pit_teps(tk,d); p=pxv(tk,d); f20=fwd(tk,d,20)
        if not te or te<0.5 or not p or f20 is None: continue
        tpe=p/te; fpe=p/nc; gap=tpe/fpe
        obs.append({'tk':tk,'tpe':tpe,'fpe':fpe,'gap':gap,'f20':f20})
df=pd.DataFrame(obs)
print(f"관측 {len(df)}건 (trailing PE 계산가능, EPS상향 top30 유니버스)")

print("\n=== 2×2: trailing PE × gap → 향후20일 평균수익 ===")
print("(사용자 가설: 저PE+저gap 칸이 제일 나쁨/우하향)")
print(f"{'':16}{'gap≈1 (<1.3)':>16}{'gap 큼 (≥1.3)':>16}")
for plo,phi,plbl in [(0,20,'trailPE<20'),(20,99999,'trailPE≥20')]:
    cells=[]
    for glo,ghi in [(0,1.3),(1.3,999)]:
        s=df[(df.tpe>=plo)&(df.tpe<phi)&(df.gap>=glo)&(df.gap<ghi)]
        cells.append(f"{s['f20'].mean():+.1f}% (n={len(s)})" if len(s) else "n=0")
    print(f"  {plbl:14}{cells[0]:>16}{cells[1]:>16}")

print("\n=== 사용자 콕집은 칸: trailing PE<20 AND gap<1.2 ===")
s=df[(df.tpe<20)&(df.gap<1.2)]
print(f"  n={len(s)}, 향후20d 평균 {s['f20'].mean():+.1f}% / 중앙 {s['f20'].median():+.1f}% / 음수비율 {(s['f20']<0).mean()*100:.0f}%")
print(f"  (비교) 전체 평균 {df['f20'].mean():+.1f}%, 우하향(음수)비율 {(df['f20']<0).mean()*100:.0f}%")
hi=df[(df.tpe>=20)&(df.gap>=2)]
print(f"  (대조) trailing PE≥20 AND gap≥2: n={len(hi)}, 향후20d 평균 {hi['f20'].mean():+.1f}% 음수비율 {(hi['f20']<0).mean()*100:.0f}%")
