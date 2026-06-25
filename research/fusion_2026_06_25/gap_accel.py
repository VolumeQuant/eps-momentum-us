# -*- coding: utf-8 -*-
"""전문가 #1 검증: gap 가속도가 트랩(CIEN)을 winner(SNDK)와 가르나 + min_seg 대비 증분?"""
import sqlite3, pandas as pd, pickle, numpy as np
from scipy.stats import spearmanr
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
D={}
for d in all_dates:
    D[d]={r[0]:{'nc':r[1],'n7':r[2],'n30':r[3],'n60':r[4],'n90':r[5],'cr':r[6]} for r in cur.execute("SELECT ticker,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,composite_rank FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def fwd(tk,d,n=20):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    p0=px(tk,d)
    if not p0: return None
    j=min(i+n,len(PX)-1); v=PX[tk].iloc[j]
    return (float(v)/p0-1)*100 if pd.notna(v) else None
def pit_te(tk,d):
    rec=pit.get(tk); val=None
    if not rec: return None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val
def seg(a,b): return (a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0.0
def gaps(tk,d):
    """gap을 5개 NTM 스냅샷으로 — TTM 고정. (gap_cur,gap_30,gap_60), min_seg"""
    v=D.get(d,{}).get(tk)
    if not v or not v['nc'] or v['nc']<=0: return None
    te=pit_te(tk,d)
    if not te or te<0.5: return None
    g=lambda nt: nt/te if nt and nt>0 else None
    gc,g30,g60,g90=g(v['nc']),g(v['n30']),g(v['n60']),g(v['n90'])
    if gc is None or gc>10: return None
    ms=min(seg(v['nc'],v['n7']),seg(v['n7'],v['n30']),seg(v['n30'],v['n60']),seg(v['n60'],v['n90']))
    return gc,g30,g60,g90,ms

# 1) CIEN vs SNDK gap 궤적
print("=== CIEN(트랩) vs SNDK(winner) gap 궤적 ===")
for tk in ['CIEN','SNDK','MU','LITE']:
    print(f"--- {tk} ---")
    for d in ['2026-05-01','2026-05-15','2026-06-01','2026-06-12','2026-06-23']:
        r=gaps(tk,d)
        if r:
            gc,g30,g60,g90,ms=r
            accel='↑확장' if (g30 and gc>g30) else '↓꺾임' if (g30 and gc<g30) else '-'
            f=fwd(tk,d)
            g30s=f"{g30:.1f}" if g30 else "?"; fs=f"{f:+.0f}%" if f is not None else "?"
            print(f"  {d}: gap {gc:.1f} (30일전 {g30s}) {accel} · min_seg{ms:+.0f} · 향후20d {fs}")

# 2) 고gap 종목 전체: 가속도 부호별 forward + min_seg 통제
print("\n=== 고gap(상위30%) 종목: gap 가속도별 향후20d ===")
rows=[]
for d in all_dates:
    cands=[(t,gaps(t,d)) for t in D.get(d,{})]
    cands=[(t,r) for t,r in cands if r is not None]
    if len(cands)<10: continue
    gv=sorted([r[0] for _,r in cands],reverse=True)
    thr=gv[int(len(gv)*0.3)]
    for t,r in cands:
        gc,g30,g60,g90,ms=r
        if gc<thr or g30 is None: continue
        f=fwd(t,d)
        if f is None: continue
        accel=gc-g30   # gap 30일 변화(가속 proxy)
        rows.append({'t':t,'d':d,'gc':gc,'accel':accel,'ms':ms,'f20':f})
df=pd.DataFrame(rows)
print(f"  관측 {len(df)}건 (고gap)")
up=df[df.accel>0]; dn=df[df.accel<=0]
print(f"  gap 확장(accel>0): 향후20d {up['f20'].mean():+.1f}% (n={len(up)})")
print(f"  gap 꺾임(accel<=0): 향후20d {dn['f20'].mean():+.1f}% (n={len(dn)})")
print(f"  → 차이 {up['f20'].mean()-dn['f20'].mean():+.1f}%p")
# 3) min_seg 통제: min_seg>=0(EPS건강)인 것만 보고도 gap가속도가 가르나?
healthy=df[df.ms>=0]
hu=healthy[healthy.accel>0]; hd=healthy[healthy.accel<=0]
print(f"\n=== min_seg>=0(EPS건강)만: gap가속도 증분 있나 (중복 검증) ===")
print(f"  EPS건강 중 gap확장: {hu['f20'].mean():+.1f}% (n={len(hu)}) | gap꺾임: {hd['f20'].mean():+.1f}% (n={len(hd)}) → Δ{hu['f20'].mean()-hd['f20'].mean():+.1f}%p")
print(f"  gap가속도 vs min_seg 상관: {spearmanr(df['accel'],df['ms']).correlation:+.2f}")
print(f"  gap가속도 IC(향후20d): {spearmanr(df['accel'],df['f20']).correlation:+.3f} / min_seg IC: {spearmanr(df['ms'],df['f20']).correlation:+.3f}")
