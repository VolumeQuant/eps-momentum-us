# -*- coding: utf-8 -*-
"""EDA: 'trailing PER 높은데 forward PER 낮음(<20)' = 알파인가? (사용자 원래 통찰)
유니버스 137 × 88일 패널. trailing_PE=price/trailingEPS(스냅샷·약 look-ahead), forward_PE=price/ntm_current(DB,PIT).
forward수익=가격히스토리 parquet. 셀분석·2D·단조·robust(winner제외/시기)·모멘텀이미캡처 여부.
"""
import sys, os, json, sqlite3
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr
_P = os.path.dirname(os.path.abspath(__file__))

TEPS = json.load(open(os.path.join(_P, '_trailing_eps_cache.json')))
PX = pd.read_parquet(os.path.join(_P, '_eda_px.parquet')); PX.index = pd.to_datetime(PX.index)
pidx = {d.strftime('%Y-%m-%d'): i for i, d in enumerate(PX.index)}

conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
rows = c.execute('''SELECT date,ticker,price,ntm_current,part2_rank,composite_rank
                    FROM ntm_screening WHERE composite_rank IS NOT NULL AND ntm_current IS NOT NULL''').fetchall()
conn.close()


def fwd_ret(tk, d, n):
    i = pidx.get(d)
    if i is None or tk not in PX.columns or i + n >= len(PX):
        return None
    p0, p1 = PX[tk].iloc[i], PX[tk].iloc[i + n]
    return (float(p1) / float(p0) - 1) * 100 if (pd.notna(p0) and pd.notna(p1) and p0 > 0) else None


panel = []
for d, tk, px, nc, p2, cr in rows:
    te = TEPS.get(tk)
    if not px or not nc or nc <= 0:
        continue
    fpe = px / nc
    tpe = (px / te) if (te and te > 0) else None   # trailing EPS>0만 (적자=별도)
    panel.append({'date': d, 'tk': tk, 'tpe': tpe, 'fpe': fpe, 'p2': p2, 'cr': cr,
                  'f5': fwd_ret(tk, d, 5), 'f20': fwd_ret(tk, d, 20), 'f60': fwd_ret(tk, d, 60)})
df = pd.DataFrame(panel)
print(f'패널: {len(df)}관측 / trailing PE 계산가능(적자제외) {df.tpe.notna().sum()} / f20 유효 {df.f20.notna().sum()}')
base20 = df.f20.mean(); base60 = df.f60.mean()
print(f'유니버스 baseline: f20 평균 {base20:+.1f}% (음수비율 {(df.f20<0).mean()*100:.0f}%) / f60 {base60:+.1f}%')

print('\n=== [A] 사용자 셀: trailing PE 높음(≥T) AND forward PE <20 ===')
print(f'{"trailing≥":>10}{"n":>6}{"종목수":>7}{"f20평균":>9}{"f20중앙":>9}{"f20양수%":>9}{"f60평균":>9}{"vs base20":>10}')
d20 = df[df.f20.notna() & df.tpe.notna()]
for T in [25, 30, 40, 50]:
    s = d20[(d20.tpe >= T) & (d20.fpe < 20)]
    if len(s):
        print(f'{T:>10}{len(s):>6}{s.tk.nunique():>7}{s.f20.mean():>+9.1f}{s.f20.median():>+9.1f}{(s.f20>0).mean()*100:>8.0f}%{s.f60.mean():>+9.1f}{s.f20.mean()-base20:>+10.1f}')

print('\n=== [B] 2D 히트맵: trailing PE(행) × forward PE(열) → f20 평균 (n) ===')
tbins = [(0,20,'tPE<20'),(20,40,'20-40'),(40,80,'40-80'),(80,1e9,'≥80')]
fbins = [(0,15,'fPE<15'),(15,20,'15-20'),(20,30,'20-30'),(30,1e9,'≥30')]
print(f'{"":>10}'+''.join(f'{fl:>14}' for _,_,fl in fbins))
for tlo,thi,tl in tbins:
    cells=[]
    for flo,fhi,fl in fbins:
        s=d20[(d20.tpe>=tlo)&(d20.tpe<thi)&(d20.fpe>=flo)&(d20.fpe<fhi)]
        cells.append(f'{s.f20.mean():+.1f}(n{len(s)})' if len(s)>=5 else (f'·(n{len(s)})' if len(s) else '—'))
    print(f'{tl:>10}'+''.join(f'{x:>14}' for x in cells))

print('\n=== [C] 핵심검증: forward PE<20 안에서 trailing PE 높을수록 더 오르나? (사용자 가설) ===')
lowf = d20[d20.fpe < 20]
print(f'  forward PE<20 표본 {len(lowf)}, f20 평균 {lowf.f20.mean():+.1f}%')
for tlo,thi,tl in [(0,30,'tPE<30'),(30,60,'30-60'),(60,1e9,'≥60')]:
    s=lowf[(lowf.tpe>=tlo)&(lowf.tpe<thi)]
    if len(s): print(f'    {tl:>8}: f20 {s.f20.mean():+.1f}% (n{len(s)}, 종목{s.tk.nunique()})')
print(f'  (대조) forward PE≥20 전체: f20 {d20[d20.fpe>=20].f20.mean():+.1f}% (n{len(d20[d20.fpe>=20])})')

print('\n=== [D] robustness: 위닝셀(tPE≥40 & fPE<20) 누가 채우나 + winner제외 + 시기분할 ===')
win = d20[(d20.tpe >= 40) & (d20.fpe < 20)]
print('  위닝셀 종목별 관측수:', dict(win.tk.value_counts().head(10)))
WIN = ['SNDK','MU','STX','NVDA','LITE','COHR']
nw = win[~win.tk.isin(WIN)]
print(f'  위닝셀 f20: 전체 {win.f20.mean():+.1f}%(n{len(win)}) / winner6제외 {nw.f20.mean():+.1f}%(n{len(nw)}, 종목{nw.tk.nunique()})')
mid = sorted(df.date.unique())[len(df.date.unique())//2]
for lab,sub in [('전반',win[win.date<mid]),('후반',win[win.date>=mid])]:
    if len(sub): print(f'  {lab}: f20 {sub.f20.mean():+.1f}% (n{len(sub)})')

print('\n=== [E] 이미 모멘텀이 잡나? 위닝셀 관측의 part2_rank 분포 ===')
wr = win.dropna(subset=['p2'])
print(f'  위닝셀 중 part2_rank 보유 {len(wr)}/{len(win)} | rank≤5 비율 {(wr.p2<=5).mean()*100:.0f}% | rank≤12 {(wr.p2<=12).mean()*100:.0f}% | rank>12(모멘텀 놓침) {(wr.p2>12).mean()*100:.0f}%')
miss = wr[wr.p2 > 12]
if len(miss):
    misstks = sorted(miss.tk.unique())[:12]
    print(f'  ★모멘텀이 놓친(rank>12) 위닝셀: n{len(miss)}, f20 {miss.f20.mean():+.1f}%, 종목 {misstks}')
wincr = win.dropna(subset=['cr'])
cr30 = (wincr.cr <= 30).mean() * 100 if len(wincr) else 0
print(f'  cr(composite) 보유 위닝셀 rank<=30 비율 {cr30:.0f}%')

print('\n=== [F] IC: 신호별 cross-sectional 상관 (날짜별 → 평균) ===')
def daily_ic(col):
    ics=[]
    for d,g in df.dropna(subset=['f20',col]).groupby('date'):
        if len(g)>=8 and g[col].std()>0: ics.append(g[col].corr(g.f20,method='spearman'))
    return np.mean(ics) if ics else np.nan
df['inv_fpe']=-df.fpe                      # forward PE 낮을수록 좋다는 가설
df['gap']=df.tpe/df.fpe                    # 비율
df['hi_t_lo_f']=np.where((df.fpe<20),df.tpe,np.nan)  # fPE<20 조건부 trailing PE
ic_invf=daily_ic('inv_fpe'); ic_gap=daily_ic('gap'); ic_htlf=daily_ic('hi_t_lo_f')
print(f'  forward PE 낮음(inv_fpe) IC: {ic_invf:+.3f}')
print(f'  gap(trailing/forward) IC: {ic_gap:+.3f}')
print(f'  fPE<20 조건부 trailing PE 높음 IC: {ic_htlf:+.3f}')
