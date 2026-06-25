# -*- coding: utf-8 -*-
"""A: gap(high trailing PE + low forward PE)을 넓은 유니버스(us-4factor 1483종목·8년·PIT EDGAR)서 검증.
forward PE는 애널리스트 consensus가 없으니 PIT 실현성장 프록시: fwd_PE = trailing_PE/(1+g), g=EPS YoY.
(gap = trailing/forward = 1+g = 성장률. 즉 gap≈Growth팩터지만, 형의 '절대 forward<20 + trailing 높음'
 셀과 growth×value 상호작용을 직접 검증.) IC/셀/2D/연도별/광범위성.
⚠️ us-4factor는 생존편향(delisted 누락) → 절대수익 과대, but cross-sectional IC/셀 상대비교는 유효.
"""
import sys, os, json
import numpy as np, pandas as pd
from scipy.stats import spearmanr
sys.stdout.reconfigure(encoding='utf-8')
U = r'C:\dev\claude-code\us-4factor\research\mf_data'
px = pd.read_parquet(os.path.join(U, 'prices.parquet')).sort_index(); px.index = pd.to_datetime(px.index)
funda = json.load(open(os.path.join(U, 'funda.json')))
LAG = 120
KEYS = ['rev', 'ni', 'eps']
fy, pa = {}, {}
for tk, f in funda.items():
    if '_err' in f or not f.get('eps') or tk not in px.columns:
        continue
    rows = [(pd.Timestamp(d), {k: f.get(k, {}).get(d) for k in KEYS}) for d in sorted(f['eps'].keys())]
    fy[tk] = ([d + pd.Timedelta(days=LAG) for d, _ in rows], [r for _, r in rows])
    s = px[tk].dropna(); pa[tk] = (s.index.values, s.values)
UNIV = list(fy.keys())
print(f'유니버스(EPS+가격) {len(UNIV)}종목, 가격 {px.index[0].date()}~{px.index[-1].date()}')


def price_at(tk, d):
    a = pa.get(tk)
    if a is None: return None
    i = np.searchsorted(a[0], np.datetime64(d), side='right') - 1
    return float(a[1][i]) if i >= 0 else None


def avail(tk, asof):
    avd, recs = fy[tk]; i = -1
    for j in range(len(avd)):
        if avd[j] <= asof: i = j
        else: break
    return (recs[i], recs[i - 1] if i >= 1 else None) if i >= 0 else (None, None)


def feat(tk, d):
    cur, prev = avail(tk, d)
    if cur is None: return None
    eps = cur.get('eps'); p = price_at(tk, d)
    if not eps or not p or eps <= 0 or p <= 0: return None   # 적자 제외(trailing PE 양수만)
    tpe = p / eps
    g = None
    if prev and prev.get('eps') and prev['eps'] > 0:
        g = eps / prev['eps'] - 1
    fpe = tpe / (1 + g) if (g is not None and g > -0.99) else None
    return {'tpe': tpe, 'g': g, 'fpe': fpe}


me = [d for d in px.resample('ME').last().index if px.index[0] <= d <= px.index[-1]]
def fwd1m(tk, d, dn):
    p0, p1 = price_at(tk, d), price_at(tk, dn)
    return (p1 / p0 - 1) * 100 if (p0 and p1 and p0 > 0) else None


# 패널 구축
panel = []
for i in range(len(me) - 1):
    d, dn = me[i], me[i + 1]
    for tk in UNIV:
        f = feat(tk, d)
        if not f or f['fpe'] is None: continue
        fr = fwd1m(tk, d, dn)
        if fr is None: continue
        panel.append({'date': d, 'tk': tk, 'yr': d.year, **f, 'fwd': fr})
P = pd.DataFrame(panel)
print(f'패널 관측: {len(P)} / 종목 {P.tk.nunique()} / 월 {P.date.nunique()}\n')

# [1] IC (날짜별 Spearman 평균)
def ic(col, sign=1):
    rs = []
    for d, g in P.groupby('date'):
        gg = g.dropna(subset=[col, 'fwd'])
        if len(gg) >= 30 and gg[col].std() > 0:
            rs.append(spearmanr(sign * gg[col], gg.fwd)[0])
    return np.nanmean(rs), len(rs)
print('=== [1] IC (broad 1483, 익월수익) ===')
for nm, col, s in [('gap=1+g(성장)', 'g', 1), ('forward PE 낮음', 'fpe', -1), ('trailing PE 높음', 'tpe', 1)]:
    a, n = ic(col, s); print(f'  {nm:<16} IC {a:+.3f} ({n}개월)')

# [2] 형의 셀: trailing PE 높음 + forward PE<20 → 익월수익 vs 유니버스
print('\n=== [2] 형 셀: trailing PE 높음(≥T분위) AND forward PE<20 → 익월수익 ===')
base = P.fwd.mean()
print(f'  유니버스 baseline 익월 {base:+.2f}%')
for q in [0.5, 0.7, 0.8]:
    # 날짜별 trailing PE 상위 분위 + fpe<20
    sel = []
    for d, g in P.groupby('date'):
        thr = g.tpe.quantile(q)
        sel.append(g[(g.tpe >= thr) & (g.fpe < 20)])
    s = pd.concat(sel)
    print(f'  trailing PE 상위{(1-q)*100:.0f}% & fwd<20: 익월 {s.fwd.mean():+.2f}% (vs base {s.fwd.mean()-base:+.2f}p) n{len(s)} 종목{s.tk.nunique()}')

# [3] 2D 상호작용: trailing PE 분위 × forward PE 구간
print('\n=== [3] 2D: trailing PE 삼분위 × forward PE 구간 → 익월수익 평균 (n) ===')
P['tpe_t'] = P.groupby('date').tpe.transform(lambda x: pd.qcut(x, 3, labels=['저','중','고'], duplicates='drop'))
fb = [(0,15,'fPE<15'),(15,20,'15-20'),(20,40,'20-40'),(40,1e9,'≥40')]
print(f'{"trailingPE":>10}'+''.join(f'{l:>14}' for _,_,l in fb))
for t in ['저','중','고']:
    cells=[]
    for lo,hi,_ in fb:
        s=P[(P.tpe_t==t)&(P.fpe>=lo)&(P.fpe<hi)]
        cells.append(f'{s.fwd.mean():+.2f}(n{len(s)})' if len(s)>=20 else f'·(n{len(s)})')
    print(f'{t:>10}'+''.join(f'{x:>14}' for x in cells))

# [4] 연도별 (regime 안정성, 2019-22 약세 포함)
print('\n=== [4] 형 셀(trailing 상위30% & fwd<20) 연도별 익월수익 (robust?) ===')
for yr in sorted(P.yr.unique()):
    g=P[P.yr==yr]
    sel=[]
    for d,gg in g.groupby('date'):
        sel.append(gg[(gg.tpe>=gg.tpe.quantile(0.7))&(gg.fpe<20)])
    s=pd.concat(sel) if sel else pd.DataFrame()
    yb=g.fwd.mean()
    if len(s)>=10: print(f'  {yr}: 셀 {s.fwd.mean():+.2f}% vs 유니버스 {yb:+.2f}% (Δ{s.fwd.mean()-yb:+.2f}p, n{len(s)})')
