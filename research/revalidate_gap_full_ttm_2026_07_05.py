# -*- coding: utf-8 -*-
"""gap 게이트 전종목 TTM 재검증 — 커버리지 14%(현행 sparse) vs 전체(full) 비교.

질문: 게이트가 '설계 의도대로' 전 종목을 검사하면 재설계(top4 R5 PER30 gap2.5)가 살아남는가?
셀: TTM 소스(sparse/full) × gap(없음/2.0/2.5/3.0), 위상 0~4 평균 + LOWO ex-SNDK/MU + 오늘 픽 diff."""
import sys, os, json, sqlite3
import numpy as np
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr

conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
FULL = {}
for tk, d, px, nc, n7, n30, n60, n90 in c.execute(
        'SELECT ticker,date,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d '
        'FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0'):
    FULL.setdefault(d, {})[tk] = dict(px=px, nc=nc, n7=n7, n30=n30, n60=n60, n90=n90)
conn.close()
AP = {d: {t: v['px'] for t, v in FULL.get(d, {}).items()} for d in ad}
DVF = pd.read_parquet(os.path.join(BASE, 'research', 'dv_full_2026_07_04.parquet'))
DVF.index = pd.to_datetime(DVF.index).strftime('%Y-%m-%d')
DV = {d: {t: (None if pd.isna(DVF.loc[d, t]) else float(DVF.loc[d, t])) for t in DVF.columns} for d in DVF.index if d in set(ad)}
TC = json.load(open(os.path.join(BASE, 'ticker_info_cache.json'), encoding='utf-8'))
BAD = dr.COMMODITY_INDUSTRIES | dr.OFF_STRATEGY_INDUSTRIES
BADTK = set(dr.COMMODITY_TICKERS)
def iok(tk):
    if tk in BADTK: return False
    v = TC.get(tk)
    ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v)
    return not (isinstance(ind, str) and ind in BAD)
TE_SPARSE = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm.json'), encoding='utf-8'))
TE_FULL = json.load(open(os.path.join(BASE, 'research', 'trailing_eps_ttm_full_2026_07_05.json'), encoding='utf-8'))
def pit_te(TE, tk, d):
    r = TE.get(tk)
    if not r or tk == '_meta': return None
    v = None
    for rd, e in r:
        if rd <= d: v = e
        else: break
    return v
def ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)
def rev90(v):
    return (v['nc'] - v['n90']) / abs(v['n90']) * 100 if (v['n90'] and abs(v['n90']) > 0.01) else 0
def pick(d, TE, gap_thr, exclude=frozenset()):
    cand = []
    for tk, v in FULL.get(d, {}).items():
        if tk in exclude or not iok(tk): continue
        dv = DV.get(d, {}).get(tk)
        if dv is None or dv < 1000: continue
        if ms(v) < 0: continue
        if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
        if v['px'] / v['nc'] > 30: continue
        if gap_thr:
            te = pit_te(TE, tk, d)
            g = (v['nc'] / te) if (te and te > 0) else None
            if g is not None and g < gap_thr: continue
        cand.append((tk, rev90(v)))
    cand.sort(key=lambda x: -x[1])
    return [t for t, _ in cand[:4]]
def run(TE, gap_thr, phase=0, exclude=frozenset()):
    hold = []; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        drr = 0.0; n = len(hold)
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp
        nav *= (1 + drr); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if i % 5 == 2 % 5:
            hold = pick(d, TE, gap_thr, exclude)
    return (nav - 1) * 100, mdd * 100

nonempty = sum(1 for k, v in TE_FULL.items() if v and k != '_meta')
print(f'full TTM 유효 종목: {nonempty}/{len(TE_FULL)}')
print()
print('=== TTM 소스 × gap 임계 (top4 R5 PER30, 위상평균 / MDD / ex-SNDK·MU) ===')
for name, TE in [('sparse(현행 141)', TE_SPARSE), ('full(전종목)', TE_FULL)]:
    for g in [None, 2.0, 2.5, 3.0]:
        rs = [run(TE, g, phase=p) for p in range(5)]
        lo = run(TE, g, phase=2, exclude=frozenset(['SNDK', 'MU']))
        gl = '없음' if g is None else f'>={g}'
        print(f'{name:18} gap{gl:>6}: {np.mean([r[0] for r in rs]):+5.0f}% (범위 {min(r[0] for r in rs):+.0f}~{max(r[0] for r in rs):+.0f}) / MDD {np.mean([r[1] for r in rs]):+4.0f} / ex2 {lo[0]:+5.0f}%')
    print()
d_last = ad[-1]
print(f'오늘({d_last}) 픽 비교 (gap>=2.5):')
print('  sparse:', pick(d_last, TE_SPARSE, 2.5))
print('  full  :', pick(d_last, TE_FULL, 2.5))
# 게이트 실효(최근 20일, full)
kn = cut = tot = 0
for d in ad[-20:]:
    for tk, v in FULL.get(d, {}).items():
        if not iok(tk): continue
        dv = DV.get(d, {}).get(tk)
        if dv is None or dv < 1000: continue
        if ms(v) < 0 or v['nc'] <= 0 or (v['n90'] or 0) <= 0.1 or v['px'] / v['nc'] > 30: continue
        tot += 1
        te = pit_te(TE_FULL, tk, d)
        if te and te > 0:
            kn += 1
            if v['nc'] / te < 2.5: cut += 1
print(f'\nfull 기준 게이트 실효(최근 20일 풀): 계산가능 {kn}/{tot}={kn/tot*100:.0f}%, 컷 {cut}')
