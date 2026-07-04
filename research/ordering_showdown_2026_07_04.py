# -*- coding: utf-8 -*-
"""정렬기준 판정전: 게이트 내 ①싼순(참조구현 rebuild_reference_impl) ②모멘텀순(rev90)
③블렌드(PER백분위+rev90백분위) — 동일 하네스·복구 dv·위상평균·LOWO로 공정 비교.
(참조구현의 carry-forward dv 대신 재구축 dv 사용, 업종제외 동일 적용)"""
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
BAD_IND = dr.COMMODITY_INDUSTRIES | dr.OFF_STRATEGY_INDUSTRIES
BAD_TK = set(dr.COMMODITY_TICKERS)
def industry_ok(tk):
    if tk in BAD_TK: return False
    v = TC.get(tk)
    ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v)
    return not (isinstance(ind, str) and ind in BAD_IND)
def ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)
def rev90(v):
    return (v['nc'] - v['n90']) / abs(v['n90']) * 100 if (v['n90'] and abs(v['n90']) > 0.01) else 0

def elig(d, pe_max, exclude):
    out = []
    for tk, v in FULL.get(d, {}).items():
        if tk in exclude or not industry_ok(tk): continue
        dv = DV.get(d, {}).get(tk)
        if dv is None or dv < 1000: continue
        if ms(v) < 0: continue
        if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
        if v['px'] / v['nc'] > pe_max: continue
        out.append((tk, v))
    return out

def pick(d, N, pe_max, order, exclude=frozenset()):
    cand = elig(d, pe_max, exclude)
    if order == 'cheap':
        cand.sort(key=lambda x: x[1]['px'] / x[1]['nc'])
    elif order == 'mom':
        cand.sort(key=lambda x: -rev90(x[1]))
    else:  # blend: 백분위 합 (싼 백분위 + 모멘텀 백분위)
        pes = sorted(x[1]['px'] / x[1]['nc'] for x in cand)
        rvs = sorted(rev90(x[1]) for x in cand)
        import bisect
        def pct(arr, val): return bisect.bisect_left(arr, val) / max(len(arr) - 1, 1)
        cand.sort(key=lambda x: -((1 - pct(pes, x[1]['px'] / x[1]['nc'])) + pct(rvs, rev90(x[1]))))
    return [t for t, _ in cand[:N]]

def run(N, R, pe_max, order, start=2, end=None, phase=0, exclude=frozenset()):
    end = end if end is not None else len(ad)
    hold = []; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(start, end):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(hold); drr = 0.0
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp
        nav *= (1 + drr); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if i % R == phase:
            hold = pick(d, N, pe_max, order, exclude)
    return (nav - 1) * 100, mdd * 100, set(hold)

print('=== 정렬기준 × 게이트 (top5 R5, 위상 0~4 평균) ===')
print(f'{"게이트":>8} | {"①싼순(참조구현)":^22} | {"②모멘텀순":^18} | {"③블렌드":^18}')
for pe in [15, 18, 20, 25]:
    row = f'PER<={pe:>3} |'
    for od in ['cheap', 'mom', 'blend']:
        rs = [run(5, 5, pe, od, phase=p) for p in range(5)]
        row += f'  {np.mean([r[0] for r in rs]):+6.0f}% / MDD{np.mean([r[1] for r in rs]):+5.0f}  |'
    print(row)

print('\n=== 반기 walk-forward (top5 R5 위상0) ===')
half = len(ad) // 2
for od, pe in [('cheap', 18), ('mom', 25), ('blend', 20)]:
    a = run(5, 5, pe, od, 2, half); b = run(5, 5, pe, od, half, len(ad))
    print(f'  {od:6} PER{pe}: 전반 {a[0]:+5.0f}%/{a[1]:+4.0f}  후반 {b[0]:+5.0f}%/{b[1]:+4.0f}')

print('\n=== LOWO −SNDK−MU (top5 R5 위상0) ===')
for od, pe in [('cheap', 18), ('mom', 25), ('blend', 20), ('blend', 25)]:
    r = run(5, 5, pe, od, exclude=frozenset(['SNDK', 'MU']))
    f = run(5, 5, pe, od)
    print(f'  {od:6} PER{pe}: full {f[0]:+6.0f}%/{f[1]:+4.0f} → ex2 {r[0]:+6.0f}%/{r[1]:+4.0f}')

print('\n=== 오늘 보유 비교 ===')
for od, pe in [('cheap', 18), ('mom', 25), ('blend', 20)]:
    print(f'  {od:6} PER{pe}: {pick(ad[-1], 5, pe, od)}')
