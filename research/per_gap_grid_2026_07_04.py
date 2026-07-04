# -*- coding: utf-8 -*-
"""PER게이트 × gap게이트 2차원 그리드 (top5 R5, 위상 0~4 평균 + LOWO ex-SNDK/MU).
질문: 'PER<=25 모멘텀순 top5'와 'gap>=2.5'를 합치면? 채택안이 고원 중앙인가 절벽 끝인가."""
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
TE = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm.json'), encoding='utf-8'))
def pit_te(tk, d):
    r = TE.get(tk); v = None
    if not r: return None
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

def run(pe_max, gap_thr, phase=0, exclude=frozenset(), N=5, R=5, start=2):
    hold = []; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(hold); drr = 0.0
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp
        nav *= (1 + drr); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if i % R == phase:
            cand = []
            for tk, v in FULL.get(d, {}).items():
                if tk in exclude or not industry_ok(tk): continue
                dv = DV.get(d, {}).get(tk)
                if dv is None or dv < 1000: continue
                if ms(v) < 0: continue
                if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
                if v['px'] / v['nc'] > pe_max: continue
                if gap_thr:
                    te = pit_te(tk, d); g = (v['nc'] / te) if (te and te > 0) else None
                    if g is not None and g < gap_thr: continue
                cand.append((tk, rev90(v)))
            cand.sort(key=lambda x: -x[1]); hold = [t for t, _ in cand[:N]]
    return (nav - 1) * 100, mdd * 100

PES = [15, 18, 20, 25, 30, 999]
GAPS = [None, 2.0, 2.5, 3.0]
print('=== PER × gap 그리드 (top5 R5) — 셀: 위상평균수익% / MDD평균 / LOWO ex-SNDK/MU ===')
hdr = f'{"":>8} | ' + ' | '.join(f'{"gap없음" if g is None else f"gap>={g}":^20}' for g in GAPS)
print(hdr)
for pe in PES:
    row = f'PER<={pe if pe < 999 else "∞":>3} |'
    for g in GAPS:
        rs = [run(pe, g, phase=p) for p in range(5)]
        lo = run(pe, g, exclude=frozenset(['SNDK', 'MU']))
        row += f' {np.mean([r[0] for r in rs]):+5.0f}/{np.mean([r[1] for r in rs]):+4.0f}/ex2{lo[0]:+4.0f} |'
    print(row)
print('\n읽는법: 수익/MDD는 위상 0~4 평균(단일위상 노이즈 제거), ex2=SNDK·MU 둘 다 제외 시 수익(위상0).')
