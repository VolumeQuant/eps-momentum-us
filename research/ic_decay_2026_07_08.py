# -*- coding: utf-8 -*-
"""rev90 신호의 IC decay(알파 반감기) 측정.
질문: R5(5거래일 리밸) 주기가 신호 수명과 맞는지.
게이트(=research/per_gap_grid_2026_07_04.py run() 필터 그대로): 업종제외 + dv>=$1B +
min_seg(ms)>=0 + nc>0,n90>0.1 + fwd_PER(price/nc)<=30 + gap(nc/trailing_eps)>=2.5(missing=pass).
각 게이트일 d의 rev90 횡단면과 h={1,2,3,5,7,10,15,20} 거래일 forward 수익의 Spearman IC.
production 코드/데이터 변경 없음 (research 전용).
"""
import sys, os, json, sqlite3
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr

conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
FULL = {}
for tk, d, px, nc, n7, n30, n60, n90 in c.execute(
        'SELECT ticker,date,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d '
        'FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0'):
    FULL.setdefault(d, {})[tk] = dict(px=px, nc=nc, n7=n7, n30=n30, n60=n60, n90=n90)
conn.close()
PD = sorted(FULL.keys())  # 전체 거래일 (price 존재)
AP = {d: {t: v['px'] for t, v in FULL.get(d, {}).items()} for d in PD}

DVF = pd.read_parquet(os.path.join(BASE, 'research', 'dv_full_2026_07_04.parquet'))
DVF.index = pd.to_datetime(DVF.index).strftime('%Y-%m-%d')
DV = {d: {t: (None if pd.isna(DVF.loc[d, t]) else float(DVF.loc[d, t])) for t in DVF.columns}
      for d in DVF.index if d in set(PD)}

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

PE_MAX, GAP_THR = 30, 2.5

def gate_candidates(d):
    """per_gap_grid run()과 동일한 게이트. (tk, rev90) 리스트 반환."""
    out = []
    for tk, v in FULL.get(d, {}).items():
        if not industry_ok(tk):
            continue
        dv = DV.get(d, {}).get(tk)
        if dv is None or dv < 1000:
            continue
        if ms(v) < 0:
            continue
        if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1:
            continue
        if v['px'] / v['nc'] > PE_MAX:
            continue
        te = pit_te(tk, d)
        g = (v['nc'] / te) if (te and te > 0) else None
        if g is not None and g < GAP_THR:
            continue
        out.append((tk, rev90(v)))
    return out

HORIZONS = [1, 2, 3, 5, 7, 10, 15, 20]
MAX_H = max(HORIZONS)

# DV(dv_full)가 존재하는 날짜만 신호일 후보로 사용 (게이트 계산 가능한 날)
sig_dates = sorted(set(DV.keys()) & set(PD))
pd_index = {d: i for i, d in enumerate(PD)}

ic_by_h = {h: [] for h in HORIZONS}
n_names_by_h = {h: [] for h in HORIZONS}

for d in sig_dates:
    i = pd_index[d]
    if i + MAX_H >= len(PD):
        continue
    cand = gate_candidates(d)
    if len(cand) < 5:
        continue
    px_d = AP.get(d, {})
    for h in HORIZONS:
        d_h = PD[i + h]
        px_h = AP.get(d_h, {})
        sig, fwd = [], []
        for tk, s in cand:
            p0, p1 = px_d.get(tk), px_h.get(tk)
            if p0 and p1 and p0 > 0:
                sig.append(s); fwd.append(p1 / p0 - 1)
        if len(sig) < 5:
            continue
        ic, _ = spearmanr(sig, fwd)
        if not np.isnan(ic):
            ic_by_h[h].append(ic)
            n_names_by_h[h].append(len(sig))

print('=== rev90 IC decay (게이트 통과자, PE<=30 & gap>=2.5) ===')
print(f'{"h(거래일)":>9} | {"일수":>4} | {"평균종목수":>7} | {"평균IC":>7} | {"t값":>6} | {"양수비율":>7}')
means = {}
for h in HORIZONS:
    ics = np.array(ic_by_h[h])
    n = len(ics)
    if n == 0:
        print(f'{h:>9} | {0:>4} | {"-":>7} | {"-":>7} | {"-":>6} | {"-":>7}')
        continue
    mean_ic = ics.mean()
    t = mean_ic / (ics.std(ddof=1) / np.sqrt(n)) if n > 1 and ics.std(ddof=1) > 0 else float('nan')
    pos = (ics > 0).mean()
    avg_n = np.mean(n_names_by_h[h])
    means[h] = mean_ic
    print(f'{h:>9} | {n:>4} | {avg_n:>7.1f} | {mean_ic:>+7.4f} | {t:>6.2f} | {pos:>6.1%}')

# 반감기 추정: peak |IC| 대비 절반 이하로 처음 떨어지는 h
if means:
    peak_h = max(means, key=lambda h: abs(means[h]))
    peak_v = abs(means[peak_h])
    half = peak_v / 2
    hl = None
    for h in HORIZONS:
        if h <= peak_h:
            continue
        if abs(means[h]) < half:
            hl = h
            break
    print(f'\npeak IC: h={peak_h} (IC={means[peak_h]:+.4f})')
    print(f'반감기(peak 대비 |IC| 50% 이하로 첫 하락하는 h): {hl if hl else "20일 내 미도달"}')
