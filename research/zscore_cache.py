"""
Step 1: 공통 데이터 캐시 — 전 일자 conv_gaps + z-score + NTM/가격 + 포트폴리오 시뮬용
한 번 실행 → pickle 저장 → 이후 모든 Step에서 재사용 (DB/yfinance 재조회 0회)
"""
import sqlite3
import numpy as np
import pickle
import time
import sys
from datetime import datetime, timedelta
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = '../eps_momentum_data.db'
CACHE_FILE = 'zscore_cache.pkl'

t0 = time.time()

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# 1. 전 일자 (composite_rank 있는)
cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date')
all_dates = [r[0] for r in cur.fetchall()]
print(f"전체 일자: {len(all_dates)} ({all_dates[0]}~{all_dates[-1]})")

# part2_rank 있는 일자
cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')
p2_dates = [r[0] for r in cur.fetchall()]
print(f"part2 일자: {len(p2_dates)}")

# 2. 전 일자 raw 데이터
raw = {}  # date → {ticker: {adj_gap, rev_up30, num_analysts, ntm_cur, ntm_90d, rev_growth, ntm_7d, ntm_30d, ntm_60d, price, comp_rank, p2_rank}}
for d in all_dates:
    rows = cur.execute('''
        SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d,
               rev_growth, ntm_7d, ntm_30d, ntm_60d, price, composite_rank, part2_rank
        FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
    ''', (d,)).fetchall()
    raw[d] = {}
    for r in rows:
        raw[d][r[0]] = {
            'adj_gap': r[1], 'rev_up30': r[2] or 0, 'num_analysts': r[3] or 0,
            'ntm_cur': r[4] or 0, 'ntm_90d': r[5] or 0, 'rev_growth': r[6],
            'ntm_7d': r[7] or 0, 'ntm_30d': r[8] or 0, 'ntm_60d': r[9] or 0,
            'price': r[10] or 0, 'comp_rank': r[11], 'p2_rank': r[12],
        }

# 3. conviction adj_gap 계산 (시스템 _apply_conviction 재현)
def apply_conviction(v):
    ag = v['adj_gap'] or 0
    ratio = (v['rev_up30'] / v['num_analysts']) if v['num_analysts'] > 0 else 0
    eps_floor = min(abs((v['ntm_cur'] - v['ntm_90d']) / v['ntm_90d']), 1.0) \
        if v['ntm_90d'] and abs(v['ntm_90d']) > 0.01 else 0
    base_conv = max(ratio, eps_floor)
    rev_bonus = 0.3 if (v['rev_growth'] is not None and v['rev_growth'] >= 0.30) else 0
    return ag * (1 + base_conv + rev_bonus)

conv_gaps_by_date = {}  # date → {ticker: conv_adj_gap}
for d in all_dates:
    conv_gaps_by_date[d] = {tk: apply_conviction(v) for tk, v in raw[d].items()}

# 4. z-score (현재 시스템: mean/std, 30~100 clamp)
zscore_by_date = {}    # date → {ticker: z_score(30~100)}
zscore_raw_by_date = {}  # date → {ticker: z_raw(clamp 전)}
zscore_stats_by_date = {}  # date → {mean, std, n, min, max, skew}
for d in all_dates:
    vals = list(conv_gaps_by_date[d].values())
    if len(vals) >= 2:
        m, s = np.mean(vals), np.std(vals)
        if s > 0:
            raw_scores = {tk: 65 + (-(v - m) / s) * 15 for tk, v in conv_gaps_by_date[d].items()}
            clamped = {tk: min(100.0, max(30.0, v)) for tk, v in raw_scores.items()}
        else:
            raw_scores = {tk: 65 for tk in conv_gaps_by_date[d]}
            clamped = raw_scores.copy()
    else:
        raw_scores = {tk: 65 for tk in conv_gaps_by_date[d]}
        clamped = raw_scores.copy()
        m, s = 0, 0

    zscore_by_date[d] = clamped
    zscore_raw_by_date[d] = raw_scores
    arr = np.array(vals)
    zscore_stats_by_date[d] = {
        'mean': m, 'std': s, 'n': len(vals),
        'min': arr.min() if len(arr) else 0, 'max': arr.max() if len(arr) else 0,
        'skew': float(((arr - m) ** 3).mean() / (s ** 3)) if s > 0 else 0,
    }

# 5. 3일 가중 w_gap (현재 시스템 재현)
MISS = 30
wgap_by_date = {}
for di, d in enumerate(p2_dates):
    dates_3 = [p2_dates[max(0, di-2)], p2_dates[max(0, di-1)], p2_dates[di]]
    dates_3 = [dd for dd in dates_3 if dd in zscore_by_date]
    weights = [0.2, 0.3, 0.5][-len(dates_3):]
    if len(dates_3) == 2: weights = [0.4, 0.6]
    elif len(dates_3) == 1: weights = [1.0]

    all_tickers = set()
    for dd in dates_3:
        all_tickers.update(zscore_by_date[dd].keys())

    wgap = {}
    for tk in all_tickers:
        wg = 0
        for i, dd in enumerate(dates_3):
            sc = zscore_by_date[dd].get(tk, MISS)
            wg += sc * weights[i]
        wgap[tk] = wg
    wgap_by_date[d] = wgap

# 6. NTM/가격 변화율 (Case 1 보너스용)
def get_past_date(d, days):
    target = (datetime.strptime(d, '%Y-%m-%d') - timedelta(days=days)).strftime('%Y-%m-%d')
    r = cur.execute('SELECT MAX(date) FROM ntm_screening WHERE date <= ?', (target,)).fetchone()
    return r[0] if r and r[0] else None

# 가격 캐시 (전 일자)
all_prices = {}
for d in all_dates:
    rows = cur.execute('SELECT ticker, price FROM ntm_screening WHERE date=? AND price>0', (d,)).fetchall()
    all_prices[d] = {r[0]: r[1] for r in rows}

chg_data = {}  # date → {ticker: {ntm_Xd_chg, px_Xd_chg, min_seg, blend_gap}}
for d in all_dates:
    chg_data[d] = {}
    past_d = {p: get_past_date(d, p) for p in [7, 30, 60, 90]}
    past_px = {p: all_prices.get(pd, {}) if pd else {} for p, pd in past_d.items()}

    for tk, v in raw[d].items():
        nc = v['ntm_cur']; px_now = v['price']
        ntm_map = {'7d': v['ntm_7d'], '30d': v['ntm_30d'], '60d': v['ntm_60d'], '90d': v.get('ntm_90d', 0)}
        chgs = {}
        for period, nval in ntm_map.items():
            days = int(period.replace('d', ''))
            chgs[f'ntm_{period}'] = ((nc - nval) / nval * 100) if nval and abs(nval) > 0.01 else 0
            pp = past_px.get(days, {}).get(tk)
            chgs[f'px_{period}'] = ((px_now - pp) / pp * 100) if pp and pp > 0 and px_now else 0

        # min_seg
        segs = []
        for a, b in [(nc, v['ntm_7d']), (v['ntm_7d'], v['ntm_30d']),
                     (v['ntm_30d'], v['ntm_60d']), (v['ntm_60d'], v.get('ntm_90d', 0))]:
            if b and abs(b) > 0.01:
                segs.append(max(-100, min(100, (a - b) / abs(b) * 100)))
            else:
                segs.append(0)
        chgs['min_seg'] = min(segs)

        chg_data[d][tk] = chgs

conn.close()

# 7. 저장
cache = {
    'all_dates': all_dates,
    'p2_dates': p2_dates,
    'raw': raw,
    'conv_gaps_by_date': conv_gaps_by_date,
    'zscore_by_date': zscore_by_date,
    'zscore_raw_by_date': zscore_raw_by_date,
    'zscore_stats_by_date': zscore_stats_by_date,
    'wgap_by_date': wgap_by_date,
    'chg_data': chg_data,
    'all_prices': all_prices,
}

with open(CACHE_FILE, 'wb') as f:
    pickle.dump(cache, f)

elapsed = time.time() - t0
print(f"\n캐시 저장: {CACHE_FILE}")
print(f"  all_dates: {len(all_dates)}, p2_dates: {len(p2_dates)}")
print(f"  conv_gaps: {sum(len(v) for v in conv_gaps_by_date.values())}건")
print(f"  zscore: {sum(len(v) for v in zscore_by_date.values())}건")
print(f"  wgap: {sum(len(v) for v in wgap_by_date.values())}건")
print(f"  chg_data: {sum(len(v) for v in chg_data.values())}건")
print(f"  소요: {elapsed:.1f}초")
