"""v80.9 (X2) base 위에서 후보 B/C/D/E 단건 검증 (12시작일)

후보:
  B. min_seg = min(s1~s4) 변경
     B1: 평균 (avg)
     B2: 중앙값 (median)
     B3: 시간 가중 평균
     B4: 최근 2개만 검사 (s1, s2)
  C. direction smoothing (현재: (s1+s2)/2 - (s3+s4)/2)
     C1: 시간 가중 derivative
  D. eps_quality min_seg ±2% 임계값
     D1: ±1% (좁게)
     D2: ±3% (넓게)
  E. rev_bonus cap 0.3 변경
     E1: cap 0.5
     E2: cap 0.2

base = v80.9 (X2 적용된 상태, rev_up30 ≥ 3)
"""
import sqlite3
import shutil
import sys
import statistics
import math
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import numpy as np
import daily_runner as dr
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'v80_9_extras_dbs'
GRID.mkdir(exist_ok=True)
SEG_CAP = 100


def calc_segs(nc, n7, n30, n60, n90):
    if not all(x and abs(x) > 0.01 for x in (n7, n30, n60, n90)):
        return [0, 0, 0, 0]
    s1 = max(-SEG_CAP, min(SEG_CAP, (nc-n7)/abs(n7)*100))
    s2 = max(-SEG_CAP, min(SEG_CAP, (n7-n30)/abs(n30)*100))
    s3 = max(-SEG_CAP, min(SEG_CAP, (n30-n60)/abs(n60)*100))
    s4 = max(-SEG_CAP, min(SEG_CAP, (n60-n90)/abs(n90)*100))
    return [s1, s2, s3, s4]


def calc_min_seg_variant(segs, variant):
    if variant == 'baseline':
        return min(segs)
    elif variant == 'avg':
        return sum(segs) / len(segs)
    elif variant == 'median':
        s = sorted(segs)
        return (s[1] + s[2]) / 2
    elif variant == 'weighted':
        return segs[0]*0.4 + segs[1]*0.3 + segs[2]*0.2 + segs[3]*0.1
    elif variant == 'recent2':
        return min(segs[0], segs[1])
    return min(segs)


def x2_conv(adj_gap, ru, na, nc, n90, rev_growth=None, eps_q_thr=2.0, rev_bonus_cap=0.3):
    """X2 base + 추가 변형"""
    ru = ru or 0; na = na or 0; rg = rev_growth or 0
    ratio = ru/na if na > 0 else 0
    eps_floor = 0
    if nc is not None and n90 and abs(n90) > 0.01:
        eps_floor = min(abs((nc-n90)/n90), 3.0)
    base = max(ratio, eps_floor)
    rb = min(min(rg, 0.5)*0.6, rev_bonus_cap)
    return adj_gap * (1 + base + rb)


def regenerate(test_db, min_seg_variant='baseline', conv_kw=None):
    """elig 컷 + min_seg variant 결과를 별도 dict로 반환 (simulate에서 사용)."""
    if conv_kw is None: conv_kw = {}
    original_path = dr.DB_PATH
    original_fn = dr._apply_conviction
    dr.DB_PATH = str(test_db)
    dr._apply_conviction = lambda ag, ru, na, nc, n90, rev_growth=None: x2_conv(ag, ru, na, nc, n90, rev_growth=rev_growth, **conv_kw)
    ms_map = {}  # (date, ticker) -> variant min_seg
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]
        for today in dates:
            rows = cur.execute('''SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_7d,
                       ntm_30d, ntm_60d, ntm_90d, rev_growth
                       FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL''', (today,)).fetchall()
            elig_conv = []
            for r in rows:
                tk, ag, ru, na, nc, n7, n30, n60, n90, rg = r
                segs = calc_segs(nc or 0, n7 or 0, n30 or 0, n60 or 0, n90 or 0)
                ms_v = calc_min_seg_variant(segs, min_seg_variant)
                ms_map[(today, tk)] = ms_v
                if ag is None: continue
                if ms_v < -2: continue
                if (ru or 0) < 3: continue
                cg = x2_conv(ag, ru, na, nc, n90, rev_growth=rg, **conv_kw)
                if cg is not None: elig_conv.append((tk, cg))
            elig_conv.sort(key=lambda x: x[1])
            new_cr = {tk: i + 1 for i, (tk, _) in enumerate(elig_conv)}
            cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
            for tk, cr in new_cr.items():
                cur.execute('UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?', (cr, today, tk))
            tickers = list(new_cr.keys())
            wmap = dr._compute_w_gap_map(cur, today, tickers)
            sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
            top30 = sorted_w[:30]
            cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
            for rk, tk in enumerate(top30, 1):
                cur.execute('UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?', (rk, today, tk))
            conn.commit()
        conn.close()
    finally:
        dr.DB_PATH = original_path
        dr._apply_conviction = original_fn
    return ms_map


def simulate_full(dates_all, data, entry_top, exit_top, max_slots, start_date, ms_map=None, min_seg_variant='baseline'):
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    daily_returns = []
    trades = []
    consecutive = defaultdict(int)
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    consecutive[tk] = consecutive.get(tk, 0) + 1
    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consecutive = defaultdict(int)
        for tk in rank_map:
            new_consecutive[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consecutive
        day_ret = 0
        if portfolio:
            for tk in portfolio:
                price = today_data.get(tk, {}).get('price')
                if price and di > 0:
                    prev = data.get(dates[di - 1], {}).get(tk, {}).get('price')
                    if prev and prev > 0:
                        day_ret += (price - prev) / prev * 100
            day_ret /= len(portfolio)
            daily_returns.append(day_ret)
        else:
            daily_returns.append(0)
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            if ms_map is not None and (today, tk) in ms_map:
                ms = ms_map[(today, tk)]
            else:
                ms = today_data.get(tk, {}).get('min_seg', 0)
            price = today_data.get(tk, {}).get('price')
            should_exit = False
            if rank is None or rank > exit_top: should_exit = True
            if ms < -2: should_exit = True
            if should_exit and price:
                ep = portfolio[tk]['entry_price']
                ret = (price - ep) / ep * 100
                trades.append(ret)
                exited.append(tk)
        for tk in exited: del portfolio[tk]
        vacancies = max_slots - len(portfolio)
        if vacancies > 0:
            cands = []
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > entry_top: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                if ms_map is not None and (today, tk) in ms_map:
                    ms = ms_map[(today, tk)]
                else:
                    ms = today_data.get(tk, {}).get('min_seg', 0)
                if ms < 0: continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0: cands.append((tk, price))
            for tk, price in cands[:vacancies]:
                portfolio[tk] = {'entry_price': price, 'entry_date': today}
    return daily_returns, trades


def calc_metrics(daily_returns, trades, n_days):
    cum_ret = 1.0
    peak = 1.0
    max_dd = 0
    for r in daily_returns:
        cum_ret *= (1 + r / 100)
        peak = max(peak, cum_ret)
        dd = (cum_ret - peak) / peak * 100
        max_dd = min(max_dd, dd)
    total_return = (cum_ret - 1) * 100
    if len(daily_returns) > 1:
        avg_d = sum(daily_returns) / len(daily_returns)
        std_d = statistics.pstdev(daily_returns)
        sharpe = (avg_d / std_d * math.sqrt(252)) if std_d > 0 else 0
    else:
        avg_d = 0; std_d = 0; sharpe = 0
    downside = [r for r in daily_returns if r < 0]
    if len(downside) > 1:
        downside_std = math.sqrt(sum(r**2 for r in downside) / len(downside))
        sortino = (avg_d / downside_std * math.sqrt(252)) if downside_std > 0 else 0
    else:
        sortino = 0
    return {'total_return': total_return, 'max_dd': max_dd, 'sharpe': sharpe, 'sortino': sortino}


VARIANTS = [
    ('★ X2 base',                        'baseline', {}),
    # B: min_seg 변형
    ('B1. min_seg = avg',                'avg',      {}),
    ('B2. min_seg = median',             'median',   {}),
    ('B3. min_seg = weighted (시간)',     'weighted', {}),
    ('B4. min_seg = min(s1, s2) (최근)',  'recent2',  {}),
    # E: rev_bonus cap
    ('E1. rev_bonus cap=0.5',            'baseline', {'rev_bonus_cap': 0.5}),
    ('E2. rev_bonus cap=0.2',            'baseline', {'rev_bonus_cap': 0.2}),
]


def main():
    print('=' * 100)
    print('v80.9 (X2) base 위에서 후보 B/E 단건 검증 (12시작일)')
    print('=' * 100)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates_all, _ = bts2.load_data()
    start_dates = dates_all[2:14]
    print(f'시작일 12개\n')

    rows = []
    for name, ms_var, conv_kw in VARIANTS:
        slug = ''.join(c if c.isalnum() else '_' for c in name)[:25]
        db = GRID / f'{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        ms_map = regenerate(db, min_seg_variant=ms_var, conv_kw=conv_kw)
        bts2.DB_PATH = str(db)
        dates_v, data = bts2.load_data()
        per_start = []
        for sd in start_dates:
            sub_dates = [d for d in dates_v if d >= sd]
            n_days = len(sub_dates)
            drs, trades = simulate_full(dates_v, data, 3, 8, 3, start_date=sd,
                                        ms_map=ms_map, min_seg_variant=ms_var)
            per_start.append(calc_metrics(drs, trades, n_days))
        avg_m = {key: sum(p[key] for p in per_start) / len(per_start) for key in per_start[0]}
        rows.append({'name': name, **avg_m})

    base = rows[0]
    print(f'{"변형":<35} {"ret":>8} {"MDD":>8} {"Sharpe":>7} {"Sortino":>8} {"ΔRet":>8} {"ΔMDD":>8}')
    print('-' * 100)
    for r in rows:
        marker = '★' if 'X2 base' in r['name'] else (' ✓' if r['total_return'] - base['total_return'] >= 1 else '  ')
        d_ret = r['total_return'] - base['total_return']
        d_mdd = r['max_dd'] - base['max_dd']
        print(f'{marker} {r["name"]:<33} {r["total_return"]:+7.2f}% {r["max_dd"]:+7.2f}% '
              f'{r["sharpe"]:>6.2f} {r["sortino"]:>7.2f} {d_ret:+7.2f}%p {d_mdd:+7.2f}%p')


if __name__ == '__main__':
    main()
