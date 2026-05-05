"""F 변형: w_gap 시간 가중치 (T0×0.5 + T1×0.3 + T2×0.2) 변경 12시작일 BT

X2 base 위에서:
  baseline:  0.5 / 0.3 / 0.2 (현재)
  F1: 0.4 / 0.35 / 0.25 (덜 최근 편향)
  F2: 0.6 / 0.25 / 0.15 (더 최근 편향)
  F3: 0.7 / 0.2 / 0.1 (매우 최근)
  F4: 0.333 / 0.333 / 0.333 (균등)
  F5: 0.45 / 0.3 / 0.25 (소폭 완화)

각 변형마다 _compute_w_gap_map의 weights를 monkey patch 해서 part2_rank 재계산.
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
GRID = ROOT / 'research' / 'v80_9_F_dbs'
GRID.mkdir(exist_ok=True)
SEG_CAP = 100
MISSING_PENALTY = 30


def make_w_gap_map_fn(weights3):
    """T-2/T-1/T0 가중치를 받아 _compute_w_gap_map 대체 함수 반환."""
    def _compute(cursor, today_str, tickers):
        dates = dr._get_recent_dates(cursor, 'composite_rank', today_str, 3)
        dates = sorted(dates)
        score_by_date = {}
        for d in dates:
            rows = cursor.execute(
                'SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, '
                'rev_growth FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
                (d,)
            ).fetchall()
            conv_gaps = {}
            for r in rows:
                tk = r[0]
                conv_gaps[tk] = dr._apply_conviction(r[1], r[2], r[3], r[4], r[5], rev_growth=r[6])
            vals = list(conv_gaps.values())
            if len(vals) >= 2:
                mean_v = np.mean(vals)
                std_v = np.std(vals)
                if std_v > 0:
                    z_by = {tk: max(30, -((v - mean_v) / std_v) * 30 + 70) for tk, v in conv_gaps.items()}
                else:
                    z_by = {tk: 70 for tk in conv_gaps}
            else:
                z_by = {tk: 70 for tk in conv_gaps}
            score_by_date[d] = z_by

        out = {}
        for tk in tickers:
            scores = []
            for di, d in enumerate(dates):
                s = score_by_date.get(d, {}).get(tk)
                if s is None:
                    p2_check = cursor.execute(
                        'SELECT part2_rank FROM ntm_screening WHERE date=? AND ticker=?',
                        (d, tk)
                    ).fetchone()
                    if di < len(dates) - 1:
                        if not p2_check or p2_check[0] is None:
                            scores.append(MISSING_PENALTY)
                        else:
                            scores.append(MISSING_PENALTY)
                    else:
                        scores.append(MISSING_PENALTY)
                else:
                    scores.append(s)
            while len(scores) < 3:
                scores.insert(0, MISSING_PENALTY)
            w = weights3
            out[tk] = scores[0] * w[0] + scores[1] * w[1] + scores[2] * w[2]
        return out
    return _compute


def regenerate(test_db, weights3):
    original_path = dr.DB_PATH
    original_fn = dr._compute_w_gap_map
    dr.DB_PATH = str(test_db)
    dr._compute_w_gap_map = make_w_gap_map_fn(weights3)
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]
        for today in dates:
            tickers = [r[0] for r in cur.execute(
                'SELECT ticker FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL', (today,)
            ).fetchall()]
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
        dr._compute_w_gap_map = original_fn


def simulate_full(dates_all, data, entry_top, exit_top, max_slots, start_date):
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
                ms = today_data.get(tk, {}).get('min_seg', 0)
                if ms < 0: continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0: cands.append((tk, price))
            for tk, price in cands[:vacancies]:
                portfolio[tk] = {'entry_price': price, 'entry_date': today}
    return daily_returns, trades


def calc_metrics(daily_returns, trades, n_days):
    cum_ret = 1.0; peak = 1.0; max_dd = 0
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
    ('★ X2 base (T0=0.5/0.3/0.2)',  [0.2, 0.3, 0.5]),
    ('F1. 0.4/0.35/0.25 (덜 최근)',  [0.25, 0.35, 0.4]),
    ('F2. 0.6/0.25/0.15 (더 최근)',  [0.15, 0.25, 0.6]),
    ('F3. 0.7/0.2/0.1 (매우 최근)',  [0.1, 0.2, 0.7]),
    ('F4. 0.333 균등',                [1/3, 1/3, 1/3]),
    ('F5. 0.45/0.3/0.25 (소폭 완화)', [0.25, 0.3, 0.45]),
]


def main():
    print('=' * 100)
    print('v80.9 (X2) base 위에서 후보 F (w_gap 시간 가중치) 단건 검증 (12시작일)')
    print('=' * 100)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates_all, _ = bts2.load_data()
    start_dates = dates_all[2:14]
    print(f'시작일 12개\n')

    rows = []
    for name, w in VARIANTS:
        slug = ''.join(c if c.isalnum() else '_' for c in name)[:25]
        db = GRID / f'{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, w)
        bts2.DB_PATH = str(db)
        dates_v, data = bts2.load_data()
        per_start = []
        for sd in start_dates:
            sub_dates = [d for d in dates_v if d >= sd]
            n_days = len(sub_dates)
            drs, trades = simulate_full(dates_v, data, 3, 8, 3, start_date=sd)
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
