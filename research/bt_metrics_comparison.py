"""baseline/A/B 종합 지표 비교

지표:
- Total Return (총 수익률)
- CAGR (연환산)
- MDD (최대 낙폭)
- Calmar = CAGR / |MDD|
- Sharpe = avg_daily_ret / std_daily × sqrt(252)
- Sortino = avg_daily_ret / downside_std × sqrt(252)
- Win Rate (승률)
- Avg Win / Avg Loss
- Profit Factor
- Days in market
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
GRID = ROOT / 'research' / 'metrics_dbs'
GRID.mkdir(exist_ok=True)


def calc_min_seg(nc, n7, n30, n60, n90):
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        if b and abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0)
    return min(segs) if segs else 0


def compute_wgap_with_t0(cursor, today_str, tickers, t0_weight=0.5):
    dates = dr._get_recent_dates(cursor, 'composite_rank', today_str, 3)
    dates = sorted(dates)
    MISSING_PENALTY = 30
    score_by_date = {}
    for d in dates:
        rows = cursor.execute(
            'SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, rev_growth '
            'FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL', (d,)
        ).fetchall()
        conv_gaps = {r[0]: dr._apply_conviction(r[1], r[2], r[3], r[4], r[5], rev_growth=r[6]) for r in rows}
        vals = list(conv_gaps.values())
        if len(vals) >= 2:
            mean_v = np.mean(vals)
            std_v = np.std(vals)
            if std_v > 0:
                score_by_date[d] = {tk: max(30.0, 65 + (-(v - mean_v) / std_v) * 15) for tk, v in conv_gaps.items()}
            else:
                score_by_date[d] = {tk: 65 for tk in conv_gaps}
        else:
            score_by_date[d] = {tk: 65 for tk in conv_gaps}
    if t0_weight == 0.5:
        weights = [0.2, 0.3, 0.5]
    elif t0_weight == 0.45:
        weights = [0.225, 0.325, 0.45]
    else:
        rest = 1 - t0_weight
        weights = [rest * 0.4, rest * 0.6, t0_weight]
    if len(dates) == 2: weights = [0.4, 0.6]
    elif len(dates) == 1: weights = [1.0]
    p2_by_date = {}
    for d in dates:
        rows = cursor.execute('SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,)).fetchall()
        p2_by_date[d] = {r[0] for r in rows}
    result = {}
    for tk in tickers:
        wg = 0
        for i, d in enumerate(dates):
            is_today = (d == today_str)
            if not is_today and tk not in p2_by_date.get(d, set()):
                score = MISSING_PENALTY
            else:
                score = score_by_date.get(d, {}).get(tk)
                if score is None: score = MISSING_PENALTY
            wg += score * weights[i]
        result[tk] = wg
    return result


def regenerate(test_db, rev_up_min=0, t0_weight=0.5):
    original_path = dr.DB_PATH
    dr.DB_PATH = str(test_db)
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
                if ag is None: continue
                ms = calc_min_seg(nc or 0, n7 or 0, n30 or 0, n60 or 0, n90 or 0)
                if ms < -2: continue
                if rev_up_min > 0 and (ru or 0) < rev_up_min: continue
                cg = dr._apply_conviction(ag, ru, na, nc, n90, rev_growth=rg)
                if cg is not None: elig_conv.append((tk, cg))
            elig_conv.sort(key=lambda x: x[1])
            new_cr = {tk: i + 1 for i, (tk, _) in enumerate(elig_conv)}
            cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
            for tk, cr in new_cr.items():
                cur.execute('UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?', (cr, today, tk))
            tickers = list(new_cr.keys())
            wmap = compute_wgap_with_t0(cur, today, tickers, t0_weight)
            sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
            top30 = sorted_w[:30]
            cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
            for rk, tk in enumerate(top30, 1):
                cur.execute('UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?', (rk, today, tk))
            conn.commit()
        conn.close()
    finally:
        dr.DB_PATH = original_path


def simulate_full(dates_all, data, entry_top, exit_top, max_slots, start_date=None):
    """모든 지표 계산용 시뮬 — daily_returns 시계열 반환"""
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
        # day_ret 먼저 (어제 portfolio 기준, v80.7)
        day_ret = 0
        days_in_market = 1 if portfolio else 0
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
        # 이탈
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            price = today_data.get(tk, {}).get('price')
            should_exit = False
            if rank is None or rank > exit_top: should_exit = True
            if min_seg < -2: should_exit = True
            if should_exit and price:
                ep = portfolio[tk]['entry_price']
                ret = (price - ep) / ep * 100
                trades.append(ret)
                exited.append(tk)
        for tk in exited:
            del portfolio[tk]
        # 진입
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
                if price and price > 0:
                    cands.append((tk, price))
            for tk, price in cands[:vacancies]:
                portfolio[tk] = {'entry_price': price, 'entry_date': today}
    return daily_returns, trades


def calc_metrics(daily_returns, trades, n_days):
    """모든 지표 계산"""
    cum_ret = 1.0
    peak = 1.0
    max_dd = 0
    for r in daily_returns:
        cum_ret *= (1 + r / 100)
        peak = max(peak, cum_ret)
        dd = (cum_ret - peak) / peak * 100
        max_dd = min(max_dd, dd)
    total_return = (cum_ret - 1) * 100

    # CAGR (연환산, 252거래일 기준)
    if n_days > 0:
        cagr = ((1 + total_return / 100) ** (252 / n_days) - 1) * 100
    else:
        cagr = 0

    # Sharpe (일간 수익률 기준)
    if len(daily_returns) > 1:
        avg_d = sum(daily_returns) / len(daily_returns)
        std_d = statistics.pstdev(daily_returns)
        sharpe = (avg_d / std_d * math.sqrt(252)) if std_d > 0 else 0
    else:
        avg_d = 0
        std_d = 0
        sharpe = 0

    # Sortino (downside risk only)
    downside = [r for r in daily_returns if r < 0]
    if len(downside) > 1:
        downside_std = math.sqrt(sum(r**2 for r in downside) / len(downside))
        sortino = (avg_d / downside_std * math.sqrt(252)) if downside_std > 0 else 0
    else:
        sortino = 0

    # Calmar
    calmar = cagr / abs(max_dd) if max_dd < 0 else 0

    # Trade stats
    n_trades = len(trades)
    wins = [t for t in trades if t > 0]
    losses = [t for t in trades if t <= 0]
    win_rate = len(wins) / n_trades * 100 if n_trades > 0 else 0
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 0
    profit_factor = (sum(wins) / abs(sum(losses))) if losses and sum(losses) < 0 else 0

    # Time in market
    days_in_market = sum(1 for r in daily_returns if r != 0)
    market_pct = days_in_market / len(daily_returns) * 100 if daily_returns else 0

    return {
        'total_return': total_return, 'cagr': cagr, 'max_dd': max_dd,
        'sharpe': sharpe, 'sortino': sortino, 'calmar': calmar,
        'n_trades': n_trades, 'win_rate': win_rate,
        'avg_win': avg_win, 'avg_loss': avg_loss,
        'profit_factor': profit_factor,
        'avg_daily_ret': avg_d, 'std_daily': std_d,
        'market_pct': market_pct,
    }


def main():
    print('=' * 130)
    print('baseline / A / B 종합 지표 비교 (6시작일 평균)')
    print('=' * 130)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates_all, _ = bts2.load_data()
    start_dates = dates_all[2:14]  # 12개 시작일 (2/12~3/2 부근)
    print(f'시작일 12개: {start_dates[0]} ~ {start_dates[-1]}')

    configs = [
        ('Baseline',                      0,  0.5),
        ('A. rev_up30≥3',                  3,  0.5),
        ('B. rev_up30≥3 + T0=0.45',        3,  0.45),
    ]

    all_metrics = {}
    for name, ru, t0 in configs:
        slug = ''.join(c if c.isalnum() else '_' for c in name)[:20]
        db = GRID / f'metrics_{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, rev_up_min=ru, t0_weight=t0)

        bts2.DB_PATH = str(db)
        dates_v, data = bts2.load_data()

        # 시작일별 지표 + 평균
        per_start = []
        for sd in start_dates:
            sub_dates = [d for d in dates_v if d >= sd]
            n_days = len(sub_dates)
            drs, trades = simulate_full(dates_v, data, 3, 8, 3, start_date=sd)
            m = calc_metrics(drs, trades, n_days)
            per_start.append(m)

        # 평균 (6시작일)
        avg_m = {}
        for key in per_start[0]:
            avg_m[key] = sum(p[key] for p in per_start) / len(per_start)
        all_metrics[name] = avg_m

    # 출력
    metrics_list = [
        ('Total Return', 'total_return', '+.2f%', 'high'),
        ('CAGR (연환산)', 'cagr', '+.2f%', 'high'),
        ('MDD',          'max_dd', '+.2f%', 'high'),  # 음수, 높을수록 좋음
        ('Sharpe',       'sharpe', '.2f', 'high'),
        ('Sortino',      'sortino', '.2f', 'high'),
        ('Calmar',       'calmar', '.2f', 'high'),
        ('Win Rate',     'win_rate', '.1f%', 'high'),
        ('Avg Win',      'avg_win', '+.2f%', 'high'),
        ('Avg Loss',     'avg_loss', '+.2f%', 'high'),
        ('Profit Factor', 'profit_factor', '.2f', 'high'),
        ('# Trades',     'n_trades', '.0f', 'mid'),
        ('Daily Avg',    'avg_daily_ret', '+.3f%', 'high'),
        ('Daily Std',    'std_daily', '.3f', 'low'),
        ('Market %',     'market_pct', '.1f%', 'mid'),
    ]

    print()
    print(f'{"지표":<18}', end='')
    for name, _, _ in configs:
        print(f' {name:>22}', end='')
    print(f' {"A vs Base":>12} {"B vs Base":>12}')
    print('-' * 130)

    for label, key, fmt, direction in metrics_list:
        print(f'{label:<18}', end='')
        vals = [all_metrics[name][key] for name, _, _ in configs]
        for v in vals:
            if fmt == '+.2f%':
                s = f'{v:+.2f}%'
            elif fmt == '+.3f%':
                s = f'{v:+.3f}%'
            elif fmt == '.1f%':
                s = f'{v:.1f}%'
            elif fmt == '.2f':
                s = f'{v:.2f}'
            elif fmt == '.3f':
                s = f'{v:.3f}'
            elif fmt == '.0f':
                s = f'{v:.0f}'
            else:
                s = str(v)
            print(f' {s:>22}', end='')
        # vs base
        base_v = vals[0]
        diff_a = vals[1] - base_v
        diff_b = vals[2] - base_v
        print(f' {diff_a:>+11.2f} {diff_b:>+11.2f}')

    # best 판정
    print()
    print('=' * 130)
    print('best 판정 (각 지표별)')
    print('=' * 130)
    for label, key, fmt, direction in metrics_list:
        if direction == 'mid': continue
        vals = [(name, all_metrics[name][key]) for name, _, _ in configs]
        if direction == 'high':
            best = max(vals, key=lambda x: x[1])
        else:
            best = min(vals, key=lambda x: x[1])
        print(f'  {label:<18}: {best[0]}')


if __name__ == '__main__':
    main()
