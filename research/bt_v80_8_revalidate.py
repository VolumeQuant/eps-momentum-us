"""v80.8 (rev_up30 ≥ 3) 적용된 base 위에서 12시작일 재검증

이전 BT는 6시작일로 진행 → 표본 노이즈 가능성 (특히 B 옵션의 MDD 우위)
12시작일로 확장해서 재검증:

base = v80.8 (rev_up30 ≥ 3 적용)
변형:
  1. 변형 11 보너스 (둘 다 강함, ratio≥0.5 + eps_floor≥0.3 → +0.2)
  2. eps_floor cap 1.5
  3. eps_floor cap 3.0
  4. rev_bonus 비례화 (rg×0.6, cap 0.3)
  5. T0 가중치 0.45
  6. T0 가중치 0.40
  7. 변형 11 + T0=0.45 결합
  8. eps_cap=3.0 + T0=0.45 결합
  9. rev_bonus 비례 + T0=0.45 결합
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
GRID = ROOT / 'research' / 'v80_8_revalidate_dbs'
GRID.mkdir(exist_ok=True)


def calc_min_seg(nc, n7, n30, n60, n90):
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        if b and abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0)
    return min(segs) if segs else 0


def make_conv(eps_cap=1.0, bonus=False, rev_bonus_proportional=False):
    def fn(adj_gap, ru, na, nc, n90, rev_growth=None):
        ru = ru or 0
        na = na or 0
        rg = rev_growth or 0
        ratio = ru / na if na > 0 else 0
        eps_floor = 0
        if nc is not None and n90 and abs(n90) > 0.01:
            eps_floor = min(abs((nc - n90) / n90), eps_cap)
        base = max(ratio, eps_floor)
        if bonus and ratio >= 0.5 and eps_floor >= 0.3:
            base = min(base + 0.2, max(eps_cap, 1.0))
        if rev_bonus_proportional:
            rb = min(min(rg, 0.5) * 0.6, 0.3)
        else:
            rb = 0.3 if rg >= 0.30 else 0
        return adj_gap * (1 + base + rb)
    return fn


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
    elif t0_weight == 0.40:
        weights = [0.25, 0.35, 0.40]
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


def regenerate(test_db, conv_fn, t0_weight=0.5, rev_up_min=3):
    """base = rev_up30 ≥ 3 (v80.8)"""
    original_path = dr.DB_PATH
    original_fn = dr._apply_conviction
    dr.DB_PATH = str(test_db)
    dr._apply_conviction = conv_fn
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
                cg = conv_fn(ag, ru, na, nc, n90, rev_growth=rg)
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
        dr._apply_conviction = original_fn


def simulate_full(dates_all, data, entry_top, exit_top, max_slots, start_date=None):
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
    return {
        'total_return': total_return, 'max_dd': max_dd,
        'sharpe': sharpe, 'sortino': sortino,
    }


VARIANTS = [
    ('★ v80.8 base (rev_up30≥3)',          {'eps_cap': 1.0}),
    ('변형11 보너스',                         {'eps_cap': 1.0, 'bonus': True}),
    ('eps_cap=1.5',                         {'eps_cap': 1.5}),
    ('eps_cap=3.0',                         {'eps_cap': 3.0}),
    ('rev_bonus 비례',                        {'eps_cap': 1.0, 'rev_bonus_proportional': True}),
    ('변형11 + eps_cap=3.0',                 {'eps_cap': 3.0, 'bonus': True}),
    ('변형11 + rev_bonus 비례',                {'eps_cap': 1.0, 'bonus': True, 'rev_bonus_proportional': True}),
    ('eps_cap=3.0 + rev_bonus 비례',          {'eps_cap': 3.0, 'rev_bonus_proportional': True}),
    ('all 3 (변형11+eps3.0+rev_bonus)',     {'eps_cap': 3.0, 'bonus': True, 'rev_bonus_proportional': True}),
]
T0_VARIANTS = [
    ('T0=0.50 (default)',  0.50),
    ('T0=0.45',            0.45),
    ('T0=0.40',            0.40),
]


def main():
    print('=' * 110)
    print('v80.8 12시작일 재검증 — base = rev_up30 ≥ 3')
    print('=' * 110)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates_all, _ = bts2.load_data()
    start_dates = dates_all[2:14]
    print(f'시작일 12개: {start_dates[0]} ~ {start_dates[-1]}\n')

    rows = []
    # T0=0.50 기준 variants
    for name, kw in VARIANTS:
        slug = ''.join(c if c.isalnum() else '_' for c in name)[:25]
        db = GRID / f'rev_{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        fn = make_conv(**kw)
        regenerate(db, fn, t0_weight=0.5)
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

    # T0 variants (base만)
    for name, t0 in T0_VARIANTS:
        if t0 == 0.5: continue  # already in VARIANTS
        slug = f'T0_{int(t0*100)}'
        db = GRID / f'rev_{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        fn = make_conv(eps_cap=1.0)
        regenerate(db, fn, t0_weight=t0)
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

    # 출력
    base = rows[0]
    print(f'{"변형":<35} {"ret":>8} {"MDD":>8} {"Sharpe":>7} {"Sortino":>8} {"ΔRet":>8} {"ΔMDD":>8}')
    print('-' * 110)
    for r in rows:
        marker = '★' if 'base' in r['name'] else (' ✓' if r['total_return'] - base['total_return'] >= 1 else '  ')
        d_ret = r['total_return'] - base['total_return']
        d_mdd = r['max_dd'] - base['max_dd']
        print(f'{marker} {r["name"]:<33} {r["total_return"]:+7.2f}% {r["max_dd"]:+7.2f}% '
              f'{r["sharpe"]:>6.2f} {r["sortino"]:>7.2f} {d_ret:+7.2f}%p {d_mdd:+7.2f}%p')

    print()
    print('=' * 110)
    print('결론: ✓ 표시는 base 대비 +1%p 이상 개선')
    print('=' * 110)


if __name__ == '__main__':
    main()
