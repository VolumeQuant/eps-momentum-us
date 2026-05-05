"""D 변형: eps_quality 임계값 — X2 base 위에서 12시작일 BT

⚠️ PRODUCTION SAFETY:
- 원본 DB(eps_momentum_data.db)는 절대 수정하지 않음
- 모든 작업은 v80_9_D_dbs/ 하위 복사본에서만 진행

eps_quality 식:
  eps_q = 1.0 + 0.3 × clamp(min_seg / divisor, -1, 1)
  baseline: divisor=2  (±2% 경계)
  D1: divisor=1   (±1% 경계, 더 좁게 → 더 빨리 saturation)
  D2: divisor=3   (±3% 경계)
  D3: divisor=4   (±4% 경계, 매우 넓게)
  D4: divisor=0.5 (±0.5% 경계, 매우 좁게)
  D5: cap 폭 ±0.3 → ±0.5 (영향력 강화, divisor=2 그대로)

영향: adj_gap = fwd_pe_chg × (1 + dir_factor) × eps_q
  → adj_gap_new = adj_gap_old × eps_q_new / eps_q_old
  fwd_pe_chg, dir_factor는 변형 무관 — eps_q 비율만 곱.
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
GRID = ROOT / 'research' / 'v80_9_D_dbs'
GRID.mkdir(exist_ok=True)
SEG_CAP = 100


def calc_segs(nc, n7, n30, n60, n90):
    if not all(x and abs(x) > 0.01 for x in (n7, n30, n60, n90)):
        return None
    s1 = max(-SEG_CAP, min(SEG_CAP, (nc-n7)/abs(n7)*100))
    s2 = max(-SEG_CAP, min(SEG_CAP, (n7-n30)/abs(n30)*100))
    s3 = max(-SEG_CAP, min(SEG_CAP, (n30-n60)/abs(n60)*100))
    s4 = max(-SEG_CAP, min(SEG_CAP, (n60-n90)/abs(n90)*100))
    return [s1, s2, s3, s4]


def eps_q_value(min_seg, divisor=2.0, cap=0.3):
    return 1.0 + cap * max(-1, min(1, min_seg / divisor))


def regenerate(test_db, divisor=2.0, cap=0.3):
    """eps_q 변형 적용. divisor/cap 조정."""
    original_path = dr.DB_PATH
    dr.DB_PATH = str(test_db)
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]
        for today in dates:
            rows = cur.execute('''SELECT ticker, adj_gap, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                                  rev_up30, num_analysts, rev_growth
                                  FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL''',
                               (today,)).fetchall()

            new_adj_gaps = {}
            min_segs = {}
            for r in rows:
                tk, ag, nc, n7, n30, n60, n90 = r[:7]
                segs = calc_segs(nc, n7, n30, n60, n90)
                if segs is None:
                    new_adj_gaps[tk] = ag
                    min_segs[tk] = 0
                    continue
                # cap_hit segment 제외 후 min_seg
                valid = [s for s in segs if abs(s) < SEG_CAP]
                ms = min(valid) if valid else 0
                min_segs[tk] = ms
                if ag is None:
                    new_adj_gaps[tk] = None
                    continue
                eps_q_old = eps_q_value(ms, divisor=2.0, cap=0.3)
                eps_q_new = eps_q_value(ms, divisor=divisor, cap=cap)
                if abs(eps_q_old) < 1e-9:
                    new_adj_gaps[tk] = ag
                else:
                    new_adj_gaps[tk] = ag * eps_q_new / eps_q_old

            elig_conv = []
            for r in rows:
                tk, _, nc, _, _, _, n90, ru, na, rg = r
                if (ru or 0) < 3: continue
                ms = min_segs.get(tk, 0)
                if ms < -2: continue
                ag_new = new_adj_gaps.get(tk)
                if ag_new is None: continue
                cg = dr._apply_conviction(ag_new, ru, na, nc, n90, rev_growth=rg)
                if cg is not None: elig_conv.append((tk, cg))
            elig_conv.sort(key=lambda x: x[1])
            new_cr = {tk: i + 1 for i, (tk, _) in enumerate(elig_conv)}

            cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
            for tk, ag_new in new_adj_gaps.items():
                if ag_new is not None:
                    cur.execute('UPDATE ntm_screening SET adj_gap=? WHERE date=? AND ticker=?', (ag_new, today, tk))
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
    ('★ X2 base (divisor=2, cap=0.3)', 2.0, 0.3),
    ('D1. divisor=1 (±1% 경계, 좁게)',  1.0, 0.3),
    ('D2. divisor=3 (±3% 경계, 넓게)',  3.0, 0.3),
    ('D3. divisor=4 (±4% 매우 넓게)',   4.0, 0.3),
    ('D4. divisor=0.5 (±0.5% 매우 좁게)', 0.5, 0.3),
    ('D5. cap=0.5 (영향력 강화)',        2.0, 0.5),
    ('D6. cap=0.2 (영향력 약화)',        2.0, 0.2),
]


def main():
    print('=' * 100)
    print('v80.9 (X2) base 위에서 후보 D (eps_quality) 단건 검증 (12시작일)')
    print('=' * 100)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates_all, _ = bts2.load_data()
    start_dates = dates_all[2:14]
    print(f'시작일 12개\n')

    rows = []
    for name, divisor, cap in VARIANTS:
        slug = ''.join(c if c.isalnum() else '_' for c in name)[:25]
        db = GRID / f'{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, divisor=divisor, cap=cap)
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
    print(f'{"변형":<40} {"ret":>8} {"MDD":>8} {"Sharpe":>7} {"Sortino":>8} {"ΔRet":>8} {"ΔMDD":>8}')
    print('-' * 110)
    for r in rows:
        marker = '★' if 'X2 base' in r['name'] else (' ✓' if r['total_return'] - base['total_return'] >= 1 else '  ')
        d_ret = r['total_return'] - base['total_return']
        d_mdd = r['max_dd'] - base['max_dd']
        print(f'{marker} {r["name"]:<38} {r["total_return"]:+7.2f}% {r["max_dd"]:+7.2f}% '
              f'{r["sharpe"]:>6.2f} {r["sortino"]:>7.2f} {d_ret:+7.2f}%p {d_mdd:+7.2f}%p')


if __name__ == '__main__':
    main()
