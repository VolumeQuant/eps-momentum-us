"""백테스트 엔진 — 모든 metric 반환

simulate() 함수: daily_returns, trades, n_days를 모두 반환
compute_metrics()로 종합 metric 계산 가능
"""
import sqlite3
from collections import defaultdict
from bt_metrics import compute_metrics

DB_PATH = 'eps_momentum_data.db'


def load_data(db_path=DB_PATH):
    """DB 로드"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    data = {}
    for d in dates:
        rows = cursor.execute('''
            SELECT ticker, part2_rank, price, composite_rank,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   rev_up30, num_analysts, ma60, ma120
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append((a - b) / abs(b) * 100)
                else:
                    segs.append(0)
            segs = [max(-100, min(100, s)) for s in segs]
            min_seg = min(segs) if segs else 0
            data[d][tk] = {
                'p2': r[1], 'price': r[2], 'comp_rank': r[3],
                'ntm_current': nc, 'ntm_90d': n90,
                'rev_up30': r[9] or 0, 'num_analysts': r[10] or 0,
                'ma60': r[11], 'ma120': r[12], 'min_seg': min_seg,
            }
    conn.close()
    return dates, data


def check_breakout(data, dates_all, today_idx, ticker, params):
    """Breakout hold 조건 체크"""
    today = dates_all[today_idx]
    v = data[today].get(ticker)
    if not v:
        return False
    if today_idx < params['lookback_days']:
        return False
    past = data.get(dates_all[today_idx - params['lookback_days']], {}).get(ticker)
    if not past or not past.get('price') or past['price'] <= 0:
        return False
    price_chg = (v['price'] - past['price']) / past['price'] * 100
    if price_chg < params['price_threshold']:
        return False
    if not v.get('ntm_90d') or v['ntm_90d'] <= 0:
        return False
    if v['ntm_current'] <= v['ntm_90d']:
        return False
    if v['num_analysts'] < 1:
        return False
    if (v['rev_up30'] / v['num_analysts']) < params['rev_up_ratio']:
        return False
    if params['check_ma60']:
        if not v.get('ma60') or v['price'] <= v['ma60']:
            return False
    return True


def simulate(dates_all, data, entry_top, exit_top, max_slots,
             start_date=None, hold_params=None,
             rank_field='p2'):
    """백테스트 시뮬레이션 - 모든 metric 반환

    Args:
        rank_field: 'p2' (DB part2_rank) or other field name (sim recompute)
    """
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    portfolio = {}
    daily_returns = []
    trades = []
    consecutive = defaultdict(int)
    breakout_holds_used = 0

    if start_date:
        for d in dates_all:
            if d >= start_date:
                break
            for tk, v in data.get(d, {}).items():
                if v.get(rank_field) and v[rank_field] <= 30:
                    consecutive[tk] = consecutive.get(tk, 0) + 1

    for di, today in enumerate(dates):
        if today not in data:
            continue
        today_data = data[today]
        rank_map = {tk: v[rank_field] for tk, v in today_data.items()
                    if v.get(rank_field) is not None}

        new_consecutive = defaultdict(int)
        for tk in rank_map:
            new_consecutive[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consecutive

        # 이탈
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            price = today_data.get(tk, {}).get('price')
            should_exit = False
            exit_reason = ''
            if rank is None or rank > exit_top:
                should_exit = True
                exit_reason = 'rank'
            if min_seg < -2:
                should_exit = True
                exit_reason = 'min_seg'

            # Breakout hold
            if should_exit and exit_reason == 'rank' and hold_params:
                global_idx = dates_all.index(today)
                if check_breakout(data, dates_all, global_idx, tk, hold_params):
                    grace = portfolio[tk].get('grace_days', 0)
                    if grace < hold_params['max_grace']:
                        portfolio[tk]['grace_days'] = grace + 1
                        breakout_holds_used += 1
                        should_exit = False

            if should_exit and price:
                entry_price = portfolio[tk]['entry_price']
                ret = (price - entry_price) / entry_price * 100
                trades.append({
                    'ticker': tk, 'entry_date': portfolio[tk]['entry_date'],
                    'exit_date': today, 'entry_price': entry_price,
                    'exit_price': price, 'return': ret,
                    'reason': exit_reason,
                })
                exited.append(tk)
        for tk in exited:
            del portfolio[tk]

        # 진입
        vacancies = max_slots - len(portfolio)
        if vacancies > 0:
            candidates = []
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > entry_top:
                    break
                if tk in portfolio:
                    continue
                if consecutive.get(tk, 0) < 3:
                    continue
                min_seg = today_data.get(tk, {}).get('min_seg', 0)
                if min_seg < 0:
                    continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0:
                    candidates.append((tk, price))
            for tk, price in candidates[:vacancies]:
                portfolio[tk] = {'entry_price': price, 'grace_days': 0,
                                  'entry_date': today}

        # 일간 수익
        if portfolio:
            day_ret = 0
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

    # metrics
    n_days = len(daily_returns)
    metrics = compute_metrics(daily_returns, trades, n_days)
    metrics['n_open'] = len(portfolio)
    metrics['breakout_holds'] = breakout_holds_used
    metrics['daily_returns'] = daily_returns
    metrics['trades'] = trades
    return metrics
