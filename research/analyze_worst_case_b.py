"""B (80/20 + C1 b=5)의 M24 worst -37.50%p 출처 분석

24 multistart 각 시작일 비교 + worst 시작일 trade-by-trade 분석.
"""
import sys
import sqlite3
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'

MIN_HOLD_DAYS = 10
LOOKBACK = 30

sys.path.insert(0, str(ROOT))


def load_all_with_score(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    import daily_runner as dr
    score_cache = {}
    for d in dates:
        try:
            _, sm = dr._build_score_100_map(d)
            score_cache[d] = sm
        except Exception:
            score_cache[d] = {}
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price, eps_chg_weighted,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else:
                    segs.append(0)
            data[d][tk] = {
                'p2': r[1], 'price': r[2], 'eps_w': r[3],
                'min_seg': min(segs) if segs else 0,
                'score_100': score_cache.get(d, {}).get(tk, 0),
            }
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def get_price_30d(tk, today, dates_list, price_full):
    if today not in dates_list: return None
    di = dates_list.index(today)
    if di < LOOKBACK: return None
    past_d = dates_list[di - LOOKBACK]
    past_p = price_full.get(past_d, {}).get(tk)
    cur_p = price_full.get(today, {}).get(tk)
    if past_p and cur_p and past_p > 0:
        return (cur_p - past_p) / past_p * 100
    return None


def is_c1(info, tk, today, dates_list, price_full):
    eps_w = info.get('eps_w')
    if eps_w is None or eps_w <= 0: return False
    p30 = get_price_30d(tk, today, dates_list, price_full)
    if p30 is None: return False
    return p30 > 0


def rerank(today, today_data, c1_boost, dates_list, price_full):
    if c1_boost == 0:
        return {tk: info.get('p2') for tk, info in today_data.items() if info.get('p2') is not None}
    candidates = []
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        is_c1_now = is_c1(info, tk, today, dates_list, price_full)
        score = (31 - p2) + (c1_boost if is_c1_now else 0)
        candidates.append((-score, tk))
    candidates.sort()
    return {tk: i+1 for i, (_, tk) in enumerate(candidates)}


def simulate(dates_all, data, price_full, weights, entry, exit_, c1_boost,
             start_date=None, log_trades=False):
    slots = len(weights)
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    consecutive = defaultdict(int)
    daily_returns = []
    trades = []
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            today_data = data.get(d, {})
            new_ranks = rerank(d, today_data, c1_boost, dates_all, price_full)
            new_c = defaultdict(int)
            for tk, r in new_ranks.items():
                if r <= 30:
                    new_c[tk] = consecutive.get(tk, 0) + 1
            consecutive = new_c
    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        new_ranks = rerank(today, today_data, c1_boost, dates_all, price_full)
        new_consec = defaultdict(int)
        for tk, r in new_ranks.items():
            if r <= 30:
                new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec
        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di-1]
            for tk, info in portfolio.items():
                cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                prev_p = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if cur_p and prev_p and prev_p > 0:
                    w = info['weight'] / 100.0
                    day_ret += w * (cur_p - prev_p) / prev_p * 100
        daily_returns.append(day_ret)
        exited = []
        for tk in list(portfolio.keys()):
            rank = new_ranks.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            if min_seg < -2 or rank is None or rank > exit_:
                if log_trades and cur_p:
                    info = portfolio[tk]
                    ret = (cur_p - info['entry_price']) / info['entry_price'] * 100
                    trades.append({'tk': tk, 'entry_d': info['entry_d'], 'exit_d': today,
                                   'entry_p': info['entry_price'], 'exit_p': cur_p,
                                   'ret': ret, 'days': di - info['entry_di'], 'weight': info['weight']})
                exited.append(tk)
        for tk in exited: del portfolio[tk]
        used_slots = {info['slot_idx'] for info in portfolio.values()}
        free_slots = sorted([i for i in range(slots) if i not in used_slots])
        cands = []
        for tk, new_r in sorted(new_ranks.items(), key=lambda x: x[1]):
            if new_r > entry: break
            if tk in portfolio: continue
            if consecutive.get(tk, 0) < 3: continue
            info = today_data.get(tk, {})
            min_seg = info.get('min_seg', 0)
            if min_seg < 0: continue
            price = info.get('price')
            if price and price > 0:
                cands.append((tk, price))
        for slot_idx in free_slots:
            if not cands: break
            tk, price = cands.pop(0)
            portfolio[tk] = {'entry_price': price, 'slot_idx': slot_idx, 'weight': weights[slot_idx],
                             'entry_di': di, 'entry_d': today}
    # Open positions
    if log_trades:
        last_d = dates[-1]
        for tk, info in portfolio.items():
            final_p = data[last_d].get(tk, {}).get('price') or price_full.get(last_d, {}).get(tk)
            if final_p:
                ret = (final_p - info['entry_price']) / info['entry_price'] * 100
                trades.append({'tk': tk, 'entry_d': info['entry_d'], 'exit_d': last_d + ' (open)',
                               'entry_p': info['entry_price'], 'exit_p': final_p,
                               'ret': ret, 'days': len(dates) - info['entry_di'] - 1, 'weight': info['weight']})
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd, 'trades': trades}


def main():
    print('=' * 110)
    print('B (80/20 + C1 b=5) M24 worst -37.50%p 출처 분석')
    print('=' * 110)
    dates, data, price_full = load_all_with_score(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    step24 = max(1, len(eligible) // 25)
    starts24 = [eligible[step24 * i] for i in range(1, 25)]

    BASE = ([33, 34, 33], 30, 10, 0)
    OPT_A = ([70, 30], 30, 10, 0)
    OPT_B = ([80, 20], 30, 10, 5)

    print(f'\n시작일 풀: {starts24[0]} ~ {starts24[-1]}')
    print(f'24 multistart 결과 (base vs A vs B):')
    print(f'{"start_date":<12} {"base ret":>10} {"A ret":>10} {"B ret":>10} {"A lift":>10} {"B lift":>10}')
    print('-' * 75)

    worst_lift = float('inf')
    worst_sd = None
    for sd in starts24:
        r_base = simulate(dates, data, price_full, *BASE, start_date=sd)
        r_a = simulate(dates, data, price_full, *OPT_A, start_date=sd)
        r_b = simulate(dates, data, price_full, *OPT_B, start_date=sd)
        a_lift = r_a['total_return'] - r_base['total_return']
        b_lift = r_b['total_return'] - r_base['total_return']
        marker_b = ''
        if b_lift < worst_lift:
            worst_lift = b_lift
            worst_sd = sd
        if b_lift < -20: marker_b = ' ★ worst'
        print(f'  {sd:<12} {r_base["total_return"]:+8.2f}% {r_a["total_return"]:+8.2f}% {r_b["total_return"]:+8.2f}% {a_lift:+8.2f}%p {b_lift:+8.2f}%p{marker_b}')

    print()
    print(f'★ Worst 시작일: {worst_sd}, B lift {worst_lift:+.2f}%p')
    print()

    # Worst 시작일 trade-by-trade
    print('=' * 110)
    print(f'Worst 시작일 ({worst_sd}) trade 추적')
    print('=' * 110)

    r_b_trace = simulate(dates, data, price_full, *OPT_B, start_date=worst_sd, log_trades=True)
    r_base_trace = simulate(dates, data, price_full, *BASE, start_date=worst_sd, log_trades=True)
    r_a_trace = simulate(dates, data, price_full, *OPT_A, start_date=worst_sd, log_trades=True)

    print(f'\n[BASELINE (3,10,3) 균등] 누적 {r_base_trace["total_return"]:+.2f}%, MDD {r_base_trace["max_dd"]:+.2f}%')
    print(f'{"tk":<7} {"entry":<12} {"exit":<22} {"ret":>10} {"days":>5} {"weight":>7}')
    for t in r_base_trace['trades']:
        print(f'  {t["tk"]:<7} {t["entry_d"]:<12} {t["exit_d"]:<22} {t["ret"]:+8.2f}% {t["days"]:>4} {t["weight"]:>5}%')

    print(f'\n[A (70/30 b=0)] 누적 {r_a_trace["total_return"]:+.2f}%, MDD {r_a_trace["max_dd"]:+.2f}%')
    for t in r_a_trace['trades']:
        print(f'  {t["tk"]:<7} {t["entry_d"]:<12} {t["exit_d"]:<22} {t["ret"]:+8.2f}% {t["days"]:>4} {t["weight"]:>5}%')

    print(f'\n[B (80/20 + C1 b=5)] 누적 {r_b_trace["total_return"]:+.2f}%, MDD {r_b_trace["max_dd"]:+.2f}%')
    for t in r_b_trace['trades']:
        c1_marker = ' ← C1' if t.get('was_c1') else ''
        print(f'  {t["tk"]:<7} {t["entry_d"]:<12} {t["exit_d"]:<22} {t["ret"]:+8.2f}% {t["days"]:>4} {t["weight"]:>5}%')

    # 비교: 어떤 trade가 차이를 만들었나
    print()
    print('=' * 110)
    print('B가 baseline에 진 이유 분석')
    print('=' * 110)
    base_tkrs = set((t['tk'], t['entry_d']) for t in r_base_trace['trades'])
    a_tkrs = set((t['tk'], t['entry_d']) for t in r_a_trace['trades'])
    b_tkrs = set((t['tk'], t['entry_d']) for t in r_b_trace['trades'])
    print(f'\nbaseline에만 잡힌 trade (B는 놓침): {base_tkrs - b_tkrs}')
    print(f'B에만 잡힌 trade (baseline은 놓침): {b_tkrs - base_tkrs}')
    print(f'A에만 잡힌 trade: {a_tkrs - base_tkrs}')


if __name__ == '__main__':
    main()
