"""거래 비용 / 슬리피지 stress test

(3,10,2) 80/20 boost=3가 실제 운용 시 거래비용 고려해도 강할지 검증.

매수/매도 시 -X% 비용 가정 (0%, 0.1%, 0.25%, 0.5%, 1.0%).
회전율이 다른 두 구성 비교:
  - baseline (3,10,3) 균등
  - (3,10,2) 80/20 boost=3
"""
import sys
import sqlite3
import random
import statistics
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'

N_SEEDS = 500
SAMPLES = 3
MIN_HOLD_DAYS = 10
LOOKBACK = 30


def load_all(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
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
            data[d][tk] = {'p2': r[1], 'price': r[2], 'eps_w': r[3], 'min_seg': min(segs) if segs else 0}
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


def classify_c2(info, tk, today, dates_list, price_full):
    eps_w = info.get('eps_w')
    if eps_w is None: return False
    p30 = get_price_30d(tk, today, dates_list, price_full)
    if p30 is None: return False
    return eps_w > 0 and p30 < 0


def rerank(today, today_data, c2_boost, dates_list, price_full):
    if c2_boost == 0:
        return {tk: info.get('p2') for tk, info in today_data.items() if info.get('p2') is not None}
    candidates = []
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        is_c2 = classify_c2(info, tk, today, dates_list, price_full)
        score = (31 - p2) + (c2_boost if is_c2 else 0)
        candidates.append((-score, tk))
    candidates.sort()
    return {tk: i+1 for i, (_, tk) in enumerate(candidates)}


def simulate(dates_all, data, price_full, weights, entry, exit_, c2_boost,
             cost_pct=0.0, start_date=None):
    """cost_pct: 매수/매도 시 가격에서 차감 % (예: 0.001 = 0.1%)"""
    slots = len(weights)
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    consecutive = defaultdict(int)
    daily_returns = []
    n_trades = 0
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            today_data = data.get(d, {})
            new_ranks = rerank(d, today_data, c2_boost, dates_all, price_full)
            new_c = defaultdict(int)
            for tk, r in new_ranks.items():
                if r <= 30:
                    new_c[tk] = consecutive.get(tk, 0) + 1
            consecutive = new_c
    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        new_ranks = rerank(today, today_data, c2_boost, dates_all, price_full)
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
                    w = weights[info['slot_idx']] / 100.0
                    day_ret += w * (cur_p - prev_p) / prev_p * 100
        daily_returns.append(day_ret)
        exited = []
        for tk in list(portfolio.keys()):
            rank = new_ranks.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2: exited.append(tk); continue
            if rank is None or rank > exit_: exited.append(tk); continue
        for tk in exited:
            # 매도 비용 차감 (slot weight 기준)
            info = portfolio[tk]
            w = weights[info['slot_idx']] / 100.0
            daily_returns[-1] -= cost_pct * 100 * w  # day ret에서 비용 차감
            n_trades += 1
            del portfolio[tk]
        if len(portfolio) < slots:
            used_idx = {info['slot_idx'] for info in portfolio.values()}
            free_idx = sorted([i for i in range(slots) if i not in used_idx])
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
            for slot_idx in free_idx:
                if not cands: break
                tk, price = cands.pop(0)
                # 매수 비용 차감
                w = weights[slot_idx] / 100.0
                daily_returns[-1] -= cost_pct * 100 * w
                n_trades += 1
                portfolio[tk] = {'entry_price': price, 'slot_idx': slot_idx}
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd, 'n_trades': n_trades}


def main():
    print('=' * 100)
    print('거래 비용 stress test — (3,10,2) 80/20 boost=3 vs baseline')
    print('=' * 100)
    dates, data, price_full = load_all(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    configs = [
        ('baseline (3,10,3) 균등', [33,33,34], 3, 10, 0),
        ('(3,10,2) 80/20 b=3',     [80,20], 3, 10, 3),
    ]
    cost_levels = [0.0, 0.001, 0.0025, 0.005, 0.01]

    print()
    print(f'{"config":<27} {"cost":>6} {"avg":>9} {"vs cost=0":>10} {"avg trades":>11} {"worst MDD":>10}')
    print('-' * 80)
    for label, w, entry, exit_, b in configs:
        base_ret = None
        for cost in cost_levels:
            rets = []
            mdds = []
            n_trades = []
            for chosen in seed_starts:
                for sd in chosen:
                    r = simulate(dates, data, price_full, w, entry, exit_, b, cost_pct=cost, start_date=sd)
                    rets.append(r['total_return'])
                    mdds.append(r['max_dd'])
                    n_trades.append(r['n_trades'])
            avg = sum(rets)/len(rets)
            if base_ret is None: base_ret = avg
            diff = avg - base_ret
            avg_n = sum(n_trades)/len(n_trades)
            worst_mdd = min(mdds)
            cost_str = f'{cost*100:.2f}%'
            print(f'{label:<27} {cost_str:>6} {avg:+8.2f}% {diff:+8.2f}%p {avg_n:>10.1f} {worst_mdd:+9.2f}%')
        print()


if __name__ == '__main__':
    main()
