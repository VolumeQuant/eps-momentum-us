"""C2 정의 sensitivity — eps_chg_weighted vs NTM ratio vs 다른 기간

3가지 EPS 변화 정의 × 3가지 price 변화 기간 = 9개 조합 비교.
가장 강한 C2 알파 정의 찾기.

D1: eps_chg_weighted (시스템 표준)
D2: (ntm_current - ntm_90d) / ntm_90d
D3: (ntm_30d - ntm_60d) / ntm_60d
D4: (ntm_7d - ntm_30d) / ntm_30d (최근 변화)

price 기간: 7d, 30d, 60d
"""
import sys
import sqlite3
import random
import statistics
import time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'

N_SEEDS = 500
SAMPLES = 3
MIN_HOLD_DAYS = 10


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
            data[d][tk] = {
                'p2': r[1], 'price': r[2], 'eps_w': r[3],
                'nc': nc, 'n7': n7, 'n30': n30, 'n60': n60, 'n90': n90,
                'min_seg': min(segs) if segs else 0,
            }
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def get_price_chg(tk, today, lookback, dates_list, price_full):
    if today not in dates_list: return None
    di = dates_list.index(today)
    if di < lookback: return None
    past_d = dates_list[di - lookback]
    past_p = price_full.get(past_d, {}).get(tk)
    cur_p = price_full.get(today, {}).get(tk)
    if past_p and cur_p and past_p > 0:
        return (cur_p - past_p) / past_p * 100
    return None


def get_eps_chg(info, definition):
    """4가지 EPS 변화 정의"""
    if definition == 'D1':
        return info.get('eps_w')
    elif definition == 'D2':
        nc, n90 = info.get('nc'), info.get('n90')
        return ((nc - n90) / n90 * 100) if (n90 and n90 > 0) else None
    elif definition == 'D3':
        n30, n60 = info.get('n30'), info.get('n60')
        return ((n30 - n60) / n60 * 100) if (n60 and n60 > 0) else None
    elif definition == 'D4':
        n7, n30 = info.get('n7'), info.get('n30')
        return ((n7 - n30) / n30 * 100) if (n30 and n30 > 0) else None
    return None


def classify_c2(info, tk, today, dates_list, price_full, eps_def, price_period):
    eps_chg = get_eps_chg(info, eps_def)
    if eps_chg is None: return False
    p_chg = get_price_chg(tk, today, price_period, dates_list, price_full)
    if p_chg is None: return False
    return eps_chg > 0 and p_chg < 0


def rerank(today, today_data, c2_boost, dates_list, price_full, eps_def, price_period):
    if c2_boost == 0:
        return {tk: info.get('p2') for tk, info in today_data.items() if info.get('p2') is not None}
    candidates = []
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        is_c2 = classify_c2(info, tk, today, dates_list, price_full, eps_def, price_period)
        score = (31 - p2) + (c2_boost if is_c2 else 0)
        candidates.append((-score, tk))
    candidates.sort()
    return {tk: i+1 for i, (_, tk) in enumerate(candidates)}


def simulate(dates_all, data, price_full, weights, entry, exit_, c2_boost,
             eps_def, price_period, start_date=None):
    slots = len(weights)
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    consecutive = defaultdict(int)
    daily_returns = []
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            today_data = data.get(d, {})
            new_ranks = rerank(d, today_data, c2_boost, dates_all, price_full, eps_def, price_period)
            new_c = defaultdict(int)
            for tk, r in new_ranks.items():
                if r <= 30:
                    new_c[tk] = consecutive.get(tk, 0) + 1
            consecutive = new_c
    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        new_ranks = rerank(today, today_data, c2_boost, dates_all, price_full, eps_def, price_period)
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
        for tk in exited: del portfolio[tk]
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
                portfolio[tk] = {'entry_price': price, 'slot_idx': slot_idx}
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run_random(eps_def, price_period, dates, data, price_full, seed_starts):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full,
                         [80, 20], 3, 10, 3, eps_def, price_period, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 100)
    print(f'C2 정의 sensitivity — eps_def × price_period grid')
    print(f'(3,10,2) 80/20 boost=3 고정, eps/price 정의만 변경')
    print('=' * 100)
    t0 = time.time()
    dates, data, price_full = load_all(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    # baseline (boost=0)
    base = run_random('D1', 30, dates, data, price_full, seed_starts)
    # 일단 boost=0 보다 가벼운 baseline 계산
    # (3,10,2) 80/20 b=0
    def run_b0():
        rets, mdds, seed_avgs = [], [], []
        for chosen in seed_starts:
            sr = []
            for sd in chosen:
                r = simulate(dates, data, price_full,
                             [80, 20], 3, 10, 0, 'D1', 30, start_date=sd)
                rets.append(r['total_return']); mdds.append(r['max_dd'])
                sr.append(r['total_return'])
            seed_avgs.append(sum(sr)/len(sr))
        return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}
    b0 = run_b0()
    b0_avg = sum(b0['rets'])/len(b0['rets'])
    print(f'\n(3,10,2) 80/20 boost=0 (no C2 boost): {b0_avg:+.2f}%')
    print()

    print(f'{"definition":<35} {"avg":>9} {"vs b=0":>9} {"wins":>10}')
    print('-' * 70)
    eps_defs = ['D1', 'D2', 'D3', 'D4']
    price_periods = [7, 30, 60]
    eps_labels = {
        'D1': 'eps_chg_weighted',
        'D2': '(nc - n90) / n90',
        'D3': '(n30 - n60) / n60',
        'D4': '(n7 - n30) / n30',
    }
    for eps_def in eps_defs:
        for pp in price_periods:
            label = f'{eps_labels[eps_def]} + p{pp}d'
            res = run_random(eps_def, pp, dates, data, price_full, seed_starts)
            avg = sum(res['rets'])/len(res['rets'])
            lifts = [b - a for a, b in zip(b0['seed_avgs'], res['seed_avgs'])]
            wins = sum(1 for l in lifts if l > 0)
            lift = sum(lifts)/len(lifts)
            print(f'{label:<35} {avg:+7.2f}% {lift:+7.2f}%p {wins:>5}/{N_SEEDS}')

    print(f'\n총 소요: {time.time()-t0:.1f}s')


if __name__ == '__main__':
    main()
