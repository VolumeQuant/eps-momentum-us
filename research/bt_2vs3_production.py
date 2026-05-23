"""(2,10,2) vs (3,10,2) 80/20 boost=3 — production v80.2 동작 일치 검증

production v80.2: entry cap 제거, 슬롯 채울 때까지 슬라이드.
가설: (2,10,2)와 (3,10,2)가 production-aligned (entry=30)에서 동일 결과.
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


def simulate(dates_all, data, price_full, weights, entry, exit_, c2_boost, start_date=None):
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


def run(weights, entry, exit_, c2_boost, dates, data, price_full, seed_starts):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, weights, entry, exit_, c2_boost, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 100)
    print('(2,10,2) vs (3,10,2) 80/20 boost=3 — production v80.2 일치 (entry=30)')
    print('=' * 100)
    dates, data, price_full = load_all(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))
    step = max(1, len(eligible) // 13)
    fixed_starts = [eligible[step * i] for i in range(1, 13)]

    configs = [
        ('baseline (3,10,3) 균등 entry=30', [33,33,34], 30, 10, 0),
        ('(2,10,2) 80/20 b=3 entry=2 (BT cap)', [80,20], 2, 10, 3),
        ('(2,10,2) 80/20 b=3 entry=30 (production)', [80,20], 30, 10, 3),
        ('(3,10,2) 80/20 b=3 entry=3 (BT cap)', [80,20], 3, 10, 3),
        ('(3,10,2) 80/20 b=3 entry=30 (production)', [80,20], 30, 10, 3),
    ]
    print()
    print(f'{"config":<45} {"R avg":>9} {"M avg":>9} {"MDD":>9}')
    print('-' * 80)
    results = {}
    for label, w, entry, exit_, b in configs:
        r_random = run(w, entry, exit_, b, dates, data, price_full, seed_starts)
        multi_rets = []
        multi_mdds = []
        for sd in fixed_starts:
            r = simulate(dates, data, price_full, w, entry, exit_, b, start_date=sd)
            multi_rets.append(r['total_return'])
            multi_mdds.append(r['max_dd'])
        results[label] = {'random': r_random, 'multi': multi_rets, 'multi_mdds': multi_mdds}
        r_avg = sum(r_random['rets'])/len(r_random['rets'])
        m_avg = sum(multi_rets)/12
        worst_mdd = min(r_random['mdds'])
        print(f'{label:<45} {r_avg:+7.2f}% {m_avg:+7.2f}% {worst_mdd:+7.2f}%')

    # Production 일치 비교 (둘 다 entry=30)
    print()
    print('=' * 100)
    print('★ Production-aligned 비교 (entry=30, cap 없음)')
    print('=' * 100)
    base_r = results['baseline (3,10,3) 균등 entry=30']['random']['seed_avgs']
    two_r = results['(2,10,2) 80/20 b=3 entry=30 (production)']['random']['seed_avgs']
    three_r = results['(3,10,2) 80/20 b=3 entry=30 (production)']['random']['seed_avgs']
    print(f'(2,10,2) vs (3,10,2) production-aligned 차이:')
    diffs_23 = [b - a for a, b in zip(two_r, three_r)]
    avg_d = sum(diffs_23) / len(diffs_23)
    wins_d = sum(1 for x in diffs_23 if x > 0)
    losses_d = sum(1 for x in diffs_23 if x < 0)
    print(f'  (3,10,2) - (2,10,2) = {avg_d:+.4f}%p, wins {wins_d}, losses {losses_d}, equal {len(diffs_23)-wins_d-losses_d}')
    print(f'  → 사용자 가설: (2,10,2)와 (3,10,2) production에서 동일하다')
    print(f'  → 검증: 차이 거의 0이면 가설 맞음')

    print()
    print(f'각각 vs baseline (production-aligned):')
    lift_2 = sum(b - a for a, b in zip(base_r, two_r)) / len(base_r)
    lift_3 = sum(b - a for a, b in zip(base_r, three_r)) / len(base_r)
    w_2 = sum(1 for a, b in zip(base_r, two_r) if b > a)
    w_3 = sum(1 for a, b in zip(base_r, three_r) if b > a)
    print(f'  (2,10,2) 80/20 b=3: lift {lift_2:+.2f}%p, wins {w_2}/500')
    print(f'  (3,10,2) 80/20 b=3: lift {lift_3:+.2f}%p, wins {w_3}/500')


if __name__ == '__main__':
    main()
