"""(3,10,2) + C2 boost grid search — 최강 조합 sweet spot 확정

Phase 1 발견: (3,10,2) + C2 boost=3가 +37.11%p로 최고
검증: boost=0/1/2/3/4/5/7 grid + 12 multistart 동시
추가: weights도 함께 grid (균등 vs 70/30 vs 60/40)
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


def run_random(weights, entry, exit_, c2_boost, dates, data, price_full, seed_starts):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, weights, entry, exit_, c2_boost, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def run_multistart(weights, entry, exit_, c2_boost, dates, data, price_full, fixed_starts):
    rets, mdds = [], []
    for sd in fixed_starts:
        r = simulate(dates, data, price_full, weights, entry, exit_, c2_boost, start_date=sd)
        rets.append(r['total_return']); mdds.append(r['max_dd'])
    return {'rets': rets, 'mdds': mdds}


def main():
    print('=' * 110)
    print(f'(3,10,2) + C2 boost / weights grid')
    print('=' * 110)
    t0 = time.time()
    dates, data, price_full = load_all(DB_PATH)
    print(f'[Load] {time.time()-t0:.1f}s')
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))
    step = max(1, len(eligible) // 13)
    fixed_starts = [eligible[step * i] for i in range(1, 13)]

    # configs: (label, weights, entry, exit_, c2_boost)
    configs = []
    # baseline
    configs.append(('baseline (3,10,3) 균등', [33,33,34], 3, 10, 0))
    # (3,10,2) variants
    for weights, w_label in [([50,50], '50/50'), ([60,40], '60/40'), ([70,30], '70/30'), ([80,20], '80/20')]:
        for b in [0, 1, 2, 3, 4, 5, 7]:
            configs.append((f'(3,10,2) {w_label} boost={b}', weights, 3, 10, b))

    print()
    print(f'{"config":<37} {"R avg":>9} {"R wins":>10} {"M avg":>9} {"M wins":>8} {"worst MDD":>10}')
    print('-' * 100)
    results = {}
    for label, w, entry, exit_, b in configs:
        t1 = time.time()
        r_random = run_random(w, entry, exit_, b, dates, data, price_full, seed_starts)
        r_multi = run_multistart(w, entry, exit_, b, dates, data, price_full, fixed_starts)
        results[label] = {'random': r_random, 'multi': r_multi}
        r_avg = sum(r_random['rets'])/len(r_random['rets'])
        m_avg = sum(r_multi['rets'])/12
        worst_mdd = min(r_random['mdds'])
        baseline_avg = results.get('baseline (3,10,3) 균등', {}).get('random', {}).get('seed_avgs', [])
        baseline_multi = results.get('baseline (3,10,3) 균등', {}).get('multi', {}).get('rets', [])
        if baseline_avg and label != 'baseline (3,10,3) 균등':
            r_lifts = [b_ - a for a, b_ in zip(baseline_avg, r_random['seed_avgs'])]
            r_wins = sum(1 for l in r_lifts if l > 0)
            m_lifts = [b_ - a for a, b_ in zip(baseline_multi, r_multi['rets'])]
            m_wins = sum(1 for l in m_lifts if l > 0)
            r_wstr = f'{r_wins}/{N_SEEDS}'
            m_wstr = f'{m_wins}/12'
        else:
            r_wstr = '★'
            m_wstr = '★'
        print(f'{label:<37} {r_avg:+7.2f}% {r_wstr:>10} {m_avg:+7.2f}% {m_wstr:>8} {worst_mdd:+9.2f}% [{time.time()-t1:.0f}s]')

    # Best by random
    print()
    print('=' * 110)
    print('TOP 5 by random avg lift')
    print('=' * 110)
    base_r = results['baseline (3,10,3) 균등']['random']['seed_avgs']
    base_m = results['baseline (3,10,3) 균등']['multi']['rets']
    rankings = []
    for label in results:
        if 'baseline' in label: continue
        new_r = results[label]['random']['seed_avgs']
        r_lift = sum(b - a for a, b in zip(base_r, new_r)) / len(base_r)
        new_m = results[label]['multi']['rets']
        m_lift = sum(b - a for a, b in zip(base_m, new_m)) / 12
        m_lifts = [b - a for a, b in zip(base_m, new_m)]
        m_min = min(m_lifts)
        r_lifts = [b - a for a, b in zip(base_r, new_r)]
        r_wins = sum(1 for l in r_lifts if l > 0)
        m_wins = sum(1 for l in m_lifts if l > 0)
        rankings.append((r_lift, label, r_wins, m_lift, m_wins, m_min))
    rankings.sort(reverse=True)
    print(f'{"rank":<5} {"config":<37} {"R lift":>10} {"R wins":>10} {"M lift":>10} {"M wins":>8} {"M min":>10}')
    print('-' * 100)
    for i, (r_lift, label, r_wins, m_lift, m_wins, m_min) in enumerate(rankings[:10], 1):
        print(f'{i:>4} {label:<37} {r_lift:+8.2f}%p {r_wins:>5}/{N_SEEDS} {m_lift:+8.2f}%p {m_wins:>5}/12 {m_min:+8.2f}%p')

    print(f'\n총 소요: {time.time()-t0:.1f}s')


if __name__ == '__main__':
    main()
