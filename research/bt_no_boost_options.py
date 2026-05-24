"""(2,10,2) 80/20 boost 없이 — C2 boost 제거하고 슬롯 축소 + 비중만"""
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


def calc_weights(strategy, scores, slots):
    n = len(scores)
    if n == 0: return [0] * slots
    if n == 1: return [100] + [0] * (slots-1)
    if strategy == 'baseline_equal_3':
        base = 100 // slots
        rem = 100 - base * slots
        return [base + (1 if i < rem else 0) for i in range(slots)]
    elif strategy == 'A2_3slot':
        return [50, 30, 20]
    elif strategy == 'fixed_80_20':
        return [80, 20]
    elif strategy == 'fixed_70_30':
        return [70, 30]
    elif strategy == 'fixed_60_40':
        return [60, 40]
    elif strategy == 'fixed_50_50':
        return [50, 50]
    elif strategy == 'dynamic_5':
        if n >= 2 and scores[0] - scores[1] > 5:
            return [80, 20]
        return [50, 50]
    return [50, 50]


def simulate(dates_all, data, price_full, strategy, slots, entry, exit_, c2_boost, start_date=None):
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
                    w = info['weight'] / 100.0
                    day_ret += w * (cur_p - prev_p) / prev_p * 100
        daily_returns.append(day_ret)
        exited = []
        for tk in list(portfolio.keys()):
            rank = new_ranks.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2: exited.append(tk); continue
            if rank is None or rank > exit_: exited.append(tk); continue
        for tk in exited: del portfolio[tk]
        if len(portfolio) < slots:
            cands = []
            for tk, new_r in sorted(new_ranks.items(), key=lambda x: x[1]):
                if new_r > entry: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                info = today_data.get(tk, {})
                min_seg = info.get('min_seg', 0)
                if min_seg < 0: continue
                price = info.get('price')
                score = info.get('score_100', 0)
                if price and price > 0:
                    cands.append((tk, price, score))
            free_slots = slots - len(portfolio)
            new_entries = cands[:free_slots]
            if new_entries:
                existing = [(tk, today_data.get(tk, {}).get('score_100', 0), 'old', info['entry_price'])
                            for tk, info in portfolio.items()]
                all_in_pf = existing + [(tk, sc, 'new', price) for tk, price, sc in new_entries]
                all_in_pf.sort(key=lambda x: -x[1])
                scores = [sc for _, sc, _, _ in all_in_pf]
                weights = calc_weights(strategy, scores, slots)
                new_portfolio = {}
                for i, (tk, sc, typ, price) in enumerate(all_in_pf):
                    w = weights[i] if i < len(weights) else 0
                    ep = portfolio[tk]['entry_price'] if typ == 'old' else price
                    new_portfolio[tk] = {'entry_price': ep, 'weight': w, 'score': sc}
                portfolio = new_portfolio
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run_random(strategy, slots, entry, exit_, c2_boost, dates, data, price_full, seed_starts):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, strategy, slots, entry, exit_, c2_boost, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 110)
    print('C2 boost 없이 vs 있을 때 — (2,10,2) 80/20 비교')
    print('=' * 110)
    dates, data, price_full = load_all_with_score(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))
    step = max(1, len(eligible) // 13)
    fixed_starts = [eligible[step * i] for i in range(1, 13)]

    configs = [
        ('baseline (3,10,3) 균등',     'baseline_equal_3', 3, 30, 10, 0),
        ('A2 (3,10,3) 50/30/20 b=0',  'A2_3slot',         3, 30, 10, 0),
        ('(2,10,2) 50/50 b=0',         'fixed_50_50',     2, 30, 10, 0),
        ('(2,10,2) 70/30 b=0',         'fixed_70_30',     2, 30, 10, 0),
        ('(2,10,2) 80/20 b=0',         'fixed_80_20',     2, 30, 10, 0),
        ('(2,10,2) dynamic_5 b=0',     'dynamic_5',       2, 30, 10, 0),
        # b=3 비교용
        ('(2,10,2) 80/20 b=3',         'fixed_80_20',     2, 30, 10, 3),
        ('(2,10,2) dynamic_5 b=3',     'dynamic_5',       2, 30, 10, 3),
    ]
    print()
    print(f'{"config":<35} {"R avg":>9} {"M avg":>9} {"M min":>9} {"worst MDD":>10} {"vs baseline":>12}')
    print('-' * 95)
    results = {}
    for label, strat, slots, entry, exit_, b in configs:
        r_random = run_random(strat, slots, entry, exit_, b, dates, data, price_full, seed_starts)
        multi_rets = []
        multi_mdds = []
        for sd in fixed_starts:
            r = simulate(dates, data, price_full, strat, slots, entry, exit_, b, start_date=sd)
            multi_rets.append(r['total_return'])
            multi_mdds.append(r['max_dd'])
        results[label] = {'random': r_random, 'multi': multi_rets, 'multi_mdds': multi_mdds}
        r_avg = sum(r_random['rets'])/len(r_random['rets'])
        m_avg = sum(multi_rets)/12
        worst_mdd = min(r_random['mdds'])
        baseline_avg = results.get('baseline (3,10,3) 균등', {}).get('random', {}).get('seed_avgs', [])
        if baseline_avg and 'baseline' not in label:
            lifts = [b_ - a for a, b_ in zip(baseline_avg, r_random['seed_avgs'])]
            wins = sum(1 for l in lifts if l > 0)
            avg_l = sum(lifts)/len(lifts)
            vs_str = f'{avg_l:+.2f}%p ({wins}/{N_SEEDS})'
        else:
            vs_str = '★'
        # M min lift
        if 'baseline' not in label:
            base_m = results.get('baseline (3,10,3) 균등', {}).get('multi', [])
            m_lifts = [b_ - a for a, b_ in zip(base_m, multi_rets)]
            m_min_lift = min(m_lifts) if m_lifts else 0
            m_min_str = f'{m_min_lift:+.2f}%p'
        else:
            m_min_str = '—'
        print(f'{label:<35} {r_avg:+7.2f}% {m_avg:+7.2f}% {m_min_str:>9} {worst_mdd:+9.2f}% {vs_str:>17}')


if __name__ == '__main__':
    main()
