"""사용자 실제 운용 BT — 자동 rebalance 없음

진입 시 weight 부여 (슬롯 idx 기반), 보유 중 변경 X, 매도 시 슬롯 빔.
신규 진입 시 빈 슬롯의 weight 사용.

이전 BT 가정 (점수 기반 rebalance) vs 새 BT 가정 (슬롯 idx 기반, drift) 비교.
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


def simulate_realistic(dates_all, data, price_full, weights, entry, exit_, c1_boost, start_date=None):
    """실제 운용 simulator — 슬롯 idx 기반 weight, rebalance 없음"""
    slots = len(weights)
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    # portfolio = {ticker: {'entry_price', 'slot_idx', 'weight'}}
    portfolio = {}
    consecutive = defaultdict(int)
    daily_returns = []
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
        # Day return: 보유 종목 weight × (price 변화)
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
        # Exit
        exited = []
        for tk in list(portfolio.keys()):
            rank = new_ranks.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2: exited.append(tk); continue
            if rank is None or rank > exit_: exited.append(tk); continue
        for tk in exited: del portfolio[tk]
        # Entry — 빈 슬롯에 메시지 순서대로 (점수 1위부터 들어옴 = 슬롯 idx 작은 거부터)
        # 슬롯 idx 0 = 80%, 슬롯 idx 1 = 20%
        used_slots = {info['slot_idx'] for info in portfolio.values()}
        free_slots = sorted([i for i in range(slots) if i not in used_slots])
        # 진입 후보 (메시지 순)
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
        # 빈 슬롯에 순서대로 배치 (idx 작은 슬롯부터)
        for slot_idx in free_slots:
            if not cands: break
            tk, price = cands.pop(0)
            portfolio[tk] = {'entry_price': price, 'slot_idx': slot_idx, 'weight': weights[slot_idx]}
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run_random(weights, entry, exit_, c1_boost, dates, data, price_full, seed_starts):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate_realistic(dates, data, price_full, weights, entry, exit_, c1_boost, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 110)
    print('실제 운용 BT (rebalance 없음) — 슬롯 idx 기반 weight')
    print('=' * 110)
    dates, data, price_full = load_all_with_score(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))
    step = max(1, len(eligible) // 13)
    fixed_starts = [eligible[step * i] for i in range(1, 13)]

    # baseline
    BASE = ([33, 34, 33], 30, 10, 0)
    base_r = run_random(*BASE, dates, data, price_full, seed_starts)
    base_m = [simulate_realistic(dates, data, price_full, *BASE, start_date=sd)['total_return'] for sd in fixed_starts]

    configs = [
        ('(3,10,3) 50/30/20 b=0',        [50, 30, 20], 30, 10, 0),
        ('(2,10,2) 70/30 b=0 [현재 v82]', [70, 30], 30, 10, 0),
        ('(2,10,2) 80/20 b=0',           [80, 20], 30, 10, 0),
        ('(2,10,2) 70/30 + C1 b=5',      [70, 30], 30, 10, 5),
        ('(2,10,2) 70/30 + C1 b=8',      [70, 30], 30, 10, 8),
        ('(2,10,2) 80/20 + C1 b=5',      [80, 20], 30, 10, 5),
        ('(2,10,2) 80/20 + C1 b=8',      [80, 20], 30, 10, 8),
        ('(2,10,2) 90/10 + C1 b=8',     [90, 10], 30, 10, 8),
    ]
    print()
    print(f'baseline (3,10,3) 균등 (rebalance 없음): {sum(base_r["rets"])/len(base_r["rets"]):+.2f}% (random), {sum(base_m)/12:+.2f}% (multistart)')
    print()
    print(f'{"config":<32} {"R avg":>9} {"R lift":>10} {"R wins":>10} {"M avg":>9} {"M lift":>10} {"M wins":>8} {"M min":>10} {"MDD":>9}')
    print('-' * 110)
    for label, w, entry, exit_, b in configs:
        cfg = (w, entry, exit_, b)
        r_random = run_random(*cfg, dates, data, price_full, seed_starts)
        multi_rets = [simulate_realistic(dates, data, price_full, *cfg, start_date=sd)['total_return'] for sd in fixed_starts]
        r_avg = sum(r_random['rets'])/len(r_random['rets'])
        m_avg = sum(multi_rets)/12
        lifts = [b_ - a for a, b_ in zip(base_r['seed_avgs'], r_random['seed_avgs'])]
        r_wins = sum(1 for l in lifts if l > 0)
        r_lift = sum(lifts)/len(lifts)
        m_lifts = [b_ - a for a, b_ in zip(base_m, multi_rets)]
        m_wins = sum(1 for l in m_lifts if l > 0)
        m_lift = sum(m_lifts)/12
        m_min = min(m_lifts)
        worst_mdd = min(r_random['mdds'])
        print(f'  {label:<30} {r_avg:+7.2f}% {r_lift:+8.2f}%p {r_wins:>5}/500 {m_avg:+7.2f}% {m_lift:+8.2f}%p {m_wins:>4}/12 {m_min:+8.2f}%p {worst_mdd:+8.2f}%')


if __name__ == '__main__':
    main()
