"""Phase 4 — 더 극단적 변형 + 새 아이디어

1. entry pool 확장: (4,10,2), (5,10,2), (10,10,2) — boost로 C2 더 잡기
2. slot 1: 가장 집중 (1,10,1) 100%
3. C3 penalty: EPS↓ + 가격↑ 종목 차단
4. C1 penalty (반대 시도): EPS↑ + 가격↑ 약간 약화
5. price threshold strict: price 30d < -5% 만 C2 인정 (얕은 dip 차단)

baseline (3,10,2) 80/20 boost=3 vs 새 아이디어들 비교
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


def classify_case(info, tk, today, dates_list, price_full,
                  c2_price_threshold=0.0, c3_price_threshold=0.0):
    """Returns 'C1', 'C2', 'C3', 'C4', or None
       c2_price_threshold: C2가 되려면 price_30d < threshold (-5 같이 음수로 설정 시 strict)
       c3_price_threshold: C3가 되려면 price_30d > threshold
    """
    eps_w = info.get('eps_w')
    if eps_w is None: return None
    p30 = get_price_30d(tk, today, dates_list, price_full)
    if p30 is None: return None
    if eps_w > 0:
        if p30 < c2_price_threshold:
            return 'C2'
        else:
            return 'C1'
    else:  # eps_w <= 0
        if p30 > c3_price_threshold:
            return 'C3'
        else:
            return 'C4'


def rerank(today, today_data, c2_boost, c1_penalty, c3_penalty, c4_penalty,
           dates_list, price_full, c2_price_thr=0.0):
    candidates = []
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        case = classify_case(info, tk, today, dates_list, price_full, c2_price_thr)
        adj = 0
        if case == 'C2': adj = c2_boost
        elif case == 'C1': adj = -c1_penalty
        elif case == 'C3': adj = -c3_penalty
        elif case == 'C4': adj = -c4_penalty
        score = (31 - p2) + adj
        candidates.append((-score, tk))
    candidates.sort()
    return {tk: i+1 for i, (_, tk) in enumerate(candidates)}


def simulate(dates_all, data, price_full, weights, entry, exit_,
             c2_boost, c1_penalty=0, c3_penalty=0, c4_penalty=0,
             c2_price_thr=0.0, start_date=None):
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
            new_ranks = rerank(d, today_data, c2_boost, c1_penalty, c3_penalty, c4_penalty,
                               dates_all, price_full, c2_price_thr)
            new_c = defaultdict(int)
            for tk, r in new_ranks.items():
                if r <= 30:
                    new_c[tk] = consecutive.get(tk, 0) + 1
            consecutive = new_c
    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        new_ranks = rerank(today, today_data, c2_boost, c1_penalty, c3_penalty, c4_penalty,
                           dates_all, price_full, c2_price_thr)
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


def run_random(cfg, dates, data, price_full, seed_starts):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, **cfg, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def run_multistart(cfg, dates, data, price_full, fixed_starts):
    rets, mdds = [], []
    for sd in fixed_starts:
        r = simulate(dates, data, price_full, **cfg, start_date=sd)
        rets.append(r['total_return']); mdds.append(r['max_dd'])
    return {'rets': rets, 'mdds': mdds}


def main():
    print('=' * 110)
    print('Phase 4 — 극단 변형 + 새 아이디어')
    print('=' * 110)
    t0 = time.time()
    dates, data, price_full = load_all(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))
    step = max(1, len(eligible) // 13)
    fixed_starts = [eligible[step * i] for i in range(1, 13)]

    # configs
    configs = [
        # baseline references
        ('baseline (3,10,3) 균등', dict(weights=[33,33,34], entry=3, exit_=10, c2_boost=0)),
        ('★ Phase 3 winner (3,10,2) 80/20 b=3', dict(weights=[80,20], entry=3, exit_=10, c2_boost=3)),

        # entry pool 확장
        ('(4,10,2) 80/20 b=3', dict(weights=[80,20], entry=4, exit_=10, c2_boost=3)),
        ('(5,10,2) 80/20 b=3', dict(weights=[80,20], entry=5, exit_=10, c2_boost=3)),
        ('(10,10,2) 80/20 b=3', dict(weights=[80,20], entry=10, exit_=10, c2_boost=3)),

        # slot 1 (가장 집중)
        ('(1,10,1) 100% b=0', dict(weights=[100], entry=1, exit_=10, c2_boost=0)),
        ('(3,10,1) 100% b=3', dict(weights=[100], entry=3, exit_=10, c2_boost=3)),
        ('(5,10,1) 100% b=3', dict(weights=[100], entry=5, exit_=10, c2_boost=3)),

        # C2 strict (얕은 dip 차단)
        ('(3,10,2) 80/20 b=3 strict (-5%)', dict(weights=[80,20], entry=3, exit_=10, c2_boost=3, c2_price_thr=-0.05)),
        ('(3,10,2) 80/20 b=3 strict (-10%)', dict(weights=[80,20], entry=3, exit_=10, c2_boost=3, c2_price_thr=-0.10)),

        # case penalty
        ('(3,10,2) 80/20 b=3 + C1 penalty=1', dict(weights=[80,20], entry=3, exit_=10, c2_boost=3, c1_penalty=1)),
        ('(3,10,2) 80/20 b=3 + C1 penalty=2', dict(weights=[80,20], entry=3, exit_=10, c2_boost=3, c1_penalty=2)),
        # C3, C4 penalty (sample 작아서 효과 없을 수도)
        ('(3,10,2) 80/20 b=3 + C3 penalty=5', dict(weights=[80,20], entry=3, exit_=10, c2_boost=3, c3_penalty=5)),

        # (4,10,2) C2 strict
        ('(4,10,2) 80/20 b=3 strict (-5%)', dict(weights=[80,20], entry=4, exit_=10, c2_boost=3, c2_price_thr=-0.05)),
        # entry 확장 + slot 2 + 80/20 + boost grid
        ('(5,10,2) 70/30 b=3', dict(weights=[70,30], entry=5, exit_=10, c2_boost=3)),
        ('(5,10,2) 90/10 b=3', dict(weights=[90,10], entry=5, exit_=10, c2_boost=3)),
    ]

    print()
    print(f'{"config":<42} {"R avg":>9} {"R wins":>9} {"M avg":>9} {"M wins":>8} {"worst MDD":>10}')
    print('-' * 110)
    results = {}
    for label, cfg in configs:
        t1 = time.time()
        r_random = run_random(cfg, dates, data, price_full, seed_starts)
        r_multi = run_multistart(cfg, dates, data, price_full, fixed_starts)
        results[label] = {'random': r_random, 'multi': r_multi}
        r_avg = sum(r_random['rets'])/len(r_random['rets'])
        m_avg = sum(r_multi['rets'])/12
        worst_mdd = min(r_random['mdds'])
        baseline_avg = results.get('baseline (3,10,3) 균등', {}).get('random', {}).get('seed_avgs', [])
        baseline_multi = results.get('baseline (3,10,3) 균등', {}).get('multi', {}).get('rets', [])
        if baseline_avg and 'baseline' not in label:
            r_lifts = [b_ - a for a, b_ in zip(baseline_avg, r_random['seed_avgs'])]
            r_wins = sum(1 for l in r_lifts if l > 0)
            m_lifts = [b_ - a for a, b_ in zip(baseline_multi, r_multi['rets'])]
            m_wins = sum(1 for l in m_lifts if l > 0)
            r_wstr = f'{r_wins}/{N_SEEDS}'
            m_wstr = f'{m_wins}/12'
        else:
            r_wstr = '★'
            m_wstr = '★'
        print(f'{label:<42} {r_avg:+7.2f}% {r_wstr:>9} {m_avg:+7.2f}% {m_wstr:>8} {worst_mdd:+9.2f}% [{time.time()-t1:.0f}s]')

    # TOP rankings
    print()
    print('=' * 110)
    print('TOP 10 by random avg lift')
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
        m_min = min(m_lifts); m_max = max(m_lifts)
        r_lifts = [b - a for a, b in zip(base_r, new_r)]
        r_wins = sum(1 for l in r_lifts if l > 0)
        m_wins = sum(1 for l in m_lifts if l > 0)
        worst_mdd = min(results[label]['random']['mdds'])
        rankings.append((r_lift, label, r_wins, m_lift, m_wins, m_min, m_max, worst_mdd))
    rankings.sort(reverse=True)
    print(f'{"rank":<5} {"config":<42} {"R lift":>10} {"R wins":>9} {"M lift":>10} {"M wins":>8} {"M min":>10} {"M max":>10} {"MDD":>8}')
    print('-' * 120)
    for i, (r_lift, label, r_wins, m_lift, m_wins, m_min, m_max, mdd) in enumerate(rankings, 1):
        print(f'{i:>4} {label:<42} {r_lift:+8.2f}%p {r_wins:>4}/{N_SEEDS} {m_lift:+8.2f}%p {m_wins:>4}/12 {m_min:+8.2f}%p {m_max:+8.2f}%p {mdd:+7.2f}%')

    print(f'\n총 소요: {time.time()-t0:.1f}s')


if __name__ == '__main__':
    main()
