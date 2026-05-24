"""C2 boost dense grid + 4/2 worst 시작일 재분석"""
import sys
import sqlite3
import random
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


def load_all(db_path):
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


def is_c2(info, tk, today, dates_list, price_full):
    eps_w = info.get('eps_w')
    if eps_w is None or eps_w <= 0: return False
    p30 = get_price_30d(tk, today, dates_list, price_full)
    if p30 is None: return False
    return p30 < 0


def rerank(today, today_data, c2_boost, dates_list, price_full):
    if c2_boost == 0:
        return {tk: info.get('p2') for tk, info in today_data.items() if info.get('p2') is not None}
    candidates = []
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        is_c2_now = is_c2(info, tk, today, dates_list, price_full)
        score = (31 - p2) + (c2_boost if is_c2_now else 0)
        candidates.append((-score, tk))
    candidates.sort()
    return {tk: i+1 for i, (_, tk) in enumerate(candidates)}


def simulate(dates_all, data, price_full, weights, entry, exit_, c2_boost,
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
            cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            if min_seg < -2 or rank is None or rank > exit_:
                if log_trades and cur_p:
                    info = portfolio[tk]
                    ret = (cur_p - info['entry_price']) / info['entry_price'] * 100
                    trades.append({'tk': tk, 'entry_d': info['entry_d'], 'exit_d': today,
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
            if info.get('min_seg', 0) < 0: continue
            price = info.get('price')
            if price and price > 0:
                cands.append((tk, price, is_c2(info, tk, today, dates_all, price_full)))
        for slot_idx in free_slots:
            if not cands: break
            tk, price, is_c2_now = cands.pop(0)
            portfolio[tk] = {'entry_price': price, 'slot_idx': slot_idx, 'weight': weights[slot_idx],
                             'entry_di': di, 'entry_d': today, 'is_c2': is_c2_now}
    if log_trades:
        last_d = dates[-1]
        for tk, info in portfolio.items():
            final_p = data[last_d].get(tk, {}).get('price') or price_full.get(last_d, {}).get(tk)
            if final_p:
                ret = (final_p - info['entry_price']) / info['entry_price'] * 100
                trades.append({'tk': tk, 'entry_d': info['entry_d'], 'exit_d': last_d + ' (open)',
                               'ret': ret, 'days': len(dates) - info['entry_di'] - 1, 'weight': info['weight']})
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd, 'trades': trades}


def main():
    dates, data, price_full = load_all(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))
    step24 = max(1, len(eligible) // 25)
    starts24 = [eligible[step24 * i] for i in range(1, 25)]

    BASE = ([33,34,33], 30, 10, 0)
    base_seed_avgs = []
    for chosen in seed_starts:
        sr = [simulate(dates, data, price_full, *BASE, start_date=sd)['total_return'] for sd in chosen]
        base_seed_avgs.append(sum(sr)/len(sr))
    base_24 = [simulate(dates, data, price_full, *BASE, start_date=sd)['total_return'] for sd in starts24]

    print('=' * 130)
    print('C2 boost dense grid — 70/30 + 80/20')
    print('=' * 130)
    print(f'{"config":<32} {"R lift":>10} {"R wins":>10} {"M24 lift":>10} {"M24 wins":>11} {"M24 min":>11} {"M24 max":>11} {"MDD":>8}')
    print('-' * 130)
    for w_label, weights in [('70/30', [70,30]), ('80/20', [80,20])]:
        for boost in [0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 15]:
            cfg = (weights, 30, 10, boost)
            seed_avgs, mdds = [], []
            for chosen in seed_starts:
                sr = []
                for sd in chosen:
                    r = simulate(dates, data, price_full, *cfg, start_date=sd)
                    sr.append(r['total_return']); mdds.append(r['max_dd'])
                seed_avgs.append(sum(sr)/len(sr))
            m24 = [simulate(dates, data, price_full, *cfg, start_date=sd)['total_return'] for sd in starts24]
            r_lifts = [b - a for a, b in zip(base_seed_avgs, seed_avgs)]
            r_lift = sum(r_lifts)/len(r_lifts)
            r_wins = sum(1 for l in r_lifts if l > 0)
            m24_lifts = [b - a for a, b in zip(base_24, m24)]
            m24_lift = sum(m24_lifts)/24
            m24_wins = sum(1 for l in m24_lifts if l > 0)
            m24_min = min(m24_lifts); m24_max = max(m24_lifts)
            worst_mdd = min(mdds)
            print(f'{w_label} + C2 b={boost:<2}                  {r_lift:+8.2f}%p {r_wins:>5}/500 {m24_lift:+8.2f}%p {m24_wins:>5}/24 {m24_min:+9.2f}%p {m24_max:+9.2f}%p {worst_mdd:+7.2f}%')

    print()
    print('=' * 130)
    print('★ 4/2 worst 시작일 재분석 — C2 boost 적용 시 어떻게 달라지나')
    print('=' * 130)
    sd = '2026-04-02'
    print(f'\n시작일: {sd}\n')
    configs = [
        ('baseline 균등', [33,34,33], 30, 10, 0),
        ('A: 70/30 b=0', [70,30], 30, 10, 0),
        ('B: 80/20 + C1 b=5 (worst)', [80,20], 30, 10, 5),  # C1 boost는 별도 처리
        ('NEW: 70/30 + C2 b=3', [70,30], 30, 10, 3),
        ('NEW: 80/20 + C2 b=3', [80,20], 30, 10, 3),
        ('NEW: 80/20 + C2 b=5', [80,20], 30, 10, 5),
    ]
    for label, w, e, x, b in configs:
        if 'C1' in label:
            # C1 boost는 다른 BT 결과 활용
            print(f'\n[{label}] 보유 시뮬레이션 (C1 boost — 이전 결과 참고)')
            print(f'  → MU 못 잡고 FIVE 80% 진입 → -2.07% loss')
            print(f'  결과: +65.06% (baseline 대비 -37%p)')
            continue
        r = simulate(dates, data, price_full, w, e, x, b, start_date=sd, log_trades=True)
        print(f'\n[{label}] 누적 {r["total_return"]:+.2f}%, MDD {r["max_dd"]:+.2f}%')
        for t in r['trades']:
            c2_marker = ' ← C2' if t.get('weight', 0) > 50 else ''
            print(f'  {t["tk"]:<7} {t["entry_d"]:<12} {t["exit_d"]:<22} {t["ret"]:+8.2f}% {t["days"]:>4}일 {t["weight"]:>5}%')


if __name__ == '__main__':
    main()
