"""entry cap 제거 (production v80.2 일치) BT — entry=3 vs entry=무한 비교

production code: ENTRY_THRESHOLD 캡 제거 (v80.2, 2026-04-29)
  자격미달 시 4, 5, 6위로 자동 슬라이드, 슬롯 채울 때까지

내 BT 들은 entry=3로 cap 적용했음. mismatch.
이 BT로 entry=무한 (entry=30) 동작과 결과 비교.
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
    slide_events = 0  # 4위 이상 슬라이드 발생 횟수
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
                    cands.append((tk, price, new_r))
                    if new_r > 3:
                        slide_events += 1
            for slot_idx in free_idx:
                if not cands: break
                tk, price, _ = cands.pop(0)
                portfolio[tk] = {'entry_price': price, 'slot_idx': slot_idx}
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd, 'slide_events': slide_events}


def run(weights, entry, exit_, c2_boost, dates, data, price_full, seed_starts):
    rets, mdds, seed_avgs, slides = [], [], [], 0
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, weights, entry, exit_, c2_boost, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
            slides += r['slide_events']
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs, 'slides': slides}


def main():
    print('=' * 100)
    print(f'entry cap 영향 검증 — entry=3 (BT 기존) vs entry=30 (production v80.2 일치)')
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
        # entry=3 (BT 기존)
        ('baseline (3,10,3) entry=3 균등', [33,33,34], 3, 10, 0),
        ('baseline entry=30 (prod-aligned)', [33,33,34], 30, 10, 0),
        ('new (3,10,2) 80/20 b=3 entry=3', [80,20], 3, 10, 3),
        ('new entry=30 (prod-aligned)',   [80,20], 30, 10, 3),
    ]
    print()
    print(f'{"config":<40} {"R avg":>9} {"M avg":>9} {"슬라이드(>3) 발생":>15}')
    print('-' * 80)
    results = {}
    for label, w, entry, exit_, b in configs:
        r_random = run(w, entry, exit_, b, dates, data, price_full, seed_starts)
        rets = []
        mdds = []
        slides_total = 0
        for sd in fixed_starts:
            r = simulate(dates, data, price_full, w, entry, exit_, b, start_date=sd)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            slides_total += r['slide_events']
        results[label] = {'random': r_random, 'multi_rets': rets}
        r_avg = sum(r_random['rets'])/len(r_random['rets'])
        m_avg = sum(rets)/12
        print(f'{label:<40} {r_avg:+7.2f}% {m_avg:+7.2f}% {slides_total:>14}')

    print()
    print('=' * 100)
    print('entry cap 영향 — 같은 config 내부 entry=3 vs entry=30 차이')
    print('=' * 100)
    base_r = results['baseline (3,10,3) entry=3 균등']['random']['seed_avgs']
    base30_r = results['baseline entry=30 (prod-aligned)']['random']['seed_avgs']
    new3_r = results['new (3,10,2) 80/20 b=3 entry=3']['random']['seed_avgs']
    new30_r = results['new entry=30 (prod-aligned)']['random']['seed_avgs']

    diff_base = [b - a for a, b in zip(base_r, base30_r)]
    diff_new = [b - a for a, b in zip(new3_r, new30_r)]
    print(f'baseline: entry=30 vs entry=3 lift = {sum(diff_base)/len(diff_base):+.2f}%p (wins {sum(1 for x in diff_base if x>0)}/{len(diff_base)})')
    print(f'new:      entry=30 vs entry=3 lift = {sum(diff_new)/len(diff_new):+.2f}%p (wins {sum(1 for x in diff_new if x>0)}/{len(diff_new)})')

    # 새 후보 lift (entry=30 일치 기준)
    new_vs_base = [b - a for a, b in zip(base30_r, new30_r)]
    print()
    print(f'★ 새 후보 vs baseline (entry=30 production-aligned):')
    print(f'  lift = {sum(new_vs_base)/len(new_vs_base):+.2f}%p, wins {sum(1 for x in new_vs_base if x>0)}/{len(new_vs_base)}')


if __name__ == '__main__':
    main()
