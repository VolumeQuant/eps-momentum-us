"""v82a robust 검증 — MA120 풀 + 명시적 MA20 매도 (사용자 직관)

NEW simulator 기반. v80.10c production 대비 paired 비교.
설계:
  - 500 random seed × 3 starts (paired 강건성)
  - 12 fixed 시작일 multistart (구간 일관성)
  - 핵심 metric: avg lift, win rate, MDD diff, worst case

베이스라인:
  v80.10c: MA120(MA60 fallback) 매수 풀, exit rank>10, slots=3, entry=3
변형:
  v82a:    위와 동일 + 보유 중 price<MA20 시 즉시 매도
  v82a-strict:  MA20 매도 + (3,8,3) 회전 (v80.10b 이전, 더 빠른 회전)
  v82a-wide:    MA20 매도 + (5,10,3) 풀 확장

데이터: research/ma_filter_dbs/ext_current.db
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
DB_PATH = ROOT / 'research' / 'ma_filter_dbs' / 'ext_current.db'

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
            SELECT ticker, part2_rank, price, ma20,
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
                'p2': r[1], 'price': r[2], 'ma20': r[3],
                'min_seg': min(segs) if segs else 0,
            }
    ma20_full = defaultdict(dict)
    for d, tk, ma20 in cur.execute(
        'SELECT date, ticker, ma20 FROM ntm_screening WHERE ma20 IS NOT NULL'
    ):
        ma20_full[d][tk] = ma20
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'
    ):
        price_full[d][tk] = px
    conn.close()
    return dates, data, ma20_full, price_full


def simulate(dates_all, data, ma20_full, price_full,
             entry, exit_, slots, use_ma20_exit, start_date=None):
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    daily_returns = []
    consecutive = defaultdict(int)
    if start_date:
        for d in dates_all:
            if d >= start_date:
                break
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    consecutive[tk] = consecutive.get(tk, 0) + 1

    for di, today in enumerate(dates):
        if today not in data:
            continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consec = defaultdict(int)
        for tk in rank_map:
            new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di-1]
            n = 0
            for tk in portfolio:
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                pr = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if p and pr and pr > 0:
                    day_ret += (p - pr) / pr * 100
                    n += 1
            if n > 0:
                day_ret /= n
        daily_returns.append(day_ret)

        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            price = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            ma20 = today_data.get(tk, {}).get('ma20') or ma20_full.get(today, {}).get(tk)
            if min_seg < -2:
                exited.append(tk); continue
            if rank is None or rank > exit_:
                exited.append(tk); continue
            if use_ma20_exit and price and ma20 and price < ma20:
                exited.append(tk); continue
        for tk in exited:
            del portfolio[tk]

        vacancies = slots - len(portfolio)
        if vacancies > 0:
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > entry or vacancies <= 0:
                    break
                if tk in portfolio:
                    continue
                if consecutive.get(tk, 0) < 3:
                    continue
                min_seg = today_data.get(tk, {}).get('min_seg', 0)
                if min_seg < 0:
                    continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0:
                    portfolio[tk] = {'entry_price': price}
                    vacancies -= 1

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run_random(dates, data, ma20_full, price_full, entry, exit_, slots, ma20_exit, seed_starts):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, ma20_full, price_full,
                         entry, exit_, slots, ma20_exit, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def run_multistart(dates, data, ma20_full, price_full, entry, exit_, slots, ma20_exit, start_dates):
    rets, mdds = [], []
    for sd in start_dates:
        r = simulate(dates, data, ma20_full, price_full,
                     entry, exit_, slots, ma20_exit, start_date=sd)
        rets.append(r['total_return']); mdds.append(r['max_dd'])
    return {'rets': rets, 'mdds': mdds}


def main():
    print('=' * 110)
    print(f'v82a robust 검증 (NEW simulator, MA120 풀 + 명시 MA20 매도)')
    print(f'DB: {DB_PATH.name}')
    print(f'N_SEEDS={N_SEEDS} × {SAMPLES} samples + 12 fixed multistart')
    print('=' * 110)

    t0 = time.time()
    dates, data, ma20_full, price_full = load_all(DB_PATH)
    print(f'[Load] {time.time()-t0:.1f}s, 거래일 {len(dates)}일')

    # seed_starts (paired)
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    # 12 fixed multistart (균등 간격, 첫/끝 제외)
    step = max(1, len(eligible) // 13)
    fixed_starts = [eligible[step * i] for i in range(1, 13)]
    print(f'[12 multistart] {fixed_starts[0]} ~ {fixed_starts[-1]}')

    candidates = [
        # (label, entry, exit, slots, ma20_exit)
        ('v80.10c production',   3, 10, 3, False),
        ('v82a (MA20 매도)',      3, 10, 3, True),
        ('v82a-strict (3,8,3)',   3,  8, 3, True),
        ('v82a-wide (5,10,3)',    5, 10, 3, True),
    ]

    print()
    print('=' * 110)
    print(f'Random {N_SEEDS} seed × {SAMPLES} starts paired')
    print('=' * 110)
    rand_results = {}
    print(f'  {"variant":<25} {"avg":>9} {"mdd worst":>10} {"sharpe":>8} [{"time":>5}]')
    print('  ' + '-' * 80)
    for label, e, x, s, m in candidates:
        t1 = time.time()
        res = run_random(dates, data, ma20_full, price_full, e, x, s, m, seed_starts)
        rand_results[label] = res
        avg = sum(res['rets'])/len(res['rets'])
        worst_mdd = min(res['mdds'])
        std = statistics.pstdev(res['rets'])
        sharpe = avg/std if std > 0 else 0
        marker = '★' if 'production' in label else ' '
        print(f'  {marker} {label:<23} {avg:+8.2f}% {worst_mdd:+9.2f}% {sharpe:+7.2f} [{time.time()-t1:5.1f}s]')

    print()
    print('  paired vs v80.10c production:')
    base = rand_results['v80.10c production']['seed_avgs']
    for label, *_ in candidates:
        if 'production' in label:
            continue
        new_ = rand_results[label]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new_)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts)/len(lifts)
        worst = min(lifts)
        best = max(lifts)
        rate = wins / N_SEEDS * 100
        verdict = '✓✓' if rate >= 90 else '✓' if rate >= 75 else '◐' if rate >= 60 else '✗'
        print(f'    {label:<25} lift {avg_l:+6.2f}%p wins {wins:>3}/{N_SEEDS} ({rate:>5.1f}%) '
              f'worst {worst:+6.2f}%p best {best:+6.2f}%p {verdict}')

    print()
    print('=' * 110)
    print('12 fixed multistart')
    print('=' * 110)
    multi_results = {}
    for label, e, x, s, m in candidates:
        res = run_multistart(dates, data, ma20_full, price_full, e, x, s, m, fixed_starts)
        multi_results[label] = res
        avg = sum(res['rets'])/len(res['rets'])
        med = sorted(res['rets'])[len(res['rets'])//2]
        worst = min(res['rets'])
        worst_mdd = min(res['mdds'])
        marker = '★' if 'production' in label else ' '
        print(f'  {marker} {label:<25} avg={avg:+7.2f}% med={med:+7.2f}% worst={worst:+7.2f}% mdd={worst_mdd:+7.2f}%')

    print()
    print('  multistart paired vs v80.10c (12 시작일별 lift):')
    base_m = multi_results['v80.10c production']['rets']
    for label, *_ in candidates:
        if 'production' in label:
            continue
        new_m = multi_results[label]['rets']
        lifts = [b - a for a, b in zip(base_m, new_m)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts)/len(lifts)
        print(f'    {label:<25} avg lift {avg_l:+6.2f}%p  wins {wins}/12')
        # 시작일별
        for sd, l in zip(fixed_starts, lifts):
            marker = '+' if l > 0 else '-' if l < 0 else '='
            print(f'      [{sd}] {marker}{l:+6.2f}%p')

    print(f'\n총 소요: {time.time()-t0:.1f}s')


if __name__ == '__main__':
    main()
