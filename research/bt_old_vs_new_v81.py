"""v81 (ext_ma20) vs v80.10c (ext_current) — OLD simulator vs NEW simulator 직접 비교

목적:
  bt_ma_filter_extended (OLD)에서 v81=+27.57%p / 100 wins
  bt_buy_ma120_sell_ma20 (NEW)에서 v81=-7.59%p / 116 wins (500 seed)
  → 35%p swing. simulator 차이가 진짜 원인인지 동일 seed/DB로 검증.

설계:
  100 seed × 3 starts paired (bt_ma_filter_extended와 동일)
  ext_current.db (v80.10c pool) vs ext_ma20.db (v81 pool)
  OLD simulator (price fallback X) vs NEW simulator (price_full fallback O)
  → 4 조합: (DB, simulator)
"""
import sys
import sqlite3
import random
import statistics
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'ma_filter_dbs'
DB_CURRENT = GRID / 'ext_current.db'
DB_MA20 = GRID / 'ext_ma20.db'

N_SEEDS = 100
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
            SELECT ticker, part2_rank, price,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[3:8])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else:
                    segs.append(0)
            data[d][tk] = {
                'p2': r[1], 'price': r[2],
                'min_seg': min(segs) if segs else 0,
            }
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'
    ):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def simulate(dates_all, data, price_full, mode,
             entry=3, exit_=10, slots=3, start_date=None):
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    daily_returns = []
    miss_days = 0  # 매도일 누락 일수 (디버그)
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
            if mode == 'OLD':
                # bt_breakout_hold 방식: today_data만, len(portfolio)로 나눔
                for tk in portfolio:
                    p = today_data.get(tk, {}).get('price')
                    pr = data.get(prev_d, {}).get(tk, {}).get('price')
                    if p and pr and pr > 0:
                        day_ret += (p - pr) / pr * 100
                    else:
                        miss_days += 1
                day_ret /= len(portfolio)
            else:  # NEW
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

        # 이탈 (MA20 명시 매도 없음 — pool 이탈로 자동)
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2:
                exited.append(tk); continue
            if rank is None or rank > exit_:
                exited.append(tk); continue
        for tk in exited:
            del portfolio[tk]

        # 진입
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
    return {'total_return': (cum-1)*100, 'max_dd': max_dd,
            'miss_days': miss_days}


def run(db_path, mode, seed_starts):
    dates, data, price_full = load_all(db_path)
    rets, mdds, seed_avgs, miss_total = [], [], [], 0
    for chosen in seed_starts:
        seed_rets = []
        for sd in chosen:
            r = simulate(dates, data, price_full, mode, start_date=sd)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            seed_rets.append(r['total_return'])
            miss_total += r['miss_days']
        seed_avgs.append(sum(seed_rets)/len(seed_rets))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs,
            'miss_total': miss_total}


def main():
    print('=' * 100)
    print('v81 (ext_ma20) vs v80.10c (ext_current) — OLD vs NEW simulator 직접 비교')
    print(f'N_SEEDS={N_SEEDS} × {SAMPLES} = {N_SEEDS*SAMPLES} sim/조합')
    print('=' * 100)

    # 시작일 풀 (DB_CURRENT 기준 — 양쪽 DB 같은 dates 가정)
    dates_cur, _, _ = load_all(DB_CURRENT)
    eligible = dates_cur[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    combos = [
        ('v80.10c (ext_current)', DB_CURRENT),
        ('v81 (ext_ma20)',         DB_MA20),
    ]
    results = {}
    for label, db in combos:
        for mode in ['OLD', 'NEW']:
            key = f'{label} [{mode}]'
            res = run(db, mode, seed_starts)
            results[key] = res
            avg = sum(res['rets'])/len(res['rets'])
            mdd = min(res['mdds'])
            print(f'  {key:<35} avg={avg:+7.2f}% mdd={mdd:+6.2f}% '
                  f'miss_total={res["miss_total"]}')

    # OLD에서: v81 vs v80.10c paired
    print()
    print('=' * 100)
    print('OLD simulator: v81 vs v80.10c paired')
    print('=' * 100)
    base = results['v80.10c (ext_current) [OLD]']['seed_avgs']
    new_ = results['v81 (ext_ma20) [OLD]']['seed_avgs']
    lifts = [b - a for a, b in zip(base, new_)]
    wins = sum(1 for l in lifts if l > 0)
    print(f'  lift avg={sum(lifts)/len(lifts):+7.2f}%p  v81 wins {wins}/{N_SEEDS}  '
          f'min={min(lifts):+7.2f}%p max={max(lifts):+7.2f}%p')

    # NEW에서: v81 vs v80.10c paired
    print()
    print('=' * 100)
    print('NEW simulator: v81 vs v80.10c paired')
    print('=' * 100)
    base = results['v80.10c (ext_current) [NEW]']['seed_avgs']
    new_ = results['v81 (ext_ma20) [NEW]']['seed_avgs']
    lifts = [b - a for a, b in zip(base, new_)]
    wins = sum(1 for l in lifts if l > 0)
    print(f'  lift avg={sum(lifts)/len(lifts):+7.2f}%p  v81 wins {wins}/{N_SEEDS}  '
          f'min={min(lifts):+7.2f}%p max={max(lifts):+7.2f}%p')

    # simulator 효과: 같은 DB에서 OLD vs NEW
    print()
    print('=' * 100)
    print('Simulator 효과 — 같은 DB 내부 OLD vs NEW (NEW-OLD)')
    print('=' * 100)
    for label, _ in combos:
        old = results[f'{label} [OLD]']['seed_avgs']
        new = results[f'{label} [NEW]']['seed_avgs']
        diffs = [n - o for o, n in zip(old, new)]
        wins = sum(1 for d in diffs if d > 0)
        print(f'  {label:<30} avg={sum(diffs)/len(diffs):+6.2f}%p  NEW>OLD {wins}/{N_SEEDS}')


if __name__ == '__main__':
    main()
