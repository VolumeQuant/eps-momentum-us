"""Pyramiding Up + Time-based DCA BT

옵션 A: 상승 시 추매 (Pyramiding up)
  - 진입 후 +X% 오르면 add_pct 추가
  - 예: 50% 매수, +5% 오르면 50% 추매

옵션 B: 시간 분산 (Time-based DCA)
  - 진입 후 N일 경과 시 add_pct 추가
  - 예: 신호일 50% + 다음날 50%

통합 Strategy 형식: [(trigger_type, value, add_pct)]
  trigger_type = 'init': 진입일 매수 (value=0)
  trigger_type = 'price_rise': 진입가 대비 +value 도달 시 매수 (value > 0)
  trigger_type = 'day': 진입 후 value 거래일 경과 시 매수 (value >= 1)

NEW simulator + production rules + 65일 production DB.
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
N_SLOTS = 3


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
            data[d][tk] = {'p2': r[1], 'price': r[2], 'min_seg': min(segs) if segs else 0}
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def simulate_strategy(dates_all, data, price_full, strategy, start_date=None,
                       entry=3, exit_=10, slots=N_SLOTS):
    """통합 simulator.
    strategy: list of (trigger_type, value, add_pct)
      ('init', 0, X): 진입일 X% 매수
      ('price_rise', V, X): 진입가 대비 +V% 도달 시 X% 매수
      ('day', N, X): 진입 후 N거래일 경과 시 X% 매수
    """
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    PER_SLOT = 1.0 / slots
    portfolio = {}
    consecutive = defaultdict(int)
    daily_returns = []

    if start_date:
        for d in dates_all:
            if d >= start_date:
                break
            new_c = defaultdict(int)
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    new_c[tk] = consecutive.get(tk, 0) + 1
            consecutive = new_c

    for di, today in enumerate(dates):
        if today not in data:
            continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consec = defaultdict(int)
        for tk in rank_map:
            new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        # Day return
        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di-1]
            for tk, info in portfolio.items():
                cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                prev_p = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if cur_p and prev_p and prev_p > 0:
                    day_ret_tk = (cur_p - prev_p) / prev_p
                    weight = (info['total_pct'] / 100.0) * PER_SLOT
                    day_ret += weight * day_ret_tk * 100
        daily_returns.append(day_ret)

        # Trigger check (existing positions)
        for tk, info in list(portfolio.items()):
            cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            if cur_p is None:
                continue
            days_held = di - info['entry_di']
            for i, (ttype, val, add_pct) in enumerate(strategy):
                if i in info['triggered']:
                    continue
                if ttype == 'init':
                    continue
                fired = False
                if ttype == 'price_rise':
                    rise = (cur_p - info['entry_price']) / info['entry_price']
                    if rise >= val:
                        fired = True
                elif ttype == 'price_drop':
                    drop = (cur_p - info['entry_price']) / info['entry_price']
                    if drop <= val:
                        fired = True
                elif ttype == 'day':
                    if days_held >= val:
                        fired = True
                if fired:
                    actual_add = min(add_pct, 100 - info['total_pct'])
                    if actual_add <= 0:
                        info['triggered'].add(i); continue
                    new_total = info['total_pct'] + actual_add
                    info['avg_cost'] = (info['avg_cost'] * info['total_pct'] + cur_p * actual_add) / new_total
                    info['total_pct'] = new_total
                    info['triggered'].add(i)

        # Exit
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2: exited.append(tk); continue
            if rank is None or rank > exit_: exited.append(tk); continue
        for tk in exited: del portfolio[tk]

        # Entry
        vacancies = slots - len(portfolio)
        if vacancies > 0:
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > entry or vacancies <= 0: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                min_seg = today_data.get(tk, {}).get('min_seg', 0)
                if min_seg < 0: continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0:
                    initial_pct = strategy[0][2]
                    portfolio[tk] = {
                        'entry_price': price,
                        'entry_di': di,
                        'triggered': {0},
                        'total_pct': initial_pct,
                        'avg_cost': price,
                    }
                    vacancies -= 1

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run(strategy, dates, data, price_full, seed_starts):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate_strategy(dates, data, price_full, strategy, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 100)
    print(f'Pyramiding Up + Time-based DCA BT')
    print(f'N_SEEDS={N_SEEDS} × {SAMPLES} = {N_SEEDS*SAMPLES} sim/strategy')
    print('=' * 100)

    dates, data, price_full = load_all(DB_PATH)
    print(f'[Load] {len(dates)} 거래일 ({dates[0]} ~ {dates[-1]})')

    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    strategies = {
        # baseline
        '100_0 (baseline)':       [('init', 0, 100)],

        # === Option A: Pyramiding Up ===
        'A1: 80_20 at +5%':       [('init', 0, 80), ('price_rise', 0.05, 20)],
        'A2: 70_30 at +5%':       [('init', 0, 70), ('price_rise', 0.05, 30)],
        'A3: 60_40 at +5%':       [('init', 0, 60), ('price_rise', 0.05, 40)],
        'A4: 50_50 at +3%':       [('init', 0, 50), ('price_rise', 0.03, 50)],
        'A5: 50_50 at +5%':       [('init', 0, 50), ('price_rise', 0.05, 50)],
        'A6: 50_50 at +10%':      [('init', 0, 50), ('price_rise', 0.10, 50)],
        'A7: 50_25_25 at +5/+10%': [('init', 0, 50), ('price_rise', 0.05, 25), ('price_rise', 0.10, 25)],
        'A8: 40_30_30 at +5/+10%': [('init', 0, 40), ('price_rise', 0.05, 30), ('price_rise', 0.10, 30)],

        # === Option B: Time-based ===
        'B1: 50_50 day 0/1':      [('init', 0, 50), ('day', 1, 50)],
        'B2: 50_50 day 0/2':      [('init', 0, 50), ('day', 2, 50)],
        'B3: 33_33_34 day 0/1/2': [('init', 0, 33), ('day', 1, 33), ('day', 2, 34)],
        'B4: 50_25_25 day 0/1/2': [('init', 0, 50), ('day', 1, 25), ('day', 2, 25)],
        'B5: 70_30 day 0/1':      [('init', 0, 70), ('day', 1, 30)],
        'B6: 50_50 day 0/3':      [('init', 0, 50), ('day', 3, 50)],
    }

    print()
    print(f'{"strategy":<35} {"avg":>9} {"med":>9} {"worst MDD":>10} {"sharpe":>7}')
    print('-' * 80)
    results = {}
    for name, strat in strategies.items():
        res = run(strat, dates, data, price_full, seed_starts)
        results[name] = res
        avg = sum(res['rets'])/len(res['rets'])
        med = sorted(res['rets'])[len(res['rets'])//2]
        worst_mdd = min(res['mdds'])
        std = statistics.pstdev(res['rets'])
        sharpe = avg/std if std > 0 else 0
        marker = '★' if 'baseline' in name else (' A' if name.startswith('A') else ' B' if name.startswith('B') else ' ')
        print(f'{marker} {name:<33} {avg:+8.2f}% {med:+8.2f}% {worst_mdd:+9.2f}% {sharpe:+6.2f}')

    # Paired vs baseline
    print()
    print('=' * 100)
    print('paired vs 100_0 baseline')
    print('=' * 100)
    print(f'{"strategy":<35} {"avg lift":>10} {"worst":>10} {"best":>10} {"wins":>10} {"verdict":>15}')
    print('-' * 95)
    base = results['100_0 (baseline)']['seed_avgs']
    for name in strategies:
        if 'baseline' in name:
            continue
        new_ = results[name]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new_)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts)/len(lifts)
        worst = min(lifts); best = max(lifts)
        rate = wins / N_SEEDS * 100
        verdict = '✓✓ 우월' if rate >= 90 else '✓ 우월' if rate >= 75 else '◐ 약함' if rate >= 60 else '~ 동등' if rate >= 40 else '✗ 열세'
        print(f'  {name:<33} {avg_l:+8.2f}%p {worst:+8.2f}%p {best:+8.2f}%p {wins:>5}/{N_SEEDS}  {verdict}')


if __name__ == '__main__':
    main()
