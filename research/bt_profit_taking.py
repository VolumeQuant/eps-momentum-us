"""Profit Taking (부분 익절) BT

설계:
  - 종목 매수 후 +X% 도달 시 부분/전체 매도
  - 매도된 자금은 cash (slot 유지, 종목은 portfolio에 남음)
  - rank > 10 / min_seg < -2 매도 룰은 그대로

변형:
  C0: baseline (익절 X, 현재 production)
  C1: +30% 시 50% 매도 (절반 락인)
  C2: +50% 시 50% 매도
  C3: +30% 시 50% + +60% 시 다시 50% (남은 절반의)
  C4: +20% 시 30% + +50% 시 30% + +100% 시 40% (분할)
  C5: +30% 시 100% 매도 (전부 익절)
  C6: +50% 시 100% 매도
  C7: +100% 시 50% 매도 (MU 같은 슈퍼위너만)

NEW simulator + production rules + 5/22 production DB.
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


def simulate(dates_all, data, price_full, profit_rules, start_date=None,
             entry=3, exit_=10, slots=N_SLOTS):
    """
    profit_rules: list of (gain_threshold, sell_pct) — 진입가 대비 +gain% 시 sell_pct% 매도
                  예: [(0.30, 50), (0.60, 50)] = +30% 시 50%, +60% 시 남은 50%
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

        # Profit-take trigger check
        for tk, info in list(portfolio.items()):
            cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            if cur_p is None:
                continue
            gain_from_entry = (cur_p - info['entry_price']) / info['entry_price']
            for i, (gain_thr, sell_pct) in enumerate(profit_rules):
                if i in info['pt_triggered']:
                    continue
                if gain_from_entry >= gain_thr:
                    # 부분 매도 — total_pct에서 sell_pct만큼 차감
                    actual_sell = min(sell_pct, info['total_pct'])
                    info['total_pct'] -= actual_sell
                    info['pt_triggered'].add(i)
                    if info['total_pct'] <= 0:
                        info['total_pct'] = 0  # 완전 매도

        # Exit (rank/min_seg)
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2: exited.append(tk); continue
            if rank is None or rank > exit_: exited.append(tk); continue
            # 또한 total_pct = 0이면 자동 제거
            if portfolio[tk]['total_pct'] <= 0:
                exited.append(tk); continue
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
                    portfolio[tk] = {
                        'entry_price': price,
                        'entry_di': di,
                        'total_pct': 100,  # 전체 매수
                        'avg_cost': price,
                        'pt_triggered': set(),
                    }
                    vacancies -= 1

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run(rules, dates, data, price_full, seed_starts):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, rules, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 100)
    print(f'C: Profit Taking BT')
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
        'C0: baseline (익절 X)':       [],
        'C1: +30% 시 50% 매도':         [(0.30, 50)],
        'C2: +50% 시 50% 매도':         [(0.50, 50)],
        'C3: +30/+60% 각 50%':          [(0.30, 50), (0.60, 50)],
        'C4: +20/+50/+100% 30/30/40':   [(0.20, 30), (0.50, 30), (1.00, 40)],
        'C5: +30% 시 100% 매도':        [(0.30, 100)],
        'C6: +50% 시 100% 매도':        [(0.50, 100)],
        'C7: +100% 시 50% 매도':        [(1.00, 50)],
        'C8: +20% 시 25% 매도':         [(0.20, 25)],
        'C9: +50% 시 30% 매도':         [(0.50, 30)],
    }

    print()
    print(f'{"strategy":<35} {"avg":>9} {"med":>9} {"worst MDD":>10} {"avg MDD":>9} {"sharpe":>7}')
    print('-' * 90)
    results = {}
    for name, rules in strategies.items():
        res = run(rules, dates, data, price_full, seed_starts)
        results[name] = res
        avg = sum(res['rets'])/len(res['rets'])
        med = sorted(res['rets'])[len(res['rets'])//2]
        worst_mdd = min(res['mdds'])
        avg_mdd = sum(res['mdds'])/len(res['mdds'])
        std = statistics.pstdev(res['rets'])
        sharpe = avg/std if std > 0 else 0
        marker = '★' if 'baseline' in name else '  '
        print(f'{marker} {name:<33} {avg:+8.2f}% {med:+8.2f}% {worst_mdd:+9.2f}% {avg_mdd:+8.2f}% {sharpe:+6.2f}')

    # Paired vs baseline
    print()
    print('=' * 100)
    print('paired vs baseline')
    print('=' * 100)
    print(f'{"strategy":<35} {"avg lift":>10} {"worst":>10} {"best":>10} {"wins":>10} {"verdict":>15}')
    print('-' * 95)
    base = results['C0: baseline (익절 X)']['seed_avgs']
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
