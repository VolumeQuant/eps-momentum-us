"""DCA (분할매수) 전략 BT — 초기 매수 % + 하락 시 추매

설계:
  - 종목당 할당 = 33.33% (3슬롯 균등 capital)
  - 진입 시 initial_pct만 매수, 나머지는 cash
  - 진입가 대비 -X% 도달 시 add_pct 추가 매수 (한 트리거당 한 번만)
  - 매도 조건: 기존 production 룰 (rank > 10, min_seg < -2)
  - 미배치 capital은 cash (0% return, NAV에 보존)

NEW simulator (price_full fallback + n_valid 분모).
production DB (eps_momentum_data.db) — 5/22까지.
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


def simulate_dca(dates_all, data, price_full, strategy, start_date=None,
                 entry=3, exit_=10, slots=N_SLOTS):
    """DCA simulator.
    strategy: list of (drop_threshold, add_pct) tuples.
              First entry (drop=0) is initial buy.
              E.g., [(-0.0, 50), (-0.05, 25), (-0.10, 25)]
              = 진입 50%, -5%일 때 25% 추가, -10%일 때 25% 추가
    Total of add_pct must be 100.
    """
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    # 종목당 capital = 1.0 / slots (즉 portfolio NAV 1.0 중 종목당 1/slots)
    PER_SLOT = 1.0 / slots

    # portfolio: {ticker: {entry_price, entry_di, triggered: set of trigger indices,
    #                      total_pct (0~100 deployed), avg_cost, cost_per_pct}}
    portfolio = {}
    consecutive = defaultdict(int)

    # NAV-based tracking
    nav = 1.0  # 시작 NAV
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

        # === Day return 계산 ===
        # 종목별 contribution = deployed_pct/100 × PER_SLOT × (today/prev - 1)
        # cash 부분 = 0% return
        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di-1]
            for tk, info in portfolio.items():
                cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                prev_p = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if cur_p and prev_p and prev_p > 0:
                    day_ret_tk = (cur_p - prev_p) / prev_p
                    # deployed weight × per-slot weight
                    weight = (info['total_pct'] / 100.0) * PER_SLOT
                    day_ret += weight * day_ret_tk * 100
        daily_returns.append(day_ret)

        # === DCA trigger check (existing positions) ===
        for tk, info in list(portfolio.items()):
            cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            if cur_p is None:
                continue
            drop_from_entry = (cur_p - info['entry_price']) / info['entry_price']
            # 트리거 순회
            for i, (drop_thr, add_pct) in enumerate(strategy):
                if i in info['triggered']:
                    continue
                if i == 0:
                    # initial buy already done at entry
                    continue
                if drop_from_entry <= drop_thr:
                    # add!
                    if info['total_pct'] + add_pct > 100:
                        add_pct = 100 - info['total_pct']
                        if add_pct <= 0:
                            info['triggered'].add(i)
                            continue
                    # update avg cost (weighted)
                    new_total = info['total_pct'] + add_pct
                    info['avg_cost'] = (info['avg_cost'] * info['total_pct'] + cur_p * add_pct) / new_total
                    info['total_pct'] = new_total
                    info['triggered'].add(i)

        # === Exit check ===
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

        # === Entry check ===
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
                    initial_pct = strategy[0][1]
                    portfolio[tk] = {
                        'entry_price': price,
                        'entry_di': di,
                        'triggered': {0},  # initial buy done
                        'total_pct': initial_pct,
                        'avg_cost': price,
                    }
                    vacancies -= 1

    # NAV 계산
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
            r = simulate_dca(dates, data, price_full, strategy, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 100)
    print(f'DCA 전략 grid search BT — NEW simulator + production DB')
    print(f'N_SEEDS={N_SEEDS} × {SAMPLES} = {N_SEEDS*SAMPLES} sim/strategy')
    print('=' * 100)

    dates, data, price_full = load_all(DB_PATH)
    print(f'[Load] {len(dates)} 거래일 ({dates[0]} ~ {dates[-1]})')

    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    # Strategy definitions: [(drop_threshold, add_pct)...]
    # drop_threshold는 음수 (진입가 대비), 첫 entry는 0
    strategies = {
        '100_0 (baseline)':         [(0.0, 100)],
        '80_20 at -5%':             [(0.0, 80), (-0.05, 20)],
        '70_30 at -5%':             [(0.0, 70), (-0.05, 30)],
        '60_40 at -5%':             [(0.0, 60), (-0.05, 40)],
        '50_50 at -3%':             [(0.0, 50), (-0.03, 50)],
        '50_50 at -5%':             [(0.0, 50), (-0.05, 50)],
        '50_50 at -8%':             [(0.0, 50), (-0.08, 50)],
        '50_25_25 at -5/-10%':      [(0.0, 50), (-0.05, 25), (-0.10, 25)],
        '60_20_20 at -5/-10%':      [(0.0, 60), (-0.05, 20), (-0.10, 20)],
        '40_30_30 at -5/-10%':      [(0.0, 40), (-0.05, 30), (-0.10, 30)],
        '50_15_15_15_5 -3/-6/-10/-15': [(0.0, 50), (-0.03, 15), (-0.06, 15), (-0.10, 15), (-0.15, 5)],
        '33_33_34 at -5/-10%':      [(0.0, 33), (-0.05, 33), (-0.10, 34)],
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
        marker = '★' if 'baseline' in name else ' '
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
