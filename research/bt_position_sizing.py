"""A: Position sizing — rank-weighted BT

현재: 1, 2, 3위 모두 33.33% 균등 (v71 채택, 메모리에 +19.7% > 역변동성 +10.9% 검증)
검증: rank-weighted (1위에 더 비중) 비교 — v80.10c 환경 재검증

변형:
  A0: 33/33/34 baseline (균등, 현재)
  A1: 40/33/27 (1위 약간 강화)
  A2: 50/30/20 (1위 강화)
  A3: 60/25/15 (1위 매우 강화)
  A4: 50/35/15 (3위 약화)
  A5: 45/35/20 (균형)
  A6: rank score-weighted (w_gap 비율로 분배)
  A7: 50/50/0 (3위 제거, 2슬롯 균등)
  A8: 70/30 (2슬롯 차등)

w_gap 점수 정보 활용 가능 (이미 simulator에 score_display_map 있음).
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


def simulate(dates_all, data, price_full, weights, start_date=None,
             entry=3, exit_=10):
    """
    weights: list of allocation % per slot rank (e.g., [50, 30, 20] for 1st, 2nd, 3rd)
             합이 100이어야 함. 길이가 슬롯 수 정함.
    """
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    slots = len(weights)
    portfolio = {}  # {ticker: {entry_price, entry_di, slot_idx (rank position 0/1/2)}}
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

        # Day return — 보유 시점의 slot weight 적용
        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di-1]
            for tk, info in portfolio.items():
                cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                prev_p = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if cur_p and prev_p and prev_p > 0:
                    day_ret_tk = (cur_p - prev_p) / prev_p
                    w = weights[info['slot_idx']] / 100.0
                    day_ret += w * day_ret_tk * 100
        daily_returns.append(day_ret)

        # Exit
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2: exited.append(tk); continue
            if rank is None or rank > exit_: exited.append(tk); continue
        for tk in exited: del portfolio[tk]

        # Entry — slot_idx는 진입 시점의 part2_rank 순위에 따라 결정
        # 진입 가능한 종목들을 rank로 정렬 후, 빈 슬롯에 순서대로 배치
        if len(portfolio) < slots:
            # 현재 슬롯 점유 인덱스
            used_idx = {info['slot_idx'] for info in portfolio.values()}
            free_idx = [i for i in range(slots) if i not in used_idx]
            # 진입 후보 (rank ≤ entry, ✅, min_seg ≥ 0)
            candidates = []
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > entry: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                min_seg = today_data.get(tk, {}).get('min_seg', 0)
                if min_seg < 0: continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0:
                    candidates.append((tk, price))
            # 빈 슬롯에 순서대로 배치 (낮은 idx부터 = 1위가 가장 큰 비중)
            for slot_idx in free_idx:
                if not candidates: break
                tk, price = candidates.pop(0)
                portfolio[tk] = {
                    'entry_price': price,
                    'entry_di': di,
                    'slot_idx': slot_idx,
                }

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run(weights, dates, data, price_full, seed_starts):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, weights, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 100)
    print(f'A: Position Sizing BT — rank-weighted vs 균등')
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
        'A0: 33/33/34 (baseline)':   [33, 33, 34],
        'A1: 40/33/27':              [40, 33, 27],
        'A2: 50/30/20':              [50, 30, 20],
        'A3: 60/25/15':              [60, 25, 15],
        'A4: 50/35/15':              [50, 35, 15],
        'A5: 45/35/20':              [45, 35, 20],
        'A6: 40/35/25':              [40, 35, 25],
        'A7: 50/50 (2슬롯 균등)':    [50, 50],
        'A8: 70/30 (2슬롯 차등)':    [70, 30],
        'A9: 60/40 (2슬롯)':         [60, 40],
        'A10: 25/25/25/25 (4슬롯)':  [25, 25, 25, 25],
        'A11: 40/30/20/10 (4슬롯)':  [40, 30, 20, 10],
    }

    print()
    print(f'{"strategy":<35} {"avg":>9} {"med":>9} {"worst MDD":>10} {"sharpe":>7}')
    print('-' * 80)
    results = {}
    for name, weights in strategies.items():
        res = run(weights, dates, data, price_full, seed_starts)
        results[name] = res
        avg = sum(res['rets'])/len(res['rets'])
        med = sorted(res['rets'])[len(res['rets'])//2]
        worst_mdd = min(res['mdds'])
        std = statistics.pstdev(res['rets'])
        sharpe = avg/std if std > 0 else 0
        marker = '★' if 'baseline' in name else '  '
        print(f'{marker} {name:<33} {avg:+8.2f}% {med:+8.2f}% {worst_mdd:+9.2f}% {sharpe:+6.2f}')

    # Paired vs baseline
    print()
    print('=' * 100)
    print('paired vs baseline (33/33/34)')
    print('=' * 100)
    print(f'{"strategy":<35} {"avg lift":>10} {"worst":>10} {"best":>10} {"wins":>10} {"verdict":>15}')
    print('-' * 95)
    base = results['A0: 33/33/34 (baseline)']['seed_avgs']
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
