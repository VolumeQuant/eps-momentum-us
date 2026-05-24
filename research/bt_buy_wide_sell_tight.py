"""매수 관대 + 매도 엄격 (buy wide, sell tight) BT

사용자 제안: entry_top > exit_top
  - 매수: Top N까지 후보 (관대)
  - 매도: Top M 밖 즉시 매도 (엄격, N > M)

기존 grid는 entry ≤ exit만 봤음. 새 조합 탐색:
  (3, 1, 3): 매수 Top 3, 매도 Top 1 밖 — 가장 엄격
  (3, 2, 3): 매수 Top 3, 매도 Top 2 밖
  (5, 3, 3): 매수 Top 5, 매도 Top 3 밖
  (5, 5, 3): 대칭
  (5, 8, 3): 매수 Top 5, 매도 Top 8 — 관대
  (10, 5, 3): 매수 Top 10, 매도 Top 5
  (3, 10, 3): production (baseline)

500 seed × 5 samples = 2500 sim/조합
"""
import sys
import random
import statistics
import time
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import bt_breakout_hold as bth

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'ma_filter_dbs'
DB_MA20 = GRID / 'ext_ma20.db'

N_SEEDS = 500
SAMPLES = 5
MIN_HOLD_DAYS = 10


def run(entry, exit_, slots, seed_starts):
    bth.DB_PATH = DB_MA20
    dates, data, price_series = bth.load_data_ext()
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        seed_rets = []
        for sd in chosen:
            r = bth.simulate_hold(
                dates, data, price_series, hold_days=0,
                entry_top=entry, exit_top=exit_,
                max_slots=slots, start_date=sd
            )
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            seed_rets.append(r['total_return'])
        seed_avgs.append(sum(seed_rets) / len(seed_rets))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


CANDIDATES = [
    # (entry, exit, slots, label, category)
    (3, 1, 3, '매수3+매도1 (가장 엄격)', 'asym'),
    (3, 2, 3, '매수3+매도2', 'asym'),
    (3, 3, 3, '매수3+매도3 (대칭)', 'sym'),
    (3, 5, 3, '매수3+매도5', 'sym'),
    (3, 10, 3, 'production (baseline)', 'baseline'),
    (5, 3, 3, '매수5+매도3 (사용자 직관)', 'asym'),
    (5, 5, 3, '매수5+매도5 (대칭)', 'sym'),
    (5, 8, 3, '매수5+매도8', 'sym'),
    (5, 10, 3, '매수5+매도10 (관대)', 'sym'),
    (10, 5, 3, '매수10+매도5', 'asym'),
    (10, 3, 3, '매수10+매도3 (극단)', 'asym'),
]


def main():
    print('=' * 100)
    print('매수 관대 + 매도 엄격 (buy wide, sell tight) BT')
    print(f'{N_SEEDS} seed × {SAMPLES} = {N_SEEDS*SAMPLES} sim/조합')
    print('=' * 100)

    bth.DB_PATH = DB_MA20
    dates, _, _ = bth.load_data_ext()
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    print()
    print(f'{"params":<28} {"avg":>9} {"mdd":>8} {"ra":>7} {"sharpe":>7} {"std":>6}')
    print('-' * 80)

    results = {}
    for entry, exit_, slots, label, cat in CANDIDATES:
        spec = f'({entry},{exit_},{slots})'
        t0 = time.time()
        res = run(entry, exit_, slots, seed_starts)
        avg = sum(res['rets'])/len(res['rets'])
        mdd = min(res['mdds'])
        ra = avg/abs(mdd) if mdd<0 else 0
        std = statistics.pstdev(res['rets'])
        sharpe = avg/std if std>0 else 0
        results[spec] = res
        marker = '★' if cat=='baseline' else ('▲' if cat=='asym' else '  ')
        print(f'  {marker} {spec:<10} {label:<22} {avg:+8.2f}% {mdd:+7.2f}% {ra:+6.2f} {sharpe:+6.2f} {std:>5.1f} [{time.time()-t0:.1f}s]')

    # paired vs baseline (3,10,3)
    print()
    print('=' * 100)
    print('paired vs (3,10,3) production baseline')
    print('=' * 100)
    base = results['(3,10,3)']['seed_avgs']
    print(f'{"params":<28} {"lift":>10} {"min lift":>10} {"max lift":>10} {"wins":>10}')
    print('-' * 80)
    for entry, exit_, slots, label, cat in CANDIDATES:
        spec = f'({entry},{exit_},{slots})'
        if spec == '(3,10,3)':
            continue
        new = results[spec]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts) / len(lifts)
        verdict = '✓' if wins >= N_SEEDS * 0.9 else '◐' if wins >= N_SEEDS * 0.6 else '✗'
        print(f'  {spec:<12} {label:<24} {avg_l:+9.2f}%p {min(lifts):+9.2f}%p {max(lifts):+9.2f}%p {wins}/{N_SEEDS} [{verdict}]')


if __name__ == '__main__':
    main()
