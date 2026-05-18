"""MA20 환경 Top 후보들 paired 비교

(3,10,3) production 대비:
  (2,10,2) — grid best
  (2,12,2) — close 2nd
  (2,10,3) — slots만 유지, entry만 2
  (3,8,3)  — Sharpe best
  (3,10,2) — slots만 2
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

CANDIDATES = [
    (3, 10, 3, 'production'),
    (2, 10, 2, 'grid_best'),
    (2, 12, 2, 'close_2nd'),
    (2, 10, 3, 'entry2_only'),
    (3, 10, 2, 'slots2_only'),
    (3, 8, 3, 'sharpe_best'),
]


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


def main():
    print('=' * 100)
    print(f'MA20 환경 Top 후보 paired ({N_SEEDS} seed × {SAMPLES} samples = {N_SEEDS*SAMPLES} sim)')
    print('=' * 100)

    bth.DB_PATH = DB_MA20
    dates, _, _ = bth.load_data_ext()
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    all_results = {}
    for entry, exit_, slots, label in CANDIDATES:
        spec = f'({entry},{exit_},{slots})'
        t0 = time.time()
        res = run(entry, exit_, slots, seed_starts)
        avg = sum(res['rets']) / len(res['rets'])
        med = sorted(res['rets'])[len(res['rets']) // 2]
        mdd = min(res['mdds'])
        std = statistics.pstdev(res['rets'])
        ra = avg / abs(mdd) if mdd < 0 else 0
        sharpe = avg / std if std > 0 else 0
        all_results[spec] = res
        print(f'  [{time.time()-t0:>5.1f}s] {spec:<10} {label:<14} '
              f'avg={avg:+6.2f}% med={med:+6.2f}% mdd={mdd:+6.2f}% '
              f'ra={ra:+5.2f} sharpe={sharpe:+.2f}')

    # paired vs production
    print()
    print('=' * 100)
    print('vs (3,10,3) production paired comparison')
    print('=' * 100)
    base = all_results['(3,10,3)']['seed_avgs']
    print(f'{"variant":<14} {"avg lift":>10} {"min lift":>10} {"max lift":>10} '
          f'{"wins":>10} {"losses":>10}')
    print('-' * 80)
    for entry, exit_, slots, label in CANDIDATES:
        spec = f'({entry},{exit_},{slots})'
        if spec == '(3,10,3)':
            continue
        new = all_results[spec]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        losses = sum(1 for l in lifts if l < 0)
        avg_l = sum(lifts) / len(lifts)
        verdict = '✓ 우월' if wins >= N_SEEDS * 0.95 else '✓ 약우월' if wins >= N_SEEDS * 0.75 else '~ 동등'
        print(f'  {spec:<12} {avg_l:+9.2f}%p {min(lifts):+9.2f}%p {max(lifts):+9.2f}%p '
              f'{wins:>5}/{N_SEEDS} {losses:>5}/{N_SEEDS}  {verdict}')


if __name__ == '__main__':
    main()
