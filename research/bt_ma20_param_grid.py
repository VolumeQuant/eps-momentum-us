"""MA20 환경에서 최적 (entry, exit, slots) 파라미터 탐색

이전 robust BT는 "MA20 lift가 모든 파라미터에서 보존되나"를 확인.
이 BT는 "MA20 환경에서 절대 수익률/MDD/Sharpe가 최적인 조합은?"을 탐색.

Grid:
  entry: 2, 3, 4, 5
  exit:  8, 10, 12, 15
  slots: 2, 3, 4
  → 4 × 4 × 3 = 48 조합

100 seed × 3 starts paired (12000 sim/조합)

평가:
  - avg 수익률 (높을수록 좋음)
  - MDD (절대값 작을수록 좋음)
  - risk_adj = avg / |MDD| (높을수록 좋음)
  - sharpe (rets std로 normalize)
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

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10

DB_MA20 = GRID / 'ext_ma20.db'

ENTRIES = [2, 3, 4, 5]
EXITS = [8, 10, 12, 15]
SLOTS = [2, 3, 4]


def run(entry, exit_, slots, seed_starts):
    bth.DB_PATH = DB_MA20
    dates, data, price_series = bth.load_data_ext()
    rets, mdds = [], []
    for chosen in seed_starts:
        for sd in chosen:
            r = bth.simulate_hold(
                dates, data, price_series, hold_days=0,
                entry_top=entry, exit_top=exit_,
                max_slots=slots, start_date=sd
            )
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
    return rets, mdds


def main():
    print('=' * 110)
    print('MA20 환경 (entry, exit, slots) grid 최적화')
    print(f'seeds: {N_SEEDS} × {SAMPLES_PER_SEED} = {N_SEEDS*SAMPLES_PER_SEED} 시뮬/조합')
    print(f'grid: entry {ENTRIES} × exit {EXITS} × slots {SLOTS} = {len(ENTRIES)*len(EXITS)*len(SLOTS)} 조합')
    print('=' * 110)

    # seed_starts 공통 사용
    bth.DB_PATH = DB_MA20
    dates, _, _ = bth.load_data_ext()
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    print()
    print(f'{"params":<14} {"avg":>9} {"median":>9} {"std":>6} {"min":>9} {"max":>9} '
          f'{"MDD":>8} {"risk_adj":>9} {"sharpe":>7}')
    print('-' * 100)

    results = []
    n_done = 0
    n_total = len(ENTRIES) * len(EXITS) * len(SLOTS)
    t0 = time.time()

    for entry in ENTRIES:
        for exit_ in EXITS:
            for slots in SLOTS:
                if exit_ < entry:  # exit ≥ entry 보장
                    continue
                rets, mdds = run(entry, exit_, slots, seed_starts)
                rets_s = sorted(rets)
                n = len(rets)
                avg = sum(rets) / n
                med = rets_s[n // 2]
                std = statistics.pstdev(rets)
                mdd = min(mdds)
                ra = avg / abs(mdd) if mdd < 0 else 0
                sharpe = avg / std if std > 0 else 0
                spec = f'({entry},{exit_},{slots})'
                results.append({
                    'spec': spec, 'entry': entry, 'exit': exit_, 'slots': slots,
                    'avg': avg, 'med': med, 'std': std, 'min': min(rets),
                    'max': max(rets), 'mdd': mdd, 'ra': ra, 'sharpe': sharpe,
                })
                marker = ' ★' if spec == '(3,10,3)' else '  '
                print(f'{marker}{spec:<12} {avg:+8.2f}% {med:+8.2f}% {std:>5.2f} '
                      f'{min(rets):+8.2f}% {max(rets):+8.2f}% '
                      f'{mdd:+7.2f}% {ra:+8.2f} {sharpe:+6.2f}')
                n_done += 1

    print()
    print(f'총 소요: {time.time()-t0:.1f}s')

    # Top 5 by 각 metric
    print()
    print('=' * 110)
    print('Top 5 — avg 수익률 기준')
    print('=' * 110)
    for r in sorted(results, key=lambda x: x['avg'], reverse=True)[:5]:
        print(f'  {r["spec"]:<10} avg={r["avg"]:+7.2f}% mdd={r["mdd"]:+7.2f}% '
              f'ra={r["ra"]:+6.2f} sharpe={r["sharpe"]:+5.2f}')

    print()
    print('Top 5 — risk_adj 기준 (avg / |MDD|)')
    print('=' * 110)
    for r in sorted(results, key=lambda x: x['ra'], reverse=True)[:5]:
        print(f'  {r["spec"]:<10} avg={r["avg"]:+7.2f}% mdd={r["mdd"]:+7.2f}% '
              f'ra={r["ra"]:+6.2f} sharpe={r["sharpe"]:+5.2f}')

    print()
    print('Top 5 — Sharpe 기준')
    print('=' * 110)
    for r in sorted(results, key=lambda x: x['sharpe'], reverse=True)[:5]:
        print(f'  {r["spec"]:<10} avg={r["avg"]:+7.2f}% mdd={r["mdd"]:+7.2f}% '
              f'ra={r["ra"]:+6.2f} sharpe={r["sharpe"]:+5.2f}')

    # production (3,10,3) 위치
    print()
    print('=' * 110)
    print('Production (3,10,3) — v81 채택 후보')
    print('=' * 110)
    cur = next(r for r in results if r['spec'] == '(3,10,3)')
    rank_avg = sum(1 for r in results if r['avg'] > cur['avg']) + 1
    rank_ra = sum(1 for r in results if r['ra'] > cur['ra']) + 1
    rank_sharpe = sum(1 for r in results if r['sharpe'] > cur['sharpe']) + 1
    print(f'  (3,10,3): avg={cur["avg"]:+.2f}% (순위 {rank_avg}/{len(results)}) '
          f'ra={cur["ra"]:+.2f} (순위 {rank_ra}) sharpe={cur["sharpe"]:+.2f} (순위 {rank_sharpe})')


if __name__ == '__main__':
    main()
