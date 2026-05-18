"""MA20 robustness BT — 다양한 파라미터 조합 + 표본 확장

다양화 차원:
  1. seed × samples: 500 × 5 = 2500 시뮬/조합 (이전 8배)
  2. 파라미터 grid (entry, exit, slots):
     - (3, 10, 3) — production v80.10c (★ 기존 검증)
     - (3, 8, 3)  — v80.10 이전 (tighter exit)
     - (3, 12, 3) — looser exit
     - (5, 10, 3) — looser entry
     - (5, 15, 3) — looser both
     - (3, 10, 5) — more slots

각 파라미터 조합에서 baseline (production MA120+fallback) vs MA20 비교.
모든 조합에서 MA20 lift > 0 + paired wins 압도 → robustness 확정.
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

N_SEEDS = 500
SAMPLES_PER_SEED = 5
MIN_HOLD_DAYS = 10

PARAM_GRID = [
    # (entry, exit, slots, label)
    (3, 10, 3, 'v80.10c production'),
    (3,  8, 3, 'tight exit'),
    (3, 12, 3, 'loose exit'),
    (5, 10, 3, 'loose entry'),
    (5, 15, 3, 'loose both'),
    (3, 10, 5, 'more slots'),
]


def run_grid(db_path, entry_top, exit_top, max_slots, seed_starts):
    bth.DB_PATH = db_path
    dates, data, price_series = bth.load_data_ext()
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        seed_rets = []
        for sd in chosen:
            r = bth.simulate_hold(
                dates, data, price_series, hold_days=0,
                entry_top=entry_top, exit_top=exit_top,
                max_slots=max_slots, start_date=sd
            )
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            seed_rets.append(r['total_return'])
        seed_avgs.append(sum(seed_rets) / len(seed_rets))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 110)
    print(f'MA20 robustness BT — 파라미터 grid + 표본 확장')
    print(f'seeds: {N_SEEDS} × {SAMPLES_PER_SEED} = {N_SEEDS*SAMPLES_PER_SEED} 시뮬/조합')
    print(f'조합: {len(PARAM_GRID)}개')
    print('=' * 110)

    db_base = GRID / 'ext_current.db'
    db_ma20 = GRID / 'ext_ma20.db'

    # seed_starts는 공통 사용 (paired 보장)
    bth.DB_PATH = db_base
    dates, _, _ = bth.load_data_ext()
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    print(f'\n시작일 풀: {len(eligible_starts)}일')

    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    print()
    print(f'{"params":<28} {"base avg":>9} {"ma20 avg":>9} {"lift":>9} '
          f'{"base MDD":>9} {"ma20 MDD":>9} {"wins":>9} {"losses":>9}')
    print('-' * 110)

    all_results = []
    for (entry, exit_, slots, label) in PARAM_GRID:
        t0 = time.time()
        res_b = run_grid(db_base, entry, exit_, slots, seed_starts)
        res_m = run_grid(db_ma20, entry, exit_, slots, seed_starts)

        base_avg = sum(res_b['rets']) / len(res_b['rets'])
        ma20_avg = sum(res_m['rets']) / len(res_m['rets'])
        lift = ma20_avg - base_avg

        base_mdd = min(res_b['mdds'])
        ma20_mdd = min(res_m['mdds'])

        # paired wins
        lifts_paired = [b - a for a, b in zip(res_b['seed_avgs'], res_m['seed_avgs'])]
        wins = sum(1 for l in lifts_paired if l > 0)
        losses = sum(1 for l in lifts_paired if l < 0)

        spec = f'({entry},{exit_},{slots}) {label}'
        verdict_mark = '✓' if wins > losses * 5 else ('=' if abs(wins - losses) <= losses else '✗')
        print(f'  {spec:<26} {base_avg:+8.2f}% {ma20_avg:+8.2f}% '
              f'{lift:+8.2f}%p {base_mdd:+8.2f}% {ma20_mdd:+8.2f}% '
              f'{wins:>5}/{N_SEEDS} {losses:>5}/{N_SEEDS} [{verdict_mark}] '
              f'({time.time()-t0:.1f}s)')
        all_results.append({
            'spec': spec, 'lift': lift, 'wins': wins, 'losses': losses,
            'base_mdd': base_mdd, 'ma20_mdd': ma20_mdd,
        })

    print()
    print('=' * 110)
    print(f'종합 ({N_SEEDS} seed paired comparison)')
    print('=' * 110)
    all_positive = all(r['lift'] > 0 for r in all_results)
    all_wins_dominant = all(r['wins'] > r['losses'] * 5 for r in all_results)
    avg_lift = sum(r['lift'] for r in all_results) / len(all_results)
    min_lift = min(r['lift'] for r in all_results)
    max_lift = max(r['lift'] for r in all_results)
    avg_mdd_improve = sum(r['ma20_mdd'] - r['base_mdd'] for r in all_results) / len(all_results)

    print(f'  파라미터 조합 수: {len(all_results)}')
    print(f'  모든 조합에서 lift > 0: {all_positive}')
    print(f'  모든 조합에서 wins 압도 (5x+): {all_wins_dominant}')
    print(f'  평균 lift: {avg_lift:+.2f}%p')
    print(f'  lift 범위: [{min_lift:+.2f}%p ~ {max_lift:+.2f}%p]')
    print(f'  평균 MDD 개선: {avg_mdd_improve:+.2f}%p (음수면 ma20이 더 안전)')

    if all_positive and all_wins_dominant:
        print()
        print('  ★★★ 모든 파라미터 조합에서 MA20 일관 우월 — robustness 확정')
    elif all_positive:
        print()
        print('  ✓ 모든 조합에서 lift 양수지만 일부 조합에서 wins 약함 — 부분 robust')
    else:
        print()
        print('  ⚠️ 일부 조합에서 MA20 열세 — robustness 의심')


if __name__ == '__main__':
    main()
