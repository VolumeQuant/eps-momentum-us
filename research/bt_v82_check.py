"""v82 결정 전 누락 검증 2가지

1. (2,10,2) 단독 효과 — MA120 baseline 위에서 (2,10,2)만의 lift 측정
2. MA20 + smooth 12시작일 — random 100/0과 일관성 확인

이 검증 후 commit 진행:
  - (2,10,2) 단독으로도 양수 lift → v82 진행 근거
  - MA20+smooth 12시작일 12/12 → smooth 추가 적용 근거
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
STACK_GRID = ROOT / 'research' / 'ma_smooth_stacking_dbs'

N_SEEDS = 500
SAMPLES = 5
MIN_HOLD_DAYS = 10
N_STARTS = 12


def run(db_path, entry, exit_, slots, seed_starts):
    bth.DB_PATH = db_path
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


def run_multistart(db_path, entry, exit_, slots):
    bth.DB_PATH = db_path
    dates, data, price_series = bth.load_data_ext()
    starts = dates[:N_STARTS]
    rets, mdds = [], []
    for sd in starts:
        r = bth.simulate_hold(
            dates, data, price_series, hold_days=0,
            entry_top=entry, exit_top=exit_,
            max_slots=slots, start_date=sd
        )
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return starts, rets, mdds


def main():
    # ===== Test 1: (2,10,2) 단독 효과 (MA120 baseline) =====
    print('=' * 100)
    print('[Test 1] (2,10,2) 단독 효과 — MA120 baseline 위에서')
    print(f'  {N_SEEDS} seed × {SAMPLES} = {N_SEEDS*SAMPLES} sim/조합')
    print('=' * 100)

    db_current = GRID / 'ext_current.db'  # MA120 + fallback baseline

    bth.DB_PATH = db_current
    dates, _, _ = bth.load_data_ext()
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    print()
    candidates = [
        (3, 10, 3, 'current production'),
        (2, 10, 2, '(2,10,2)만 변경'),
        (1, 10, 1, '(1,10,1) 단일'),
    ]
    results_t1 = {}
    for entry, exit_, slots, label in candidates:
        spec = f'({entry},{exit_},{slots})'
        t0 = time.time()
        res = run(db_current, entry, exit_, slots, seed_starts)
        avg = sum(res['rets']) / len(res['rets'])
        mdd = min(res['mdds'])
        ra = avg / abs(mdd) if mdd < 0 else 0
        results_t1[spec] = res
        marker = ' ★' if spec == '(3,10,3)' else '  '
        print(f'  [{time.time()-t0:>4.1f}s] {marker}{spec:<10} {label:<22} '
              f'avg={avg:+6.2f}% mdd={mdd:+6.2f}% ra={ra:+5.2f}')

    # paired
    print()
    base = results_t1['(3,10,3)']['seed_avgs']
    for entry, exit_, slots, label in candidates:
        spec = f'({entry},{exit_},{slots})'
        if spec == '(3,10,3)':
            continue
        new = results_t1[spec]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts) / len(lifts)
        verdict = '✓' if wins >= N_SEEDS * 0.9 else '◐' if wins >= N_SEEDS * 0.5 else '✗'
        print(f'  paired vs (3,10,3): {spec} lift {avg_l:+.2f}%p, {wins}/{N_SEEDS} wins  [{verdict}]')

    # ===== Test 2: MA20 + smooth 12시작일 =====
    print()
    print('=' * 100)
    print('[Test 2] MA20 + smooth 12시작일 multistart (vs MA20만)')
    print(f'  12 fixed starts × 1 simulation = 12 sim/조합')
    print('=' * 100)

    db_ma20 = STACK_GRID / 'ma20_only.db'
    db_ma20_smooth = STACK_GRID / 'ma20+smooth.db'

    print('\n  [MA20만] 12시작일...')
    starts_a, rets_a, mdds_a = run_multistart(db_ma20, 3, 10, 3)
    print('  [MA20+smooth 25/30/45] 12시작일...')
    starts_b, rets_b, mdds_b = run_multistart(db_ma20_smooth, 3, 10, 3)

    print()
    print(f'  {"start_date":<13} {"ma20만":>10} {"ma20+smooth":>13} {"lift":>10}')
    print('  ' + '-' * 60)
    lifts = []
    for sd, ra, rb in zip(starts_a, rets_a, rets_b):
        lift = rb - ra
        lifts.append(lift)
        print(f'  {sd:<13} {ra:+9.2f}% {rb:+12.2f}% {lift:+9.2f}%p')
    print('  ' + '-' * 60)
    avg_a = sum(rets_a)/len(rets_a)
    avg_b = sum(rets_b)/len(rets_b)
    avg_lift = sum(lifts)/len(lifts)
    wins = sum(1 for l in lifts if l > 0)
    losses = sum(1 for l in lifts if l < 0)
    print(f'  {"avg":<13} {avg_a:+9.2f}% {avg_b:+12.2f}% {avg_lift:+9.2f}%p')
    print(f'  paired wins: ma20+smooth vs ma20만: {wins}/12 wins, {losses}/12 losses')
    print()
    print(f'  random 100seed×3 (300 sim) 결과 비교:')
    print(f'    ma20+smooth vs ma20만: +0.81%p, 100/0 wins')
    print(f'    이번 12시작일: {avg_lift:+.2f}%p, {wins}/12 wins')


if __name__ == '__main__':
    main()
