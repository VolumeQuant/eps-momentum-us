"""MA20 + smooth weights stacking BT — 두 알파의 독립성 확인

가설:
  - MA20 단독: +27.57%p (random 100/0)
  - smooth 25/30/45 단독: +13.31%p (random 100/0)
  - 두 알파가 독립이면: 둘 다 적용 시 +40%p+ stacking
  - 두 알파가 겹치면: lift 27%p 또는 13%p 정도에 머무름

비교 4-5 변형:
  baseline   — current MA + current weights (production)
  ma20_only  — MA20 + current weights (단독 A)
  smooth_only— current MA + smooth weights (단독 B)
  combined   — MA20 + smooth weights (stacking 후보)
  combined_today — MA20 + today_only weights (alt)
"""
import sys
import shutil
import sqlite3
import random
import statistics
import time
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import numpy as np
import pandas as pd
import daily_runner as dr
import bt_breakout_hold as bth
import bt_ma_filter_extended as ext
import bt_wgap_weights as wgw

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_smooth_stacking_dbs'
GRID.mkdir(exist_ok=True)
PRICE_PARQUET = Path(__file__).parent / 'price_history_for_ma_bt.parquet'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 3
EXIT_TOP = 10
MAX_SLOTS = 3
HOLD_DAYS = 0


def regenerate_combined(test_db, ma_variant, ma_df, weights_3):
    """MA filter 변형 + w_gap weights 변형 동시 적용"""
    original_fn = dr._compute_w_gap_map
    dr._compute_w_gap_map = wgw.make_wgap_with_weights(weights_3)
    try:
        ext.regenerate(test_db, ma_variant, ma_df)
    finally:
        dr._compute_w_gap_map = original_fn


def run_bt(db_path):
    bth.DB_PATH = db_path
    dates, data, price_series = bth.load_data_ext()
    if len(dates) <= MIN_HOLD_DAYS:
        return None
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        seed_rets = []
        for sd in chosen:
            r = bth.simulate_hold(
                dates, data, price_series, hold_days=HOLD_DAYS,
                entry_top=ENTRY_TOP, exit_top=EXIT_TOP,
                max_slots=MAX_SLOTS, start_date=sd
            )
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            seed_rets.append(r['total_return'])
        seed_avgs.append(sum(seed_rets) / len(seed_rets))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


CURRENT_WEIGHTS = [0.20, 0.30, 0.50]
SMOOTH_WEIGHTS = [0.25, 0.30, 0.45]
TODAY_WEIGHTS = [0.00, 0.00, 1.00]

VARIANTS = [
    # (name, ma_variant, ma_period, weights)
    ('baseline',          'current', None, CURRENT_WEIGHTS),
    ('ma20_only',         'ma20',    20,   CURRENT_WEIGHTS),
    ('smooth_only',       'current', None, SMOOTH_WEIGHTS),
    ('today_only_only',   'current', None, TODAY_WEIGHTS),
    ('ma20+smooth',       'ma20',    20,   SMOOTH_WEIGHTS),
    ('ma20+today_only',   'ma20',    20,   TODAY_WEIGHTS),
]


def main():
    print('=' * 110)
    print('MA20 + smooth/today_only stacking BT — 알파 독립성 확인')
    print(f'params: entry={ENTRY_TOP}, exit={EXIT_TOP}, slots={MAX_SLOTS}, hold={HOLD_DAYS}')
    print(f'seeds: {N_SEEDS} × {SAMPLES_PER_SEED} = {N_SEEDS*SAMPLES_PER_SEED} 시뮬/변형')
    print('=' * 110)

    close_df = pd.read_parquet(PRICE_PARQUET)
    ma20_df = close_df.rolling(window=20, min_periods=20).mean()

    all_results = {}
    for name, ma_v, period, weights in VARIANTS:
        db = GRID / f'{name}.db'
        print(f'\n[{name}] MA={ma_v}, weights={weights}')
        t0 = time.time()
        shutil.copy(DB_ORIGINAL, db)
        ma_df = ma20_df if period == 20 else None
        regenerate_combined(db, ma_v, ma_df, weights)
        print(f'  regenerate: {time.time()-t0:.1f}s')

        t1 = time.time()
        res = run_bt(db)
        if res is None:
            print('  데이터 부족')
            continue
        all_results[name] = res
        avg = sum(res['rets']) / len(res['rets'])
        med = sorted(res['rets'])[len(res['rets']) // 2]
        mdd = min(res['mdds'])
        ra = avg / abs(mdd) if mdd < 0 else 0
        print(f'  BT: {time.time()-t1:.1f}s | avg={avg:+6.2f}% med={med:+6.2f}% '
              f'mdd={mdd:+6.2f}% ra={ra:+.2f}')

    print()
    print('=' * 110)
    print(f'결과 분포 ({N_SEEDS*SAMPLES_PER_SEED}개 시뮬)')
    print('=' * 110)
    print(f'{"variant":<22} {"avg":>9} {"median":>9} {"std":>6} '
          f'{"p25":>9} {"p75":>9} {"min":>9} {"max":>9} {"MDD":>8} {"ra":>7}')
    print('-' * 110)
    for name, *_ in VARIANTS:
        if name not in all_results:
            continue
        r = all_results[name]
        rets = sorted(r['rets'])
        n = len(rets)
        avg = sum(rets) / n
        med = rets[n // 2]
        std = statistics.pstdev(rets)
        p25 = rets[n // 4]
        p75 = rets[3 * n // 4]
        mdd = min(r['mdds'])
        ra = avg / abs(mdd) if mdd < 0 else 0
        marker = ' ★' if name == 'baseline' else '  '
        print(f'{marker}{name:<20} {avg:+8.2f}% {med:+8.2f}% {std:>5.2f} '
              f'{p25:+8.2f}% {p75:+8.2f}% {min(rets):+8.2f}% {max(rets):+8.2f}% '
              f'{mdd:+7.2f}% {ra:+6.2f}')

    if 'baseline' in all_results:
        print()
        print('=' * 110)
        print('baseline (production) 대비 paired lift')
        print('=' * 110)
        base = all_results['baseline']['seed_avgs']
        print(f'{"vs":<22} {"avg lift":>10} {"min lift":>10} {"max lift":>10} '
              f'{"#wins":>6} {"#losses":>8} {"#ties":>6}')
        print('-' * 90)
        single_lifts = {}
        for name, *_ in VARIANTS:
            if name == 'baseline' or name not in all_results:
                continue
            new = all_results[name]['seed_avgs']
            lifts = [b - a for a, b in zip(base, new)]
            wins = sum(1 for l in lifts if l > 0)
            losses = sum(1 for l in lifts if l < 0)
            ties = sum(1 for l in lifts if l == 0)
            avg_lift = sum(lifts) / len(lifts)
            single_lifts[name] = avg_lift
            verdict = '✓ 우월' if wins >= 70 else '✗ 열세' if losses >= 70 else '~ 동등'
            print(f'  {name:<20} {avg_lift:+9.2f}%p {min(lifts):+9.2f}%p {max(lifts):+9.2f}%p '
                  f'{wins:>5} {losses:>7} {ties:>5}  {verdict}')

        # Stacking 분석
        print()
        print('=' * 110)
        print('Stacking 분석 — 두 알파가 독립인지 겹치는지')
        print('=' * 110)
        if 'ma20_only' in single_lifts and 'smooth_only' in single_lifts and 'ma20+smooth' in single_lifts:
            a = single_lifts['ma20_only']
            b = single_lifts['smooth_only']
            ab = single_lifts['ma20+smooth']
            expected_indep = a + b
            ratio = ab / expected_indep if expected_indep != 0 else 0
            verdict = ('✓ 완전 독립 (full stacking)' if ratio >= 0.9 else
                       '◐ 부분 독립' if ratio >= 0.5 else
                       '○ 거의 겹침')
            print(f'  ma20 단독:        {a:+.2f}%p')
            print(f'  smooth 단독:      {b:+.2f}%p')
            print(f'  ma20+smooth 결합: {ab:+.2f}%p')
            print(f'  독립 시 예상 (a+b): {expected_indep:+.2f}%p')
            print(f'  실제/예상 비율:    {ratio*100:.1f}%  → {verdict}')
        if 'ma20_only' in single_lifts and 'today_only_only' in single_lifts and 'ma20+today_only' in single_lifts:
            a = single_lifts['ma20_only']
            b = single_lifts['today_only_only']
            ab = single_lifts['ma20+today_only']
            expected_indep = a + b
            ratio = ab / expected_indep if expected_indep != 0 else 0
            verdict = ('✓ 완전 독립' if ratio >= 0.9 else
                       '◐ 부분 독립' if ratio >= 0.5 else
                       '○ 거의 겹침')
            print()
            print(f'  ma20 단독:           {a:+.2f}%p')
            print(f'  today_only 단독:     {b:+.2f}%p')
            print(f'  ma20+today_only 결합: {ab:+.2f}%p')
            print(f'  독립 시 예상 (a+b):   {expected_indep:+.2f}%p')
            print(f'  실제/예상 비율:       {ratio*100:.1f}%  → {verdict}')


if __name__ == '__main__':
    main()
