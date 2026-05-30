"""깔끔한 step 룰 grid — 5% 단위 정수 비중만 사용

후보:
  2단계 (high vs low):
    100/0 | 50/50 (threshold T)
    90/10 | 50/50
    80/20 | 50/50
    100/0 | 60/40
  3단계 (high/mid/low):
    100/0 | 80/20 | 50/50
    100/0 | 75/25 | 50/50
    100/0 | 70/30 | 50/50 (= step_100_70_50)
    100/0 | 70/30 | 60/40
    100/0 | 80/20 | 60/40
    90/10 | 70/30 | 50/50
    90/10 | 75/25 | 60/40
    90/10 | 80/20 | 70/30
  4단계:
    100/0 | 90/10 | 70/30 | 50/50

각 변형의 threshold도 다양 (gap 10/20/30 등).
"""
import sys
import shutil
import sqlite3
import random
import statistics
import time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')
import daily_runner as dr
import bt_dynamic_sweetspot as sw

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 3
EXIT_TOP = 10


def step_fn(thresholds, weights_for_h):
    """thresholds=[T1, T2, ...] (큰순, 격차 큰 시점), weights_for_h=[w_top, w_t1, w_t2, ...]
    gap >= T1: weights_for_h[0]
    gap >= T2: weights_for_h[1]
    ...
    gap < lowest_T: weights_for_h[-1]
    각 weight는 w_high (1위 비중)
    """
    def fn(g, s1, s2):
        for i, t in enumerate(thresholds):
            if g >= t:
                w = weights_for_h[i]
                return [w, 1-w]
        w = weights_for_h[-1]
        return [w, 1-w]
    return fn


VARIANTS = [
    # 2단계 (gap T → high, else low)
    ('2step_t20_100_50',   step_fn([20],     [1.0, 0.5])),
    ('2step_t15_100_50',   step_fn([15],     [1.0, 0.5])),
    ('2step_t25_100_50',   step_fn([25],     [1.0, 0.5])),
    ('2step_t30_100_50',   step_fn([30],     [1.0, 0.5])),
    ('2step_t20_90_50',    step_fn([20],     [0.9, 0.5])),
    ('2step_t15_90_50',    step_fn([15],     [0.9, 0.5])),
    ('2step_t20_80_50',    step_fn([20],     [0.8, 0.5])),
    ('2step_t20_100_60',   step_fn([20],     [1.0, 0.6])),
    ('2step_t20_100_70',   step_fn([20],     [1.0, 0.7])),

    # 3단계 (current step_100_70_50 = [30, 10] → [1.0, 0.7, 0.5])
    ('3step_30_10_100_70_50',  step_fn([30, 10], [1.0, 0.7, 0.5])),  # = step_100_70_50
    ('3step_30_10_100_80_50',  step_fn([30, 10], [1.0, 0.8, 0.5])),
    ('3step_30_10_100_75_50',  step_fn([30, 10], [1.0, 0.75, 0.5])),
    ('3step_30_10_100_80_60',  step_fn([30, 10], [1.0, 0.8, 0.6])),
    ('3step_30_10_100_70_60',  step_fn([30, 10], [1.0, 0.7, 0.6])),
    ('3step_30_10_90_70_50',   step_fn([30, 10], [0.9, 0.7, 0.5])),
    ('3step_30_10_90_75_50',   step_fn([30, 10], [0.9, 0.75, 0.5])),
    ('3step_30_10_90_75_60',   step_fn([30, 10], [0.9, 0.75, 0.6])),
    ('3step_20_5_100_70_50',   step_fn([20, 5],  [1.0, 0.7, 0.5])),
    ('3step_20_10_100_70_50',  step_fn([20, 10], [1.0, 0.7, 0.5])),
    ('3step_25_10_100_70_50',  step_fn([25, 10], [1.0, 0.7, 0.5])),

    # 4단계
    ('4step_30_20_10_100_90_70_50', step_fn([30, 20, 10], [1.0, 0.9, 0.7, 0.5])),
    ('4step_30_15_5_100_80_70_50',  step_fn([30, 15, 5],  [1.0, 0.8, 0.7, 0.5])),

    # baseline
    ('fixed_90_10',  sw.make_weight_fn({'type': 'fixed', 'weights': [0.9, 0.1]})),

    # 비교용 (continuous)
    ('linear_15',    sw.make_weight_fn({'type': 'linear', 'anchor': 15})),
    ('linear_20',    sw.make_weight_fn({'type': 'linear', 'anchor': 20})),
]


def run_paired(db_path, weight_items):
    scores = sw.precompute_scores(db_path)
    dates, data, price_full = sw.load_data(db_path)
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
    results = {}
    for name, wfn in weight_items:
        rets, mdds, seed_avgs = [], [], []
        for chosen in seed_starts:
            sr = []
            for sd in chosen:
                r = sw.simulate_dynamic(dates, data, price_full, scores, wfn, sd,
                                      max_slots=2, entry=ENTRY_TOP, exit_=EXIT_TOP)
                rets.append(r['total_return'])
                mdds.append(r['max_dd'])
                sr.append(r['total_return'])
            seed_avgs.append(sum(sr)/len(sr))
        avg = sum(rets)/len(rets); med = sorted(rets)[len(rets)//2]
        std = statistics.pstdev(rets); mdd = min(mdds); sharpe = avg/std if std > 0 else 0
        results[name] = {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs,
                        'avg': avg, 'med': med, 'std': std, 'mdd': mdd, 'sharpe': sharpe}
    return results


def main():
    print('=' * 110)
    print('깔끔한 step 룰 grid — 5% 단위 비중')
    print('=' * 110)

    db_incl = GRID / 'v_incl.db'  # 이미 검증 1에서 생성됨
    db_excl = GRID / 'v_MU_SNDK.db'

    print('\n[incl 환경]')
    res_incl = run_paired(db_incl, VARIANTS)

    print('[excl 환경]')
    res_excl = run_paired(db_excl, VARIANTS)

    # 종합
    print()
    print('=' * 110)
    print('★ step 룰 종합 (두 환경 평균 lift, 정렬: 평균 lift)')
    print('=' * 110)
    base_i = res_incl['fixed_90_10']['seed_avgs']
    base_e = res_excl['fixed_90_10']['seed_avgs']
    rows = []
    for name, _ in VARIANTS:
        ri = res_incl[name]; re = res_excl[name]
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        avg_both = (avgi + avge) / 2
        rows.append((name, ri['avg'], avgi, wi, re['avg'], avge, we, avg_both))
    rows.sort(key=lambda x: -x[7])
    print(f'  {"variant":<32} {"i_avg":>8} {"i_lift":>9} {"i_win":>6} | {"e_avg":>7} {"e_lift":>9} {"e_win":>6} | {"평균":>8}')
    print('  ' + '-' * 100)
    for r in rows:
        marker = ' ★' if r[0] == 'fixed_90_10' else '  '
        print(f'{marker}{r[0]:<30} {r[1]:+7.2f}% {r[2]:+7.2f}%p {r[3]:>4}/100 | '
              f'{r[4]:+6.2f}% {r[5]:+7.2f}%p {r[6]:>4}/100 | {r[7]:+7.2f}%p')


if __name__ == '__main__':
    main()
