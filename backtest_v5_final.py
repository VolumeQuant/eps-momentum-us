"""정밀 백테스트 v5 - 최종 통합

새 인사이트 (CAGR 노이즈 제거 후):
  - S1 몰빵 위험 명확
  - E3/X11/S3 = 위험조정 최고
  - E3/X11/S2 = 학습 일관 1등
  - 41일 raw return 기반

테스트:
  1. 핵심 후보 정밀 비교 (multistart + walk-forward 학습 1등 추적)
  2. Conviction 변형 (raw return 기반)
  3. Breakout Hold (raw return 기반)
  4. 통합 최적: 후보 × conviction × hold
"""
import sys
import os
import shutil
import sqlite3
from collections import defaultdict
from bt_engine import load_data, simulate

sys.path.insert(0, '.')
import daily_runner as dr
from backtest_v3 import (
    conv_none, conv_base, conv_strong,
    regenerate_part2_with_conviction,
)

DB_ORIGINAL = 'eps_momentum_data.db'

HOLD_CONFIGS = {
    'strict':       {'lookback_days': 20, 'price_threshold': 25, 'rev_up_ratio': 0.4, 'check_ma60': True, 'max_grace': 2},
    'moderate':     {'lookback_days': 10, 'price_threshold': 15, 'rev_up_ratio': 0.3, 'check_ma60': True, 'max_grace': 2},
    'loose':        {'lookback_days': 5, 'price_threshold': 10, 'rev_up_ratio': 0.3, 'check_ma60': True, 'max_grace': 3},
    'very_loose':   {'lookback_days': 5, 'price_threshold': 5, 'rev_up_ratio': 0.2, 'check_ma60': False, 'max_grace': 3},
}


def multistart_raw(dates, data, entry, exit_th, slots, hold_params=None,
                    start_step=2):
    """multistart with 41-day raw return only"""
    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    samples = start_dates[::start_step]

    rets = []
    mdds = []
    sharpes = []
    n_trades_list = []
    for sd in samples:
        r = simulate(dates, data, entry, exit_th, slots,
                     start_date=sd, hold_params=hold_params)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
        sharpes.append(r['sharpe'])
        n_trades_list.append(r['n_trades'])

    n = len(rets)
    avg_ret = sum(rets) / n
    sorted_rets = sorted(rets)
    median_ret = sorted_rets[n // 2]
    std_ret = (sum((r - avg_ret) ** 2 for r in rets) / n) ** 0.5
    avg_mdd = sum(mdds) / n
    worst_mdd = min(mdds)

    risk_adj = avg_ret / abs(worst_mdd) if worst_mdd != 0 else 0

    return {
        'avg_ret': round(avg_ret, 2),
        'med_ret': round(median_ret, 2),
        'min_ret': round(min(rets), 2),
        'max_ret': round(max(rets), 2),
        'std_ret': round(std_ret, 2),
        'avg_mdd': round(avg_mdd, 2),
        'worst_mdd': round(worst_mdd, 2),
        'avg_sharpe': round(sum(sharpes) / n, 2),
        'risk_adj': round(risk_adj, 2),
        'n_samples': n,
        'avg_trades': round(sum(n_trades_list) / n, 1),
    }


def print_row(label, m):
    print(f"  {label:<26s} avg {m['avg_ret']:+5.1f}% med {m['med_ret']:+5.1f}% "
          f"min {m['min_ret']:+5.1f}% max {m['max_ret']:+5.1f}% std {m['std_ret']:>4.1f} | "
          f"MDDavg {m['avg_mdd']:+5.1f}% worst {m['worst_mdd']:+5.1f}% | "
          f"위험조정 {m['risk_adj']:>4.2f}")


def step1_top_candidates(dates, data):
    """Top 후보 5개 정밀 비교"""
    print("\n" + "=" * 130)
    print("Step 1: Top 후보 정밀 비교")
    print("=" * 130)
    print()

    candidates = [
        ('A: E5/X12/S3 (현재)', 5, 12, 3),
        ('★ E3/X11/S3 (위험조정)', 3, 11, 3),
        ('★ E3/X11/S2 (Walk-F 1등)', 3, 11, 2),
        ('G: E3/X10/S2 (균형)', 3, 10, 2),
        ('E2/X10/S2 (Sharpe)', 2, 10, 2),
        ('D: E3/X10/S3', 3, 10, 3),
    ]

    for name, e, x, s in candidates:
        m = multistart_raw(dates, data, e, x, s)
        print_row(name, m)


def step2_walkforward_track(dates, data):
    """학습 1등이 검증에서 몇 위인지 정확히 추적"""
    print("\n" + "=" * 130)
    print("Step 2: Walk-Forward - 학습 1등의 검증 순위")
    print("=" * 130)

    splits = [(15, 26), (20, 21), (25, 16), (30, 11), (35, 6)]
    variants = [
        ('A: E5/X12/S3', 5, 12, 3),
        ('B: E3/X12/S3', 3, 12, 3),
        ('D: E3/X10/S3', 3, 10, 3),
        ('E3/X11/S3', 3, 11, 3),
        ('G: E3/X10/S2', 3, 10, 2),
        ('E3/X11/S2', 3, 11, 2),
        ('E2/X10/S2', 2, 10, 2),
        ('E3/X10/S1', 3, 10, 1),
    ]

    for train_n, test_n in splits:
        train_dates = dates[:train_n]
        test_start = dates[train_n]

        train_results = []
        test_results = []
        for name, e, x, s in variants:
            tr = simulate(train_dates, data, e, x, s)
            te = simulate(dates, data, e, x, s, start_date=test_start)
            train_results.append((name, tr['total_return'], tr['max_dd']))
            test_results.append((name, te['total_return'], te['max_dd']))

        train_results.sort(key=lambda x: -x[1])
        test_results.sort(key=lambda x: -x[1])

        train_best_name = train_results[0][0]
        test_rank_of_train_best = next(
            (i + 1 for i, r in enumerate(test_results) if r[0] == train_best_name), -1
        )

        print(f"\n[Split {train_n}/{test_n}] train_end={train_dates[-1]}")
        print(f"  학습 1등: {train_best_name:<18s} {train_results[0][1]:+6.1f}% MDD {train_results[0][2]:+5.1f}%")
        print(f"  검증 1등: {test_results[0][0]:<18s} {test_results[0][1]:+6.1f}% MDD {test_results[0][2]:+5.1f}%")
        print(f"  학습 1등의 검증 순위: {test_rank_of_train_best}/{len(variants)}")
        if test_rank_of_train_best <= 3:
            print(f"  → robust (Top 3 유지)")


def step3_conviction(dates, data):
    """Conviction 변형 (raw return 기반)"""
    print("\n" + "=" * 130)
    print("Step 3: Conviction 변형 (raw return)")
    print("=" * 130)

    # 3 DB 생성
    test_dbs = {}
    for name, fn in [('none', conv_none), ('base', conv_base), ('strong', conv_strong)]:
        db = f'eps_test_{name}.db'
        if os.path.exists(db):
            os.remove(db)
        shutil.copy(DB_ORIGINAL, db)
        regenerate_part2_with_conviction(db, fn)
        test_dbs[name] = db
        print(f"  생성: {db}")

    data_by_conv = {}
    for name, db in test_dbs.items():
        _, d = load_data(db)
        data_by_conv[name] = d

    candidates = [
        ('E3/X11/S3', 3, 11, 3),
        ('E3/X11/S2', 3, 11, 2),
        ('G: E3/X10/S2', 3, 10, 2),
    ]

    for label, e, x, s in candidates:
        print(f"\n--- {label} ---")
        for cn in ['none', 'base', 'strong']:
            m = multistart_raw(dates, data_by_conv[cn], e, x, s)
            print_row(cn, m)


def step4_hold(dates, data):
    """Hold 변형 (raw return 기반)"""
    print("\n" + "=" * 130)
    print("Step 4: Breakout Hold 변형 (raw return)")
    print("=" * 130)

    candidates = [
        ('E3/X11/S3', 3, 11, 3),
        ('E3/X11/S2', 3, 11, 2),
        ('G: E3/X10/S2', 3, 10, 2),
    ]

    for label, e, x, s in candidates:
        print(f"\n--- {label} ---")

        # no_hold
        m_base = multistart_raw(dates, data, e, x, s, hold_params=None)
        print_row('no_hold', m_base)
        base_avg = m_base['avg_ret']

        for hold_name, hold_params in HOLD_CONFIGS.items():
            m = multistart_raw(dates, data, e, x, s, hold_params=hold_params)
            diff = m['avg_ret'] - base_avg
            label_with_diff = f"{hold_name} (Δ{diff:+.1f}%p)"
            print_row(label_with_diff, m)


def main():
    dates, data = load_data()
    print(f"기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")

    step1_top_candidates(dates, data)
    step2_walkforward_track(dates, data)
    step3_conviction(dates, data)
    step4_hold(dates, data)


if __name__ == '__main__':
    main()
