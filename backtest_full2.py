"""정밀 백테스트 Part 2: Conviction + Breakout Hold

새 metric으로 conviction 변형, hold 변형 측정
"""
import sqlite3
import shutil
import os
import sys
from collections import defaultdict
from bt_engine import load_data, simulate
from bt_metrics import compute_metrics, aggregate_multistart

DB_ORIGINAL = 'eps_momentum_data.db'

sys.path.insert(0, '.')
import daily_runner as dr
from backtest_v3 import (
    conv_none, conv_base, conv_strong,
    regenerate_part2_with_conviction,
)


HOLD_CONFIGS = {
    'strict':       {'lookback_days': 20, 'price_threshold': 25, 'rev_up_ratio': 0.4, 'check_ma60': True, 'max_grace': 2},
    'moderate':     {'lookback_days': 10, 'price_threshold': 15, 'rev_up_ratio': 0.3, 'check_ma60': True, 'max_grace': 2},
    'loose':        {'lookback_days': 5, 'price_threshold': 10, 'rev_up_ratio': 0.3, 'check_ma60': True, 'max_grace': 3},
    'very_loose':   {'lookback_days': 5, 'price_threshold': 5, 'rev_up_ratio': 0.2, 'check_ma60': False, 'max_grace': 3},
}


def multistart(dates, data, entry, exit_th, slots, hold_params=None,
               start_step=2):
    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    samples = start_dates[::start_step]
    results = []
    for sd in samples:
        r = simulate(dates, data, entry, exit_th, slots,
                     start_date=sd, hold_params=hold_params)
        results.append(r)
    agg = aggregate_multistart(results)
    return agg, results


def print_summary(label, agg):
    cagr = agg['cagr']
    mdd = agg['max_dd']
    print(f"  {label:<22s} "
          f"CAGR avg {cagr['avg']:+6.0f}% (med {cagr['median']:+5.0f}, min {cagr['min']:+5.0f}) | "
          f"MDD avg {mdd['avg']:+5.1f}% worst {mdd['min']:+5.1f}% | "
          f"Sharpe {agg['sharpe']['avg']:>5.2f} | "
          f"Sortino {agg['sortino']['avg']:>5.2f}")


def s3_conviction(dates_db_default, data_db_default):
    """Conviction 변형: 3개 DB 생성 + 백테스트"""
    print("\n" + "=" * 110)
    print("S3: Conviction 변형 (모든 metric, 차분 측정)")
    print("=" * 110)

    # 3개 DB 생성
    test_dbs = {}
    for name, fn in [('none', conv_none), ('base', conv_base), ('strong', conv_strong)]:
        db = f'eps_test_{name}.db'
        if os.path.exists(db):
            os.remove(db)
        shutil.copy(DB_ORIGINAL, db)
        regenerate_part2_with_conviction(db, fn)
        test_dbs[name] = db
        print(f"  생성: {db}")

    # 각 DB의 데이터 로드
    data_by_conv = {}
    for name, db in test_dbs.items():
        _, d = load_data(db)
        data_by_conv[name] = d

    # 4 baseline에서 비교
    baselines = [
        ('A: E5/X12/S3 (현재)', 5, 12, 3),
        ('E3/X11/S2 (Sharpe 최고)', 3, 11, 2),
        ('G: E3/X10/S2', 3, 10, 2),
        ('E3/X10/S1 (CAGR 최고)', 3, 10, 1),
    ]

    for label, e, x, s in baselines:
        print(f"\n--- {label}: E{e}/X{x}/S{s} ---")
        for conv_name in ['none', 'base', 'strong']:
            agg, _ = multistart(dates_db_default, data_by_conv[conv_name], e, x, s)
            print_summary(conv_name, agg)


def s4_hold(dates, data):
    """Breakout Hold 변형 (DB part2_rank 위에)"""
    print("\n" + "=" * 110)
    print("S4: Breakout Hold 변형 (모든 metric)")
    print("=" * 110)

    # 트리거 빈도 표본 테스트
    print("\n[표본] G 위에서 hold 트리거 빈도:")
    for name, params in HOLD_CONFIGS.items():
        r = simulate(dates, data, 3, 10, 2, hold_params=params)
        print(f"  {name:<14s} 트리거={r['breakout_holds']:>3d}회 "
              f"return={r['total_return']:+.1f}% MDD={r['max_dd']:+.1f}%")

    # 3 baseline에서 hold 효과
    baselines = [
        ('G: E3/X10/S2', 3, 10, 2),
        ('E3/X11/S2', 3, 11, 2),
        ('E3/X10/S1', 3, 10, 1),
    ]

    for label, e, x, s in baselines:
        print(f"\n--- {label}: E{e}/X{x}/S{s} ---")

        # no_hold (baseline)
        agg_base, _ = multistart(dates, data, e, x, s, hold_params=None)
        print_summary('no_hold', agg_base)
        base_cagr = agg_base['cagr']['avg']

        # 4 hold 변형
        for hold_name, hold_params in HOLD_CONFIGS.items():
            agg, _ = multistart(dates, data, e, x, s, hold_params=hold_params)
            diff = agg['cagr']['avg'] - base_cagr
            print_summary(hold_name, agg)
            print(f"    diff vs no_hold: CAGR {diff:+5.0f}%p")


def main():
    dates, data = load_data()
    print(f"기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")

    s3_conviction(dates, data)
    s4_hold(dates, data)


if __name__ == '__main__':
    main()
