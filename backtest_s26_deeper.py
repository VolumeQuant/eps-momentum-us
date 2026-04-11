"""S2.6: S2.5에서 발견한 맹점 추가 검증

1. S1 vs S2 깊이 비교 - 진짜 S1이 더 좋은지, 위험 대비 가치
2. Rolling Walk-Forward - 다양한 train/test split
3. G의 시작일 변동성 분석 - 왜 +12.45 vs +25.86?
"""
import sqlite3
from collections import defaultdict
from backtest_s2_params import simulate, load_data


def s1_vs_s2_deep_dive(dates, data):
    """S1 vs S2 직접 비교 - 다양한 E/X 조합"""
    print("=" * 70)
    print("Test 1: S1 vs S2 Deep Dive (S1이 진짜 더 좋은가?)")
    print("=" * 70)

    e_range = [3, 4]
    x_range = [9, 10, 11, 12]

    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    samples = start_dates[::2]

    print(f"\nMultistart {len(samples)}개 시작일\n")
    print(f"{'Config':<12s} {'평균':>7s} {'표준편차':>8s} {'최저':>7s} {'최고':>7s} {'Sharpe*':>8s}")
    print("-" * 60)

    for e in e_range:
        for x in x_range:
            for s in [1, 2]:
                rets = []
                for sd in samples:
                    r = simulate(dates, data, e, x, s, start_date=sd)
                    rets.append(r['total_return'])
                avg = sum(rets) / len(rets)
                std = (sum((r - avg) ** 2 for r in rets) / len(rets)) ** 0.5
                sharpe = avg / std if std > 0 else 0  # 가짜 sharpe
                cfg = f"E{e}/X{x}/S{s}"
                print(f"{cfg:<12s} {avg:+6.2f}% {std:>7.2f} "
                      f"{min(rets):+6.1f}% {max(rets):+6.1f}% {sharpe:>7.2f}")

    print("\nSharpe* = 평균/표준편차 (단위 무시, 비교용)")


def rolling_walkforward(dates, data):
    """Rolling Walk-Forward: 여러 train/test split"""
    print("\n" + "=" * 70)
    print("Test 2: Rolling Walk-Forward (다양한 train/test split)")
    print("=" * 70)

    n_total = len(dates)
    splits = [
        (15, 26),  # 15일 학습, 26일 검증
        (20, 21),  # 20일 학습, 21일 검증
        (25, 16),  # 25일 학습, 16일 검증
        (30, 11),  # 30일 학습, 11일 검증
        (35, 6),   # 35일 학습, 6일 검증
    ]

    variants = [
        ('A: E5/X12/S3 (현재)', 5, 12, 3),
        ('D: E3/X10/S3', 3, 10, 3),
        ('F: E3/X12/S2', 3, 12, 2),
        ('G: E3/X10/S2', 3, 10, 2),
        ('I: E3/X8/S2', 3, 8, 2),
        ('E2/X10/S2', 2, 10, 2),
        ('E3/X10/S1', 3, 10, 1),
    ]

    for train_n, test_n in splits:
        train_end = train_n
        if train_end >= n_total - 2:
            continue
        train_dates = dates[:train_end]
        test_start_date = dates[train_end]

        print(f"\n[Split {train_n}/{test_n}] 학습={train_dates[-1]}, 검증 시작={test_start_date}")

        train_results = []
        test_results = []
        for name, e, x, s in variants:
            tr = simulate(train_dates, data, e, x, s)
            te = simulate(dates, data, e, x, s, start_date=test_start_date)
            train_results.append((name, tr['total_return']))
            test_results.append((name, te['total_return']))

        train_results.sort(key=lambda x: -x[1])
        test_results.sort(key=lambda x: -x[1])

        train_best = train_results[0]
        test_best = test_results[0]

        # train 1등이 test에서 몇 위?
        rank_in_test = next((i+1 for i, r in enumerate(test_results) if r[0] == train_best[0]), -1)

        print(f"  train 1등: {train_best[0]:<22s} ({train_best[1]:+6.2f}%)")
        print(f"  test  1등: {test_best[0]:<22s} ({test_best[1]:+6.2f}%)")
        print(f"  train 1등의 test 순위: {rank_in_test}/{len(variants)}")


def g_startdate_variance(dates, data):
    """G의 시작일별 수익 분포 분석"""
    print("\n" + "=" * 70)
    print("Test 3: G의 시작일 변동성 (왜 +12.45 vs +25.86?)")
    print("=" * 70)

    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]

    g_results = []
    for sd in start_dates:
        r = simulate(dates, data, 3, 10, 2, start_date=sd)
        g_results.append((sd, r['total_return'], r['n_trades'], r['n_open']))

    print(f"\n시작일별 G(E3/X10/S2) 수익:")
    print(f"{'시작일':<12s} {'수익':>7s} {'거래':>4s} {'오픈':>4s}")
    print("-" * 35)
    for sd, ret, t, o in g_results:
        print(f"{sd:<12s} {ret:+6.2f}% {t:>4d} {o:>4d}")

    rets = [r[1] for r in g_results]
    avg = sum(rets) / len(rets)
    std = (sum((r - avg) ** 2 for r in rets) / len(rets)) ** 0.5
    print(f"\n평균: {avg:+.2f}%, 표준편차: {std:.2f}, 범위: {min(rets):+.1f}% ~ {max(rets):+.1f}%")
    print(f"최저-최고 차이: {max(rets) - min(rets):.1f}%p")


def main():
    print("S2.6: S2.5 발견 맹점 추가 검증\n")
    dates, data = load_data()
    print(f"기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)\n")

    s1_vs_s2_deep_dive(dates, data)
    rolling_walkforward(dates, data)
    g_startdate_variance(dates, data)


if __name__ == '__main__':
    main()
