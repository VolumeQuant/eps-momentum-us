"""정밀 백테스트 v4 - 41일 raw return 기반 (현실적 metric)

핵심 변경:
  - CAGR 환산 빼고 41일 total_return으로 비교
  - Slot별 분산 효과 명시
  - Multistart 최저/최고/표준편차 강조
  - MDD avg + worst
  - Sharpe는 일간 수익 기반 (annualization 유지하지만 보조 metric)
  - Realistic Sortino (하방 N>=3 일 때만)
"""
import sys
from collections import defaultdict
from bt_engine import load_data, simulate
from bt_metrics import aggregate_multistart


def multistart_realistic(dates, data, entry, exit_th, slots,
                          hold_params=None, start_step=2):
    """multistart with realistic metrics focused on raw return"""
    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    samples = start_dates[::start_step]

    results = []
    for sd in samples:
        r = simulate(dates, data, entry, exit_th, slots,
                     start_date=sd, hold_params=hold_params)
        results.append(r)

    # raw 41-day total return statistics
    rets = [r['total_return'] for r in results]
    mdds = [r['max_dd'] for r in results]
    sharpes = [r['sharpe'] for r in results]
    n_trades_list = [r['n_trades'] for r in results]

    n = len(rets)
    avg_ret = sum(rets) / n
    sorted_rets = sorted(rets)
    median_ret = sorted_rets[n // 2]
    std_ret = (sum((r - avg_ret) ** 2 for r in rets) / n) ** 0.5

    avg_mdd = sum(mdds) / n
    worst_mdd = min(mdds)
    avg_sharpe = sum(sharpes) / n

    # 위험조정 수익률 (직접 계산)
    risk_adj = avg_ret / abs(worst_mdd) if worst_mdd != 0 else 0
    risk_adj_avg_mdd = avg_ret / abs(avg_mdd) if avg_mdd != 0 else 0

    return {
        'avg_ret': round(avg_ret, 2),
        'med_ret': round(median_ret, 2),
        'min_ret': round(min(rets), 2),
        'max_ret': round(max(rets), 2),
        'std_ret': round(std_ret, 2),
        'avg_mdd': round(avg_mdd, 2),
        'worst_mdd': round(worst_mdd, 2),
        'avg_sharpe': round(avg_sharpe, 2),
        'risk_adj': round(risk_adj, 2),
        'risk_adj_avg': round(risk_adj_avg_mdd, 2),
        'n_samples': n,
        'avg_trades': round(sum(n_trades_list) / n, 1),
        'all_rets': rets,
    }


def print_row(label, m):
    """결과 한 줄 출력"""
    print(f"{label:<24s} "
          f"평균 {m['avg_ret']:+6.1f}% "
          f"중앙 {m['med_ret']:+6.1f}% "
          f"min {m['min_ret']:+6.1f}% "
          f"max {m['max_ret']:+6.1f}% "
          f"std {m['std_ret']:>5.1f} | "
          f"MDDavg {m['avg_mdd']:+5.1f}% "
          f"MDDworst {m['worst_mdd']:+5.1f}% | "
          f"위험조정 {m['risk_adj']:>4.2f}")


def step_grid(dates, data):
    """파라미터 그리드 - 41일 raw return"""
    print("\n" + "=" * 130)
    print("Step 1: 파라미터 그리드 (41일 raw return)")
    print("=" * 130)
    print()

    variants = [
        ('A: E5/X12/S3 (현재)', 5, 12, 3),
        ('B: E3/X12/S3', 3, 12, 3),
        ('D: E3/X10/S3', 3, 10, 3),
        ('F: E3/X12/S2', 3, 12, 2),
        ('G: E3/X10/S2', 3, 10, 2),
        ('I: E3/X8/S2', 3, 8, 2),
        ('E3/X9/S2', 3, 9, 2),
        ('E3/X11/S2', 3, 11, 2),
        ('E3/X10/S1 (몰빵)', 3, 10, 1),
        ('E3/X9/S1 (몰빵)', 3, 9, 1),
        ('E2/X10/S2', 2, 10, 2),
        ('E2/X12/S3', 2, 12, 3),
    ]

    results = {}
    for name, e, x, s in variants:
        m = multistart_realistic(dates, data, e, x, s)
        results[name] = m
        print_row(name, m)

    # 위험조정 순위 (worst MDD 기준)
    print("\n--- 위험조정 수익률 순위 (avg_ret / |worst_MDD|) ---")
    sorted_by_risk = sorted(results.items(), key=lambda x: -x[1]['risk_adj'])
    for i, (name, m) in enumerate(sorted_by_risk, 1):
        print(f"  {i:2d}. {name:<22s} {m['risk_adj']:>5.2f}  "
              f"(평균 {m['avg_ret']:+5.1f}% / worst MDD {m['worst_mdd']:+5.1f}%)")

    return results


def step_robustness(dates, data):
    """3 슬롯에서 entry/exit 변동 안정성"""
    print("\n" + "=" * 130)
    print("Step 2: 슬롯별 안정성 비교 (S1 vs S2 vs S3)")
    print("=" * 130)
    print()

    # 동일 entry/exit, slot만 변화
    base_configs = [
        ('E3/X10/?', 3, 10),
        ('E3/X11/?', 3, 11),
        ('E3/X12/?', 3, 12),
    ]

    print(f"{'Config':<22s} {'avg':>7s} {'med':>7s} {'min':>7s} {'max':>7s} "
          f"{'std':>5s} {'MDDavg':>7s} {'MDDworst':>9s} {'위험조정':>8s}")
    print("-" * 100)
    for ex_label, e, x in base_configs:
        for s in [1, 2, 3]:
            m = multistart_realistic(dates, data, e, x, s)
            label = f"E{e}/X{x}/S{s}"
            print(f"{label:<22s} {m['avg_ret']:+6.1f}% {m['med_ret']:+6.1f}% "
                  f"{m['min_ret']:+6.1f}% {m['max_ret']:+6.1f}% "
                  f"{m['std_ret']:>4.1f} {m['avg_mdd']:+6.1f}% "
                  f"{m['worst_mdd']:+8.1f}% {m['risk_adj']:>7.2f}")
        print()


def step_walkforward(dates, data):
    """Walk-forward with raw returns"""
    print("\n" + "=" * 130)
    print("Step 3: Walk-Forward (41일 raw return)")
    print("=" * 130)

    splits = [(15, 26), (20, 21), (25, 16), (30, 11), (35, 6)]
    variants = [
        ('A: E5/X12/S3', 5, 12, 3),
        ('B: E3/X12/S3', 3, 12, 3),
        ('G: E3/X10/S2', 3, 10, 2),
        ('E3/X11/S2', 3, 11, 2),
        ('E3/X9/S2', 3, 9, 2),
        ('E3/X10/S1', 3, 10, 1),
        ('E2/X10/S2', 2, 10, 2),
    ]

    win_count = defaultdict(int)
    for train_n, test_n in splits:
        train_dates = dates[:train_n]
        test_start = dates[train_n]
        print(f"\n[Split {train_n}/{test_n}] train_end={train_dates[-1]}, test_start={test_start}")

        train_results = []
        test_results = []
        for name, e, x, s in variants:
            tr = simulate(train_dates, data, e, x, s)
            te = simulate(dates, data, e, x, s, start_date=test_start)
            train_results.append((name, tr['total_return'], tr['max_dd']))
            test_results.append((name, te['total_return'], te['max_dd']))

        train_results.sort(key=lambda x: -x[1])
        test_results.sort(key=lambda x: -x[1])

        print(f"  train 1등: {train_results[0][0]:<18s} {train_results[0][1]:+6.1f}% MDD {train_results[0][2]:+5.1f}%")
        print(f"  test  1등: {test_results[0][0]:<18s} {test_results[0][1]:+6.1f}% MDD {test_results[0][2]:+5.1f}%")
        win_count[test_results[0][0]] += 1

    print(f"\n검증기간 1등 빈도:")
    for name, cnt in sorted(win_count.items(), key=lambda x: -x[1]):
        print(f"  {name}: {cnt}/{len(splits)}")


def main():
    dates, data = load_data()
    print(f"기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")

    step_grid(dates, data)
    step_robustness(dates, data)
    step_walkforward(dates, data)


if __name__ == '__main__':
    main()
