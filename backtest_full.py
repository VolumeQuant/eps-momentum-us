"""정밀 백테스트 - 모든 metric + 모든 단계 통합

Metric:
  - CAGR (연환산)
  - Sharpe (일간 수익 std 기반)
  - Sortino
  - Calmar (CAGR / |MDD|)
  - MDD (단일 + multistart 분포)
  - Profit Factor
  - Win Rate

단계:
  1. 표본 검증
  2. 파라미터 그리드 + multistart
  3. 인접 안정성
  4. Walk-forward
  5. Conviction 변형 (sim 재계산)
  6. Breakout Hold
  7. 통합 최적
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


def multistart(dates, data, entry, exit_th, slots, hold_params=None,
               start_step=2):
    """multistart 백테스트 - 모든 시작일에서 metric 수집"""
    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    samples = start_dates[::start_step]

    results = []
    for sd in samples:
        r = simulate(dates, data, entry, exit_th, slots,
                     start_date=sd, hold_params=hold_params)
        results.append(r)

    agg = aggregate_multistart(results)
    agg['n_samples'] = len(samples)
    return agg, results


def print_summary(label, agg):
    """변형 결과 한 줄 요약"""
    cagr = agg['cagr']
    mdd = agg['max_dd']
    print(f"{label:<22s} "
          f"CAGR {cagr['avg']:+6.0f}% (med {cagr['median']:+5.0f}, "
          f"min {cagr['min']:+5.0f}, max {cagr['max']:+5.0f}) | "
          f"MDD avg {mdd['avg']:+5.1f}% worst {mdd['min']:+5.1f}% | "
          f"Sharpe {agg['sharpe']['avg']:>5.2f} | "
          f"Sortino {agg['sortino']['avg']:>5.2f} | "
          f"Calmar {agg['calmar']['avg']:>5.2f}")


def step1_sample(dates, data):
    print("\n" + "=" * 110)
    print("Step 1: 표본 검증 (E5/X12/S3 = +30.69% 예상)")
    print("=" * 110)
    r = simulate(dates, data, 5, 12, 3)
    print(f"total_return: {r['total_return']:+.2f}% (예상 +30.69%)")
    if abs(r['total_return'] - 30.69) > 0.5:
        print("[!] 표본 검증 실패")
        return False
    print("[OK] 표본 검증 통과")
    return True


def step2_grid_multistart(dates, data):
    print("\n" + "=" * 110)
    print("Step 2: 파라미터 그리드 + Multistart (모든 metric)")
    print("=" * 110)

    variants = [
        ('A: E5/X12/S3 (현재)', 5, 12, 3),
        ('B: E3/X12/S3', 3, 12, 3),
        ('D: E3/X10/S3', 3, 10, 3),
        ('F: E3/X12/S2', 3, 12, 2),
        ('G: E3/X10/S2', 3, 10, 2),
        ('I: E3/X8/S2', 3, 8, 2),
        ('E3/X9/S2', 3, 9, 2),
        ('E3/X11/S2', 3, 11, 2),
        ('E3/X10/S1', 3, 10, 1),
        ('E3/X9/S1', 3, 9, 1),
        ('E2/X10/S2', 2, 10, 2),
        ('E2/X12/S3', 2, 12, 3),
    ]

    print()
    all_results = {}
    for name, e, x, s in variants:
        agg, _ = multistart(dates, data, e, x, s)
        all_results[name] = agg
        print_summary(name, agg)

    return all_results


def step3_adjacency(dates, data):
    """G 인접 안정성"""
    print("\n" + "=" * 110)
    print("Step 3: 인접 안정성 (G와 후보들 주변)")
    print("=" * 110)

    centers = [
        ('G: E3/X10/S2', 3, 10, 2),
        ('E3/X9/S2', 3, 9, 2),
        ('E3/X10/S1', 3, 10, 1),
    ]

    for cname, ce, cx, cs in centers:
        print(f"\n--- {cname} 인접 ---")
        adj_variants = []
        for de in [-1, 0, 1]:
            for dx in [-1, 0, 1]:
                for ds in [-1, 0, 1]:
                    e, x, s = ce + de, cx + dx, cs + ds
                    if e < 2 or x <= e or s < 1 or s > 5:
                        continue
                    adj_variants.append((f"E{e}/X{x}/S{s}", e, x, s))

        center_cagr = None
        rows = []
        for name, e, x, s in adj_variants:
            agg, _ = multistart(dates, data, e, x, s)
            rows.append((name, e, x, s, agg))
            if e == ce and x == cx and s == cs:
                center_cagr = agg['cagr']['avg']

        rows.sort(key=lambda r: -r[4]['cagr']['avg'])
        for name, e, x, s, agg in rows[:8]:
            marker = ' ★' if (e == ce and x == cx and s == cs) else '  '
            diff = agg['cagr']['avg'] - center_cagr if center_cagr else 0
            print(f"  {marker}{name:<14s} CAGR {agg['cagr']['avg']:+6.0f}% "
                  f"(diff {diff:+5.0f}) MDD {agg['max_dd']['avg']:+5.1f}% "
                  f"Sharpe {agg['sharpe']['avg']:.2f}")


def step4_walkforward(dates, data):
    """Rolling walk-forward"""
    print("\n" + "=" * 110)
    print("Step 4: Walk-Forward (다양한 split)")
    print("=" * 110)

    splits = [(15, 26), (20, 21), (25, 16), (30, 11), (35, 6)]
    variants = [
        ('A: E5/X12/S3', 5, 12, 3),
        ('G: E3/X10/S2', 3, 10, 2),
        ('E3/X9/S2', 3, 9, 2),
        ('E3/X10/S1', 3, 10, 1),
        ('E2/X10/S2', 2, 10, 2),
    ]

    consistent_winners = defaultdict(int)
    for train_n, test_n in splits:
        train_dates = dates[:train_n]
        test_start = dates[train_n]
        print(f"\n[Split {train_n}/{test_n}] train={train_dates[-1]}, test_start={test_start}")

        train_results = []
        test_results = []
        for name, e, x, s in variants:
            tr = simulate(train_dates, data, e, x, s)
            te = simulate(dates, data, e, x, s, start_date=test_start)
            train_results.append((name, tr['total_return'], tr['sharpe']))
            test_results.append((name, te['total_return'], te['sharpe']))

        train_results.sort(key=lambda x: -x[1])
        test_results.sort(key=lambda x: -x[1])

        print(f"  train 1등: {train_results[0][0]:<22s} {train_results[0][1]:+6.1f}% Sharpe {train_results[0][2]:.2f}")
        print(f"  test  1등: {test_results[0][0]:<22s} {test_results[0][1]:+6.1f}% Sharpe {test_results[0][2]:.2f}")

        consistent_winners[test_results[0][0]] += 1

    print(f"\n검증기간 1등 빈도:")
    for name, cnt in sorted(consistent_winners.items(), key=lambda x: -x[1]):
        print(f"  {name}: {cnt}/{len(splits)}")


def main():
    dates, data = load_data()
    print(f"기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")

    if not step1_sample(dates, data):
        return

    grid_results = step2_grid_multistart(dates, data)
    step3_adjacency(dates, data)
    step4_walkforward(dates, data)


if __name__ == '__main__':
    main()
