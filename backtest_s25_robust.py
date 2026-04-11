"""S2.5: G(E3/X10/S2) 인접 안정성 + Walk-Forward 검증

테스트 1: 인접 안정성
  - G 주변 9개 변형 (E±1, X±1, S±1) 모두 multistart
  - 결과가 ±2%p 이내면 robust, 아니면 과적합

테스트 2: Walk-Forward
  - 41일을 30(학습) + 11(검증)으로 나눔
  - 학습기간 최적 → 검증기간에서도 1등인지 확인
"""
import sqlite3
from collections import defaultdict
from backtest_s2_params import simulate, load_data


def adjacency_test(dates, data):
    """G 주변 인접 파라미터 안정성"""
    print("=" * 70)
    print("Test 1: G(E3/X10/S2) 인접 안정성")
    print("=" * 70)

    # G 중심 9 + alpha 변형
    variants = [
        # 중심
        ('★ G: E3/X10/S2', 3, 10, 2),

        # Entry 인접 (Exit/Slot 동일)
        ('  E2/X10/S2', 2, 10, 2),
        ('  E4/X10/S2', 4, 10, 2),

        # Exit 인접 (Entry/Slot 동일)
        ('  E3/X8/S2', 3, 8, 2),
        ('  E3/X9/S2', 3, 9, 2),
        ('  E3/X11/S2', 3, 11, 2),
        ('  E3/X12/S2', 3, 12, 2),

        # Slot 인접 (Entry/Exit 동일)
        ('  E3/X10/S1', 3, 10, 1),
        ('  E3/X10/S3', 3, 10, 3),

        # 2-step 인접 (대각)
        ('  E2/X9/S2', 2, 9, 2),
        ('  E4/X11/S2', 4, 11, 2),
        ('  E3/X9/S3', 3, 9, 3),
        ('  E3/X11/S1', 3, 11, 1),
    ]

    # multistart 시작일
    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    samples = start_dates[::2]

    print(f"\nMultistart {len(samples)}개 시작일\n")
    print(f"{'Variant':<22s} {'평균':>7s} {'중앙값':>7s} {'표준편차':>8s} {'최저':>7s} {'최고':>7s}")
    print("-" * 65)

    g_avg = None
    results = []
    for name, e, x, s in variants:
        rets = []
        for sd in samples:
            r = simulate(dates, data, e, x, s, start_date=sd)
            rets.append(r['total_return'])
        rets_sorted = sorted(rets)
        avg = sum(rets) / len(rets)
        median = rets_sorted[len(rets_sorted) // 2]
        std = (sum((r - avg) ** 2 for r in rets) / len(rets)) ** 0.5
        results.append((name, avg, median, std, min(rets), max(rets)))
        if 'G:' in name:
            g_avg = avg
        print(f"{name:<22s} {avg:+6.2f}% {median:+6.2f}% {std:>7.2f} "
              f"{min(rets):+6.1f}% {max(rets):+6.1f}%")

    # 인접 변형의 평균 차이
    print(f"\nG와의 차이:")
    for name, avg, *_ in results:
        if 'G:' not in name:
            diff = avg - g_avg
            marker = ' OK' if abs(diff) <= 2.0 else ' WARN'
            print(f"  {name:<22s}: {diff:+5.2f}%p{marker}")

    # 인접 변형 중 G와 ±2%p 이내인 것 카운트
    near_count = sum(1 for n, a, *_ in results
                     if 'G:' not in n and abs(a - g_avg) <= 2.0)
    total = len(results) - 1
    print(f"\nG와 ±2%p 이내: {near_count}/{total}")
    if near_count >= total * 0.7:
        print("[OK] 인접 안정성 양호 - G는 운이 아님")
    else:
        print("[!] 인접 변형 차이 큼 - 과적합 의심")


def walkforward_test(dates, data):
    """Walk-forward: 30일 학습 + 11일 검증"""
    print("\n" + "=" * 70)
    print("Test 2: Walk-Forward (30일 학습 + 11일 검증)")
    print("=" * 70)

    n_total = len(dates)
    train_end = 30  # 30일까지 학습
    train_dates = dates[:train_end]
    test_dates = dates[train_end:]

    print(f"\n학습 기간: {train_dates[0]} ~ {train_dates[-1]} ({len(train_dates)}일)")
    print(f"검증 기간: {test_dates[0]} ~ {test_dates[-1]} ({len(test_dates)}일)")

    variants = [
        ('A: E5/X12/S3 (현재)', 5, 12, 3),
        ('B: E3/X12/S3', 3, 12, 3),
        ('D: E3/X10/S3', 3, 10, 3),
        ('F: E3/X12/S2', 3, 12, 2),
        ('★ G: E3/X10/S2', 3, 10, 2),
        ('I: E3/X8/S2', 3, 8, 2),
        ('E2/X10/S2', 2, 10, 2),
        ('E3/X10/S1', 3, 10, 1),
    ]

    # 학습기간 single backtest
    print(f"\n=== 학습기간 (전체 데이터로 single) ===")
    print(f"{'Variant':<22s} {'학습수익':>9s} {'학습MDD':>8s}")
    print("-" * 45)
    train_results = []
    for name, e, x, s in variants:
        r = simulate(train_dates, data, e, x, s)
        train_results.append((name, e, x, s, r['total_return'], r['max_dd']))
        print(f"{name:<22s} {r['total_return']:+8.2f}% {r['max_dd']:+7.2f}%")

    # 학습기간 1등
    train_results.sort(key=lambda x: -x[4])
    train_best = train_results[0]
    print(f"\n학습기간 1등: {train_best[0]} ({train_best[4]:+.2f}%)")

    # 검증기간 single backtest
    # 검증기간은 학습기간 종료 시점부터 시작 (포트폴리오 빈 상태)
    print(f"\n=== 검증기간 (학습기간 이후) ===")
    print(f"{'Variant':<22s} {'검증수익':>9s} {'검증MDD':>8s}")
    print("-" * 45)
    test_results = []
    for name, e, x, s in variants:
        # 검증기간 시작일을 start_date로 줘서 시뮬레이션
        # 단, multistart 함수의 start_date는 시작일 이전 데이터로 consecutive를 채움
        # 검증기간은 train_dates 끝 다음 날부터 시작 (시작일 = test_dates[0])
        r = simulate(dates, data, e, x, s, start_date=test_dates[0])
        test_results.append((name, e, x, s, r['total_return'], r['max_dd']))
        print(f"{name:<22s} {r['total_return']:+8.2f}% {r['max_dd']:+7.2f}%")

    # 검증기간 순위
    test_results.sort(key=lambda x: -x[4])
    test_best = test_results[0]
    print(f"\n검증기간 1등: {test_best[0]} ({test_best[4]:+.2f}%)")

    # 학습 1등이 검증에서 몇 위?
    train_best_name = train_best[0]
    test_rank_of_train_best = next(
        (i + 1 for i, r in enumerate(test_results) if r[0] == train_best_name), -1
    )
    print(f"\n학습 1등 ({train_best_name})의 검증기간 순위: {test_rank_of_train_best}/{len(variants)}")

    if test_rank_of_train_best <= 3:
        print("[OK] 학습 1등이 검증기간 Top 3 -robust")
    else:
        print("[!] 학습 1등이 검증기간에서 부진 -과적합 가능성")

    # G의 학습 vs 검증 순위
    g_train_rank = next((i+1 for i, r in enumerate(train_results) if '★' in r[0]), -1)
    g_test_rank = next((i+1 for i, r in enumerate(test_results) if '★' in r[0]), -1)
    print(f"\nG의 학습기간 순위: {g_train_rank}/{len(variants)}")
    print(f"G의 검증기간 순위: {g_test_rank}/{len(variants)}")


def main():
    print("S2.5: G(E3/X10/S2) Robustness 검증\n")
    dates, data = load_data()
    print(f"기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)\n")

    adjacency_test(dates, data)
    walkforward_test(dates, data)


if __name__ == '__main__':
    main()
