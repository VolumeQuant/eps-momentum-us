"""정밀 백테스트 v6 - E3/X11/S3 + strict hold 정밀 검증

검증 항목:
  1. 인접 안정성 (E3/X11/S3 + strict 주변 변형)
  2. 시작일별 분포 (운 효과)
  3. Walk-forward에서 검증 1등 빈도
  4. 다른 hold 변형도 strict로 통일했을 때 효과
"""
import sys
from collections import defaultdict
from bt_engine import load_data, simulate

HOLD_STRICT = {'lookback_days': 20, 'price_threshold': 25, 'rev_up_ratio': 0.4,
               'check_ma60': True, 'max_grace': 2}


def multistart(dates, data, e, x, s, hold_params=None, start_step=2):
    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    samples = start_dates[::start_step]
    rets, mdds = [], []
    for sd in samples:
        r = simulate(dates, data, e, x, s, start_date=sd, hold_params=hold_params)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    n = len(rets)
    avg = sum(rets) / n
    sorted_rets = sorted(rets)
    return {
        'avg_ret': round(avg, 2),
        'med_ret': round(sorted_rets[n // 2], 2),
        'min_ret': round(min(rets), 2),
        'max_ret': round(max(rets), 2),
        'std_ret': round((sum((r - avg) ** 2 for r in rets) / n) ** 0.5, 2),
        'avg_mdd': round(sum(mdds) / n, 2),
        'worst_mdd': round(min(mdds), 2),
        'risk_adj': round(avg / abs(min(mdds)), 2) if min(mdds) != 0 else 0,
        'all_rets': rets,
        'all_starts': samples,
    }


def step1_adjacency_with_hold(dates, data):
    """E3/X11/S3 + strict 주변 인접 변형 (모두 strict hold)"""
    print("=" * 130)
    print("Step 1: E3/X11/S3 + strict hold 인접 안정성")
    print("=" * 130)
    print()

    center = (3, 11, 3)
    variants = []
    for de in [-1, 0, 1]:
        for dx in [-1, 0, 1]:
            for ds in [-1, 0, 1]:
                e, x, s = center[0] + de, center[1] + dx, center[2] + ds
                if e < 2 or x <= e or s < 1 or s > 5:
                    continue
                variants.append((e, x, s))

    rows = []
    for e, x, s in variants:
        m = multistart(dates, data, e, x, s, hold_params=HOLD_STRICT)
        rows.append((f"E{e}/X{x}/S{s}", e, x, s, m))

    rows.sort(key=lambda r: -r[4]['avg_ret'])
    print(f"{'Config':<14s} {'avg':>7s} {'med':>7s} {'min':>7s} {'max':>7s} "
          f"{'std':>5s} {'MDDavg':>7s} {'MDDworst':>9s} {'위험조정':>8s}")
    print("-" * 90)
    for name, e, x, s, m in rows:
        marker = ' ★' if (e, x, s) == center else '  '
        print(f"{marker}{name:<12s} {m['avg_ret']:+6.1f}% {m['med_ret']:+6.1f}% "
              f"{m['min_ret']:+6.1f}% {m['max_ret']:+6.1f}% {m['std_ret']:>4.1f} "
              f"{m['avg_mdd']:+6.1f}% {m['worst_mdd']:+8.1f}% {m['risk_adj']:>7.2f}")

    # 차이 분석
    center_m = next(m for n, e, x, s, m in rows if (e, x, s) == center)
    print(f"\n중심(E3/X11/S3 + strict)과의 차이:")
    near_count = 0
    for name, e, x, s, m in rows:
        if (e, x, s) == center:
            continue
        diff = m['avg_ret'] - center_m['avg_ret']
        marker = 'OK' if abs(diff) <= 2.0 else 'WARN'
        if abs(diff) <= 2.0:
            near_count += 1
        print(f"  {name:<14s} diff {diff:+5.2f}%p {marker}")
    print(f"\n  ±2%p 이내: {near_count}/{len(rows) - 1}")


def step2_startdate_distribution(dates, data):
    """시작일별 수익 분포"""
    print("\n" + "=" * 130)
    print("Step 2: E3/X11/S3 + strict hold 시작일별 분포")
    print("=" * 130)

    m = multistart(dates, data, 3, 11, 3, hold_params=HOLD_STRICT, start_step=1)

    print(f"\n총 시작일: {len(m['all_starts'])}개")
    print(f"평균: {m['avg_ret']:+.2f}%, 중앙: {m['med_ret']:+.2f}%")
    print(f"min: {m['min_ret']:+.2f}%, max: {m['max_ret']:+.2f}%")
    print(f"표준편차: {m['std_ret']:.2f}")
    print(f"MDD avg: {m['avg_mdd']:+.2f}%, worst: {m['worst_mdd']:+.2f}%")
    print(f"위험조정: {m['risk_adj']:.2f}")

    print(f"\n시작일별 결과:")
    for sd, ret in zip(m['all_starts'], m['all_rets']):
        bar = '#' * int(max(0, ret) / 2)
        print(f"  {sd}: {ret:+6.1f}% {bar}")

    # 분포 통계
    rets = m['all_rets']
    n = len(rets)
    pos = sum(1 for r in rets if r > 0)
    neg = sum(1 for r in rets if r < 0)
    zero = sum(1 for r in rets if r == 0)
    print(f"\n양수: {pos}/{n} ({pos/n*100:.0f}%), 음수: {neg}/{n}, 0: {zero}/{n}")
    print(f"평균 - 1std: {m['avg_ret'] - m['std_ret']:+.2f}%")
    print(f"평균 + 1std: {m['avg_ret'] + m['std_ret']:+.2f}%")


def step3_walkforward_winner(dates, data):
    """후보 5개 walk-forward 검증 1등 빈도"""
    print("\n" + "=" * 130)
    print("Step 3: Walk-Forward (with strict hold)")
    print("=" * 130)

    splits = [(15, 26), (20, 21), (25, 16), (30, 11), (35, 6)]

    # hold 변형은 기본 변형과 strict 둘 다
    variants = [
        ('A: E5/X12/S3', 5, 12, 3, None),
        ('E3/X11/S3 (no hold)', 3, 11, 3, None),
        ('★E3/X11/S3 + strict', 3, 11, 3, HOLD_STRICT),
        ('E3/X11/S2 + strict', 3, 11, 2, HOLD_STRICT),
        ('G E3/X10/S2 + strict', 3, 10, 2, HOLD_STRICT),
        ('E3/X10/S3 + strict', 3, 10, 3, HOLD_STRICT),
    ]

    train_winners = defaultdict(int)
    test_winners = defaultdict(int)
    train_best_in_test = defaultdict(int)

    for train_n, test_n in splits:
        train_dates = dates[:train_n]
        test_start = dates[train_n]

        train_results = []
        test_results = []
        for name, e, x, s, hp in variants:
            tr = simulate(train_dates, data, e, x, s, hold_params=hp)
            te = simulate(dates, data, e, x, s, start_date=test_start, hold_params=hp)
            train_results.append((name, tr['total_return']))
            test_results.append((name, te['total_return']))

        train_results.sort(key=lambda x: -x[1])
        test_results.sort(key=lambda x: -x[1])

        train_winners[train_results[0][0]] += 1
        test_winners[test_results[0][0]] += 1

        # 학습 1등이 검증 Top 3?
        train_best = train_results[0][0]
        test_rank = next((i+1 for i, r in enumerate(test_results) if r[0] == train_best), -1)
        if test_rank <= 3:
            train_best_in_test[train_best] += 1

        print(f"\n[Split {train_n}/{test_n}] train={train_dates[-1]}")
        print(f"  학습 1등: {train_results[0][0]:<25s} {train_results[0][1]:+6.1f}%")
        print(f"  검증 1등: {test_results[0][0]:<25s} {test_results[0][1]:+6.1f}%")
        print(f"  학습 1등 검증 순위: {test_rank}/{len(variants)}")

    print(f"\n학습 1등 빈도:")
    for name, cnt in sorted(train_winners.items(), key=lambda x: -x[1]):
        print(f"  {name}: {cnt}/{len(splits)}")

    print(f"\n검증 1등 빈도:")
    for name, cnt in sorted(test_winners.items(), key=lambda x: -x[1]):
        print(f"  {name}: {cnt}/{len(splits)}")


def main():
    dates, data = load_data()
    print(f"기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")

    step1_adjacency_with_hold(dates, data)
    step2_startdate_distribution(dates, data)
    step3_walkforward_winner(dates, data)


if __name__ == '__main__':
    main()
