"""Breakout Hold 조건 × Grace days 그리드 서치

목표: 조건 5단계 × grace 5단계 = 25 변형의 sweet spot 발견

조건 정의 (까다로운 → 관대):
  V0 매우엄격: 25일 +30%, 0.5 ratio, MA60 체크
  V1 엄격(현재): 20일 +25%, 0.4 ratio, MA60 체크
  V2 중간a:    15일 +20%, 0.35 ratio, MA60 체크
  V3 중간b:    10일 +15%, 0.3 ratio, MA60 체크
  V4 관대:     7일 +12%, 0.25 ratio, MA60 체크

Grace days: 1, 2, 3, 5, 10
"""
import sys
from collections import defaultdict
from bt_engine import load_data, simulate


# 조건 5단계
HOLD_CONDITIONS = {
    'V0_매우엄격': {'lookback_days': 25, 'price_threshold': 30, 'rev_up_ratio': 0.5, 'check_ma60': True},
    'V1_엄격(현재)': {'lookback_days': 20, 'price_threshold': 25, 'rev_up_ratio': 0.4, 'check_ma60': True},
    'V2_중간a': {'lookback_days': 15, 'price_threshold': 20, 'rev_up_ratio': 0.35, 'check_ma60': True},
    'V3_중간b': {'lookback_days': 10, 'price_threshold': 15, 'rev_up_ratio': 0.3, 'check_ma60': True},
    'V4_관대': {'lookback_days': 7, 'price_threshold': 12, 'rev_up_ratio': 0.25, 'check_ma60': True},
}

GRACE_DAYS = [1, 2, 3, 5, 10]


def make_params(cond_name, grace):
    cond = HOLD_CONDITIONS[cond_name]
    return dict(cond, max_grace=grace)


def multistart_test(dates, data, hold_params):
    """multistart 백테스트"""
    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    rets, mdds, holds, trades = [], [], [], []
    for sd in start_dates:
        r = simulate(dates, data, 3, 11, 3, hold_params=hold_params, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
        holds.append(r['breakout_holds'])
        trades.append(r['n_trades'])

    n = len(rets)
    avg = sum(rets) / n
    return {
        'avg_ret': round(avg, 2),
        'min_ret': round(min(rets), 2),
        'max_ret': round(max(rets), 2),
        'avg_mdd': round(sum(mdds) / n, 2),
        'worst_mdd': round(min(mdds), 2),
        'risk_adj': round(avg / abs(min(mdds)), 2) if min(mdds) != 0 else 0,
        'avg_trigger': round(sum(holds) / n, 2),
        'avg_trades': round(sum(trades) / n, 1),
    }


def step1_sample_test(dates, data):
    """표본 테스트: 핵심 4 조합만 먼저"""
    print("=" * 110)
    print("Step 1: 표본 테스트 (대표 4 조합)")
    print("=" * 110)

    samples = [
        ('no_hold', None),
        ('V1_엄격 + grace 2d (현재 v74)', make_params('V1_엄격(현재)', 2)),
        ('V3_중간b + grace 2d', make_params('V3_중간b', 2)),
        ('V3_중간b + grace 5d', make_params('V3_중간b', 5)),
    ]

    print(f"\n{'변형':<30s} {'평균':>8s} {'min':>8s} {'MDDworst':>9s} "
          f"{'위험조정':>8s} {'avg트리거':>9s}")
    print("-" * 80)
    for name, hp in samples:
        m = multistart_test(dates, data, hp)
        print(f"{name:<30s} {m['avg_ret']:+7.2f}% {m['min_ret']:+7.2f}% "
              f"{m['worst_mdd']:+8.1f}% {m['risk_adj']:>7.2f} {m['avg_trigger']:>8.2f}")

    print("\n[OK] 표본 테스트 완료, 전체 그리드 진행")


def step2_full_grid(dates, data):
    """전체 5×5 그리드"""
    print("\n" + "=" * 110)
    print("Step 2: 전체 그리드 (조건 5 × grace 5 = 25 변형)")
    print("=" * 110)

    results = {}
    print(f"\n{'조건':<14s}", end='')
    for g in GRACE_DAYS:
        print(f"{'g'+str(g):>16s}", end='')
    print()
    print("-" * (14 + 16 * len(GRACE_DAYS)))

    for cond_name in HOLD_CONDITIONS:
        print(f"{cond_name:<14s}", end='')
        for grace in GRACE_DAYS:
            hp = make_params(cond_name, grace)
            m = multistart_test(dates, data, hp)
            label = f"{m['avg_ret']:+5.1f}%/{m['risk_adj']:.2f}"
            results[(cond_name, grace)] = m
            print(f"{label:>16s}", end='')
        print()

    # no_hold baseline
    print(f"\nno_hold baseline:")
    no_hold = multistart_test(dates, data, None)
    print(f"  평균 {no_hold['avg_ret']:+.2f}%, MDD worst {no_hold['worst_mdd']:+.1f}%, "
          f"위험조정 {no_hold['risk_adj']:.2f}")

    return results, no_hold


def step3_top_variants(results, no_hold):
    """Top 변형 정밀 분석"""
    print("\n" + "=" * 110)
    print("Step 3: Top 5 변형 정밀 분석")
    print("=" * 110)

    sorted_r = sorted(results.items(), key=lambda x: -x[1]['avg_ret'])

    print(f"\n[수익률 Top 5]")
    print(f"{'변형':<30s} {'평균':>8s} {'min':>8s} {'max':>8s} "
          f"{'MDDavg':>8s} {'MDDworst':>9s} {'위험조정':>8s} {'트리거':>7s}")
    print("-" * 100)
    for (cond, grace), m in sorted_r[:5]:
        label = f"{cond} g{grace}"
        print(f"{label:<30s} {m['avg_ret']:+7.2f}% {m['min_ret']:+7.2f}% "
              f"{m['max_ret']:+7.2f}% {m['avg_mdd']:+7.1f}% {m['worst_mdd']:+8.1f}% "
              f"{m['risk_adj']:>7.2f} {m['avg_trigger']:>6.2f}")

    print(f"\n[위험조정 Top 5]")
    sorted_risk = sorted(results.items(), key=lambda x: -x[1]['risk_adj'])
    for (cond, grace), m in sorted_risk[:5]:
        label = f"{cond} g{grace}"
        print(f"{label:<30s} {m['avg_ret']:+7.2f}% {m['min_ret']:+7.2f}% "
              f"{m['max_ret']:+7.2f}% {m['avg_mdd']:+7.1f}% {m['worst_mdd']:+8.1f}% "
              f"{m['risk_adj']:>7.2f} {m['avg_trigger']:>6.2f}")

    # vs no_hold baseline
    print(f"\n[vs no_hold (+{no_hold['avg_ret']:.2f}%)]")
    above_baseline = [(k, v) for k, v in results.items()
                       if v['avg_ret'] > no_hold['avg_ret']]
    below_baseline = [(k, v) for k, v in results.items()
                       if v['avg_ret'] < no_hold['avg_ret']]
    print(f"  baseline 초과: {len(above_baseline)}/25")
    print(f"  baseline 미달: {len(below_baseline)}/25")

    if above_baseline:
        best = max(above_baseline, key=lambda x: x[1]['avg_ret'])
        print(f"\n  최고 향상: {best[0][0]} g{best[0][1]} = {best[1]['avg_ret']:+.2f}% "
              f"(+{best[1]['avg_ret'] - no_hold['avg_ret']:.2f}%p)")


def main():
    dates, data = load_data()
    print(f"기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")

    step1_sample_test(dates, data)
    results, no_hold = step2_full_grid(dates, data)
    step3_top_variants(results, no_hold)


if __name__ == '__main__':
    main()
