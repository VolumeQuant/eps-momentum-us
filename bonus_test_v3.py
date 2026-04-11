"""V9 주변 인접 안정성 + 결합 탐구

V9: rev_growth >= 0.30이면 +0.2 (가장 좋음)
탐구:
  - 임계값 변형 (20%, 25%, 35%, 40%)
  - 보너스 크기 변형 (+0.1, +0.15, +0.25, +0.3)
  - V9 + 시너지 결합
"""
import sys
sys.path.insert(0, '.')
from bonus_test import make_test_db, multistart, regenerate_with_rev
from bt_engine import load_data


def make_simple_rev(threshold, bonus):
    """단순 rev 보너스 (V9 family)"""
    def conv(adj_gap, rev_up=None, num_analysts=None,
              ntm_current=None, ntm_90d=None, rev_growth=None):
        if adj_gap is None:
            return None
        ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
        eps_floor = 0
        if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
            eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
        base = max(ratio, eps_floor)
        rev_bonus = bonus if (rev_growth is not None and rev_growth >= threshold) else 0
        return adj_gap * (1 + base + rev_bonus)
    return conv


def conv_baseline(adj_gap, rev_up=None, num_analysts=None,
                   ntm_current=None, ntm_90d=None, rev_growth=None):
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    return adj_gap * (1 + max(ratio, eps_floor))


def conv_v9_plus_v7(adj_gap, rev_up=None, num_analysts=None,
                      ntm_current=None, ntm_90d=None, rev_growth=None):
    """V9 + V7 결합: simple_rev + synergy"""
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    base = max(ratio, eps_floor)
    rev_bonus = 0.2 if (rev_growth is not None and rev_growth >= 0.30) else 0
    syn = 0.15 if (rev_growth is not None and ratio >= 0.4 and rev_growth >= 0.25) else 0
    return adj_gap * (1 + base + rev_bonus + syn)


def main():
    print("=" * 110)
    print("V9 주변 인접 안정성 탐구")
    print("=" * 110)

    # V9 grid: threshold × bonus
    grid = [
        # baseline
        ('V1_baseline', conv_baseline),

        # threshold 변형 (bonus +0.2 고정)
        ('V9a_th20_b20', make_simple_rev(0.20, 0.20)),
        ('V9b_th25_b20', make_simple_rev(0.25, 0.20)),
        ('V9_th30_b20 (best)', make_simple_rev(0.30, 0.20)),
        ('V9c_th35_b20', make_simple_rev(0.35, 0.20)),
        ('V9d_th40_b20', make_simple_rev(0.40, 0.20)),

        # bonus 변형 (threshold 0.30 고정)
        ('V9e_th30_b10', make_simple_rev(0.30, 0.10)),
        ('V9f_th30_b15', make_simple_rev(0.30, 0.15)),
        ('V9g_th30_b25', make_simple_rev(0.30, 0.25)),
        ('V9h_th30_b30', make_simple_rev(0.30, 0.30)),

        # 결합 시도
        ('V9+V7_combined', conv_v9_plus_v7),
    ]

    print(f"\n{'변형':<28s} {'avg':>7s} {'med':>7s} {'min':>7s} {'max':>7s} "
          f"{'std':>5s} {'MDD avg':>8s} {'MDD worst':>10s} {'risk_adj':>9s}")
    print("-" * 100)

    results = {}
    for name, conv_fn in grid:
        suffix = name.lower().replace('+', '_').replace('(', '').replace(')', '').replace(' ', '_')[:30]
        db = make_test_db(suffix, conv_fn)
        ds, data = load_data(db)
        m = multistart(ds, data)
        results[name] = m
        print(f"{name:<28s} {m['avg']:+6.1f}% {m['med']:+6.1f}% {m['min']:+6.1f}% "
              f"{m['max']:+6.1f}% {m['std']:>4.1f} {m['mdd_avg']:+7.1f}% "
              f"{m['mdd_worst']:+9.1f}% {m['risk_adj']:>8.2f}")

    print("\n[차분 vs baseline]")
    base = results['V1_baseline']
    sorted_r = sorted(results.items(), key=lambda x: -x[1]['avg'])
    for name, m in sorted_r:
        if name == 'V1_baseline':
            continue
        ret_diff = m['avg'] - base['avg']
        risk_diff = m['risk_adj'] - base['risk_adj']
        marker = ' ⭐⭐' if ret_diff > 1.5 else (' ⭐' if ret_diff > 0.5 else (' ⚠️' if ret_diff < -0.5 else ''))
        print(f"  {name:<28s} ret {ret_diff:+5.2f}%p, risk_adj {risk_diff:+5.2f}{marker}")


if __name__ == '__main__':
    main()
