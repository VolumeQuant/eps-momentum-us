"""EPS/매출 전망 보너스 - 더 깊은 탐구 (max 아닌 add 방식 위주)

첫 라운드 인사이트: max() 안에 신호 추가는 효과 없음 (cap에 묻힘)
→ 모든 변형은 add 방식으로 진행

탐구 변형:
  V1: baseline (현재 v74)
  V6: ratio + eps_floor + rev_floor 모두 add (cap 1.5)
  V7: synergy 약함 (ratio≥0.4 & rev≥0.25)
  V8: synergy 강함 (ratio≥0.7 & rev≥0.50)
  V9: 단순 rev 보너스 (rev>=0.30이면 +0.2)
  V10: EPS+rev 곱 (ratio*rev_factor)
  V11: 단계별 보너스 (rev 30/50/100 → 0.1/0.2/0.3)
  V12: 매우 공격적 (cap 2.0까지)
"""
import sys
import os
import shutil
sys.path.insert(0, '.')
from bonus_test import (
    regenerate_with_rev, make_test_db, multistart, HOLD_STRICT
)
from bt_engine import load_data


def conv_baseline(adj_gap, rev_up=None, num_analysts=None,
                   ntm_current=None, ntm_90d=None, rev_growth=None):
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    conviction = max(ratio, eps_floor)
    return adj_gap * (1 + conviction)


def conv_v6_all_add(adj_gap, rev_up=None, num_analysts=None,
                     ntm_current=None, ntm_90d=None, rev_growth=None):
    """V6: ratio + eps_floor + rev_floor 모두 add (cap 1.5)"""
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 0.5)
    rev_floor = 0
    if rev_growth is not None:
        rev_floor = min(max(0, rev_growth - 0.10), 0.5)  # 10%부터 시작
    conviction = min(ratio + eps_floor + rev_floor, 1.5)
    return adj_gap * (1 + conviction)


def conv_v7_weak_synergy(adj_gap, rev_up=None, num_analysts=None,
                          ntm_current=None, ntm_90d=None, rev_growth=None):
    """V7: 시너지 약함 (ratio≥0.4 & rev≥0.25)"""
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    base = max(ratio, eps_floor)
    syn = 0.3 if (rev_growth is not None and ratio >= 0.4 and rev_growth >= 0.25) else 0
    return adj_gap * (1 + base + syn)


def conv_v8_strong_synergy(adj_gap, rev_up=None, num_analysts=None,
                            ntm_current=None, ntm_90d=None, rev_growth=None):
    """V8: 시너지 강함 (ratio≥0.7 & rev≥0.50, +0.5)"""
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    base = max(ratio, eps_floor)
    syn = 0.5 if (rev_growth is not None and ratio >= 0.7 and rev_growth >= 0.50) else 0
    return adj_gap * (1 + base + syn)


def conv_v9_simple_rev(adj_gap, rev_up=None, num_analysts=None,
                        ntm_current=None, ntm_90d=None, rev_growth=None):
    """V9: 단순 rev 보너스 (rev>=0.30이면 +0.2)"""
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    base = max(ratio, eps_floor)
    bonus = 0.2 if (rev_growth is not None and rev_growth >= 0.30) else 0
    return adj_gap * (1 + base + bonus)


def conv_v10_multiplicative(adj_gap, rev_up=None, num_analysts=None,
                              ntm_current=None, ntm_90d=None, rev_growth=None):
    """V10: rev_growth를 multiplier로 (1 + base) * (1 + rev_factor)"""
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    base = max(ratio, eps_floor)
    rev_factor = min(max(0, (rev_growth or 0) - 0.10) * 0.5, 0.3)
    return adj_gap * (1 + base) * (1 + rev_factor)


def conv_v11_tiered(adj_gap, rev_up=None, num_analysts=None,
                      ntm_current=None, ntm_90d=None, rev_growth=None):
    """V11: 단계별 보너스 (rev 30/50/100 → 0.1/0.2/0.3)"""
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    base = max(ratio, eps_floor)
    rev_bonus = 0
    if rev_growth is not None:
        if rev_growth >= 1.00:
            rev_bonus = 0.3
        elif rev_growth >= 0.50:
            rev_bonus = 0.2
        elif rev_growth >= 0.30:
            rev_bonus = 0.1
    return adj_gap * (1 + base + rev_bonus)


def conv_v12_aggressive(adj_gap, rev_up=None, num_analysts=None,
                          ntm_current=None, ntm_90d=None, rev_growth=None):
    """V12: 매우 공격적 (cap 2.0)"""
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    rev_floor = min(max(0, (rev_growth or 0)), 1.0)
    conviction = min(ratio + eps_floor + rev_floor, 2.0)
    return adj_gap * (1 + conviction)


def main():
    print("=" * 110)
    print("EPS/매출 전망 보너스 — 깊은 탐구 (V6 ~ V12)")
    print("=" * 110)

    variants = {
        'V1_baseline': conv_baseline,
        'V6_all_add (ratio+eps+rev)': conv_v6_all_add,
        'V7_weak_synergy (0.4&0.25,+0.3)': conv_v7_weak_synergy,
        'V8_strong_synergy (0.7&0.50,+0.5)': conv_v8_strong_synergy,
        'V9_simple_rev (>=30%,+0.2)': conv_v9_simple_rev,
        'V10_multiplicative': conv_v10_multiplicative,
        'V11_tiered (30/50/100)': conv_v11_tiered,
        'V12_aggressive (cap 2.0)': conv_v12_aggressive,
    }

    print(f"\n[1] 8 변형 백테스트\n")
    print(f"{'변형':<38s} {'avg':>7s} {'med':>7s} {'min':>7s} {'max':>7s} "
          f"{'std':>5s} {'MDD avg':>8s} {'MDD worst':>10s} {'risk_adj':>9s}")
    print("-" * 110)

    results = {}
    for name, conv_fn in variants.items():
        suffix = name.split('_')[0].lower() + '_v2'
        db = make_test_db(suffix, conv_fn)
        ds, data = load_data(db)
        m = multistart(ds, data)
        results[name] = m
        print(f"{name:<38s} {m['avg']:+6.1f}% {m['med']:+6.1f}% {m['min']:+6.1f}% "
              f"{m['max']:+6.1f}% {m['std']:>4.1f} {m['mdd_avg']:+7.1f}% "
              f"{m['mdd_worst']:+9.1f}% {m['risk_adj']:>8.2f}")

    # 차분 측정
    print("\n[2] V1 baseline 대비 차분")
    base = results['V1_baseline']
    for name, m in results.items():
        if name == 'V1_baseline':
            continue
        ret_diff = m['avg'] - base['avg']
        mdd_diff = m['mdd_worst'] - base['mdd_worst']
        risk_diff = m['risk_adj'] - base['risk_adj']
        marker = ' ⭐' if ret_diff > 0.5 and risk_diff > 0 else (' ⚠️' if ret_diff < -0.5 else '')
        print(f"  {name:<38s} ret {ret_diff:+5.2f}%p, MDD {mdd_diff:+5.2f}%p, "
              f"risk_adj {risk_diff:+5.2f}{marker}")


if __name__ == '__main__':
    main()
