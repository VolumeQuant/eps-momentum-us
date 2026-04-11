"""W9 (rev AND op) 인접 안정성 + W2 vs W9 비교

W9 조건: rev_growth >= 30% AND op_margin >= 15% → +0.3
인접:
  - rev 임계값 25/30/35/40
  - op 임계값 10/15/20/25
  - 보너스 0.2/0.25/0.3/0.35/0.4
"""
import sys
sys.path.insert(0, '.')
from bonus_test_wide import (
    conv_baseline, conv_rev_only, regenerate_full, make_test_db_full,
)
from bonus_test import multistart
from bt_engine import load_data


def make_w9(rev_th, op_th, bonus):
    def conv(adj_gap, rev_growth=None, op_margin=None, **kwargs):
        base = conv_baseline(adj_gap, **kwargs) / adj_gap - 1 if adj_gap else 0
        b = 0
        if (rev_growth is not None and rev_growth >= rev_th and
            op_margin is not None and op_margin >= op_th):
            b = bonus
        return adj_gap * (1 + base + b)
    return conv


def main():
    print("=" * 110)
    print("W9 (rev AND op) 인접 안정성")
    print("=" * 110)

    # 인접 변형: rev_th × op_th × bonus
    variants = []
    variants.append(('W1_baseline', conv_baseline))
    variants.append(('W2_V9h (rev only +0.3)', conv_rev_only))

    # rev/op 임계값 grid
    for rev_th in [0.25, 0.30, 0.35, 0.40]:
        for op_th in [0.10, 0.15, 0.20]:
            name = f'W9_r{int(rev_th*100)}_o{int(op_th*100)}_b30'
            variants.append((name, make_w9(rev_th, op_th, 0.30)))

    # 보너스 크기 변형 (rev=30%, op=15% 고정)
    for bonus in [0.20, 0.25, 0.35, 0.40]:
        name = f'W9_r30_o15_b{int(bonus*100)}'
        variants.append((name, make_w9(0.30, 0.15, bonus)))

    print(f"\n{'변형':<28s} {'avg':>7s} {'med':>7s} {'min':>7s} {'std':>5s} "
          f"{'MDD worst':>10s} {'risk_adj':>9s}")
    print("-" * 80)

    results = {}
    for name, conv_fn in variants:
        suffix = name.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('+', '_')[:30]
        db = make_test_db_full(suffix, conv_fn)
        ds, data = load_data(db)
        m = multistart(ds, data)
        results[name] = m
        print(f"{name:<28s} {m['avg']:+6.1f}% {m['med']:+6.1f}% {m['min']:+6.1f}% "
              f"{m['std']:>4.1f} {m['mdd_worst']:+9.1f}% {m['risk_adj']:>8.2f}")

    # 차분
    print("\n[차분 vs W1 baseline]")
    base = results['W1_baseline']
    sorted_r = sorted(results.items(), key=lambda x: -x[1]['avg'])
    for name, m in sorted_r:
        if name == 'W1_baseline':
            continue
        ret_diff = m['avg'] - base['avg']
        risk_diff = m['risk_adj'] - base['risk_adj']
        marker = ' ⭐⭐' if ret_diff > 1.7 else (' ⭐' if ret_diff > 0.5 else '')
        print(f"  {name:<28s} ret {ret_diff:+5.2f}%p, risk_adj {risk_diff:+5.2f}{marker}")


if __name__ == '__main__':
    main()
