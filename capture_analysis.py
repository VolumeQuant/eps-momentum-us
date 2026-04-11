"""Capture Ratio 정밀 분석

목표:
  1. v72(현재) vs v74(채택)의 capture ratio 비교
  2. 더 잡을 수 있는 알파 측정
  3. Hold grace days 변형 (2일 → 3, 5)
  4. 다른 capture 개선 메커니즘 탐색
"""
import sys
from collections import defaultdict
from bt_engine import load_data, simulate

HOLD_STRICT = {'lookback_days': 20, 'price_threshold': 25, 'rev_up_ratio': 0.4,
               'check_ma60': True, 'max_grace': 2}

HOLD_STRICT_3D = dict(HOLD_STRICT, max_grace=3)
HOLD_STRICT_5D = dict(HOLD_STRICT, max_grace=5)
HOLD_STRICT_10D = dict(HOLD_STRICT, max_grace=10)


def measure_capture(dates, data, e, x, s, hold_params=None,
                     start_date=None, lookforward=60):
    """단일 시뮬에서 closed trade들의 capture ratio 계산

    Args:
        lookforward: 진입 후 N일 동안 max gain 추적

    Returns:
        list of dicts with capture info per trade
    """
    r = simulate(dates, data, e, x, s, start_date=start_date,
                 hold_params=hold_params)

    trades = r['trades']
    captures = []

    for t in trades:
        tk = t['ticker']
        entry_date = t['entry_date']
        entry_price = t['entry_price']
        exit_date = t['exit_date']
        realized = t['return']

        try:
            entry_idx = dates.index(entry_date)
        except ValueError:
            continue

        # 진입 후 lookforward 일 동안 max 가격
        end_idx = min(entry_idx + lookforward, len(dates))
        max_price = entry_price
        for j in range(entry_idx, end_idx):
            d = dates[j]
            p = data.get(d, {}).get(tk, {}).get('price')
            if p:
                max_price = max(max_price, p)

        max_gain = (max_price - entry_price) / entry_price * 100
        capture = realized / max_gain if max_gain > 0 else (1.0 if max_gain == 0 else 0)

        captures.append({
            'ticker': tk,
            'entry_date': entry_date,
            'exit_date': exit_date,
            'realized': realized,
            'max_gain': max_gain,
            'capture': capture * 100,  # %
            'missed_alpha': max_gain - realized,
        })

    return captures, r


def aggregate_captures(captures_list):
    """여러 시작일의 capture 결과 종합"""
    all_captures = []
    for cs in captures_list:
        all_captures.extend(cs)

    if not all_captures:
        return None

    n = len(all_captures)
    avg_cap = sum(c['capture'] for c in all_captures) / n
    avg_realized = sum(c['realized'] for c in all_captures) / n
    avg_max = sum(c['max_gain'] for c in all_captures) / n
    avg_missed = sum(c['missed_alpha'] for c in all_captures) / n

    # 큰 알파 놓친 거래 (max_gain > 30%)
    big_alpha = [c for c in all_captures if c['max_gain'] > 30]
    big_alpha_cap = (sum(c['capture'] for c in big_alpha) / len(big_alpha)
                     if big_alpha else 0)

    return {
        'n_trades': n,
        'avg_capture': round(avg_cap, 1),
        'avg_realized': round(avg_realized, 2),
        'avg_max_gain': round(avg_max, 2),
        'avg_missed': round(avg_missed, 2),
        'n_big_alpha': len(big_alpha),
        'big_alpha_capture': round(big_alpha_cap, 1),
    }


def compare_variants(dates, data):
    """주요 변형 capture 비교"""
    print("=" * 100)
    print("Capture Ratio 비교 (33개 시작일 multistart)")
    print("=" * 100)

    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]

    variants = [
        ('v72 (E5/X12/S3, no hold)', 5, 12, 3, None),
        ('v74 (E3/X11/S3 + strict)', 3, 11, 3, HOLD_STRICT),
        ('v74 + grace 3d', 3, 11, 3, HOLD_STRICT_3D),
        ('v74 + grace 5d', 3, 11, 3, HOLD_STRICT_5D),
        ('v74 + grace 10d', 3, 11, 3, HOLD_STRICT_10D),
    ]

    print(f"\n{'변형':<30s} {'거래수':>6s} {'평균실현':>9s} {'평균max':>9s} "
          f"{'평균capture':>11s} {'대형알파':>9s} {'대형capture':>11s}")
    print("-" * 100)

    results = {}
    for name, e, x, s, hp in variants:
        captures_list = []
        rets = []
        for sd in start_dates:
            cs, r = measure_capture(dates, data, e, x, s, hold_params=hp,
                                     start_date=sd)
            captures_list.append(cs)
            rets.append(r['total_return'])

        agg = aggregate_captures(captures_list)
        if agg:
            avg_ret = sum(rets) / len(rets)
            results[name] = (agg, avg_ret)
            print(f"{name:<30s} {agg['n_trades']:>6d} "
                  f"{agg['avg_realized']:+8.2f}% {agg['avg_max_gain']:+8.2f}% "
                  f"{agg['avg_capture']:>10.1f}% {agg['n_big_alpha']:>8d} "
                  f"{agg['big_alpha_capture']:>10.1f}%")
            print(f"  → 평균 수익: {avg_ret:+.2f}%")

    # v72 vs v74 향상 분석
    if 'v72 (E5/X12/S3, no hold)' in results and 'v74 (E3/X11/S3 + strict)' in results:
        v72_agg, v72_ret = results['v72 (E5/X12/S3, no hold)']
        v74_agg, v74_ret = results['v74 (E3/X11/S3 + strict)']

        print(f"\n=== v72 → v74 변화 ===")
        print(f"  평균 capture: {v72_agg['avg_capture']:.1f}% → {v74_agg['avg_capture']:.1f}% "
              f"({v74_agg['avg_capture'] - v72_agg['avg_capture']:+.1f}%p)")
        print(f"  평균 실현 수익: {v72_agg['avg_realized']:+.2f}% → {v74_agg['avg_realized']:+.2f}% "
              f"({v74_agg['avg_realized'] - v72_agg['avg_realized']:+.2f}%p)")
        print(f"  평균 놓친 알파: {v72_agg['avg_missed']:+.2f}% → {v74_agg['avg_missed']:+.2f}% "
              f"({v74_agg['avg_missed'] - v72_agg['avg_missed']:+.2f}%p)")
        print(f"  포트폴리오 수익: {v72_ret:+.2f}% → {v74_ret:+.2f}% "
              f"({v74_ret - v72_ret:+.2f}%p)")


def explore_extended_hold(dates, data):
    """더 긴 grace days 효과 정밀 검증"""
    print("\n" + "=" * 100)
    print("Hold grace days 효과 정밀 검증")
    print("=" * 100)

    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]

    grace_variants = [
        ('no hold', None),
        ('grace 1d', dict(HOLD_STRICT, max_grace=1)),
        ('grace 2d (현재)', HOLD_STRICT),
        ('grace 3d', HOLD_STRICT_3D),
        ('grace 5d', HOLD_STRICT_5D),
        ('grace 10d', HOLD_STRICT_10D),
        ('unlimited (max_grace 999)', dict(HOLD_STRICT, max_grace=999)),
    ]

    print(f"\n{'변형':<28s} {'평균수익':>9s} {'min':>7s} {'max':>7s} "
          f"{'MDDworst':>9s} {'위험조정':>8s} {'평균거래':>9s}")
    print("-" * 90)

    for name, hp in grace_variants:
        rets, mdds, trades_list = [], [], []
        for sd in start_dates:
            r = simulate(dates, data, 3, 11, 3, hold_params=hp, start_date=sd)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            trades_list.append(r['n_trades'])

        avg = sum(rets) / len(rets)
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd != 0 else 0
        print(f"{name:<28s} {avg:+8.2f}% {min(rets):+6.1f}% {max(rets):+6.1f}% "
              f"{worst_mdd:+8.1f}% {risk_adj:>7.2f} {sum(trades_list)/len(trades_list):>8.1f}")


def main():
    dates, data = load_data()
    print(f"기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")

    compare_variants(dates, data)
    explore_extended_hold(dates, data)


if __name__ == '__main__':
    main()
