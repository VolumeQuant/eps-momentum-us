"""최종 통합 검증 + 결과 정리

비교:
  - 현재 (A): E5/X12/S3, base conviction, no hold
  - 최종 추천: E3/X11/S2, base conviction, strict hold
  - 차순위: E3/X11/S3, base conviction, strict hold
"""
import sys
import os
import shutil
from collections import defaultdict
from bt_engine import load_data, simulate

DB_ORIGINAL = 'eps_momentum_data.db'

HOLD_STRICT = {'lookback_days': 20, 'price_threshold': 25, 'rev_up_ratio': 0.4,
               'check_ma60': True, 'max_grace': 2}


def full_metrics(dates, data, e, x, s, hold_params=None):
    """모든 metric 측정"""
    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    samples = start_dates[::1]  # 모든 시작일

    rets, mdds, sharpes, sortinos = [], [], [], []
    for sd in samples:
        r = simulate(dates, data, e, x, s, start_date=sd, hold_params=hold_params)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
        sharpes.append(r['sharpe'])
        sortinos.append(r['sortino'])

    n = len(rets)
    avg_ret = sum(rets) / n
    sorted_rets = sorted(rets)
    median_ret = sorted_rets[n // 2]
    std_ret = (sum((r - avg_ret) ** 2 for r in rets) / n) ** 0.5
    avg_mdd = sum(mdds) / n
    worst_mdd = min(mdds)
    avg_sharpe = sum(sharpes) / n
    avg_sortino = sum(sortinos) / n

    pos_count = sum(1 for r in rets if r > 0)

    return {
        'n_samples': n,
        'avg_ret': round(avg_ret, 2),
        'med_ret': round(median_ret, 2),
        'min_ret': round(min(rets), 2),
        'max_ret': round(max(rets), 2),
        'std_ret': round(std_ret, 2),
        'avg_mdd': round(avg_mdd, 2),
        'worst_mdd': round(worst_mdd, 2),
        'avg_sharpe': round(avg_sharpe, 2),
        'avg_sortino': round(avg_sortino, 2),
        'risk_adj': round(avg_ret / abs(worst_mdd), 2) if worst_mdd != 0 else 0,
        'pos_pct': round(pos_count / n * 100, 0),
    }


def print_full(label, m):
    print(f"\n{label}")
    print(f"  Multistart {m['n_samples']}개 시작일")
    print(f"  ├ 평균 수익:    {m['avg_ret']:+.2f}%")
    print(f"  ├ 중앙값:       {m['med_ret']:+.2f}%")
    print(f"  ├ 최저:         {m['min_ret']:+.2f}%")
    print(f"  ├ 최고:         {m['max_ret']:+.2f}%")
    print(f"  ├ 표준편차:     {m['std_ret']:.2f}")
    print(f"  ├ 양의 수익률:  {m['pos_pct']:.0f}%")
    print(f"  ├ MDD 평균:     {m['avg_mdd']:+.2f}%")
    print(f"  ├ MDD 최악:     {m['worst_mdd']:+.2f}%")
    print(f"  ├ Sharpe 평균:  {m['avg_sharpe']:.2f}")
    print(f"  ├ Sortino 평균: {m['avg_sortino']:.2f}")
    print(f"  └ 위험조정:     {m['risk_adj']:.2f}")


def main():
    dates, data = load_data()
    print(f"기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")
    print("=" * 100)

    # 1. 현재 (A)
    m_current = full_metrics(dates, data, 5, 12, 3)
    print_full("[현재 A: E5/X12/S3 + base conviction + no hold]", m_current)

    # 2. 최종 추천: E3/X11/S2 + strict hold
    m_winner = full_metrics(dates, data, 3, 11, 2, hold_params=HOLD_STRICT)
    print_full("[★ 최종 추천: E3/X11/S2 + base conviction + STRICT hold]", m_winner)

    # 3. 차순위: E3/X11/S3 + strict hold
    m_alt = full_metrics(dates, data, 3, 11, 3, hold_params=HOLD_STRICT)
    print_full("[차순위: E3/X11/S3 + base conviction + STRICT hold]", m_alt)

    # 비교 표
    print("\n" + "=" * 100)
    print("개선 효과 (vs 현재 A)")
    print("=" * 100)

    def compare(name, m):
        print(f"\n{name}:")
        print(f"  평균 수익:    {m['avg_ret']:+.2f}%  ({m['avg_ret'] - m_current['avg_ret']:+.2f}%p)")
        print(f"  최저:         {m['min_ret']:+.2f}%  ({m['min_ret'] - m_current['min_ret']:+.2f}%p)")
        print(f"  표준편차:     {m['std_ret']:.2f}    ({m['std_ret'] - m_current['std_ret']:+.2f})")
        print(f"  MDD 최악:     {m['worst_mdd']:+.2f}%  ({m['worst_mdd'] - m_current['worst_mdd']:+.2f}%p)")
        print(f"  위험조정:     {m['risk_adj']:.2f}    ({m['risk_adj'] - m_current['risk_adj']:+.2f})")

    compare("E3/X11/S2 + strict", m_winner)
    compare("E3/X11/S3 + strict", m_alt)


if __name__ == '__main__':
    main()
