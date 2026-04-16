"""
맹점 체크 + 인접 안정성
1. 60d Case 1 해당 종목 수 (너무 적으면 과적합)
2. P3/P4 강도 무관 원인 (순위 변경 실제 발생 여부)
3. Top 조합에서 실제로 어떤 종목이 바뀌었는지
4. 인접 안정성 (60d 임계값 + P1 strength 주변)
"""
import pickle
import numpy as np
import sys
sys.stdout.reconfigure(encoding='utf-8')

# 결과 로드
with open('gridsearch_internal_results.pkl', 'rb') as f:
    results = pickle.load(f)

print(f"총 {len(results)} 조합\n")

# ═══════════════════════════════════════════════════════
# 1. 60d Case 1 해당 종목 수
# ═══════════════════════════════════════════════════════
print("="*90)
print("1. 기간별 Case 1 해당 종목 수")
print("="*90)

from gridsearch_internal import load_all_data, is_case1
all_dates, p2_dates, raw, chg_data, _ = load_all_data()

for period, thrs in [('7d', [(1.5, -2.0)]), ('30d', [(5.0, -5.0)]), ('60d', [(5.0, -5.0)]), ('blend', [(5.0, 0)])]:
    for nt, pt in thrs:
        total_c1 = 0
        days_with_c1 = 0
        c1_tickers_all = set()
        for d in p2_dates:
            dc = chg_data.get(d, {})
            c1_today = [tk for tk in dc if is_case1(dc[tk], period, nt, pt)]
            total_c1 += len(c1_today)
            if c1_today:
                days_with_c1 += 1
                c1_tickers_all.update(c1_today)
        print(f"  {period:>5} N{nt}/P{pt}: 총 {total_c1}건, {days_with_c1}/{len(p2_dates)}일 발생, "
              f"고유 종목 {len(c1_tickers_all)}개")
        if c1_tickers_all:
            print(f"         종목: {', '.join(sorted(c1_tickers_all)[:15])}{'...' if len(c1_tickers_all)>15 else ''}")

# ═══════════════════════════════════════════════════════
# 2. P3/P4 강도 무관 원인 — 순위 변경 실제 발생?
# ═══════════════════════════════════════════════════════
print(f"\n{'='*90}")
print("2. P3 강도별 실제 순위 변경 (E3/X8/S3, 60d N5/P-5)")
print("="*90)

from gridsearch_internal import compute_w_gap_internal

for strength in [0, 5, 15, 30]:
    changed_days = 0
    for d in p2_dates:
        tickers = [tk for tk in raw.get(d, {}) if raw[d][tk].get('comp_rank') is not None]
        wg_bl = compute_w_gap_internal(raw, chg_data, all_dates, d, tickers)
        wg_bn = compute_w_gap_internal(raw, chg_data, all_dates, d, tickers,
                                        'P3_zscore', '60d', 5.0, -5.0, strength)
        top3_bl = set(sorted(tickers, key=lambda t: wg_bl.get(t,0), reverse=True)[:3])
        top3_bn = set(sorted(tickers, key=lambda t: wg_bn.get(t,0), reverse=True)[:3])
        if top3_bl != top3_bn:
            changed_days += 1
    print(f"  P3 +{strength:>2}: Top 3 변경 {changed_days}/{len(p2_dates)}일")

print(f"\nP1 강도별:")
for strength in [0.1, 0.3, 0.5, 1.0, 1.5, 2.0]:
    changed_days = 0
    for d in p2_dates:
        tickers = [tk for tk in raw.get(d, {}) if raw[d][tk].get('comp_rank') is not None]
        wg_bl = compute_w_gap_internal(raw, chg_data, all_dates, d, tickers)
        wg_bn = compute_w_gap_internal(raw, chg_data, all_dates, d, tickers,
                                        'P1_adjgap', '60d', 5.0, -5.0, strength)
        top3_bl = set(sorted(tickers, key=lambda t: wg_bl.get(t,0), reverse=True)[:3])
        top3_bn = set(sorted(tickers, key=lambda t: wg_bn.get(t,0), reverse=True)[:3])
        if top3_bl != top3_bn:
            changed_days += 1
    print(f"  P1 ×{strength}: Top 3 변경 {changed_days}/{len(p2_dates)}일")

# ═══════════════════════════════════════════════════════
# 3. P1 60d 최적(str=1.5)에서 실제 종목 교체
# ═══════════════════════════════════════════════════════
print(f"\n{'='*90}")
print("3. P1 60d str=1.5에서 실제 종목 교체")
print("="*90)
for d in p2_dates:
    tickers = [tk for tk in raw.get(d, {}) if raw[d][tk].get('comp_rank') is not None]
    wg_bl = compute_w_gap_internal(raw, chg_data, all_dates, d, tickers)
    wg_bn = compute_w_gap_internal(raw, chg_data, all_dates, d, tickers,
                                    'P1_adjgap', '60d', 5.0, -5.0, 1.5)
    top3_bl = sorted(tickers, key=lambda t: wg_bl.get(t,0), reverse=True)[:3]
    top3_bn = sorted(tickers, key=lambda t: wg_bn.get(t,0), reverse=True)[:3]
    if set(top3_bl) != set(top3_bn):
        added = set(top3_bn) - set(top3_bl)
        removed = set(top3_bl) - set(top3_bn)
        # Case 1 종목 표시
        dc = chg_data.get(d, {})
        c1 = [tk for tk in dc if is_case1(dc[tk], '60d', 5.0, -5.0)]
        print(f"  {d}: -{','.join(removed)} +{','.join(added)} [Case1: {','.join(c1[:5])}]")

# ═══════════════════════════════════════════════════════
# 4. 인접 안정성 — 60d 임계값 주변
# ═══════════════════════════════════════════════════════
print(f"\n{'='*90}")
print("4. 인접 안정성 (P1 adjgap, E3/X8/S3)")
print("="*90)
from gridsearch_internal import simulate

center = simulate(p2_dates, raw, chg_data, all_dates, 3, 8, 3,
                  'P1_adjgap', '60d', 5.0, -5.0, 1.5)
print(f"  중심: 60d N5/P-5 str=1.5 → ret {center['ret']:+.1f}%, Sharpe {center['sharpe']:.2f}")

# 임계값 주변
print(f"\n  [임계값 변동]")
for nt, pt in [(3.0,-3.0), (5.0,-3.0), (3.0,-5.0), (5.0,-5.0), (5.0,-8.0),
               (8.0,-5.0), (8.0,-8.0), (10.0,-5.0), (10.0,-10.0)]:
    r = simulate(p2_dates, raw, chg_data, all_dates, 3, 8, 3,
                 'P1_adjgap', '60d', nt, pt, 1.5)
    print(f"    N{nt:>4}/P{pt:>5}: ret {r['ret']:+.1f}%, Sharpe {r['sharpe']:.2f}")

# strength 주변
print(f"\n  [strength 변동]")
for st in [0.3, 0.5, 0.8, 1.0, 1.2, 1.5, 2.0, 3.0, 5.0]:
    r = simulate(p2_dates, raw, chg_data, all_dates, 3, 8, 3,
                 'P1_adjgap', '60d', 5.0, -5.0, st)
    print(f"    str={st:>3}: ret {r['ret']:+.1f}%, Sharpe {r['sharpe']:.2f}")

# 기간 변동 (같은 임계값 비율)
print(f"\n  [기간 변동 (같은 조건)]")
for period, nt, pt in [('30d', 5.0, -5.0), ('60d', 5.0, -5.0), ('90d', 5.0, -5.0),
                        ('60d', 8.0, -8.0), ('blend', 5.0, 0)]:
    r = simulate(p2_dates, raw, chg_data, all_dates, 3, 8, 3,
                 'P1_adjgap', period, nt, pt, 1.5)
    print(f"    {period:>5} N{nt}/P{pt}: ret {r['ret']:+.1f}%, Sharpe {r['sharpe']:.2f}")

# E/X/S 변동
print(f"\n  [E/X/S 변동 (P1 60d N5/P-5 str=1.5)]")
for e, x, s in [(3,8,3), (3,10,3), (3,12,3), (5,8,3), (5,10,3), (5,12,3),
                 (3,8,5), (5,8,5), (5,12,5)]:
    r = simulate(p2_dates, raw, chg_data, all_dates, e, x, s,
                 'P1_adjgap', '60d', 5.0, -5.0, 1.5)
    print(f"    E{e}/X{x}/S{s}: ret {r['ret']:+.1f}%, Sharpe {r['sharpe']:.2f}, n={r['n']}")

# baseline도 동일 E/X/S
print(f"\n  [비교: baseline E/X/S]")
for e, x, s in [(3,8,3), (3,10,3), (3,12,3), (5,8,3), (5,10,3), (5,12,3)]:
    r = simulate(p2_dates, raw, chg_data, all_dates, e, x, s)
    print(f"    E{e}/X{x}/S{s}: ret {r['ret']:+.1f}%, Sharpe {r['sharpe']:.2f}, n={r['n']}")
