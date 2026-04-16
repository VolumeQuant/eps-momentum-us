"""
Step 2: z-score 결함 정량화 진단
- 100 clamp 발생 빈도
- 1~3위 z_raw 차이 (변별력 소실 정도)
- VNOM 패턴 (missing + outlier) 발생 횟수
"""
import pickle
import numpy as np
import sys
sys.stdout.reconfigure(encoding='utf-8')

with open('zscore_cache.pkl', 'rb') as f:
    c = pickle.load(f)

raw_scores = c['zscore_raw_by_date']
clamped = c['zscore_by_date']
stats = c['zscore_stats_by_date']
wgap = c['wgap_by_date']
p2_dates = c['p2_dates']
raw = c['raw']

print("="*90)
print("결함 #1: 100 clamp 발생 빈도")
print("="*90)

total_scores = 0
clamp_100 = 0
clamp_100_by_date = {}
for d in p2_dates:
    rs = raw_scores.get(d, {})
    n_clamp = sum(1 for v in rs.values() if v >= 100)
    n_total = len(rs)
    total_scores += n_total
    clamp_100 += n_clamp
    clamp_100_by_date[d] = (n_clamp, n_total)
    if n_clamp > 0:
        tickers = [tk for tk, v in rs.items() if v >= 100]
        raw_vals = [(tk, rs[tk]) for tk in tickers]
        raw_vals.sort(key=lambda x: -x[1])
        print(f"  {d}: {n_clamp}/{n_total}종목 clamp → {', '.join(f'{tk}({v:.0f})' for tk, v in raw_vals[:5])}")

print(f"\n  전체: {clamp_100}/{total_scores} ({clamp_100/total_scores*100:.1f}%)")
print(f"  발생일: {sum(1 for n, _ in clamp_100_by_date.values() if n > 0)}/{len(p2_dates)}일")

print(f"\n{'='*90}")
print("결함 #1 상세: Top 3 z_raw 차이 (변별력)")
print("="*90)

for d in p2_dates:
    rs = raw_scores.get(d, {})
    if not rs: continue
    top3 = sorted(rs.items(), key=lambda x: -x[1])[:3]
    cl3 = sorted(clamped.get(d, {}).items(), key=lambda x: -x[1])[:3]
    # 1위와 2위 차이
    if len(top3) >= 2:
        raw_gap = top3[0][1] - top3[1][1]
        clamp_gap = cl3[0][1] - cl3[1][1]
        if raw_gap > 5 and clamp_gap < 1:
            print(f"  {d}: 1위 {top3[0][0]}({top3[0][1]:.1f}) vs 2위 {top3[1][0]}({top3[1][1]:.1f})"
                  f" → raw gap {raw_gap:.1f} → clamp 후 {clamp_gap:.1f} ← 변별력 소실!")

print(f"\n{'='*90}")
print("결함 #2: Missing day + outlier = VNOM 패턴")
print("="*90)

vnom_pattern = 0
for di, d in enumerate(p2_dates):
    if di < 2: continue
    d2, d1, d0 = p2_dates[di-2], p2_dates[di-1], p2_dates[di]
    wg = wgap.get(d, {})
    # w_gap Top 5 종목
    top5 = sorted(wg.items(), key=lambda x: -x[1])[:5]
    for tk, wg_val in top5:
        # T-2에 없는 종목 (missing)
        s2 = clamped.get(d2, {}).get(tk)
        s1 = clamped.get(d1, {}).get(tk)
        s0 = clamped.get(d0, {}).get(tk)
        if s2 is None and s1 is not None and s0 is not None:
            # T-1, T0 모두 100 (outlier clamp)
            if s1 >= 99 and s0 >= 99:
                # 3일 검증 종목보다 높은지
                verified_below = False
                for tk2, wg2 in top5:
                    if tk2 == tk: continue
                    s2_2 = clamped.get(d2, {}).get(tk2)
                    if s2_2 is not None and wg2 < wg_val:
                        verified_below = True
                        break
                if verified_below:
                    vnom_pattern += 1
                    status = raw.get(d, {}).get(tk, {}).get('p2_rank', '?')
                    print(f"  {d}: {tk} (rank {status}) — T-2 missing + T-1/T0 clamp 100"
                          f" → w_gap {wg_val:.1f} (3일 검증 종목보다 높음)")

print(f"\n  VNOM 패턴 총: {vnom_pattern}건 / {len(p2_dates)}일")

print(f"\n{'='*90}")
print("conv adj_gap 분포 통계")
print("="*90)
print(f"{'date':<12}{'n':>4}{'mean':>8}{'std':>8}{'min':>10}{'max':>9}{'skew':>7}{'100clamp':>9}")
for d in p2_dates[-10:]:
    s = stats[d]
    nc = clamp_100_by_date.get(d, (0,0))[0]
    print(f"{d:<12}{s['n']:>4}{s['mean']:>8.1f}{s['std']:>8.1f}{s['min']:>10.1f}{s['max']:>9.1f}{s['skew']:>7.2f}{nc:>9}")

# 결론
print(f"\n{'='*90}")
print("진단 결론")
print("="*90)
pct = clamp_100/total_scores*100
print(f"  100 clamp 비율: {pct:.1f}% ({clamp_100}/{total_scores})")
if pct > 1:
    print(f"  → 1% 초과 = 결함 유의미, 수정 필요")
else:
    print(f"  → 1% 이하 = 결함 영향 작음, production 변경 불필요 가능")
print(f"  VNOM 패턴: {vnom_pattern}건")
if vnom_pattern > 0:
    print(f"  → 발생 확인 = 결함이 실제 순위에 영향")
