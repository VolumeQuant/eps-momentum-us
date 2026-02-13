"""
HY Spread vs 지수 관계 분석 (1997~2026, ~30년)
HY 수준별로 나눠서 급등/급락 시 다음날 지수 반응 분석
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import yfinance as yf
import pandas as pd
import numpy as np
import urllib.request
import io

# ── 1. 데이터 수집 ──────────────────────────────────────────
print("=" * 70)
print("1. 데이터 수집 (1997~2026)")
print("=" * 70)

fred_url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A0HYM2&cosd=1996-12-01&coed=2026-02-12"
req = urllib.request.Request(fred_url, headers={'User-Agent': 'Mozilla/5.0'})
with urllib.request.urlopen(req, timeout=15) as response:
    csv_data = response.read().decode('utf-8')
hy_df = pd.read_csv(io.StringIO(csv_data), parse_dates=['observation_date'])
hy_df.columns = ['date', 'hy_spread']
hy_df = hy_df.dropna(subset=['hy_spread'])
hy_df['hy_spread'] = pd.to_numeric(hy_df['hy_spread'], errors='coerce')
hy_df = hy_df.dropna(subset=['hy_spread'])
hy_df = hy_df.set_index('date')
print(f"  HY Spread: {len(hy_df)}일, {hy_df.index[0].date()} ~ {hy_df.index[-1].date()}")
print(f"  범위: {hy_df['hy_spread'].min():.2f}% ~ {hy_df['hy_spread'].max():.2f}%")

print(f"\nS&P 500 / NASDAQ 다운로드 중...")
spy = yf.download("^GSPC", start="1997-01-01", end="2026-02-13", progress=False)
ndx = yf.download("^IXIC", start="1997-01-01", end="2026-02-13", progress=False)
if isinstance(spy.columns, pd.MultiIndex):
    spy.columns = spy.columns.get_level_values(0)
    ndx.columns = ndx.columns.get_level_values(0)

spy_ret = spy['Close'].pct_change() * 100
ndx_ret = ndx['Close'].pct_change() * 100
spy_ret.name = 'sp500_ret'
ndx_ret.name = 'nasdaq_ret'
print(f"  S&P 500: {len(spy_ret)}일, NASDAQ: {len(ndx_ret)}일")

# ── 2. 병합 ─────────────────────────────────────────────────
hy_df['hy_change_bp'] = hy_df['hy_spread'].diff() * 100
df = pd.DataFrame(index=spy_ret.index)
df['sp500_ret'] = spy_ret
df['nasdaq_ret'] = ndx_ret
df = df.join(hy_df[['hy_spread', 'hy_change_bp']])
df = df.dropna()
df['sp500_next'] = df['sp500_ret'].shift(-1)
df['nasdaq_next'] = df['nasdaq_ret'].shift(-1)

# 연속 상승 카운트
df['hy_up'] = (df['hy_change_bp'] > 0).astype(int)
consec = []
count = 0
for val in df['hy_up']:
    if val == 1:
        count += 1
    else:
        count = 0
    consec.append(count)
df['hy_consec_up'] = consec

print(f"\n  병합 후: {len(df)}일, {df.index[0].date()} ~ {df.index[-1].date()}")
print(f"  HY 변화(bp) 범위: {df['hy_change_bp'].min():.0f} ~ {df['hy_change_bp'].max():.0f}")

# ── 3. 전체 기간 기본 통계 ───────────────────────────────────
print("\n" + "=" * 70)
print("2. 전체 기간 상관관계")
print("=" * 70)

valid = df.dropna(subset=['sp500_next'])
corr_same = df['hy_change_bp'].corr(df['sp500_ret'])
corr_lead = valid['hy_change_bp'].corr(valid['sp500_next'])
print(f"  동행: HY vs SP500 = {corr_same:.4f}")
print(f"  선행: HY → 내일 SP500 = {corr_lead:.4f}")

# ── 4. 핵심: HY 수준별 × 급등 크기별 다음날 지수 ─────────────
print("\n" + "=" * 70)
print("3. ★ HY 수준별 × 일일 변화별 → 다음날 SP500")
print("=" * 70)

hy_levels = [
    ('정상 <3%',    0, 3),
    ('관심 3~4%',   3, 4),
    ('주의 4~5%',   4, 5),
    ('경고 5~7%',   5, 7),
    ('위기 7%+',    7, 100),
]

change_thresholds = [
    ('≥5bp',  5),
    ('≥10bp', 10),
    ('≥15bp', 15),
    ('≥20bp', 20),
    ('≥30bp', 30),
]

for level_name, lo, hi in hy_levels:
    level_mask = (df['hy_spread'] >= lo) & (df['hy_spread'] < hi)
    n_total = level_mask.sum()

    print(f"\n  ┌── {level_name} (총 {n_total}일) ──")
    print(f"  │ {'HY변화':>8} | {'발생':>5} | {'다음날SP500':>12} | {'다음날NQ':>10} | {'SP하락%':>7} | {'NQ하락%':>7}")
    print(f"  │ " + "-" * 65)

    for th_name, th in change_thresholds:
        mask = level_mask & (df['hy_change_bp'] >= th) & df['sp500_next'].notna()
        n = mask.sum()
        if n >= 3:
            sp = df.loc[mask, 'sp500_next'].mean()
            nq = df.loc[mask, 'nasdaq_next'].mean()
            sp_down = (df.loc[mask, 'sp500_next'] < 0).mean() * 100
            nq_down = (df.loc[mask, 'nasdaq_next'] < 0).mean() * 100
            print(f"  │ {th_name:>8} | {n:>4}일 | {sp:>+10.3f}% | {nq:>+8.3f}% | {sp_down:>5.1f}% | {nq_down:>5.1f}%")
        elif n > 0:
            print(f"  │ {th_name:>8} | {n:>4}일 |  (샘플부족)  |            |         |")

    # 급락도 표시
    for th_name, th in [('≤-5bp', -5), ('≤-10bp', -10), ('≤-20bp', -20)]:
        mask = level_mask & (df['hy_change_bp'] <= th) & df['sp500_next'].notna()
        n = mask.sum()
        if n >= 3:
            sp = df.loc[mask, 'sp500_next'].mean()
            nq = df.loc[mask, 'nasdaq_next'].mean()
            sp_up = (df.loc[mask, 'sp500_next'] > 0).mean() * 100
            print(f"  │ {th_name:>8} | {n:>4}일 | {sp:>+10.3f}% | {nq:>+8.3f}% | ↑{sp_up:>4.0f}% |")

    print(f"  └──")

# ── 5. HY 수준별 × 연속 상승 → 다음날 ───────────────────────
print("\n" + "=" * 70)
print("4. ★ HY 수준별 × 연속 상승일수 → 다음날 SP500")
print("=" * 70)

for level_name, lo, hi in hy_levels:
    level_mask = (df['hy_spread'] >= lo) & (df['hy_spread'] < hi)
    n_total = level_mask.sum()
    if n_total < 10:
        continue

    print(f"\n  ┌── {level_name} ──")
    for days in [2, 3, 4, 5]:
        mask = level_mask & (df['hy_consec_up'] >= days) & df['sp500_next'].notna()
        n = mask.sum()
        if n >= 3:
            sp = df.loc[mask, 'sp500_next'].mean()
            nq = df.loc[mask, 'nasdaq_next'].mean()
            sp_down = (df.loc[mask, 'sp500_next'] < 0).mean() * 100
            print(f"  │ {days}일+ 연속상승: {n:>4}회 → SP500 {sp:>+.3f}%, NQ {nq:>+.3f}% (하락 {sp_down:.0f}%)")
    print(f"  └──")

# ── 6. HY 수준별 당일 지수 성과 (세분화) ─────────────────────
print("\n" + "=" * 70)
print("5. HY 수준 세분화 — 당일/다음날 평균 수익률")
print("=" * 70)

fine_bins = [0, 2.5, 3, 3.5, 4, 4.5, 5, 6, 7, 8, 10, 100]
fine_labels = ['<2.5', '2.5-3', '3-3.5', '3.5-4', '4-4.5', '4.5-5', '5-6', '6-7', '7-8', '8-10', '10+']
df['hy_fine'] = pd.cut(df['hy_spread'], bins=fine_bins, labels=fine_labels)

print(f"\n  {'HY수준':>8} | {'일수':>5} | {'당일SP500':>10} | {'다음날SP500':>11} | {'당일하락%':>8} | {'다음날하락%':>9}")
print("  " + "-" * 70)

for level in fine_labels:
    mask = df['hy_fine'] == level
    n = mask.sum()
    if n >= 5:
        sp_today = df.loc[mask, 'sp500_ret'].mean()
        mask_next = mask & df['sp500_next'].notna()
        sp_next = df.loc[mask_next, 'sp500_next'].mean() if mask_next.sum() > 0 else 0
        down_today = (df.loc[mask, 'sp500_ret'] < 0).mean() * 100
        down_next = (df.loc[mask_next, 'sp500_next'] < 0).mean() * 100 if mask_next.sum() > 0 else 0
        print(f"  {level:>8} | {n:>4}일 | {sp_today:>+8.3f}% | {sp_next:>+9.3f}% | {down_today:>6.1f}% | {down_next:>7.1f}%")

# ── 7. 시대별 비교 ──────────────────────────────────────────
print("\n" + "=" * 70)
print("6. 시대별 HY→다음날 지수 상관관계")
print("=" * 70)

periods = [
    ('닷컴 버블', '2000-01-01', '2003-12-31'),
    ('정상기', '2004-01-01', '2007-06-30'),
    ('GFC', '2007-07-01', '2009-12-31'),
    ('회복기', '2010-01-01', '2019-12-31'),
    ('COVID', '2020-01-01', '2020-12-31'),
    ('금리인상', '2021-01-01', '2023-12-31'),
    ('최근', '2024-01-01', '2026-12-31'),
]

for name, start, end in periods:
    mask = (df.index >= start) & (df.index <= end) & df['sp500_next'].notna()
    sub = df[mask]
    if len(sub) < 20:
        continue
    corr = sub['hy_change_bp'].corr(sub['sp500_next'])
    hy_mean = sub['hy_spread'].mean()
    hy_max = sub['hy_spread'].max()
    print(f"  {name:<8} ({len(sub):>4}일) | HY→내일SP: {corr:>+.3f} | HY 평균 {hy_mean:.1f}%, 최대 {hy_max:.1f}%")

# ── 8. 실용적 경고 신호 탐색 ────────────────────────────────
print("\n" + "=" * 70)
print("7. ★ 실용적 경고 신호 후보")
print("=" * 70)

# 신호 1: 정상 구간(HY<4%)에서 급등(≥10bp)
print("\n  [신호1] 정상(HY<4%) + 급등(≥10bp) → 다음날")
mask = (df['hy_spread'] < 4) & (df['hy_change_bp'] >= 10) & df['sp500_next'].notna()
n = mask.sum()
sp = df.loc[mask, 'sp500_next'].mean()
down = (df.loc[mask, 'sp500_next'] < 0).mean() * 100
print(f"  → {n}일, 다음날 SP500 {sp:+.3f}%, 하락확률 {down:.0f}%")

# 신호 2: 정상 구간 + 3일 연속 상승
print("\n  [신호2] 정상(HY<4%) + 3일+ 연속상승 → 다음날")
mask = (df['hy_spread'] < 4) & (df['hy_consec_up'] >= 3) & df['sp500_next'].notna()
n = mask.sum()
sp = df.loc[mask, 'sp500_next'].mean()
down = (df.loc[mask, 'sp500_next'] < 0).mean() * 100
print(f"  → {n}일, 다음날 SP500 {sp:+.3f}%, 하락확률 {down:.0f}%")

# 신호 3: 정상 구간 + 5일 연속 상승
print("\n  [신호3] 정상(HY<4%) + 5일+ 연속상승 → 다음날")
mask = (df['hy_spread'] < 4) & (df['hy_consec_up'] >= 5) & df['sp500_next'].notna()
n = mask.sum()
sp = df.loc[mask, 'sp500_next'].mean()
down = (df.loc[mask, 'sp500_next'] < 0).mean() * 100
print(f"  → {n}일, 다음날 SP500 {sp:+.3f}%, 하락확률 {down:.0f}%")

# 신호 4: HY 4% 돌파 (아래→위)
print("\n  [신호4] HY가 4% 돌파하는 날 → 다음날")
df['hy_prev'] = df['hy_spread'].shift(1)
mask = (df['hy_prev'] < 4) & (df['hy_spread'] >= 4) & df['sp500_next'].notna()
n = mask.sum()
if n >= 3:
    sp = df.loc[mask, 'sp500_next'].mean()
    down = (df.loc[mask, 'sp500_next'] < 0).mean() * 100
    print(f"  → {n}일, 다음날 SP500 {sp:+.3f}%, 하락확률 {down:.0f}%")

# 신호 5: 5일간 누적 변화
df['hy_5d_change'] = df['hy_spread'].diff(5) * 100  # 5일간 누적 변화(bp)
print("\n  [신호5] 5일간 HY 누적 변화별 → 다음날 SP500")
for th in [20, 30, 50, 70, 100]:
    mask = (df['hy_5d_change'] >= th) & df['sp500_next'].notna()
    n = mask.sum()
    if n >= 3:
        sp = df.loc[mask, 'sp500_next'].mean()
        down = (df.loc[mask, 'sp500_next'] < 0).mean() * 100
        print(f"  5일 ≥{th:>3}bp: {n:>4}일 → SP500 {sp:>+.3f}%, 하락 {down:.0f}%")

# 신호 6: 20일 변화 (추세)
df['hy_20d_change'] = df['hy_spread'].diff(20) * 100
print("\n  [신호6] 20일간 HY 누적 변화별 → 향후 5일 SP500")
df['sp500_5d_fwd'] = df['sp500_ret'].rolling(5).sum().shift(-5)
for th in [30, 50, 100, 150, 200]:
    mask = (df['hy_20d_change'] >= th) & df['sp500_5d_fwd'].notna()
    n = mask.sum()
    if n >= 3:
        sp = df.loc[mask, 'sp500_5d_fwd'].mean()
        down = (df.loc[mask, 'sp500_5d_fwd'] < 0).mean() * 100
        print(f"  20일 ≥{th:>3}bp: {n:>4}일 → 5일 SP500 {sp:>+.3f}%, 하락 {down:.0f}%")

# ── 9. 요약 통계 ────────────────────────────────────────────
print("\n" + "=" * 70)
print("8. HY 분포 요약 (전체 기간)")
print("=" * 70)

hy_all = df['hy_spread']
print(f"  평균: {hy_all.mean():.2f}%")
print(f"  중위: {hy_all.median():.2f}%")
for q in [10, 25, 50, 75, 90, 95, 99]:
    print(f"  {q}%ile: {hy_all.quantile(q/100):.2f}%")
print(f"  현재: {hy_all.iloc[-1]:.2f}% (하위 {(hy_all < hy_all.iloc[-1]).mean()*100:.0f}%ile)")
