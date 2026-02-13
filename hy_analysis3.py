"""
HY Spread 하락(해빙) 국면 분석
- 고점에서 내려올 때 지수 상승률
- 피크 대비 하락폭별 성과
- 4% 이상에서 4% 이하로 복귀할 때
- 위기 탈출 시그널 탐색
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import yfinance as yf
import pandas as pd
import numpy as np
import urllib.request
import io

# ── 데이터 수집 ─────────────────────────────────────────────
print("데이터 수집 중...")
fred_url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A0HYM2&cosd=1996-12-01&coed=2026-02-12"
req = urllib.request.Request(fred_url, headers={'User-Agent': 'Mozilla/5.0'})
with urllib.request.urlopen(req, timeout=15) as response:
    csv_data = response.read().decode('utf-8')
hy_df = pd.read_csv(io.StringIO(csv_data), parse_dates=['observation_date'])
hy_df.columns = ['date', 'hy_spread']
hy_df = hy_df.dropna(subset=['hy_spread'])
hy_df['hy_spread'] = pd.to_numeric(hy_df['hy_spread'], errors='coerce')
hy_df = hy_df.dropna().set_index('date')

spy = yf.download("^GSPC", start="1997-01-01", end="2026-02-13", progress=False)
ndx = yf.download("^IXIC", start="1997-01-01", end="2026-02-13", progress=False)
if isinstance(spy.columns, pd.MultiIndex):
    spy.columns = spy.columns.get_level_values(0)
    ndx.columns = ndx.columns.get_level_values(0)

spy_ret = spy['Close'].pct_change() * 100
ndx_ret = ndx['Close'].pct_change() * 100

# 병합
df = pd.DataFrame(index=spy_ret.index)
df['sp500_ret'] = spy_ret
df['nasdaq_ret'] = ndx_ret
df['sp500_price'] = spy['Close']
df['nasdaq_price'] = ndx['Close']
df = df.join(hy_df[['hy_spread']])
df = df.dropna()
df['hy_change_bp'] = df['hy_spread'].diff() * 100

# 미래 수익률 (1일, 5일, 10일, 20일, 60일)
for d in [1, 5, 10, 20, 60]:
    df[f'sp_fwd_{d}d'] = (df['sp500_price'].shift(-d) / df['sp500_price'] - 1) * 100
    df[f'nq_fwd_{d}d'] = (df['nasdaq_price'].shift(-d) / df['nasdaq_price'] - 1) * 100

print(f"데이터: {len(df)}일, {df.index[0].date()} ~ {df.index[-1].date()}")
print(f"HY 범위: {df['hy_spread'].min():.2f}% ~ {df['hy_spread'].max():.2f}%")

# ── 파생 변수 ───────────────────────────────────────────────
# 최근 N일 최고점
for w in [20, 60, 120]:
    df[f'hy_peak_{w}d'] = df['hy_spread'].rolling(w).max()
    df[f'hy_from_peak_{w}d'] = (df['hy_spread'] - df[f'hy_peak_{w}d']) * 100  # bp

# 20일 MA & 기울기
df['hy_ma20'] = df['hy_spread'].rolling(20).mean()
df['hy_below_ma20'] = df['hy_spread'] < df['hy_ma20']

# 연속 하락 카운트
df['hy_down'] = (df['hy_change_bp'] < 0).astype(int)
consec_down = []
count = 0
for val in df['hy_down']:
    if val == 1:
        count += 1
    else:
        count = 0
    consec_down.append(count)
df['hy_consec_down'] = consec_down

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("1. ★ HY 수준별 × 하락(축소) 시 향후 지수 수익률")
print("=" * 80)

hy_levels = [
    ('정상 <3%',    0, 3),
    ('관심 3~4%',   3, 4),
    ('주의 4~5%',   4, 5),
    ('경고 5~7%',   5, 7),
    ('위기 7~10%',  7, 10),
    ('극단 10%+',  10, 100),
]

for level_name, lo, hi in hy_levels:
    level_mask = (df['hy_spread'] >= lo) & (df['hy_spread'] < hi)
    n_total = level_mask.sum()
    if n_total < 10:
        continue

    print(f"\n  ┌── {level_name} (총 {n_total}일) ──")
    print(f"  │ {'HY변화':>8} | {'발생':>5} | {'1일후SP':>8} | {'5일후SP':>8} | {'10일후SP':>9} | {'20일후SP':>9} | {'60일후SP':>9}")
    print(f"  │ " + "-" * 75)

    # 하락(스프레드 축소) 구간별
    for th_name, th in [('≤-3bp', -3), ('≤-5bp', -5), ('≤-10bp', -10), ('≤-15bp', -15), ('≤-20bp', -20), ('≤-30bp', -30)]:
        mask = level_mask & (df['hy_change_bp'] <= th) & df['sp_fwd_20d'].notna()
        n = mask.sum()
        if n >= 3:
            d1 = df.loc[mask, 'sp_fwd_1d'].mean()
            d5 = df.loc[mask, 'sp_fwd_5d'].mean()
            d10 = df.loc[mask, 'sp_fwd_10d'].mean()
            d20 = df.loc[mask, 'sp_fwd_20d'].mean()
            d60 = df.loc[mask, 'sp_fwd_60d'].mean() if df.loc[mask, 'sp_fwd_60d'].notna().sum() >= 3 else float('nan')
            d60_str = f"{d60:>+7.2f}%" if not np.isnan(d60) else "    N/A"
            print(f"  │ {th_name:>8} | {n:>4}일 | {d1:>+6.2f}% | {d5:>+6.2f}% | {d10:>+7.2f}% | {d20:>+7.2f}% | {d60_str}")

    # 비교: 상승(스프레드 확대)
    mask_up = level_mask & (df['hy_change_bp'] >= 5) & df['sp_fwd_20d'].notna()
    n_up = mask_up.sum()
    if n_up >= 3:
        d1 = df.loc[mask_up, 'sp_fwd_1d'].mean()
        d5 = df.loc[mask_up, 'sp_fwd_5d'].mean()
        d10 = df.loc[mask_up, 'sp_fwd_10d'].mean()
        d20 = df.loc[mask_up, 'sp_fwd_20d'].mean()
        d60 = df.loc[mask_up, 'sp_fwd_60d'].mean() if df.loc[mask_up, 'sp_fwd_60d'].notna().sum() >= 3 else float('nan')
        d60_str = f"{d60:>+7.2f}%" if not np.isnan(d60) else "    N/A"
        print(f"  │ {'(비교)≥5':>8} | {n_up:>4}일 | {d1:>+6.2f}% | {d5:>+6.2f}% | {d10:>+7.2f}% | {d20:>+7.2f}% | {d60_str}")
    print(f"  └──")

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("2. ★ 피크 대비 하락폭별 향후 수익률 (고점 대비 얼마나 내려왔나)")
print("=" * 80)

print(f"\n  60일 고점 대비 현재 HY 하락폭별 → 향후 SP500")
print(f"  {'피크대비':>10} | {'발생':>5} | {'1일':>7} | {'5일':>7} | {'10일':>8} | {'20일':>8} | {'60일':>8} | {'HY평균':>6}")
print("  " + "-" * 75)

from_peak_bins = [(-50, -10), (-100, -50), (-150, -100), (-200, -150), (-300, -200), (-500, -300), (-9999, -500)]
from_peak_labels = ['-10~50bp', '-50~100bp', '-100~150bp', '-150~200bp', '-200~300bp', '-300~500bp', '-500bp+']

for (lo, hi), label in zip(from_peak_bins, from_peak_labels):
    mask = (df['hy_from_peak_60d'] >= lo) & (df['hy_from_peak_60d'] < hi) & df['sp_fwd_20d'].notna()
    n = mask.sum()
    if n >= 5:
        hy_avg = df.loc[mask, 'hy_spread'].mean()
        d1 = df.loc[mask, 'sp_fwd_1d'].mean()
        d5 = df.loc[mask, 'sp_fwd_5d'].mean()
        d10 = df.loc[mask, 'sp_fwd_10d'].mean()
        d20 = df.loc[mask, 'sp_fwd_20d'].mean()
        d60 = df.loc[mask, 'sp_fwd_60d'].mean() if df.loc[mask, 'sp_fwd_60d'].notna().sum() >= 5 else float('nan')
        d60_str = f"{d60:>+6.2f}%" if not np.isnan(d60) else "   N/A"
        print(f"  {label:>10} | {n:>4}일 | {d1:>+5.2f}% | {d5:>+5.2f}% | {d10:>+6.2f}% | {d20:>+6.2f}% | {d60_str} | {hy_avg:.1f}%")

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("3. ★ 임계치 하향 돌파 (위→아래) 향후 수익률")
print("=" * 80)

df['hy_prev'] = df['hy_spread'].shift(1)

thresholds = [10, 8, 7, 6, 5, 4, 3.5]
print(f"\n  HY가 N%를 위에서 아래로 돌파하는 날 → 향후 SP500")
print(f"  {'돌파선':>6} | {'발생':>5} | {'1일':>7} | {'5일':>7} | {'10일':>8} | {'20일':>8} | {'60일':>8}")
print("  " + "-" * 65)

for th in thresholds:
    mask = (df['hy_prev'] >= th) & (df['hy_spread'] < th) & df['sp_fwd_20d'].notna()
    n = mask.sum()
    if n >= 3:
        d1 = df.loc[mask, 'sp_fwd_1d'].mean()
        d5 = df.loc[mask, 'sp_fwd_5d'].mean()
        d10 = df.loc[mask, 'sp_fwd_10d'].mean()
        d20 = df.loc[mask, 'sp_fwd_20d'].mean()
        d60 = df.loc[mask, 'sp_fwd_60d'].mean() if df.loc[mask, 'sp_fwd_60d'].notna().sum() >= 3 else float('nan')
        d60_str = f"{d60:>+6.2f}%" if not np.isnan(d60) else "   N/A"
        print(f"  {th:>5.1f}% | {n:>4}일 | {d1:>+5.2f}% | {d5:>+5.2f}% | {d10:>+6.2f}% | {d20:>+6.2f}% | {d60_str}")

# 반대: 상향 돌파 (아래→위)
print(f"\n  [비교] HY가 N%를 아래에서 위로 돌파하는 날 → 향후 SP500")
print(f"  {'돌파선':>6} | {'발생':>5} | {'1일':>7} | {'5일':>7} | {'10일':>8} | {'20일':>8} | {'60일':>8}")
print("  " + "-" * 65)

for th in [3.5, 4, 5, 6, 7, 8]:
    mask = (df['hy_prev'] < th) & (df['hy_spread'] >= th) & df['sp_fwd_20d'].notna()
    n = mask.sum()
    if n >= 3:
        d1 = df.loc[mask, 'sp_fwd_1d'].mean()
        d5 = df.loc[mask, 'sp_fwd_5d'].mean()
        d10 = df.loc[mask, 'sp_fwd_10d'].mean()
        d20 = df.loc[mask, 'sp_fwd_20d'].mean()
        d60 = df.loc[mask, 'sp_fwd_60d'].mean() if df.loc[mask, 'sp_fwd_60d'].notna().sum() >= 3 else float('nan')
        d60_str = f"{d60:>+6.2f}%" if not np.isnan(d60) else "   N/A"
        print(f"  {th:>5.1f}% | {n:>4}일 | {d1:>+5.2f}% | {d5:>+5.2f}% | {d10:>+6.2f}% | {d20:>+6.2f}% | {d60_str}")

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("4. ★ HY 연속 하락(스프레드 축소) + 수준별 → 향후 수익률")
print("=" * 80)

for level_name, lo, hi in hy_levels:
    level_mask = (df['hy_spread'] >= lo) & (df['hy_spread'] < hi)
    n_total = level_mask.sum()
    if n_total < 20:
        continue

    print(f"\n  ┌── {level_name} ──")
    for days in [3, 5, 7, 10]:
        mask = level_mask & (df['hy_consec_down'] >= days) & df['sp_fwd_20d'].notna()
        n = mask.sum()
        if n >= 3:
            d5 = df.loc[mask, 'sp_fwd_5d'].mean()
            d20 = df.loc[mask, 'sp_fwd_20d'].mean()
            d60 = df.loc[mask, 'sp_fwd_60d'].mean() if df.loc[mask, 'sp_fwd_60d'].notna().sum() >= 3 else float('nan')
            d60_str = f"60일 {d60:>+.2f}%" if not np.isnan(d60) else ""
            print(f"  │ {days}일+ 연속축소: {n:>4}회 → 5일 {d5:>+.2f}%, 20일 {d20:>+.2f}% {d60_str}")
    print(f"  └──")

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("5. ★ MA20 크로스: HY가 20일 이평 하향 돌파 (해빙 시작)")
print("=" * 80)

df['hy_was_above_ma20'] = df['hy_spread'].shift(1) >= df['hy_ma20'].shift(1)
df['hy_now_below_ma20'] = df['hy_spread'] < df['hy_ma20']
df['hy_ma20_cross_down'] = df['hy_was_above_ma20'] & df['hy_now_below_ma20']

for level_name, lo, hi in hy_levels:
    level_mask = (df['hy_spread'] >= lo) & (df['hy_spread'] < hi)
    mask = level_mask & df['hy_ma20_cross_down'] & df['sp_fwd_20d'].notna()
    n = mask.sum()
    if n >= 3:
        d5 = df.loc[mask, 'sp_fwd_5d'].mean()
        d10 = df.loc[mask, 'sp_fwd_10d'].mean()
        d20 = df.loc[mask, 'sp_fwd_20d'].mean()
        d60 = df.loc[mask, 'sp_fwd_60d'].mean() if df.loc[mask, 'sp_fwd_60d'].notna().sum() >= 3 else float('nan')
        d60_str = f"{d60:>+.2f}%" if not np.isnan(d60) else "N/A"
        print(f"  {level_name}: {n:>3}회 → 5일 {d5:>+.2f}%, 10일 {d10:>+.2f}%, 20일 {d20:>+.2f}%, 60일 {d60_str}")

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("6. ★ 위기 사이클 분석 — HY 4% 이상 에피소드별 복귀 수익률")
print("=" * 80)

# HY >= 4% 에피소드 식별
df['above4'] = df['hy_spread'] >= 4
episodes = []
in_episode = False
ep_start = None
for date, row in df.iterrows():
    if row['above4'] and not in_episode:
        in_episode = True
        ep_start = date
    elif not row['above4'] and in_episode:
        in_episode = False
        episodes.append((ep_start, date))

if in_episode:  # 아직 진행중
    episodes.append((ep_start, df.index[-1]))

print(f"\n  총 {len(episodes)}개 에피소드 (HY ≥4% 진입 → 복귀)")
print(f"  {'#':>3} | {'시작':>10} | {'종료':>10} | {'기간':>5} | {'피크HY':>7} | {'진입후20일SP':>12} | {'복귀후20일SP':>12} | {'복귀후60일SP':>12}")
print("  " + "-" * 100)

for i, (start, end) in enumerate(episodes):
    duration = (end - start).days
    ep_data = df.loc[start:end]
    peak_hy = ep_data['hy_spread'].max()

    # 진입 후 20일 수익률
    if start in df.index:
        entry_20d = df.loc[start, 'sp_fwd_20d'] if not np.isnan(df.loc[start, 'sp_fwd_20d']) else None
    else:
        entry_20d = None

    # 복귀 후 (end날) 수익률
    if end in df.index:
        exit_20d = df.loc[end, 'sp_fwd_20d'] if pd.notna(df.loc[end, 'sp_fwd_20d']) else None
        exit_60d = df.loc[end, 'sp_fwd_60d'] if pd.notna(df.loc[end, 'sp_fwd_60d']) else None
    else:
        exit_20d = None
        exit_60d = None

    entry_str = f"{entry_20d:>+10.2f}%" if entry_20d is not None else "       N/A"
    exit_20_str = f"{exit_20d:>+10.2f}%" if exit_20d is not None else "       N/A"
    exit_60_str = f"{exit_60d:>+10.2f}%" if exit_60d is not None else "       N/A"

    print(f"  {i+1:>3} | {start.strftime('%Y-%m-%d')} | {end.strftime('%Y-%m-%d')} | {duration:>4}일 | {peak_hy:>5.1f}% | {entry_str} | {exit_20_str} | {exit_60_str}")

# 에피소드 평균
exits_20 = []
exits_60 = []
for start, end in episodes:
    if end in df.index:
        v20 = df.loc[end, 'sp_fwd_20d']
        v60 = df.loc[end, 'sp_fwd_60d']
        if pd.notna(v20): exits_20.append(v20)
        if pd.notna(v60): exits_60.append(v60)

if exits_20:
    print(f"\n  복귀 후 평균: 20일 {np.mean(exits_20):+.2f}% ({len(exits_20)}건), ", end="")
    if exits_60:
        print(f"60일 {np.mean(exits_60):+.2f}% ({len(exits_60)}건)")
    else:
        print()

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("7. ★ 해빙 속도: 피크 후 N일간 하락폭별 향후 수익률")
print("=" * 80)

# 최근 20일 내 피크 대비 하락률 + HY가 아직 높은 구간
# "HY가 5% 이상이었다가 빠르게 내려오는 중" 같은 상황
print(f"\n  HY 5%+ 경험 후 빠르게 축소 중 (20일 피크 대비)")
high_mask = df['hy_peak_20d'] >= 5  # 최근 20일 내 5% 이상 기록

decline_bins = [(-100, -30), (-150, -100), (-200, -150), (-300, -200), (-500, -300), (-9999, -500)]
decline_labels = ['-30~100bp', '-100~150bp', '-150~200bp', '-200~300bp', '-300~500bp', '-500bp+']

print(f"  {'하락폭':>12} | {'발생':>5} | {'5일SP':>7} | {'10일SP':>8} | {'20일SP':>8} | {'60일SP':>8} | {'HY평균':>6}")
print("  " + "-" * 70)

for (lo, hi), label in zip(decline_bins, decline_labels):
    mask = high_mask & (df['hy_from_peak_20d'] >= lo) & (df['hy_from_peak_20d'] < hi) & df['sp_fwd_20d'].notna()
    n = mask.sum()
    if n >= 5:
        hy_avg = df.loc[mask, 'hy_spread'].mean()
        d5 = df.loc[mask, 'sp_fwd_5d'].mean()
        d10 = df.loc[mask, 'sp_fwd_10d'].mean()
        d20 = df.loc[mask, 'sp_fwd_20d'].mean()
        d60 = df.loc[mask, 'sp_fwd_60d'].mean() if df.loc[mask, 'sp_fwd_60d'].notna().sum() >= 5 else float('nan')
        d60_str = f"{d60:>+6.2f}%" if not np.isnan(d60) else "   N/A"
        print(f"  {label:>12} | {n:>4}일 | {d5:>+5.2f}% | {d10:>+6.2f}% | {d20:>+6.2f}% | {d60_str} | {hy_avg:.1f}%")

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("8. 현재 상황 진단")
print("=" * 80)

latest = df.iloc[-1]
hy_now = latest['hy_spread']
hy_20d_peak = latest.get('hy_peak_20d', np.nan)
hy_60d_peak = latest.get('hy_peak_60d', np.nan)
from_20d_peak = latest.get('hy_from_peak_20d', np.nan)
from_60d_peak = latest.get('hy_from_peak_60d', np.nan)
consec = latest.get('hy_consec_down', 0)
below_ma = latest.get('hy_below_ma20', False)

print(f"""
  현재 HY Spread: {hy_now:.2f}%
  20일 고점: {hy_20d_peak:.2f}% (현재 대비 {from_20d_peak:.0f}bp)
  60일 고점: {hy_60d_peak:.2f}% (현재 대비 {from_60d_peak:.0f}bp)
  연속 축소: {int(consec)}일
  MA20 대비: {'하회 (해빙 중)' if below_ma else '상회 (긴장 중)'}

  ▶ 현재 체제: {'정상' if hy_now < 3 else '관심' if hy_now < 4 else '주의' if hy_now < 5 else '경고' if hy_now < 7 else '위기'}
""")
