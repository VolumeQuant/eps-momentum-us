"""
HY Spread vs S&P500/NASDAQ 관계 분석
- 동행성 vs 선행성
- HY 급등 시 다음날 지수 하락 여부
- 임계치 탐색
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ── 1. 데이터 수집 ──────────────────────────────────────────
print("=" * 70)
print("1. 데이터 수집")
print("=" * 70)

# FRED에서 HY spread (BAMLH0A0HYM2) - pandas_datareader 또는 직접 API
# yfinance로는 못 가져오므로 FRED CSV URL 사용
import urllib.request
import io

# FRED API (no key needed for CSV download)
fred_url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A0HYM2&cosd=2020-01-01&coed=2026-02-12"
print(f"FRED에서 HY Spread 다운로드 중...")
try:
    req = urllib.request.Request(fred_url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=15) as response:
        csv_data = response.read().decode('utf-8')
    hy_df = pd.read_csv(io.StringIO(csv_data), parse_dates=['observation_date'])
    hy_df.columns = ['date', 'hy_spread']
    hy_df = hy_df[hy_df['hy_spread'] != '.'].copy()  # FRED uses '.' for missing
    hy_df['hy_spread'] = hy_df['hy_spread'].astype(float)
    hy_df = hy_df.set_index('date')
    print(f"  HY Spread: {len(hy_df)}일, {hy_df.index[0].date()} ~ {hy_df.index[-1].date()}")
    print(f"  현재 수준: {hy_df['hy_spread'].iloc[-1]:.2f}%")
    print(f"  범위: {hy_df['hy_spread'].min():.2f}% ~ {hy_df['hy_spread'].max():.2f}%")
except Exception as e:
    print(f"  FRED 다운로드 실패: {e}")
    print("  대안: pandas_datareader 시도...")
    raise

# S&P 500, NASDAQ
print(f"\nS&P 500 / NASDAQ 다운로드 중...")
spy = yf.download("^GSPC", start="2020-01-01", end="2026-02-13", progress=False)
ndx = yf.download("^IXIC", start="2020-01-01", end="2026-02-13", progress=False)

# 컬럼 처리 (MultiIndex 대응)
if isinstance(spy.columns, pd.MultiIndex):
    spy.columns = spy.columns.get_level_values(0)
    ndx.columns = ndx.columns.get_level_values(0)

spy_ret = spy['Close'].pct_change() * 100
ndx_ret = ndx['Close'].pct_change() * 100
spy_ret.name = 'sp500_ret'
ndx_ret.name = 'nasdaq_ret'

print(f"  S&P 500: {len(spy_ret)}일")
print(f"  NASDAQ: {len(ndx_ret)}일")

# ── 2. 데이터 병합 ──────────────────────────────────────────
print("\n" + "=" * 70)
print("2. 데이터 병합 및 파생 변수")
print("=" * 70)

# HY spread 일일 변화량 (bp)
hy_df['hy_change'] = hy_df['hy_spread'].diff()  # 절대 변화 (%)
hy_df['hy_change_bp'] = hy_df['hy_change'] * 100  # bp 단위
hy_df['hy_pct_change'] = hy_df['hy_spread'].pct_change() * 100  # % 변화율

# 병합
df = pd.DataFrame(index=spy_ret.index)
df['sp500_ret'] = spy_ret
df['nasdaq_ret'] = ndx_ret
df = df.join(hy_df[['hy_spread', 'hy_change_bp', 'hy_pct_change']])
df = df.dropna()

# 다음날 지수 수익률
df['sp500_next'] = df['sp500_ret'].shift(-1)
df['nasdaq_next'] = df['nasdaq_ret'].shift(-1)

print(f"  병합 후: {len(df)}일, {df.index[0].date()} ~ {df.index[-1].date()}")
print(f"  HY 일일 변화(bp) 범위: {df['hy_change_bp'].min():.1f} ~ {df['hy_change_bp'].max():.1f}")

# ── 3. 동행 상관관계 ────────────────────────────────────────
print("\n" + "=" * 70)
print("3. 동행 상관관계 (같은 날)")
print("=" * 70)

corr_sp = df['hy_change_bp'].corr(df['sp500_ret'])
corr_ndx = df['hy_change_bp'].corr(df['nasdaq_ret'])
print(f"  HY 변화(bp) vs S&P 500 수익률: {corr_sp:.4f}")
print(f"  HY 변화(bp) vs NASDAQ 수익률:  {corr_ndx:.4f}")

# ── 4. 선행 관계 (HY → 다음날 지수) ─────────────────────────
print("\n" + "=" * 70)
print("4. 선행 관계 (오늘 HY → 내일 지수)")
print("=" * 70)

valid = df.dropna(subset=['sp500_next', 'nasdaq_next'])
corr_sp_lead = valid['hy_change_bp'].corr(valid['sp500_next'])
corr_ndx_lead = valid['hy_change_bp'].corr(valid['nasdaq_next'])
print(f"  HY 변화(bp) vs 내일 S&P 500: {corr_sp_lead:.4f}")
print(f"  HY 변화(bp) vs 내일 NASDAQ:  {corr_ndx_lead:.4f}")

# 역방향: 오늘 지수 → 내일 HY
df['hy_next'] = df['hy_change_bp'].shift(-1)
valid2 = df.dropna(subset=['hy_next'])
corr_rev_sp = valid2['sp500_ret'].corr(valid2['hy_next'])
corr_rev_ndx = valid2['nasdaq_ret'].corr(valid2['hy_next'])
print(f"\n  [역방향] 오늘 S&P 500 → 내일 HY: {corr_rev_sp:.4f}")
print(f"  [역방향] 오늘 NASDAQ → 내일 HY:  {corr_rev_ndx:.4f}")

# ── 5. HY 급등 시 다음날 지수 ────────────────────────────────
print("\n" + "=" * 70)
print("5. HY 급등 임계치별 다음날 지수 성과")
print("=" * 70)

thresholds_bp = [3, 5, 7, 10, 15, 20]
print(f"\n  {'HY 변화':>10} | {'발생일수':>6} | {'다음날 SP500':>14} | {'다음날 NASDAQ':>14} | {'하락확률 SP':>10} | {'하락확률 NQ':>10}")
print("  " + "-" * 85)

for th in thresholds_bp:
    mask = valid['hy_change_bp'] >= th
    n = mask.sum()
    if n > 0:
        sp_mean = valid.loc[mask, 'sp500_next'].mean()
        ndx_mean = valid.loc[mask, 'nasdaq_next'].mean()
        sp_down = (valid.loc[mask, 'sp500_next'] < 0).mean() * 100
        ndx_down = (valid.loc[mask, 'nasdaq_next'] < 0).mean() * 100
        print(f"  ≥{th:>3}bp     | {n:>5}일 | {sp_mean:>+12.3f}% | {ndx_mean:>+12.3f}% | {sp_down:>8.1f}% | {ndx_down:>8.1f}%")
    else:
        print(f"  ≥{th:>3}bp     | {n:>5}일 |      없음       |      없음       |     없음    |     없음   ")

# HY 급락 시 (스프레드 축소 = 시장 안심)
print(f"\n  {'HY 변화':>10} | {'발생일수':>6} | {'다음날 SP500':>14} | {'다음날 NASDAQ':>14} | {'상승확률 SP':>10} | {'상승확률 NQ':>10}")
print("  " + "-" * 85)

for th in [-3, -5, -7, -10, -15]:
    mask = valid['hy_change_bp'] <= th
    n = mask.sum()
    if n > 0:
        sp_mean = valid.loc[mask, 'sp500_next'].mean()
        ndx_mean = valid.loc[mask, 'nasdaq_next'].mean()
        sp_up = (valid.loc[mask, 'sp500_next'] > 0).mean() * 100
        ndx_up = (valid.loc[mask, 'nasdaq_next'] > 0).mean() * 100
        print(f"  ≤{th:>3}bp     | {n:>5}일 | {sp_mean:>+12.3f}% | {ndx_mean:>+12.3f}% | {sp_up:>8.1f}% | {ndx_up:>8.1f}%")

# ── 6. HY 수준별 지수 성과 ──────────────────────────────────
print("\n" + "=" * 70)
print("6. HY Spread 수준별 당일 지수 성과")
print("=" * 70)

bins = [0, 2.5, 3.0, 3.5, 4.0, 5.0, 100]
labels = ['<2.5%', '2.5-3%', '3-3.5%', '3.5-4%', '4-5%', '≥5%']
df['hy_level'] = pd.cut(df['hy_spread'], bins=bins, labels=labels)

print(f"\n  {'HY 수준':>10} | {'일수':>5} | {'SP500 평균':>12} | {'SP500 중위':>12} | {'NASDAQ 평균':>12} | {'하락비율':>8}")
print("  " + "-" * 75)

for level in labels:
    mask = df['hy_level'] == level
    n = mask.sum()
    if n > 0:
        sp_mean = df.loc[mask, 'sp500_ret'].mean()
        sp_med = df.loc[mask, 'sp500_ret'].median()
        ndx_mean = df.loc[mask, 'nasdaq_ret'].mean()
        down_pct = (df.loc[mask, 'sp500_ret'] < 0).mean() * 100
        print(f"  {level:>10} | {n:>4}일 | {sp_mean:>+10.3f}% | {sp_med:>+10.3f}% | {ndx_mean:>+10.3f}% | {down_pct:>6.1f}%")

# ── 7. 연속 상승 패턴 ───────────────────────────────────────
print("\n" + "=" * 70)
print("7. HY Spread 연속 상승일수와 다음날 지수")
print("=" * 70)

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

for days in [2, 3, 4, 5]:
    mask = (df['hy_consec_up'] == days) & df['sp500_next'].notna()
    n = mask.sum()
    if n > 0:
        sp_mean = df.loc[mask, 'sp500_next'].mean()
        ndx_mean = df.loc[mask, 'nasdaq_next'].mean()
        sp_down = (df.loc[mask, 'sp500_next'] < 0).mean() * 100
        print(f"  {days}일 연속 상승: {n}회 → 다음날 SP500 {sp_mean:+.3f}%, NASDAQ {ndx_mean:+.3f}% (하락확률 {sp_down:.0f}%)")

# ── 8. 20일 MA 기울기 분석 ──────────────────────────────────
print("\n" + "=" * 70)
print("8. HY Spread 20일 MA 기울기 (추세)")
print("=" * 70)

df['hy_ma20'] = df['hy_spread'].rolling(20).mean()
df['hy_ma20_slope'] = df['hy_spread'].rolling(20).apply(
    lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) == 20 else np.nan
)

valid3 = df.dropna(subset=['hy_ma20_slope', 'sp500_ret'])
slope_q = valid3['hy_ma20_slope'].quantile([0.1, 0.25, 0.5, 0.75, 0.9])
print(f"  기울기 분포:")
for q, v in slope_q.items():
    print(f"    {q*100:.0f}%ile: {v:.4f}")

# 기울기 구간별 성과
slope_bins = [(-999, -0.02), (-0.02, -0.005), (-0.005, 0.005), (0.005, 0.02), (0.02, 999)]
slope_labels = ['급하락', '완만하락', '보합', '완만상승', '급상승']

print(f"\n  {'MA20 기울기':>12} | {'일수':>5} | {'SP500':>10} | {'NASDAQ':>10} | {'하락비율':>8}")
print("  " + "-" * 60)

for (lo, hi), label in zip(slope_bins, slope_labels):
    mask = (valid3['hy_ma20_slope'] >= lo) & (valid3['hy_ma20_slope'] < hi)
    n = mask.sum()
    if n > 0:
        sp_mean = valid3.loc[mask, 'sp500_ret'].mean()
        ndx_mean = valid3.loc[mask, 'nasdaq_ret'].mean()
        down = (valid3.loc[mask, 'sp500_ret'] < 0).mean() * 100
        print(f"  {label:>12} | {n:>4}일 | {sp_mean:>+8.3f}% | {ndx_mean:>+8.3f}% | {down:>6.1f}%")

# ── 9. 최근 상황 ────────────────────────────────────────────
print("\n" + "=" * 70)
print("9. 최근 20일 HY Spread 추이")
print("=" * 70)

recent = df.tail(20)
for date, row in recent.iterrows():
    hy = row.get('hy_spread', np.nan)
    chg = row.get('hy_change_bp', np.nan)
    sp = row.get('sp500_ret', np.nan)
    ndx = row.get('nasdaq_ret', np.nan)
    arrow = '▲' if chg > 0 else '▼' if chg < 0 else '─'
    print(f"  {date.strftime('%m/%d')} | HY {hy:.2f}% ({arrow}{abs(chg):.0f}bp) | SP500 {sp:+.2f}% | NASDAQ {ndx:+.2f}%")

# ── 10. 요약 ────────────────────────────────────────────────
print("\n" + "=" * 70)
print("10. 분석 요약")
print("=" * 70)
print(f"""
  [동행 관계] HY↑ = 지수↓ (상관계수 {corr_sp:.3f})
  [선행 관계] HY→다음날 지수 (상관계수 {corr_sp_lead:.3f})
  [역방향]    지수→다음날 HY (상관계수 {corr_rev_sp:.3f})

  데이터 기간: {df.index[0].date()} ~ {df.index[-1].date()} ({len(df)}일)
  현재 HY Spread: {df['hy_spread'].iloc[-1]:.2f}%
""")
