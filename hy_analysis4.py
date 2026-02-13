"""
Verdad 4분면 모델 분석
- 수준: HY spread vs 10년 롤링 중위수
- 방향: 현재 vs 3개월(60영업일) 전
- 4분면별 향후 지수 수익률
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
rut = yf.download("^RUT", start="1997-01-01", end="2026-02-13", progress=False)  # 소형주
if isinstance(spy.columns, pd.MultiIndex):
    spy.columns = spy.columns.get_level_values(0)
    ndx.columns = ndx.columns.get_level_values(0)
    rut.columns = rut.columns.get_level_values(0)

df = pd.DataFrame(index=spy.index)
df['sp500'] = spy['Close']
df['nasdaq'] = ndx['Close']
df['russell'] = rut['Close']
df = df.join(hy_df[['hy_spread']])
df = df.dropna()

print(f"데이터: {len(df)}일, {df.index[0].date()} ~ {df.index[-1].date()}")

# ── Verdad 모델 변수 ────────────────────────────────────────
# 수준: 10년(2520 영업일) 롤링 중위수 대비
df['hy_median_10y'] = df['hy_spread'].rolling(2520, min_periods=1260).median()
df['level'] = np.where(df['hy_spread'] >= df['hy_median_10y'], 'wide', 'narrow')

# 방향: 3개월(63 영업일) 전 대비
df['hy_3m_ago'] = df['hy_spread'].shift(63)
df['direction'] = np.where(df['hy_spread'] >= df['hy_3m_ago'], 'rising', 'falling')

# 4분면
conditions = {
    'Q1 회복 (넓+하락)': (df['level'] == 'wide') & (df['direction'] == 'falling'),
    'Q2 성장 (좁+하락)': (df['level'] == 'narrow') & (df['direction'] == 'falling'),
    'Q3 과열 (좁+상승)': (df['level'] == 'narrow') & (df['direction'] == 'rising'),
    'Q4 침체 (넓+상승)': (df['level'] == 'wide') & (df['direction'] == 'rising'),
}

df['quadrant'] = 'unknown'
for name, mask in conditions.items():
    df.loc[mask, 'quadrant'] = name

# 미래 수익률
for d in [1, 5, 10, 20, 60, 120, 250]:
    df[f'sp_fwd_{d}d'] = (df['sp500'].shift(-d) / df['sp500'] - 1) * 100
    df[f'nq_fwd_{d}d'] = (df['nasdaq'].shift(-d) / df['nasdaq'] - 1) * 100
    df[f'rt_fwd_{d}d'] = (df['russell'].shift(-d) / df['russell'] - 1) * 100

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("1. ★ Verdad 4분면별 향후 수익률 (연율화)")
print("=" * 90)

valid = df.dropna(subset=['hy_median_10y', 'hy_3m_ago'])
print(f"\n  분석 기간: {valid.index[0].date()} ~ {valid.index[-1].date()} ({len(valid)}일)")
print(f"  현재 10년 중위수: {valid['hy_median_10y'].iloc[-1]:.2f}%")
print(f"  현재 HY: {valid['hy_spread'].iloc[-1]:.2f}% → {'넓음' if valid['level'].iloc[-1] == 'wide' else '좁음'}")
print(f"  3개월 전 HY: {valid['hy_3m_ago'].iloc[-1]:.2f}% → {'상승' if valid['direction'].iloc[-1] == 'rising' else '하락'}")
print(f"  현재 분면: {valid['quadrant'].iloc[-1]}")

for qname in ['Q1 회복 (넓+하락)', 'Q2 성장 (좁+하락)', 'Q3 과열 (좁+상승)', 'Q4 침체 (넓+상승)']:
    mask = valid['quadrant'] == qname
    n = mask.sum()
    if n < 10:
        continue

    print(f"\n  ┌── {qname} ({n}일, {n/len(valid)*100:.0f}%) ──")
    print(f"  │ HY 평균: {valid.loc[mask, 'hy_spread'].mean():.2f}%, 범위: {valid.loc[mask, 'hy_spread'].min():.2f}~{valid.loc[mask, 'hy_spread'].max():.2f}%")

    print(f"  │")
    print(f"  │ {'':>12} | {'1일':>8} | {'5일':>8} | {'20일':>8} | {'60일':>8} | {'120일':>9} | {'250일':>9}")
    print(f"  │ " + "-" * 75)

    for idx_name, prefix in [('S&P 500', 'sp'), ('NASDAQ', 'nq'), ('Russell', 'rt')]:
        vals = {}
        for d in [1, 5, 20, 60, 120, 250]:
            col = f'{prefix}_fwd_{d}d'
            sub = valid.loc[mask, col].dropna()
            vals[d] = sub.mean() if len(sub) >= 10 else None

        parts = []
        for d in [1, 5, 20, 60, 120, 250]:
            if vals[d] is not None:
                parts.append(f"{vals[d]:>+6.2f}%")
            else:
                parts.append(f"{'N/A':>7}")

        print(f"  │ {idx_name:>12} | {' | '.join(parts)}")

    # 연율화 (250일 수익률 기준)
    sp250 = valid.loc[mask, 'sp_fwd_250d'].dropna()
    if len(sp250) >= 10:
        ann = sp250.mean()
        win = (sp250 > 0).mean() * 100
        print(f"  │")
        print(f"  │ SP500 연율: {ann:+.1f}%, 양수확률: {win:.0f}%, 중위: {sp250.median():+.1f}%")

    print(f"  └──")

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("2. ★ 분면 전환 시점별 향후 수익률")
print("=" * 90)

valid['prev_quadrant'] = valid['quadrant'].shift(1)
valid['q_changed'] = valid['quadrant'] != valid['prev_quadrant']

transitions = [
    ('Q4→Q1 (침체→회복)', 'Q4 침체 (넓+상승)', 'Q1 회복 (넓+하락)'),
    ('Q1→Q2 (회복→성장)', 'Q1 회복 (넓+하락)', 'Q2 성장 (좁+하락)'),
    ('Q2→Q3 (성장→과열)', 'Q2 성장 (좁+하락)', 'Q3 과열 (좁+상승)'),
    ('Q3→Q4 (과열→침체)', 'Q3 과열 (좁+상승)', 'Q4 침체 (넓+상승)'),
    # 건너뛰기
    ('Q2→Q4 (성장→침체)', 'Q2 성장 (좁+하락)', 'Q4 침체 (넓+상승)'),
    ('Q3→Q1 (과열→회복)', 'Q3 과열 (좁+상승)', 'Q1 회복 (넓+하락)'),
]

print(f"\n  {'전환':>22} | {'발생':>5} | {'20일SP':>8} | {'60일SP':>8} | {'120일SP':>9} | {'250일SP':>9}")
print("  " + "-" * 75)

for name, from_q, to_q in transitions:
    mask = (valid['prev_quadrant'] == from_q) & (valid['quadrant'] == to_q)
    n = mask.sum()
    if n >= 3:
        d20 = valid.loc[mask, 'sp_fwd_20d'].dropna().mean()
        d60 = valid.loc[mask, 'sp_fwd_60d'].dropna().mean()
        d120 = valid.loc[mask, 'sp_fwd_120d'].dropna().mean() if valid.loc[mask, 'sp_fwd_120d'].notna().sum() >= 3 else float('nan')
        d250 = valid.loc[mask, 'sp_fwd_250d'].dropna().mean() if valid.loc[mask, 'sp_fwd_250d'].notna().sum() >= 3 else float('nan')
        d120_str = f"{d120:>+7.2f}%" if not np.isnan(d120) else f"{'N/A':>8}"
        d250_str = f"{d250:>+7.2f}%" if not np.isnan(d250) else f"{'N/A':>8}"
        print(f"  {name:>22} | {n:>4}일 | {d20:>+6.2f}% | {d60:>+6.2f}% | {d120_str} | {d250_str}")

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("3. ★ 분면 지속 기간별 향후 수익률")
print("=" * 90)

# 같은 분면 연속 일수 계산
consec_q = []
count = 1
for i in range(len(valid)):
    if i == 0:
        consec_q.append(1)
        continue
    if valid['quadrant'].iloc[i] == valid['quadrant'].iloc[i-1]:
        count += 1
    else:
        count = 1
    consec_q.append(count)
valid = valid.copy()
valid['q_duration'] = consec_q

for qname in ['Q1 회복 (넓+하락)', 'Q2 성장 (좁+하락)', 'Q3 과열 (좁+상승)', 'Q4 침체 (넓+상승)']:
    print(f"\n  ┌── {qname} ──")
    for dur_label, dur_lo, dur_hi in [('초기(1~20일)', 1, 20), ('중기(21~60일)', 21, 60), ('후기(61~120일)', 61, 120), ('장기(121일+)', 121, 99999)]:
        mask = (valid['quadrant'] == qname) & (valid['q_duration'] >= dur_lo) & (valid['q_duration'] <= dur_hi)
        n = mask.sum()
        if n >= 10:
            d20 = valid.loc[mask, 'sp_fwd_20d'].dropna().mean()
            d60 = valid.loc[mask, 'sp_fwd_60d'].dropna().mean()
            print(f"  │ {dur_label}: {n:>4}일 → 20일 SP {d20:>+.2f}%, 60일 SP {d60:>+.2f}%")
    print(f"  └──")

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("4. ★ 분면별 최대 낙폭 (MDD) 분석")
print("=" * 90)

for qname in ['Q1 회복 (넓+하락)', 'Q2 성장 (좁+하락)', 'Q3 과열 (좁+상승)', 'Q4 침체 (넓+상승)']:
    mask = valid['quadrant'] == qname
    sp_rets = valid.loc[mask, 'sp_fwd_20d'].dropna()
    if len(sp_rets) < 10:
        continue

    q25 = sp_rets.quantile(0.25)
    q75 = sp_rets.quantile(0.75)
    worst = sp_rets.min()
    best = sp_rets.max()
    down_pct = (sp_rets < 0).mean() * 100

    print(f"  {qname}")
    print(f"    20일 수익률 분포: 최악 {worst:+.1f}% | 25%ile {q25:+.1f}% | 중위 {sp_rets.median():+.1f}% | 75%ile {q75:+.1f}% | 최고 {best:+.1f}%")
    print(f"    하락 확률: {down_pct:.0f}%")

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("5. ★ Verdad 모델 vs 단순 수준 모델 비교")
print("=" * 90)

# 단순 수준 모델 (고정 임계치)
simple_zones = [
    ('정상 <3.5%', 0, 3.5),
    ('주의 3.5~5%', 3.5, 5),
    ('경고 5%+', 5, 100),
]

print(f"\n  [단순 수준 모델]")
print(f"  {'구간':>12} | {'일수':>5} | {'20일SP':>8} | {'60일SP':>8} | {'250일SP':>9}")
print("  " + "-" * 55)
for name, lo, hi in simple_zones:
    mask = (valid['hy_spread'] >= lo) & (valid['hy_spread'] < hi)
    n = mask.sum()
    if n >= 10:
        d20 = valid.loc[mask, 'sp_fwd_20d'].dropna().mean()
        d60 = valid.loc[mask, 'sp_fwd_60d'].dropna().mean()
        d250 = valid.loc[mask, 'sp_fwd_250d'].dropna().mean() if valid.loc[mask, 'sp_fwd_250d'].notna().sum() >= 10 else float('nan')
        d250_str = f"{d250:>+7.2f}%" if not np.isnan(d250) else f"{'N/A':>8}"
        print(f"  {name:>12} | {n:>4}일 | {d20:>+6.2f}% | {d60:>+6.2f}% | {d250_str}")

print(f"\n  [Verdad 4분면 모델]")
print(f"  {'분면':>22} | {'일수':>5} | {'20일SP':>8} | {'60일SP':>8} | {'250일SP':>9}")
print("  " + "-" * 65)
for qname in ['Q1 회복 (넓+하락)', 'Q2 성장 (좁+하락)', 'Q3 과열 (좁+상승)', 'Q4 침체 (넓+상승)']:
    mask = valid['quadrant'] == qname
    n = mask.sum()
    if n >= 10:
        d20 = valid.loc[mask, 'sp_fwd_20d'].dropna().mean()
        d60 = valid.loc[mask, 'sp_fwd_60d'].dropna().mean()
        d250 = valid.loc[mask, 'sp_fwd_250d'].dropna().mean() if valid.loc[mask, 'sp_fwd_250d'].notna().sum() >= 10 else float('nan')
        d250_str = f"{d250:>+7.2f}%" if not np.isnan(d250) else f"{'N/A':>8}"
        print(f"  {qname:>22} | {n:>4}일 | {d20:>+6.2f}% | {d60:>+6.2f}% | {d250_str}")

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("6. ★ 분면별 시대 분포")
print("=" * 90)

periods = [
    ('1997~2002 닷컴', '1997-01-01', '2002-12-31'),
    ('2003~2006 회복', '2003-01-01', '2006-12-31'),
    ('2007~2009 GFC', '2007-01-01', '2009-12-31'),
    ('2010~2019 확장', '2010-01-01', '2019-12-31'),
    ('2020 코로나', '2020-01-01', '2020-12-31'),
    ('2021~2023 인플', '2021-01-01', '2023-12-31'),
    ('2024~현재', '2024-01-01', '2026-12-31'),
]

print(f"\n  {'시대':>14} | {'Q1회복':>6} | {'Q2성장':>6} | {'Q3과열':>6} | {'Q4침체':>6} | {'10y중위':>7}")
print("  " + "-" * 60)

for pname, start, end in periods:
    mask = (valid.index >= start) & (valid.index <= end)
    sub = valid[mask]
    if len(sub) < 10:
        continue
    q1 = (sub['quadrant'] == 'Q1 회복 (넓+하락)').mean() * 100
    q2 = (sub['quadrant'] == 'Q2 성장 (좁+하락)').mean() * 100
    q3 = (sub['quadrant'] == 'Q3 과열 (좁+상승)').mean() * 100
    q4 = (sub['quadrant'] == 'Q4 침체 (넓+상승)').mean() * 100
    med = sub['hy_median_10y'].mean()
    print(f"  {pname:>14} | {q1:>4.0f}% | {q2:>4.0f}% | {q3:>4.0f}% | {q4:>4.0f}% | {med:>5.2f}%")

# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("7. ★ 우리 시스템에 적용: 매수 적극도 매핑")
print("=" * 90)

print("""
  Verdad 분면에 따른 투자 행동 제안:

  Q1 회복 (넓+하락): 스프레드 높지만 축소 중 → 적극 매수
     - 역사적 SP500 연율: 확인 위 결과
     - 소형/가치주 극강 수익률
     - 우리 시스템: EPS 모멘텀 종목 매수 적극화

  Q2 성장 (좁+하락): 안정적 확장 → 정상 매수
     - 꾸준한 수익률
     - 우리 시스템: 기본 운영

  Q3 과열 (좁+상승): 스프레드 좁지만 확대 시작 → 주의
     - 여전히 괜찮지만 변곡점 가능
     - 우리 시스템: 메시지에 "⚠️ 과열 주의" 표시

  Q4 침체 (넓+상승): 위기 → 방어/현금
     - 역사적 최악 구간
     - 우리 시스템: "🚨 방어 모드" + 신규 매수 억제
""")

# ══════════════════════════════════════════════════════════════
print("=" * 90)
print("8. 현재 진단")
print("=" * 90)

latest = valid.iloc[-1]
print(f"""
  현재 HY Spread: {latest['hy_spread']:.2f}%
  10년 롤링 중위수: {latest['hy_median_10y']:.2f}%
  수준: {'넓음 (중위수 이상)' if latest['level'] == 'wide' else '좁음 (중위수 이하)'}
  3개월 전 HY: {latest['hy_3m_ago']:.2f}%
  방향: {'상승 중' if latest['direction'] == 'rising' else '하락 중'}

  ▶ 현재 분면: {latest['quadrant']}
""")

# 최근 6개월 분면 변화
print("  최근 6개월 분면 추이:")
recent = valid.tail(130)
prev_q = None
for date, row in recent.iterrows():
    if row['quadrant'] != prev_q:
        print(f"    {date.strftime('%Y-%m-%d')} → {row['quadrant']} (HY {row['hy_spread']:.2f}%, 중위수 {row['hy_median_10y']:.2f}%)")
        prev_q = row['quadrant']
