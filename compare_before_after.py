"""
Case 1 보너스 적용 전/후 성과 비교
동일 조건: 진입 rank≤3, 이탈 rank>11, 슬롯 3, 균등비중
"""
import sqlite3
import pickle
import pandas as pd
import numpy as np
import sys
sys.stdout.reconfigure(encoding='utf-8')

# 가격 데이터 (캐시)
with open('grid_cache.pkl','rb') as f:
    cache = pickle.load(f)

# yfinance 가격 재로드
import yfinance as yf
tickers_all = sorted(cache['df']['ticker'].unique())
px = yf.download(tickers_all, start='2025-12-01', end='2026-04-25',
                 auto_adjust=True, progress=False, threads=True)['Close']
px.index = pd.to_datetime(px.index).tz_localize(None)

def get_close(tk, d):
    if tk not in px.columns: return np.nan
    s = px[tk].dropna()
    s = s[s.index <= pd.Timestamp(d)]
    return float(s.iloc[-1]) if len(s) else np.nan

def get_next_close(tk, d):
    if tk not in px.columns: return np.nan
    s = px[tk].dropna()
    s = s[s.index > pd.Timestamp(d)]
    return float(s.iloc[0]) if len(s) else np.nan

def simulate_portfolio(db_path, label):
    """진입 rank≤3, 이탈 rank>11, 슬롯 3, 균등비중"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')
    all_dates = [r[0] for r in cur.fetchall()]

    # 각 날짜의 rank 맵
    rank_by_date = {}
    for d in all_dates:
        cur.execute('SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,))
        rank_by_date[d] = {r[0]: r[1] for r in cur.fetchall()}
    conn.close()

    # 포트폴리오 시뮬
    portfolio = {}  # ticker → {'entry_date', 'entry_price'}
    trades = []     # 완료된 매매
    daily_returns = []  # 일별 포트폴리오 수익률

    for i, d in enumerate(all_dates):
        ranks = rank_by_date.get(d, {})

        # 이탈 체크 (rank > 11)
        for tk in list(portfolio.keys()):
            r = ranks.get(tk, 999)
            if r > 11:
                exit_px = get_close(tk, d)
                entry = portfolio.pop(tk)
                if entry['entry_price'] and exit_px:
                    ret = (exit_px - entry['entry_price']) / entry['entry_price'] * 100
                    hold = i - entry['entry_idx']
                    trades.append({
                        'ticker': tk, 'entry': entry['entry_date'], 'exit': d,
                        'hold_days': hold, 'ret': ret
                    })

        # 진입 체크 (rank ≤ 3, 슬롯 여유 있으면)
        if len(portfolio) < 3:
            candidates = [(tk, r) for tk, r in ranks.items() if r <= 3 and tk not in portfolio]
            candidates.sort(key=lambda x: x[1])
            for tk, r in candidates:
                if len(portfolio) >= 3:
                    break
                entry_px = get_close(tk, d)
                if entry_px and entry_px > 0:
                    portfolio[tk] = {
                        'entry_date': d, 'entry_price': entry_px, 'entry_idx': i
                    }

        # 일별 포트폴리오 수익률 (보유 종목의 평균 수익)
        if portfolio:
            day_rets = []
            for tk, info in portfolio.items():
                px_now = get_close(tk, d)
                if px_now and info['entry_price']:
                    day_rets.append((px_now - info['entry_price']) / info['entry_price'] * 100)
            if day_rets:
                daily_returns.append({'date': d, 'port_ret': np.mean(day_rets), 'n_stocks': len(portfolio)})

    # 보유중 종목 마지막 날 기준 청산
    last_date = all_dates[-1]
    for tk in list(portfolio.keys()):
        exit_px = get_close(tk, last_date)
        entry = portfolio[tk]
        if entry['entry_price'] and exit_px:
            ret = (exit_px - entry['entry_price']) / entry['entry_price'] * 100
            hold = len(all_dates) - 1 - entry['entry_idx']
            trades.append({
                'ticker': tk, 'entry': entry['entry_date'], 'exit': last_date + '(보유중)',
                'hold_days': hold, 'ret': ret
            })

    trades_df = pd.DataFrame(trades) if trades else pd.DataFrame()
    daily_df = pd.DataFrame(daily_returns) if daily_returns else pd.DataFrame()

    return trades_df, daily_df, label

# 실행
print("="*90)
print("Case 1 보너스 적용 전/후 성과 비교")
print("조건: 진입 rank≤3, 이탈 rank>11, 슬롯 3, 균등비중")
print("="*90)

results = {}
for db, label in [
    ('eps_momentum_data.db.bak_pre_case1', '기존 (보너스 없음)'),
    ('eps_momentum_data.db', '신규 (Case 1 +30점)')
]:
    trades, daily, lbl = simulate_portfolio(db, label)
    results[lbl] = (trades, daily)

    print(f"\n{'─'*60}")
    print(f"  {lbl}")
    print(f"{'─'*60}")
    if len(trades):
        print(f"  매매 건수: {len(trades)}")
        print(f"  평균 수익: {trades['ret'].mean():+.2f}%")
        print(f"  중앙값:    {trades['ret'].median():+.2f}%")
        print(f"  승률:      {(trades['ret']>0).mean()*100:.1f}%")
        print(f"  최대 이익: {trades['ret'].max():+.2f}%")
        print(f"  최대 손실: {trades['ret'].min():+.2f}%")
        print(f"  평균 보유: {trades['hold_days'].mean():.1f}일")
        print(f"  총수익합:  {trades['ret'].sum():+.2f}%")

# 직접 비교
print(f"\n{'='*90}")
print("직접 비교")
print(f"{'='*90}")

old_t, _ = results['기존 (보너스 없음)']
new_t, _ = results['신규 (Case 1 +30점)']

if len(old_t) and len(new_t):
    metrics = ['평균수익', '중앙값', '승률', '총수익합', '매매건수', '평균보유']
    old_vals = [old_t['ret'].mean(), old_t['ret'].median(),
                (old_t['ret']>0).mean()*100, old_t['ret'].sum(),
                len(old_t), old_t['hold_days'].mean()]
    new_vals = [new_t['ret'].mean(), new_t['ret'].median(),
                (new_t['ret']>0).mean()*100, new_t['ret'].sum(),
                len(new_t), new_t['hold_days'].mean()]

    print(f"\n{'지표':<12}{'기존':>12}{'신규':>12}{'차이':>12}")
    for m, o, n in zip(metrics, old_vals, new_vals):
        unit = '%' if '수익' in m or '승률' in m else ('일' if '보유' in m else '건')
        print(f"  {m:<10}{o:>+10.2f}{unit}  {n:>+10.2f}{unit}  {n-o:>+10.2f}{unit}")

# 매매 상세 비교
print(f"\n{'='*90}")
print("매매 상세 (기존 vs 신규)")
print(f"{'='*90}")
print(f"\n[기존] 최근 10건:")
if len(old_t):
    for _, r in old_t.tail(10).iterrows():
        print(f"  {r['ticker']:6s} {r['entry']}→{r['exit']:<16} {r['hold_days']:.0f}일 {r['ret']:+7.2f}%")

print(f"\n[신규] 최근 10건:")
if len(new_t):
    for _, r in new_t.tail(10).iterrows():
        print(f"  {r['ticker']:6s} {r['entry']}→{r['exit']:<16} {r['hold_days']:.0f}일 {r['ret']:+7.2f}%")
