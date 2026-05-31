"""ETF Pulse 신호 BT — 신호들이 진짜 alpha 만드는지 검증

검증 신호:
  1. 거래량 spike Top N: 매수 → N일 보유 → 수익률
  2. 5일 모멘텀 Top N: 매수 → N일 보유
  3. 신고가 추종: 30일 신고가 ETF 매수 → 다음 1주 수익
  4. 카테고리 회전: 어제 강세 카테고리 매수 → 다음 1주

30일 backfill 데이터로 walk-forward BT.
"""
import sys
import sqlite3
import statistics
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def get_dates_returns():
    """모든 ETF 날짜별 가격 + 카테고리 로드"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM etf_daily ORDER BY date').fetchall()]
    # ticker → {date: price, day_return, volume_spike, category}
    data = defaultdict(dict)
    for r in cur.execute('SELECT date, ticker, category, price, day_return, volume_spike, aum FROM etf_daily').fetchall():
        d, tk, cat, p, ret, spike, aum = r
        data[tk][d] = {'price': p, 'day_return': ret, 'spike': spike, 'aum': aum, 'category': cat}
    conn.close()
    return dates, data


def fwd_return(data, ticker, start_date, hold_days, dates_sorted):
    """start_date 종가에 매수, hold_days 후 수익률"""
    if ticker not in data: return None
    tk_data = data[ticker]
    if start_date not in tk_data: return None
    start_p = tk_data[start_date]['price']

    if start_date not in dates_sorted: return None
    idx = dates_sorted.index(start_date)
    if idx + hold_days >= len(dates_sorted): return None
    end_date = dates_sorted[idx + hold_days]
    if end_date not in tk_data: return None
    end_p = tk_data[end_date]['price']

    return (end_p - start_p) / start_p * 100


def test_volume_spike_signal(dates, data, top_n=5, spike_min=1.5, holds=[1, 3, 5]):
    """거래량 spike Top N 매수 → N일 보유 수익률"""
    print('\n[BT 1] 거래량 spike Top 5 매수 → N일 보유')
    for hold in holds:
        all_rets = []
        for d in dates[:-hold]:
            # 그날 거래량 spike Top 5
            candidates = [(tk, info[d]['spike'], info[d].get('day_return', 0))
                          for tk, info in data.items()
                          if d in info and info[d].get('spike', 0) > spike_min]
            candidates.sort(key=lambda x: -x[1])
            for tk, sp, ret in candidates[:top_n]:
                fr = fwd_return(data, tk, d, hold, dates)
                if fr is not None:
                    all_rets.append(fr)
        if all_rets:
            avg = sum(all_rets)/len(all_rets)
            med = sorted(all_rets)[len(all_rets)//2]
            wins = sum(1 for r in all_rets if r > 0) / len(all_rets) * 100
            print(f'  hold={hold}d: n={len(all_rets):>4}  avg={avg:+6.2f}%  med={med:+6.2f}%  win_rate={wins:.1f}%')


def test_momentum_signal(dates, data, top_n=5, lookback=5, holds=[1, 3, 5]):
    """5일 모멘텀 Top N 매수 → N일 보유"""
    print(f'\n[BT 2] {lookback}일 모멘텀 Top {top_n} 매수 → N일 보유')
    for hold in holds:
        all_rets = []
        for i, d in enumerate(dates):
            if i < lookback or i + hold >= len(dates): continue
            d_back = dates[i - lookback]
            # 5일 수익률 계산
            candidates = []
            for tk, info in data.items():
                if d in info and d_back in info:
                    p_now = info[d]['price']
                    p_back = info[d_back]['price']
                    aum = info[d].get('aum', 0)
                    if p_back > 0 and aum > 5e8:
                        ret_5d = (p_now - p_back) / p_back * 100
                        candidates.append((tk, ret_5d))
            candidates.sort(key=lambda x: -x[1])
            for tk, _ in candidates[:top_n]:
                fr = fwd_return(data, tk, d, hold, dates)
                if fr is not None:
                    all_rets.append(fr)
        if all_rets:
            avg = sum(all_rets)/len(all_rets)
            wins = sum(1 for r in all_rets if r > 0) / len(all_rets) * 100
            print(f'  hold={hold}d: n={len(all_rets):>4}  avg={avg:+6.2f}%  win_rate={wins:.1f}%')


def test_category_rotation(dates, data, holds=[1, 3, 5]):
    """카테고리 평균 수익률 Top 1 매수 → N일 보유 (그 카테고리 전체 등가중)"""
    print(f'\n[BT 3] 어제 강세 카테고리 (top 1) 매수 → N일 보유 (등가중)')
    for hold in holds:
        all_rets = []
        for i, d in enumerate(dates):
            if i + hold >= len(dates): continue
            # 카테고리별 평균 수익률
            cat_rets = defaultdict(list)
            for tk, info in data.items():
                if d in info:
                    cat_rets[info[d]['category']].append(info[d].get('day_return', 0))
            cat_avg = [(cat, sum(rs)/len(rs)) for cat, rs in cat_rets.items() if len(rs) >= 3]
            cat_avg.sort(key=lambda x: -x[1])
            if not cat_avg: continue
            top_cat = cat_avg[0][0]
            # 그 카테고리 전체 ETF 등가중
            cat_tks = [tk for tk, info in data.items() if d in info and info[d]['category'] == top_cat]
            for tk in cat_tks:
                fr = fwd_return(data, tk, d, hold, dates)
                if fr is not None:
                    all_rets.append(fr)
        if all_rets:
            avg = sum(all_rets)/len(all_rets)
            wins = sum(1 for r in all_rets if r > 0) / len(all_rets) * 100
            print(f'  hold={hold}d: n={len(all_rets):>4}  avg={avg:+6.2f}%  win_rate={wins:.1f}%')


def test_new_high_signal(dates, data, lookback=30, holds=[1, 3, 5]):
    """30일 신고가 ETF 매수 → N일 보유"""
    print(f'\n[BT 4] {lookback}일 신고가 ETF 매수 → N일 보유')
    for hold in holds:
        all_rets = []
        for i, d in enumerate(dates):
            if i < lookback or i + hold >= len(dates): continue
            # 30일 max
            for tk, info in data.items():
                if d not in info: continue
                aum = info[d].get('aum', 0)
                if aum < 5e8: continue
                prices = [info[dd]['price'] for dd in dates[max(0,i-lookback):i+1] if dd in info]
                if len(prices) < 10: continue
                if info[d]['price'] >= max(prices) * 0.999:  # 신고가 (0.1% tolerance)
                    fr = fwd_return(data, tk, d, hold, dates)
                    if fr is not None:
                        all_rets.append(fr)
        if all_rets:
            avg = sum(all_rets)/len(all_rets)
            wins = sum(1 for r in all_rets if r > 0) / len(all_rets) * 100
            print(f'  hold={hold}d: n={len(all_rets):>4}  avg={avg:+6.2f}%  win_rate={wins:.1f}%')


def test_benchmark(dates, data, holds=[1, 3, 5]):
    """SPY 단순 보유 baseline (비교용)"""
    print(f'\n[BT baseline] SPY 단순 보유 (등가중 vs benchmark)')
    for hold in holds:
        rets = []
        for i, d in enumerate(dates):
            if i + hold >= len(dates): continue
            fr = fwd_return(data, 'SPY', d, hold, dates)
            if fr is not None:
                rets.append(fr)
        if rets:
            avg = sum(rets)/len(rets)
            wins = sum(1 for r in rets if r > 0) / len(rets) * 100
            print(f'  hold={hold}d: n={len(rets):>3}  avg={avg:+6.2f}%  win_rate={wins:.1f}%')


def main():
    print('=' * 80)
    print('ETF Pulse 신호 BT — 30일 backfill 데이터')
    print('=' * 80)

    dates, data = get_dates_returns()
    print(f'\n기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)')
    print(f'ETF 수: {len(data)}')

    test_benchmark(dates, data)
    test_volume_spike_signal(dates, data)
    test_momentum_signal(dates, data)
    test_category_rotation(dates, data)
    test_new_high_signal(dates, data)

    print('\n' + '=' * 80)
    print('결론:')
    print('  - 평균 수익률이 SPY baseline 대비 일관 우월하면 진짜 alpha')
    print('  - 단 30일 BT는 표본 부족, 향후 1년+ 누적 후 재검증 필요')


if __name__ == '__main__':
    main()
