"""ETF Pulse 고급 BT — sector rotation, AUM growth 신호 검증

가설 검증:
  1. 강한 카테고리 N개 → 그 안의 best ETF 매수 → 우월?
  2. AUM 증가 (= fund flow) 큰 ETF 추종 → 우월?
  3. 거래량 spike + 가격 모멘텀 dual → 우월?
"""
import sys
import sqlite3
import statistics
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def load_data():
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM etf_daily ORDER BY date').fetchall()]
    data = defaultdict(dict)
    for r in cur.execute('SELECT date, ticker, category, price, day_return, volume_spike, aum FROM etf_daily').fetchall():
        d, tk, cat, p, ret, spike, aum = r
        data[tk][d] = {'price': p, 'ret': ret, 'spike': spike, 'aum': aum, 'cat': cat}
    conn.close()
    return dates, data


def fwd_return(data, ticker, start, hold, dates):
    if ticker not in data: return None
    tk_data = data[ticker]
    if start not in tk_data: return None
    sp = tk_data[start]['price']
    if start not in dates: return None
    idx = dates.index(start)
    if idx + hold >= len(dates): return None
    end = dates[idx + hold]
    if end not in tk_data: return None
    ep = tk_data[end]['price']
    return (ep - sp) / sp * 100


def test_sector_rotation_dual(dates, data, holds=[1, 3, 5]):
    """강한 카테고리 + best ETF 매수 (dual signal)"""
    print('\n[BT-A] 강한 카테고리 (Top 1) + 그 카테고리 가장 큰 ETF 매수')
    for hold in holds:
        all_rets = []
        for i, d in enumerate(dates):
            if i + hold >= len(dates): continue
            cat_rets = defaultdict(list)
            for tk, info in data.items():
                if d in info:
                    cat_rets[info[d]['cat']].append((tk, info[d]['ret'], info[d]['aum']))
            # 카테고리별 평균 수익률 + 그 카테고리 내 AUM 1위
            cat_avg = []
            for cat, lst in cat_rets.items():
                if len(lst) >= 3:
                    avg = sum(r for _, r, _ in lst) / len(lst)
                    largest = max(lst, key=lambda x: x[2])
                    cat_avg.append((cat, avg, largest[0]))
            cat_avg.sort(key=lambda x: -x[1])
            if not cat_avg: continue
            top_cat, _, top_etf = cat_avg[0]
            fr = fwd_return(data, top_etf, d, hold, dates)
            if fr is not None:
                all_rets.append(fr)
        if all_rets:
            avg = sum(all_rets)/len(all_rets)
            wins = sum(1 for r in all_rets if r > 0) / len(all_rets) * 100
            print(f'  hold={hold}d: n={len(all_rets):>3}  avg={avg:+6.2f}%  win={wins:.1f}%')


def test_aum_growth_signal(dates, data, holds=[1, 3, 5]):
    """AUM 증가율 큰 ETF Top 5 매수 (fund flow proxy)"""
    print('\n[BT-B] AUM 증가율 Top 5 매수 (fund flow proxy, 단 backfill data는 부정확)')
    for hold in holds:
        all_rets = []
        for i, d in enumerate(dates):
            if i < 1 or i + hold >= len(dates): continue
            d_back = dates[i - 1]
            growth = []
            for tk, info in data.items():
                if d in info and d_back in info:
                    aum_now = info[d]['aum']
                    aum_back = info[d_back]['aum']
                    if aum_back > 0 and aum_now > 1e8:
                        gr = (aum_now - aum_back) / aum_back * 100
                        growth.append((tk, gr))
            growth.sort(key=lambda x: -x[1])
            for tk, _ in growth[:5]:
                fr = fwd_return(data, tk, d, hold, dates)
                if fr is not None:
                    all_rets.append(fr)
        if all_rets:
            avg = sum(all_rets)/len(all_rets)
            wins = sum(1 for r in all_rets if r > 0) / len(all_rets) * 100
            print(f'  hold={hold}d: n={len(all_rets):>4}  avg={avg:+6.2f}%  win={wins:.1f}%')


def test_dual_signal(dates, data, holds=[1, 3, 5]):
    """거래량 spike + 양수 수익률 dual filter"""
    print('\n[BT-C] dual: volume spike 1.5x+ AND day_return > 1%')
    for hold in holds:
        all_rets = []
        for d in dates[:-max(holds)]:
            cands = [(tk, info[d]['ret']) for tk, info in data.items()
                     if d in info and info[d].get('spike', 0) > 1.5 and info[d].get('ret', 0) > 1
                     and info[d].get('aum', 0) > 5e8]
            cands.sort(key=lambda x: -x[1])
            for tk, _ in cands[:5]:
                fr = fwd_return(data, tk, d, hold, dates)
                if fr is not None:
                    all_rets.append(fr)
        if all_rets:
            avg = sum(all_rets)/len(all_rets)
            wins = sum(1 for r in all_rets if r > 0) / len(all_rets) * 100
            print(f'  hold={hold}d: n={len(all_rets):>4}  avg={avg:+6.2f}%  win={wins:.1f}%')


def test_strong_then_weak(dates, data, holds=[1, 3, 5]):
    """강세 후 일시 약세 (mean reversion in uptrend)"""
    print('\n[BT-D] 5일 +5%+ AND 어제 -0.5%+ (mean reversion in uptrend)')
    for hold in holds:
        all_rets = []
        for i, d in enumerate(dates):
            if i < 5 or i + hold >= len(dates): continue
            d_5 = dates[i - 5]
            for tk, info in data.items():
                if d not in info or d_5 not in info: continue
                if info[d].get('aum', 0) < 1e9: continue
                p_now = info[d]['price']; p_5 = info[d_5]['price']
                ret_5d = (p_now - p_5) / p_5 * 100 if p_5 > 0 else 0
                ret_1d = info[d].get('ret', 0)
                if ret_5d > 5 and ret_1d < -0.5:
                    fr = fwd_return(data, tk, d, hold, dates)
                    if fr is not None:
                        all_rets.append(fr)
        if all_rets:
            avg = sum(all_rets)/len(all_rets)
            wins = sum(1 for r in all_rets if r > 0) / len(all_rets) * 100
            print(f'  hold={hold}d: n={len(all_rets):>3}  avg={avg:+6.2f}%  win={wins:.1f}%')


def baseline(dates, data, holds=[1, 3, 5]):
    print('\n[BT-baseline] SPY')
    for hold in holds:
        rets = []
        for i, d in enumerate(dates):
            if i + hold >= len(dates): continue
            fr = fwd_return(data, 'SPY', d, hold, dates)
            if fr is not None: rets.append(fr)
        if rets:
            avg = sum(rets)/len(rets)
            wins = sum(1 for r in rets if r > 0)/len(rets)*100
            print(f'  hold={hold}d: n={len(rets):>2}  avg={avg:+6.2f}%  win={wins:.1f}%')


def main():
    print('=' * 70)
    print('ETF Pulse 고급 BT — 새 신호 검증')
    print('=' * 70)
    dates, data = load_data()
    print(f'기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일), {len(data)} ETF')
    baseline(dates, data)
    test_sector_rotation_dual(dates, data)
    test_aum_growth_signal(dates, data)
    test_dual_signal(dates, data)
    test_strong_then_weak(dates, data)
    print('\n' + '=' * 70)
    print('해석:')
    print('  - SPY baseline 강세장이라 절대 비교 어려움')
    print('  - dual signal (spike + return)이 단기 모멘텀 잡는지 확인')
    print('  - 30일 BT 표본 한계 — 1년+ 누적 후 정확')


if __name__ == '__main__':
    main()
