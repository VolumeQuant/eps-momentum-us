"""저커버리지 보호 룰 BT — DB 누적 데이터 기반.

매일 Top 3 + ✅ 진입 → 다음 영업일 종가 매수 → exit(p2>8 OR min_seg<-2%) 다음날 매도.
num_analysts 임계값별 비교: baseline(필터 없음) vs >=5/>=7/>=10/>=15.
"""

import sys
import sqlite3
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

DB = Path(__file__).parent.parent / 'eps_momentum_data.db'


def load_data():
    """date -> ticker -> dict of fields."""
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT date, ticker, composite_rank, part2_rank, num_analysts,
               price, adj_gap, score
        FROM ntm_screening
        WHERE date >= '2026-02-10'
    """)
    data = defaultdict(dict)
    for row in cur.fetchall():
        d, tk, cr, p2, na, px, ag, sc = row
        data[d][tk] = {
            'cr': cr, 'p2': p2, 'num_analysts': na,
            'price': px, 'adj_gap': ag, 'score': sc,
        }
    # 4-segment trend (for min_seg)
    seg_map = defaultdict(dict)
    for r in cur.execute("""
        SELECT date, ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
        FROM ntm_screening
        WHERE date >= '2026-02-10'
    """).fetchall():
        d, tk, c, n7, n30, n60, n90 = r
        if all(x is not None and x > 0 for x in (c, n7, n30, n60, n90)):
            seg1 = (c - n7) / abs(n7) * 100
            seg2 = (n7 - n30) / abs(n30) * 100
            seg3 = (n30 - n60) / abs(n60) * 100
            seg4 = (n60 - n90) / abs(n90) * 100
            seg_map[d][tk] = min(seg1, seg2, seg3, seg4)
        else:
            seg_map[d][tk] = None
    conn.close()
    return data, seg_map


def get_verified_status(data, dates, today, ticker):
    """오늘 cr Top 20 3일 연속 확인 (✅ 검증)."""
    idx = dates.index(today)
    if idx < 2:
        return False
    for i in range(3):
        d = dates[idx - i]
        e = data[d].get(ticker)
        if not e or e['cr'] is None or e['cr'] > 20:
            return False
    return True


def simulate(data, seg_map, dates, min_analysts=0):
    """num_analysts >= min_analysts 종목만 진입 허용. 슬롯 3, 균등비중.

    Reality: signal date T에서 종가 매도/매수, 다음날 T+1 종가에 익일 PnL 시작.
    여기선 단순화: 시그널 날 종가 진입 → 다음 시그널 변화 시 종가 매도.
    """
    holdings = {}  # ticker -> entry_price
    cash_curve = [1.0]
    nav = 1.0
    trades = []

    for i, d in enumerate(dates):
        # 1) 퇴출: p2 > 8 OR min_seg < -2% OR p2 NULL
        sells = []
        for tk in list(holdings.keys()):
            e = data[d].get(tk)
            ms = seg_map[d].get(tk)
            sell_reason = None
            if not e or e['p2'] is None:
                sell_reason = 'p2_null'
            elif e['p2'] > 8:
                sell_reason = f"p2>{e['p2']}"
            elif ms is not None and ms < -2.0:
                sell_reason = f'min_seg{ms:.1f}'
            if sell_reason:
                px = e['price'] if e else None
                if px and holdings[tk] > 0:
                    pnl = px / holdings[tk] - 1
                    trades.append({
                        'sell_date': d, 'ticker': tk, 'pnl': pnl,
                        'reason': sell_reason,
                        'num_analysts': e.get('num_analysts') if e else None,
                    })
                sells.append(tk)
        for tk in sells:
            del holdings[tk]

        # 2) 신규 진입: Top 3 + ✅ + (옵션 num_analysts 필터)
        candidates = []
        for tk, e in data[d].items():
            if e['p2'] is None or e['p2'] > 3:
                continue
            if not get_verified_status(data, dates, d, tk):
                continue
            ms = seg_map[d].get(tk)
            if ms is not None and ms < 0:
                continue
            na = e.get('num_analysts') or 0
            if na < min_analysts:
                continue
            candidates.append((e['p2'], tk, e['price']))
        candidates.sort()

        # 슬롯 3 채우기
        for p2, tk, px in candidates:
            if len(holdings) >= 3:
                break
            if tk in holdings:
                continue
            if px and px > 0:
                holdings[tk] = px

        # 3) NAV 계산: 균등비중 보유 종목 평균 수익률
        if i > 0 and holdings:
            prev_d = dates[i - 1]
            rets = []
            for tk in holdings:
                cur_e = data[d].get(tk)
                prev_e = data[prev_d].get(tk)
                if cur_e and prev_e and cur_e['price'] and prev_e['price']:
                    rets.append(cur_e['price'] / prev_e['price'] - 1)
            if rets:
                day_ret = sum(rets) / len(rets)
                nav *= (1 + day_ret)
        cash_curve.append(nav)

    return nav, trades, cash_curve


def metrics(trades, nav, days):
    if not trades:
        return {'nav': nav, 'n_trades': 0, 'win_rate': 0, 'avg_pnl': 0}
    n = len(trades)
    wins = sum(1 for t in trades if t['pnl'] > 0)
    avg = sum(t['pnl'] for t in trades) / n
    return {
        'nav': nav, 'cagr_proxy': (nav - 1) * (252 / max(days, 1)) * 100,
        'n_trades': n, 'win_rate': wins / n * 100, 'avg_pnl_pct': avg * 100,
    }


def main():
    data, seg_map = load_data()
    dates = sorted(data.keys())
    print(f'Date range: {dates[0]} ~ {dates[-1]} ({len(dates)} days)')
    print()

    print(f"{'Filter':<20} {'NAV':<8} {'Ret%':<8} {'Trades':<8} {'Win%':<8} {'AvgPnL%':<10} {'LowCovEntries':<14}")
    print('-' * 78)
    for thr in [0, 5, 7, 10, 15, 20]:
        nav, trades, _ = simulate(data, seg_map, dates, min_analysts=thr)
        m = metrics(trades, nav, len(dates))
        low_cov = sum(1 for t in trades if (t.get('num_analysts') or 0) < 5)
        ret_pct = (nav - 1) * 100
        label = 'baseline' if thr == 0 else f'min_analysts>={thr}'
        print(f"{label:<20} {nav:<8.4f} {ret_pct:<8.2f} {m['n_trades']:<8} "
              f"{m['win_rate']:<8.1f} {m['avg_pnl_pct']:<10.3f} {low_cov:<14}")

    print()
    print('=== baseline 저커버리지(<5) 거래 상세 ===')
    nav, trades, _ = simulate(data, seg_map, dates, min_analysts=0)
    low_cov_trades = [t for t in trades if (t.get('num_analysts') or 0) < 5]
    if low_cov_trades:
        print(f"{'sell_date':<12} {'ticker':<8} {'pnl%':<8} {'reason':<14} {'num':<5}")
        for t in low_cov_trades:
            print(f"{t['sell_date']:<12} {t['ticker']:<8} "
                  f"{t['pnl']*100:<8.2f} {t['reason']:<14} {t['num_analysts'] or 'N/A':<5}")
    else:
        print('(none)')


if __name__ == '__main__':
    main()
