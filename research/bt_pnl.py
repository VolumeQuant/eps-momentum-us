"""매매 수익률 BT — baseline vs γ vs γ'' 비교.

매일 Top 3 picks 균등비중 portfolio. 다음 영업일 종가 기준 수익률.
DB 복사본 3개 사용 (이미 bt_segment_fix.py에서 생성됨).
"""
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

DBS = {
    'baseline': 'eps_test_baseline.db',
    'gamma': 'eps_test_gamma.db',
    'gamma2': 'eps_test_gamma2.db',
    'delta': 'eps_test_delta.db',
}


def load_picks_and_prices(db_path):
    """매일 Top 3 picks (part2_rank ≤ 3, ✅ 검증) + 모든 종목 일별 가격."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 매일 part2_rank ≤ 3 (실제 매매 진입 후보)
    rows = cur.execute('''
        SELECT date, ticker, part2_rank, composite_rank
        FROM ntm_screening
        WHERE part2_rank IS NOT NULL AND part2_rank <= 3
        ORDER BY date, part2_rank
    ''').fetchall()
    picks_by_date = defaultdict(list)
    for d, tk, p2, cr in rows:
        picks_by_date[d].append(tk)

    # 모든 (date, ticker) 가격 (next-day return 계산용)
    price_rows = cur.execute(
        'SELECT date, ticker, price FROM ntm_screening WHERE price IS NOT NULL'
    ).fetchall()
    prices = {(d, tk): p for d, tk, p in price_rows if p and p > 0}

    conn.close()
    return picks_by_date, prices


def simulate(picks_by_date, prices):
    """매일 Top 3 균등비중 portfolio. T일 진입, T+1일 종가로 PnL 측정."""
    dates = sorted(picks_by_date.keys())
    nav = 1.0
    daily_returns = []
    trade_log = []

    for i in range(len(dates) - 1):
        d_today = dates[i]
        d_next = dates[i + 1]
        picks = picks_by_date[d_today]
        if not picks:
            continue

        # 종목별 next-day 수익률
        rets = []
        for tk in picks:
            p_today = prices.get((d_today, tk))
            p_next = prices.get((d_next, tk))
            if p_today and p_next:
                ret = p_next / p_today - 1
                rets.append(ret)
                trade_log.append({
                    'entry_date': d_today,
                    'exit_date': d_next,
                    'ticker': tk,
                    'ret': ret,
                })

        if rets:
            day_ret = sum(rets) / len(rets)  # 균등비중
            daily_returns.append((d_today, day_ret))
            nav *= (1 + day_ret)

    return nav, daily_returns, trade_log


def metrics(nav, daily_returns, trade_log):
    n = len(daily_returns)
    if n == 0:
        return {}
    rets = [r for _, r in daily_returns]
    avg_daily = sum(rets) / n
    max_dd = 0
    peak = 1.0
    cum = 1.0
    for _, r in daily_returns:
        cum *= (1 + r)
        if cum > peak:
            peak = cum
        dd = (cum - peak) / peak
        if dd < max_dd:
            max_dd = dd

    # Sharpe (daily, naive)
    import math
    var = sum((r - avg_daily) ** 2 for r in rets) / n
    std = math.sqrt(var)
    sharpe_d = avg_daily / std if std > 0 else 0
    sharpe_a = sharpe_d * math.sqrt(252)  # annualized

    n_trades = len(trade_log)
    n_wins = sum(1 for t in trade_log if t['ret'] > 0)

    return {
        'nav': nav,
        'total_ret_pct': (nav - 1) * 100,
        'days': n,
        'cagr_proxy': ((nav ** (252 / n)) - 1) * 100 if n > 0 else 0,
        'mdd_pct': max_dd * 100,
        'sharpe_a': sharpe_a,
        'n_trades': n_trades,
        'win_rate': n_wins / n_trades * 100 if n_trades else 0,
    }


def main():
    print('=' * 70)
    print('매매 BT — Top 3 균등비중, 일일 rebalance, T+1 종가 기준')
    print('=' * 70)
    print()

    results = {}
    for name, db in DBS.items():
        if not Path(db).exists():
            print(f'  ⏩ {name}: DB 없음 ({db}), skip')
            continue
        picks, prices = load_picks_and_prices(db)
        nav, drets, tlog = simulate(picks, prices)
        m = metrics(nav, drets, tlog)
        results[name] = m

    # 출력
    print(f"{'Variant':<12} {'NAV':<8} {'Ret%':<8} {'CAGR%':<8} {'MDD%':<8} {'Sharpe(a)':<11} {'Days':<6} {'Trades':<7} {'Win%':<6}")
    print('-' * 80)
    for name, m in results.items():
        print(f"{name:<12} {m['nav']:<8.4f} {m['total_ret_pct']:<+8.2f} "
              f"{m['cagr_proxy']:<+8.1f} {m['mdd_pct']:<+8.2f} {m['sharpe_a']:<11.2f} "
              f"{m['days']:<6} {m['n_trades']:<7} {m['win_rate']:<6.1f}")

    print()
    print('=== baseline 대비 ===')
    base = results.get('baseline')
    if base:
        for name, m in results.items():
            if name == 'baseline':
                continue
            d_ret = m['total_ret_pct'] - base['total_ret_pct']
            d_mdd = m['mdd_pct'] - base['mdd_pct']
            d_sharpe = m['sharpe_a'] - base['sharpe_a']
            print(f"  {name}: ΔRet {d_ret:+.2f}%p, ΔMDD {d_mdd:+.2f}%p, ΔSharpe {d_sharpe:+.2f}")


if __name__ == '__main__':
    main()
