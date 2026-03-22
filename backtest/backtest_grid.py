"""v55b 백테스트 그리드 서치: 진입 × 이탈 × 종목수 × 손절 전체 조합"""
import sys
import sqlite3
import numpy as np
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

DB = 'eps_momentum_data.db'


def load_data():
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    dates = [r[0] for r in c.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    daily = {}
    for date in dates:
        rows = c.execute('''
            SELECT ticker, part2_rank, composite_rank, price, adj_gap,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening
            WHERE date=? AND part2_rank IS NOT NULL
        ''', (date,)).fetchall()

        all_rows = c.execute('''
            SELECT ticker, adj_gap, price
            FROM ntm_screening
            WHERE date=? AND adj_gap IS NOT NULL
        ''', (date,)).fetchall()

        stocks = {}
        for tk, p2r, cr, price, ag, nc, n7, n30, n60, n90 in rows:
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append((a - b) / abs(b) * 100)
                else:
                    segs.append(0)
            stocks[tk] = {
                'part2_rank': p2r, 'composite_rank': cr,
                'price': price, 'adj_gap': ag,
                'min_seg': min(segs),
            }

        all_gaps = {tk: ag for tk, ag, _ in all_rows}
        all_prices = {tk: p for tk, _, p in all_rows}

        daily[date] = {'stocks': stocks, 'all_gaps': all_gaps, 'all_prices': all_prices}

    conn.close()
    return dates, daily


def compute_w_gap(ticker, date_idx, dates, daily):
    g0, g1, g2 = 0, 0, 0
    g0 = daily[dates[date_idx]]['all_gaps'].get(ticker, 0)
    if date_idx >= 1:
        g1 = daily[dates[date_idx - 1]]['all_gaps'].get(ticker, 0)
    if date_idx >= 2:
        g2 = daily[dates[date_idx - 2]]['all_gaps'].get(ticker, 0)
    return g0 * 0.5 + g1 * 0.3 + g2 * 0.2


def simulate(dates, daily, entry_top_n, exit_rule, max_stocks, stop_loss_pct=None):
    """
    entry_top_n: part2_rank 상위 N에서 진입 (3, 5, 7)
    exit_rule: ('rank', N) = part2_rank > N이면 매도
               ('wgap', threshold) = w_gap > threshold이면 매도
    """
    portfolio = {}
    trades = []
    daily_returns = []
    start_idx = 2

    for i, date in enumerate(dates):
        if i < start_idx:
            continue

        data = daily[date]
        stocks = data['stocks']

        # ── 매도 체크 ──
        for tk in list(portfolio.keys()):
            entry = portfolio[tk]
            current_price = None

            # 가격 조회: part2_rank 종목 → 전체 종목
            if tk in stocks:
                current_price = stocks[tk]['price']
            elif tk in data['all_prices']:
                current_price = data['all_prices'][tk]
            else:
                current_price = entry.get('last_price', entry['entry_price'])

            should_exit = False

            # 손절 체크
            if stop_loss_pct is not None and current_price and entry['entry_price'] > 0:
                pnl = (current_price - entry['entry_price']) / entry['entry_price'] * 100
                if pnl <= stop_loss_pct:
                    should_exit = True

            # exit rule 체크
            if not should_exit:
                if exit_rule[0] == 'rank':
                    if tk not in stocks or stocks[tk]['part2_rank'] > exit_rule[1]:
                        should_exit = True
                elif exit_rule[0] == 'wgap':
                    w = compute_w_gap(tk, i, dates, daily)
                    if w > exit_rule[1]:
                        should_exit = True

            if should_exit:
                portfolio.pop(tk)
                exit_price = current_price or entry.get('last_price', entry['entry_price'])
                ret = (exit_price - entry['entry_price']) / entry['entry_price'] * 100
                trades.append({'ticker': tk, 'return': ret, 'hold_days': i - entry['entry_idx']})

        # ── 매수 체크 ──
        vacancies = max_stocks - len(portfolio)
        if vacancies > 0:
            ranked = sorted(
                [(tk, s) for tk, s in stocks.items() if s['part2_rank'] <= entry_top_n],
                key=lambda x: x[1]['part2_rank']
            )
            for tk, s in ranked:
                if tk not in portfolio and vacancies > 0:
                    portfolio[tk] = {
                        'entry_date': date, 'entry_price': s['price'], 'entry_idx': i,
                    }
                    vacancies -= 1

        # 보유 종목 최신 가격 업데이트
        for tk in portfolio:
            if tk in stocks:
                portfolio[tk]['last_price'] = stocks[tk]['price']
            elif tk in data['all_prices']:
                portfolio[tk]['last_price'] = data['all_prices'][tk]

        # 일별 수익률
        if i > start_idx and portfolio:
            prev_date = dates[i - 1]
            day_rets = []
            for tk in portfolio:
                p_now = None
                if tk in stocks:
                    p_now = stocks[tk]['price']
                elif tk in data['all_prices']:
                    p_now = data['all_prices'][tk]

                p_prev = None
                if tk in daily[prev_date]['stocks']:
                    p_prev = daily[prev_date]['stocks'][tk]['price']
                elif tk in daily[prev_date]['all_prices']:
                    p_prev = daily[prev_date]['all_prices'][tk]

                if p_now and p_prev and p_prev > 0:
                    day_rets.append((p_now - p_prev) / p_prev)

            if day_rets:
                daily_returns.append(sum(day_rets) / len(day_rets))
            else:
                daily_returns.append(0)

    # 미실현
    for tk, info in portfolio.items():
        last = info.get('last_price', info['entry_price'])
        ret = (last - info['entry_price']) / info['entry_price'] * 100
        trades.append({'ticker': tk, 'return': ret, 'hold_days': len(dates) - 1 - info['entry_idx'],
                       'holding': True})

    return trades, daily_returns


def calc_stats(daily_returns):
    if not daily_returns or len(daily_returns) < 2:
        return None
    arr = np.array(daily_returns)
    cum = (np.prod(1 + arr) - 1) * 100
    cum_arr = np.cumprod(1 + arr)
    peak = np.maximum.accumulate(cum_arr)
    dd = (cum_arr - peak) / peak
    mdd = np.min(dd) * 100
    avg = np.mean(arr)
    std = np.std(arr)
    sharpe = avg / std if std > 0 else 0
    win_days = sum(1 for r in arr if r > 0)
    return {
        'cum': cum, 'mdd': mdd, 'sharpe': sharpe,
        'win': win_days, 'total': len(arr),
    }


def main():
    dates, daily = load_data()
    print(f"데이터: {dates[0]} ~ {dates[-1]} ({len(dates)}일)\n")

    # 그리드 정의
    entry_options = [3, 5, 7]
    exit_options = [
        ('rank', 7), ('rank', 10), ('rank', 15), ('rank', 20), ('rank', 30),
        ('wgap', 2), ('wgap', 0),
    ]
    max_options = [3, 5, 7]
    stop_options = [None, -10]

    results = []

    for entry_n in entry_options:
        for exit_rule in exit_options:
            for max_s in max_options:
                # entry_n은 max_s 이하여야 의미 있음
                if entry_n > max_s:
                    continue
                for sl in stop_options:
                    trades, daily_rets = simulate(dates, daily, entry_n, exit_rule, max_s, sl)
                    stats = calc_stats(daily_rets)
                    if stats is None:
                        continue

                    exit_label = f"Top{exit_rule[1]}" if exit_rule[0] == 'rank' else f"w>{exit_rule[1]:+d}"
                    sl_label = f"/SL{sl}%" if sl else ""
                    name = f"E{entry_n}/X{exit_label}/M{max_s}{sl_label}"

                    n_trades = len(trades)
                    completed = [t for t in trades if not t.get('holding')]
                    holding = [t for t in trades if t.get('holding')]

                    results.append({
                        'name': name,
                        'entry': entry_n, 'exit': exit_rule, 'max': max_s, 'sl': sl,
                        'cum': stats['cum'], 'mdd': stats['mdd'], 'sharpe': stats['sharpe'],
                        'win': stats['win'], 'total': stats['total'],
                        'n_trades': n_trades,
                        'n_completed': len(completed),
                        'n_holding': len(holding),
                        'avg_trade_ret': np.mean([t['return'] for t in completed]) if completed else 0,
                    })

    # 수익률 순 정렬
    results.sort(key=lambda x: x['cum'], reverse=True)

    # ── 상위 20개 ──
    print("=" * 95)
    print("  수익률 상위 20개 전략")
    print("=" * 95)
    print(f"{'#':>2} {'전략':<30s} {'수익률':>8s} {'MDD':>8s} {'Sharpe':>7s} {'승률':>10s} {'거래':>5s} {'평균거래':>8s}")
    print("-" * 95)
    for i, r in enumerate(results[:20]):
        wr = f"{r['win']}/{r['total']}"
        print(f"{i+1:>2} {r['name']:<30s} {r['cum']:>+7.1f}% {r['mdd']:>+7.1f}% {r['sharpe']:>7.2f} {wr:>10s} {r['n_trades']:>5d} {r['avg_trade_ret']:>+7.1f}%")

    # ── MDD 기준 상위 20 ──
    print()
    print("=" * 95)
    print("  MDD 최소 상위 20개 전략 (수익률 > 0)")
    print("=" * 95)
    positive = [r for r in results if r['cum'] > 0]
    positive.sort(key=lambda x: x['mdd'], reverse=True)  # MDD는 음수, 큰 게 좋음
    print(f"{'#':>2} {'전략':<30s} {'수익률':>8s} {'MDD':>8s} {'Sharpe':>7s} {'승률':>10s} {'거래':>5s}")
    print("-" * 95)
    for i, r in enumerate(positive[:20]):
        wr = f"{r['win']}/{r['total']}"
        print(f"{i+1:>2} {r['name']:<30s} {r['cum']:>+7.1f}% {r['mdd']:>+7.1f}% {r['sharpe']:>7.2f} {wr:>10s} {r['n_trades']:>5d}")

    # ── Sharpe 기준 상위 20 ──
    print()
    print("=" * 95)
    print("  Sharpe 상위 20개 전략")
    print("=" * 95)
    results_sharpe = sorted(results, key=lambda x: x['sharpe'], reverse=True)
    print(f"{'#':>2} {'전략':<30s} {'수익률':>8s} {'MDD':>8s} {'Sharpe':>7s} {'승률':>10s} {'거래':>5s}")
    print("-" * 95)
    for i, r in enumerate(results_sharpe[:20]):
        wr = f"{r['win']}/{r['total']}"
        print(f"{i+1:>2} {r['name']:<30s} {r['cum']:>+7.1f}% {r['mdd']:>+7.1f}% {r['sharpe']:>7.2f} {wr:>10s} {r['n_trades']:>5d}")

    # ── 전체 결과 (간단) ──
    print()
    print("=" * 95)
    print(f"  전체 {len(results)}개 전략 요약")
    print("=" * 95)
    print(f"  수익률 범위: {min(r['cum'] for r in results):+.1f}% ~ {max(r['cum'] for r in results):+.1f}%")
    print(f"  MDD 범위:   {min(r['mdd'] for r in results):+.1f}% ~ {max(r['mdd'] for r in results):+.1f}%")
    pos_count = sum(1 for r in results if r['cum'] > 0)
    print(f"  수익 전략: {pos_count}/{len(results)} ({pos_count/len(results)*100:.0f}%)")

    # ── 패턴 분석 ──
    print()
    print("=" * 95)
    print("  패턴 분석: 변수별 평균 수익률")
    print("=" * 95)

    # entry별
    print("\n  [진입 기준별]")
    for e in entry_options:
        group = [r['cum'] for r in results if r['entry'] == e]
        if group:
            print(f"    Top{e} 진입: 평균 {np.mean(group):+.1f}%, 중앙값 {np.median(group):+.1f}%")

    # exit별
    print("\n  [이탈 기준별]")
    for ex in exit_options:
        label = f"Top{ex[1]}" if ex[0] == 'rank' else f"w_gap>{ex[1]:+d}"
        group = [r['cum'] for r in results if r['exit'] == ex]
        if group:
            print(f"    {label} 이탈: 평균 {np.mean(group):+.1f}%, 중앙값 {np.median(group):+.1f}%")

    # max별
    print("\n  [최대 종목수별]")
    for m in max_options:
        group = [r['cum'] for r in results if r['max'] == m]
        if group:
            print(f"    max {m}: 평균 {np.mean(group):+.1f}%, 중앙값 {np.median(group):+.1f}%")

    # 손절별
    print("\n  [손절 유무별]")
    for sl in stop_options:
        label = f"SL {sl}%" if sl else "손절 없음"
        group = [r['cum'] for r in results if r['sl'] == sl]
        if group:
            print(f"    {label}: 평균 {np.mean(group):+.1f}%, 중앙값 {np.median(group):+.1f}%")


if __name__ == '__main__':
    main()
