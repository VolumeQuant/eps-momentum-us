"""v55b 동일 조건 백테스트: Top3/Top7 exit vs w_gap exit>+2

두 전략 모두 v55b DB (연속함수 eps_quality) 사용.
진입 후보는 동일 (part2_rank 상위), exit 규칙만 다름.

전략 A: Top3 진입, Top7 이탈 + min_seg<-2% 이탈 (현행 v55)
전략 B: Top3 진입, w_gap>+2 이탈 (v52 스타일 exit)
전략 C: w_gap<-6 진입, w_gap>+2 이탈 (v52 원본)
"""
import sys
import sqlite3
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

        # adj_gap이 있는 모든 종목 (part2_rank 없는 것 포함, w_gap 계산용)
        all_rows = c.execute('''
            SELECT ticker, adj_gap
            FROM ntm_screening
            WHERE date=? AND adj_gap IS NOT NULL
        ''', (date,)).fetchall()

        stocks = {}
        for tk, p2r, cr, price, ag, nc, n7, n30, n60, n90 in rows:
            # min_seg 계산
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

        all_gaps = {tk: ag for tk, ag in all_rows}

        daily[date] = {'stocks': stocks, 'all_gaps': all_gaps}

    conn.close()
    return dates, daily


def compute_w_gap(ticker, date_idx, dates, daily):
    """3일 가중 adj_gap: T0×0.5 + T1×0.3 + T2×0.2"""
    g0, g1, g2 = 0, 0, 0
    d0 = dates[date_idx]
    g0 = daily[d0]['all_gaps'].get(ticker, 0)
    if date_idx >= 1:
        d1 = dates[date_idx - 1]
        g1 = daily[d1]['all_gaps'].get(ticker, 0)
    if date_idx >= 2:
        d2 = dates[date_idx - 2]
        g2 = daily[d2]['all_gaps'].get(ticker, 0)
    return g0 * 0.5 + g1 * 0.3 + g2 * 0.2


def simulate(dates, daily, strategy_name, entry_fn, exit_fn, max_stocks, stop_loss_pct=None):
    """포트폴리오 시뮬레이션"""
    portfolio = {}  # {ticker: {'entry_date', 'entry_price', 'entry_idx'}}
    trades = []
    daily_returns = []  # 일별 포트폴리오 수익률

    # 3일 데이터 축적 후 시작 (part2_rank, w_gap 모두 3일 필요)
    start_idx = 2

    for i, date in enumerate(dates):
        if i < start_idx:
            continue

        data = daily[date]
        stocks = data['stocks']

        # ── 매도 체크 ──
        exits = []
        for tk in list(portfolio.keys()):
            entry = portfolio[tk]
            current_price = stocks[tk]['price'] if tk in stocks else entry.get('last_price', entry['entry_price'])

            # 손절 체크
            stop_triggered = False
            if stop_loss_pct is not None and current_price and entry['entry_price'] > 0:
                pnl_pct = (current_price - entry['entry_price']) / entry['entry_price'] * 100
                if pnl_pct <= stop_loss_pct:
                    stop_triggered = True

            if stop_triggered or exit_fn(tk, i, dates, daily, stocks):
                portfolio.pop(tk)
                exit_price = current_price
                ret = (exit_price - entry['entry_price']) / entry['entry_price'] * 100
                exit_reason = 'stop' if stop_triggered else 'signal'
                trades.append({
                    'ticker': tk, 'entry_date': entry['entry_date'],
                    'exit_date': date, 'return': ret,
                    'hold_days': i - entry['entry_idx'],
                    'exit_reason': exit_reason,
                })
                exits.append((tk, ret))

        # ── 매수 체크 ──
        vacancies = max_stocks - len(portfolio)
        if vacancies > 0:
            candidates = entry_fn(i, dates, daily, stocks, portfolio)
            for tk in candidates[:vacancies]:
                if tk not in portfolio and tk in stocks:
                    portfolio[tk] = {
                        'entry_date': date,
                        'entry_price': stocks[tk]['price'],
                        'entry_idx': i,
                    }

        # 보유 종목 최신 가격 업데이트
        for tk in portfolio:
            if tk in stocks:
                portfolio[tk]['last_price'] = stocks[tk]['price']

        # 일별 포트폴리오 수익률 (전일 대비)
        if i > 0 and portfolio:
            day_rets = []
            prev_date = dates[i - 1]
            prev_stocks = daily[prev_date]['stocks']
            for tk in portfolio:
                if tk in stocks and tk in prev_stocks:
                    p_now = stocks[tk]['price']
                    p_prev = prev_stocks[tk]['price']
                    if p_prev and p_prev > 0:
                        day_rets.append((p_now - p_prev) / p_prev)
            if day_rets:
                daily_returns.append(sum(day_rets) / len(day_rets))
            else:
                daily_returns.append(0)

    # 미실현 보유
    for tk, info in portfolio.items():
        last = info.get('last_price', info['entry_price'])
        ret = (last - info['entry_price']) / info['entry_price'] * 100
        trades.append({
            'ticker': tk, 'entry_date': info['entry_date'],
            'exit_date': dates[-1] + '(보유)', 'return': ret,
            'hold_days': len(dates) - 1 - info['entry_idx'],
        })

    return trades, daily_returns


def calc_stats(daily_returns):
    if not daily_returns:
        return {}
    import numpy as np
    arr = np.array(daily_returns)
    cum = np.prod(1 + arr) - 1

    # MDD
    cum_arr = np.cumprod(1 + arr)
    peak = np.maximum.accumulate(cum_arr)
    dd = (cum_arr - peak) / peak
    mdd = np.min(dd) * 100

    avg = np.mean(arr)
    std = np.std(arr)
    sharpe = avg / std if std > 0 else 0

    return {
        'cumulative': cum * 100,
        'mdd': mdd,
        'sharpe': sharpe,
        'avg_daily': avg * 100,
        'win_days': sum(1 for r in arr if r > 0),
        'total_days': len(arr),
    }


def main():
    dates, daily = load_data()
    print(f"데이터: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")
    print()

    # ── 전략 A: Top3 진입, Top7 이탈 + min_seg<-2% ──
    def entry_a(i, dates, daily, stocks, portfolio):
        ranked = sorted(stocks.items(), key=lambda x: x[1]['part2_rank'])
        return [tk for tk, s in ranked[:3]
                if s['min_seg'] >= -2 and tk not in portfolio]

    def exit_a(tk, i, dates, daily, stocks):
        if tk not in stocks:
            return True  # Top30 밖 → 이탈
        s = stocks[tk]
        if s['part2_rank'] > 7:
            return True
        if s['min_seg'] < -2:
            return True
        return False

    # ── 전략 B: Top3 진입, w_gap>+2 이탈 ──
    def entry_b(i, dates, daily, stocks, portfolio):
        ranked = sorted(stocks.items(), key=lambda x: x[1]['part2_rank'])
        return [tk for tk, s in ranked[:3] if tk not in portfolio]

    def exit_b(tk, i, dates, daily, stocks):
        if tk not in stocks:
            return True
        w = compute_w_gap(tk, i, dates, daily)
        return w > 2

    # ── 전략 C: w_gap<-6 진입, w_gap>+2 이탈 ──
    def entry_c(i, dates, daily, stocks, portfolio):
        candidates = []
        for tk, s in stocks.items():
            if tk in portfolio:
                continue
            w = compute_w_gap(tk, i, dates, daily)
            if w < -6:
                candidates.append((tk, w))
        candidates.sort(key=lambda x: x[1])  # 가장 저평가 우선
        return [tk for tk, w in candidates]

    def exit_c(tk, i, dates, daily, stocks):
        if tk not in stocks:
            # Top30 밖이어도 w_gap으로 판단
            w = compute_w_gap(tk, i, dates, daily)
            return w > 2
        w = compute_w_gap(tk, i, dates, daily)
        return w > 2

    # ── 전략 E/F/G: Top3 진입, w_gap>+2 수익실현 + 손절선 ──
    def make_hybrid_exit(stop_loss_pct):
        def exit_fn(tk, i, dates, daily, stocks):
            if tk not in stocks:
                # Top30 밖이어도 w_gap으로 판단
                w = compute_w_gap(tk, i, dates, daily)
                return w > 2
            # 수익 실현: w_gap > +2
            w = compute_w_gap(tk, i, dates, daily)
            if w > 2:
                return True
            # 손절: 진입가 대비 -X%
            # portfolio에서 entry_price를 직접 참조할 수 없으므로
            # simulate()에서 처리
            return False
        return exit_fn

    # 손절은 simulate 내부에서 처리해야 하므로 별도 로직 필요
    # → simulate 함수에 stop_loss 파라미터 추가

    strategies = [
        ('A. Top3/Top7이탈+추세둔화', entry_a, exit_a, 3, None),
        ('B. Top3/w_gap>+2', entry_b, exit_b, 3, None),
        ('E. Top3/w_gap>+2/손절-7%', entry_b, exit_b, 3, -7),
        ('F. Top3/w_gap>+2/손절-10%', entry_b, exit_b, 3, -10),
        ('G. Top3/w_gap>+2/손절-15%', entry_b, exit_b, 3, -15),
        ('H. Top3/w_gap>+2/손절-10%(max5)', entry_b, exit_b, 5, -10),
    ]

    all_stats = {}
    all_trades = {}

    for name, entry_fn, exit_fn, max_s, sl in strategies:
        trades, daily_rets = simulate(dates, daily, name, entry_fn, exit_fn, max_s, stop_loss_pct=sl)
        stats = calc_stats(daily_rets)
        all_stats[name] = stats
        all_trades[name] = trades

    # ── 결과 출력 ──
    print("=" * 70)
    print("  v55b 동일 조건 백테스트: exit 규칙 비교")
    print("=" * 70)

    header = f"{'전략':<38s} {'수익률':>8s} {'MDD':>8s} {'Sharpe':>7s} {'승률':>10s} {'거래':>5s}"
    print(header)
    print("-" * 80)

    for name in [s[0] for s in strategies]:
        s = all_stats.get(name, {})
        if not s:
            continue
        wr = f"{s['win_days']}/{s['total_days']}"
        trades = all_trades[name]
        print(f"{name:<38s} {s['cumulative']:>+7.1f}% {s['mdd']:>+7.1f}% {s['sharpe']:>7.2f} {wr:>10s} {len(trades):>5d}")

    # ── 각 전략 상세 ──
    for name in [s[0] for s in strategies]:
        trades = all_trades[name]
        if not trades:
            continue
        print(f"\n{'─' * 70}")
        print(f"  {name}")
        print(f"{'─' * 70}")

        completed = [t for t in trades if '보유' not in t['exit_date']]
        holding = [t for t in trades if '보유' in t['exit_date']]

        if completed:
            avg_ret = sum(t['return'] for t in completed) / len(completed)
            avg_hold = sum(t['hold_days'] for t in completed) / len(completed)
            winners = sum(1 for t in completed if t['return'] > 0)
            stops = sum(1 for t in completed if t.get('exit_reason') == 'stop')
            stop_info = f", 손절 {stops}건" if stops else ""
            print(f"  완료: {len(completed)}건, 평균 {avg_ret:+.1f}%, 평균 보유 {avg_hold:.1f}일, 승률 {winners}/{len(completed)}{stop_info}")
            for t in sorted(completed, key=lambda x: x['return'], reverse=True)[:5]:
                reason = ' [손절]' if t.get('exit_reason') == 'stop' else ''
                print(f"    {t['ticker']:6s} {t['entry_date']}→{t['exit_date']} {t['return']:+.1f}% ({t['hold_days']}일){reason}")
            if len(completed) > 5:
                print(f"    ... 외 {len(completed)-5}건")
            worst = sorted(completed, key=lambda x: x['return'])[:3]
            if worst and worst[0]['return'] < 0:
                print(f"  최악:")
                for t in worst:
                    reason = ' [손절]' if t.get('exit_reason') == 'stop' else ''
                    print(f"    {t['ticker']:6s} {t['entry_date']}→{t['exit_date']} {t['return']:+.1f}% ({t['hold_days']}일){reason}")

        if holding:
            print(f"  보유중: {len(holding)}건")
            for t in holding:
                print(f"    {t['ticker']:6s} {t['entry_date']}~ {t['return']:+.1f}% ({t['hold_days']}일)")


if __name__ == '__main__':
    main()
