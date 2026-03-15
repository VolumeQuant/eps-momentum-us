"""수정 v57b 백테스트: adj_gap≤-6% + min_seg≥1% + Top5/Max5 + min_seg<0% exit + -10% stop

비교 대상:
- 원본 v57b: Top3/Max3, 5일 자동, adj_gap≥-2% exit, -12% stop
- 수정 v57b: Top5/Max5, 시간이탈 없음, adj_gap exit 없음, -10% stop
- v55 현행: Top3/Top7 rank exit, min_seg<-2% exit
"""
import sys
import sqlite3
import numpy as np

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


def simulate(dates, daily, name, entry_fn, exit_fn, max_stocks):
    """포트폴리오 시뮬레이션"""
    portfolio = {}  # {ticker: {entry_date, entry_price, entry_idx}}
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
            if tk in stocks:
                current_price = stocks[tk]['price']
            elif tk in data['all_prices']:
                current_price = data['all_prices'][tk]
            else:
                current_price = entry.get('last_price', entry['entry_price'])

            should_exit, reason = exit_fn(tk, i, dates, daily, stocks, entry, current_price)

            if should_exit:
                portfolio.pop(tk)
                exit_price = current_price or entry.get('last_price', entry['entry_price'])
                ret = (exit_price - entry['entry_price']) / entry['entry_price'] * 100
                trades.append({
                    'ticker': tk, 'return': ret,
                    'hold_days': i - entry['entry_idx'],
                    'reason': reason,
                })

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
        trades.append({
            'ticker': tk, 'return': ret,
            'hold_days': len(dates) - 1 - info['entry_idx'],
            'holding': True, 'reason': 'holding',
        })

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


def compute_w_gap(ticker, date_idx, dates, daily):
    g0 = daily[dates[date_idx]]['all_gaps'].get(ticker, 0)
    g1 = daily[dates[date_idx - 1]]['all_gaps'].get(ticker, 0) if date_idx >= 1 else 0
    g2 = daily[dates[date_idx - 2]]['all_gaps'].get(ticker, 0) if date_idx >= 2 else 0
    return g0 * 0.5 + g1 * 0.3 + g2 * 0.2


def main():
    dates, daily = load_data()
    print(f"데이터: {dates[0]} ~ {dates[-1]} ({len(dates)}일)\n")

    # ── 전략 1: 수정 v57b (Top5/Max5, min_seg<0% exit, -10% stop) ──
    def entry_revised(i, dates, daily, stocks, portfolio):
        candidates = []
        for tk, s in stocks.items():
            if tk in portfolio:
                continue
            if s['adj_gap'] is not None and s['adj_gap'] <= -6 and s['min_seg'] >= 1:
                candidates.append((tk, s['adj_gap']))
        candidates.sort(key=lambda x: x[1])  # adj_gap 가장 음수 우선
        return [tk for tk, _ in candidates]

    def exit_revised(tk, i, dates, daily, stocks, entry, current_price):
        # -10% 손절
        if current_price and entry['entry_price'] > 0:
            pnl = (current_price - entry['entry_price']) / entry['entry_price'] * 100
            if pnl <= -10:
                return True, 'stop'
        # min_seg < 0% 이탈
        if tk in stocks and stocks[tk]['min_seg'] < 0:
            return True, 'min_seg'
        # Top30 밖 (데이터 없음) — min_seg 확인 불가, 보유 유지
        if tk not in stocks:
            # all_gaps에서라도 확인
            return False, ''
        return False, ''

    # ── 전략 2: 원본 v57b (Top3/Max3, 5일, adj_gap≥-2%, -12% stop) ──
    def entry_original(i, dates, daily, stocks, portfolio):
        candidates = []
        for tk, s in stocks.items():
            if tk in portfolio:
                continue
            if s['adj_gap'] is not None and s['adj_gap'] <= -6 and s['min_seg'] >= 1:
                candidates.append((tk, s['adj_gap']))
        candidates.sort(key=lambda x: x[1])
        return [tk for tk, _ in candidates]

    def exit_original(tk, i, dates, daily, stocks, entry, current_price):
        # -12% 손절
        if current_price and entry['entry_price'] > 0:
            pnl = (current_price - entry['entry_price']) / entry['entry_price'] * 100
            if pnl <= -12:
                return True, 'stop'
        # 5일 자동 이탈
        if i - entry['entry_idx'] >= 5:
            return True, '5day'
        # min_seg < 0%
        if tk in stocks and stocks[tk]['min_seg'] < 0:
            return True, 'min_seg'
        # adj_gap 2일 평균 ≥ -2%
        if tk in stocks:
            g0 = stocks[tk]['adj_gap'] or 0
            g1 = 0
            if i >= 1:
                prev = dates[i - 1]
                if tk in daily[prev]['all_gaps']:
                    g1 = daily[prev]['all_gaps'][tk]
            avg_gap = (g0 + g1) / 2
            if avg_gap >= -2:
                return True, 'gap_recover'
        return False, ''

    # ── 전략 3: v55 현행 (Top3/Top7 rank, min_seg<-2% exit) ──
    def entry_v55(i, dates, daily, stocks, portfolio):
        ranked = sorted(stocks.items(), key=lambda x: x[1]['part2_rank'])
        candidates = []
        for tk, s in ranked[:3]:
            if tk in portfolio:
                continue
            if s['min_seg'] >= -2:
                candidates.append(tk)
        return candidates

    def exit_v55(tk, i, dates, daily, stocks, entry, current_price):
        if tk not in stocks:
            return True, 'rank_out'
        if stocks[tk]['part2_rank'] > 7:
            return True, 'rank>7'
        if stocks[tk]['min_seg'] < -2:
            return True, 'min_seg'
        return False, ''

    # ── 전략 4: 수정 v57b + min_seg≥0% (1%가 아닌 0%) ──
    def entry_revised_0(i, dates, daily, stocks, portfolio):
        candidates = []
        for tk, s in stocks.items():
            if tk in portfolio:
                continue
            if s['adj_gap'] is not None and s['adj_gap'] <= -6 and s['min_seg'] >= 0:
                candidates.append((tk, s['adj_gap']))
        candidates.sort(key=lambda x: x[1])
        return [tk for tk, _ in candidates]

    # ── 전략 5: 수정 v57b + adj_gap≤-8% ──
    def entry_revised_8(i, dates, daily, stocks, portfolio):
        candidates = []
        for tk, s in stocks.items():
            if tk in portfolio:
                continue
            if s['adj_gap'] is not None and s['adj_gap'] <= -8 and s['min_seg'] >= 1:
                candidates.append((tk, s['adj_gap']))
        candidates.sort(key=lambda x: x[1])
        return [tk for tk, _ in candidates]

    # ── 전략 6: 수정 v57b + adj_gap≤-4% ──
    def entry_revised_4(i, dates, daily, stocks, portfolio):
        candidates = []
        for tk, s in stocks.items():
            if tk in portfolio:
                continue
            if s['adj_gap'] is not None and s['adj_gap'] <= -4 and s['min_seg'] >= 1:
                candidates.append((tk, s['adj_gap']))
        candidates.sort(key=lambda x: x[1])
        return [tk for tk, _ in candidates]

    strategies = [
        ('수정v57b (≤-6%,ms≥1%,Top5,ms<0%,-10%)', entry_revised, exit_revised, 5),
        ('원본v57b (≤-6%,ms≥1%,Top3,5d,gap,-12%)', entry_original, exit_original, 3),
        ('v55현행 (Top3,Top7exit,ms<-2%)', entry_v55, exit_v55, 3),
        ('변형A (≤-6%,ms≥0%,Top5,ms<0%,-10%)', entry_revised_0, exit_revised, 5),
        ('변형B (≤-8%,ms≥1%,Top5,ms<0%,-10%)', entry_revised_8, exit_revised, 5),
        ('변형C (≤-4%,ms≥1%,Top5,ms<0%,-10%)', entry_revised_4, exit_revised, 5),
    ]

    print("=" * 95)
    print("  전략 비교 백테스트")
    print("=" * 95)
    print(f"{'전략':<45s} {'수익률':>8s} {'MDD':>8s} {'Sharpe':>7s} {'승률':>10s} {'거래':>5s}")
    print("-" * 95)

    all_results = {}
    for name, entry_fn, exit_fn, max_s in strategies:
        trades, daily_rets = simulate(dates, daily, name, entry_fn, exit_fn, max_s)
        stats = calc_stats(daily_rets)
        if stats is None:
            print(f"{name:<45s} 데이터 부족")
            continue

        wr = f"{stats['win']}/{stats['total']}"
        print(f"{name:<45s} {stats['cum']:>+7.1f}% {stats['mdd']:>+7.1f}% {stats['sharpe']:>7.2f} {wr:>10s} {len(trades):>5d}")
        all_results[name] = {'stats': stats, 'trades': trades}

    # ── 상세 거래 내역 (수정 v57b) ──
    print()
    for strat_name in ['수정v57b (≤-6%,ms≥1%,Top5,ms<0%,-10%)', '원본v57b (≤-6%,ms≥1%,Top3,5d,gap,-12%)']:
        if strat_name not in all_results:
            continue
        result = all_results[strat_name]
        trades = result['trades']
        completed = [t for t in trades if not t.get('holding')]
        holding = [t for t in trades if t.get('holding')]

        print(f"{'─' * 70}")
        print(f"  {strat_name}")
        print(f"{'─' * 70}")

        if completed:
            avg_ret = sum(t['return'] for t in completed) / len(completed)
            avg_hold = sum(t['hold_days'] for t in completed) / len(completed)
            winners = sum(1 for t in completed if t['return'] > 0)

            # 이탈 사유별
            reasons = {}
            for t in completed:
                r = t.get('reason', '?')
                reasons[r] = reasons.get(r, 0) + 1
            reason_str = ', '.join(f'{k}:{v}건' for k, v in sorted(reasons.items()))

            print(f"  완료: {len(completed)}건, 평균 {avg_ret:+.1f}%, 평균 보유 {avg_hold:.1f}일, 승률 {winners}/{len(completed)}")
            print(f"  이탈 사유: {reason_str}")
            for t in sorted(completed, key=lambda x: x['return'], reverse=True):
                print(f"    {t['ticker']:6s} {t['return']:+.1f}% ({t['hold_days']}일) [{t.get('reason','')}]")

        if holding:
            print(f"  보유중: {len(holding)}건")
            for t in holding:
                print(f"    {t['ticker']:6s} {t['return']:+.1f}% ({t['hold_days']}일)")


if __name__ == '__main__':
    main()
