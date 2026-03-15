"""wTop 진입/이탈 조합 비교 백테스트"""
import sqlite3, sys, numpy as np
sys.stdout.reconfigure(encoding='utf-8')

DB = 'eps_momentum_data.db'


def load_data():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    trade_dates = [r[0] for r in c.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    all_dates = [r[0] for r in c.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE adj_gap IS NOT NULL ORDER BY date'
    ).fetchall()]
    daily = {}
    for d in all_dates:
        rows = c.execute('''
            SELECT ticker, part2_rank, price, adj_gap,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL
        ''', (d,)).fetchall()
        all_prices = {r[0]: r[1] for r in c.execute(
            'SELECT ticker, price FROM ntm_screening WHERE date=? AND price IS NOT NULL', (d,)
        ).fetchall()}
        all_gaps = {r[0]: r[1] for r in c.execute(
            'SELECT ticker, adj_gap FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL', (d,)
        ).fetchall()}
        stocks = {}
        for tk, p2r, price, ag, nc, n7, n30, n60, n90 in rows:
            if p2r is None:
                continue
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append((a - b) / abs(b) * 100)
                else:
                    segs.append(0)
            stocks[tk] = {'price': price, 'adj_gap': ag, 'min_seg': min(segs)}
        daily[d] = {'stocks': stocks, 'all_prices': all_prices, 'all_gaps': all_gaps}
    conn.close()
    return trade_dates, all_dates, daily


def compute_w_gap(ticker, date_str, all_dates, daily):
    idx = all_dates.index(date_str)
    g0 = daily[all_dates[idx]]['all_gaps'].get(ticker, 0)
    g1 = daily[all_dates[idx - 1]]['all_gaps'].get(ticker, 0) if idx >= 1 else 0
    g2 = daily[all_dates[idx - 2]]['all_gaps'].get(ticker, 0) if idx >= 2 else 0
    return g0 * 0.5 + g1 * 0.3 + g2 * 0.2


def build_w_gap_ranks(date_str, all_dates, daily):
    stocks = daily[date_str]['stocks']
    wgaps = []
    for tk in stocks:
        wg = compute_w_gap(tk, date_str, all_dates, daily)
        wgaps.append((tk, wg))
    wgaps.sort(key=lambda x: x[1])
    return {tk: rank + 1 for rank, (tk, _) in enumerate(wgaps)}


def run_bt(trade_dates, all_dates, daily, entry_top, exit_top):
    MS_ENTRY = 0
    MS_EXIT = -2
    STOP_PCT = -10
    MAX_SLOTS = max(entry_top + 2, 5)

    portfolio = {}
    trades = []
    daily_returns = []

    for d in trade_dates:
        stocks = daily[d]['stocks']
        all_prices = daily[d]['all_prices']
        wranks = build_w_gap_ranks(d, all_dates, daily)

        # Exit
        for tk in list(portfolio.keys()):
            entry = portfolio[tk]
            cp = (stocks.get(tk, {}).get('price')
                  or all_prices.get(tk)
                  or entry.get('lp', entry['ep']))
            pnl = (cp - entry['ep']) / entry['ep'] * 100

            should_exit, reason = False, ''
            if pnl <= STOP_PCT:
                should_exit, reason = True, 'stop'
            elif tk in stocks and stocks[tk]['min_seg'] < MS_EXIT:
                should_exit, reason = True, 'ms_exit'
            elif tk not in stocks:
                should_exit, reason = True, 'rank_out'
            else:
                wr = wranks.get(tk, 999)
                if wr > exit_top:
                    should_exit, reason = True, 'wrank_exit'

            if should_exit:
                ret = (cp - entry['ep']) / entry['ep'] * 100
                trades.append({
                    'ticker': tk, 'return': ret,
                    'hold': trade_dates.index(d) - trade_dates.index(entry['ed']),
                    'reason': reason, 'ep': entry['ep'], 'xp': cp,
                    'ed': entry['ed'], 'xd': d
                })
                portfolio.pop(tk)

        # Entry
        vac = MAX_SLOTS - len(portfolio)
        if vac > 0:
            ranked = sorted(stocks.items(), key=lambda x: wranks.get(x[0], 999))
            count = 0
            for tk, s in ranked[:entry_top]:
                if count >= vac:
                    break
                if tk not in portfolio and s['min_seg'] >= MS_ENTRY:
                    portfolio[tk] = {'ed': d, 'ep': s['price'], 'lp': s['price']}
                    count += 1

        # Update last price
        for tk in portfolio:
            if tk in stocks:
                portfolio[tk]['lp'] = stocks[tk]['price']
            elif tk in all_prices:
                portfolio[tk]['lp'] = all_prices[tk]

        # Daily return
        di = trade_dates.index(d)
        if di > 0 and portfolio:
            prev = trade_dates[di - 1]
            drs = []
            for tk in portfolio:
                pn = stocks.get(tk, {}).get('price') or all_prices.get(tk)
                pp = (daily[prev]['stocks'].get(tk, {}).get('price')
                      or daily[prev]['all_prices'].get(tk))
                if pn and pp and pp > 0:
                    drs.append((pn - pp) / pp)
            daily_returns.append(sum(drs) / len(drs) if drs else 0)

    # Unrealized
    for tk, info in portfolio.items():
        last = info.get('lp', info['ep'])
        ret = (last - info['ep']) / info['ep'] * 100
        trades.append({
            'ticker': tk, 'return': ret,
            'hold': trade_dates.index(trade_dates[-1]) - trade_dates.index(info['ed']),
            'reason': 'holding', 'ep': info['ep'], 'xp': last,
            'ed': info['ed'], 'xd': '~now'
        })

    arr = np.array(daily_returns) if daily_returns else np.array([0])
    cum = (np.prod(1 + arr) - 1) * 100
    ca = np.cumprod(1 + arr)
    pk = np.maximum.accumulate(ca)
    mdd = np.min((ca - pk) / pk) * 100
    sh = np.mean(arr) / np.std(arr) if np.std(arr) > 0 else 0
    wd = sum(1 for r in arr if r > 0)
    comp = [t for t in trades if t['reason'] != 'holding']
    hold = [t for t in trades if t['reason'] == 'holding']

    return {
        'cum': cum, 'mdd': mdd, 'sharpe': sh,
        'wd': wd, 'td': len(arr),
        'trades': trades, 'comp': comp, 'hold': hold
    }


def main():
    trade_dates, all_dates, daily = load_data()
    print(f"거래일: {trade_dates[0]} ~ {trade_dates[-1]} ({len(trade_dates)}일)")
    print(f"공통 조건: ms_entry>=0%, ms_exit<-2%, stop -10%")
    print(f"⚠️ {len(trade_dates)}일 데이터 — 참고용\n")

    combos = [
        (3, 15), (3, 20), (3, 25), (3, 30),
        (5, 15), (5, 20), (5, 25), (5, 30),
    ]

    results = []
    for entry, exit_ in combos:
        r = run_bt(trade_dates, all_dates, daily, entry, exit_)
        r['name'] = f'wTop{entry}/wTop{exit_}'
        r['entry'] = entry
        r['exit'] = exit_
        results.append(r)

    # Summary table
    print('=' * 115)
    print(f"{'전략':<18s} {'수익률':>8s} {'MDD':>8s} {'Sharpe':>7s} {'승률(일)':>10s} "
          f"{'완료':>4s} {'보유':>4s} {'거래승률':>10s} {'평균수익':>8s} {'평균보유':>7s} {'Max':>4s}")
    print('-' * 115)

    for r in results:
        wr = f"{r['wd']}/{r['td']}"
        comp = r['comp']
        if comp:
            wins = sum(1 for t in comp if t['return'] > 0)
            twr = f"{wins}/{len(comp)}"
            avg_r = sum(t['return'] for t in comp) / len(comp)
            avg_h = sum(t['hold'] for t in comp) / len(comp)
        else:
            twr = '-'
            avg_r = 0
            avg_h = 0
        max_slots = max(r['entry'] + 2, 5)
        print(f"{r['name']:<18s} {r['cum']:>+7.1f}% {r['mdd']:>+7.1f}% {r['sharpe']:>7.2f} "
              f"{wr:>10s} {len(comp):>4d} {len(r['hold']):>4d} {twr:>10s} {avg_r:>+7.1f}% {avg_h:>6.1f}일 {max_slots:>4d}")

    # Detailed trades for each
    for r in results:
        print(f"\n{'━' * 90}")
        print(f"  {r['name']}  →  수익 {r['cum']:+.1f}%, MDD {r['mdd']:+.1f}%, Sharpe {r['sharpe']:.2f}")
        print(f"{'━' * 90}")

        comp = r['comp']
        if comp:
            wins = sum(1 for t in comp if t['return'] > 0)
            avg_r = sum(t['return'] for t in comp) / len(comp)
            reasons = {}
            for t in comp:
                rr = t['reason']
                if rr not in reasons:
                    reasons[rr] = []
                reasons[rr].append(t['return'])
            reason_str = ' | '.join(
                f"{k}: {len(v)}건 avg{sum(v)/len(v):+.1f}%"
                for k, v in reasons.items()
            )
            print(f"  완료 {len(comp)}건 (승률 {wins}/{len(comp)}), 평균 {avg_r:+.1f}%")
            print(f"  사유: {reason_str}")

        hold = r['hold']
        if hold:
            avg_h = sum(t['return'] for t in hold) / len(hold)
            print(f"  보유중 {len(hold)}건, 평균 미실현 {avg_h:+.1f}%")

        print(f"  {'티커':8s} {'진입':12s} {'이탈':12s} {'진입가':>8s} {'이탈가':>8s} {'수익률':>8s} {'보유':>5s} {'사유'}")
        for t in sorted(r['trades'], key=lambda x: x['ed']):
            tag = '보유중' if t['reason'] == 'holding' else t['reason']
            print(f"  {t['ticker']:8s} {t['ed']:12s} {t['xd']:12s} "
                  f"${t['ep']:>7.1f} ${t['xp']:>7.1f} {t['return']:>+7.1f}% {t['hold']:>4d}일 [{tag}]")


if __name__ == '__main__':
    main()
