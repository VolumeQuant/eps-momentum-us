"""wTop 진입/이탈 조합 × 시작일 변동 백테스트"""
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


def run_bt(trade_dates, all_dates, daily, entry_top, exit_top, start_date):
    MS_ENTRY = 0
    MS_EXIT = -2
    STOP_PCT = -10
    MAX_SLOTS = max(entry_top + 2, 5)

    # start_date 이후만 사용
    try:
        si = trade_dates.index(start_date)
    except ValueError:
        return None
    active_dates = trade_dates[si:]

    portfolio = {}
    trades = []
    daily_returns = []

    for d in active_dates:
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
                    'hold': active_dates.index(d) - active_dates.index(entry['ed']),
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
        di = active_dates.index(d)
        if di > 0 and portfolio:
            prev = active_dates[di - 1]
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
            'hold': len(active_dates) - 1 - active_dates.index(info['ed']),
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
        'comp': comp, 'hold': hold, 'trades': trades
    }


def main():
    trade_dates, all_dates, daily = load_data()
    print(f"거래일: {trade_dates[0]} ~ {trade_dates[-1]} ({len(trade_dates)}일)")
    print(f"공통 조건: ms_entry>=0%, ms_exit<-2%, stop -10%\n")

    combos = [
        (3, 15), (3, 20), (3, 25), (3, 30),
        (5, 15), (5, 20), (5, 25), (5, 30),
    ]
    start_dates = ['2026-02-10', '2026-02-11', '2026-02-17', '2026-02-18']

    # ── 전략별 × 시작일별 테이블 ──
    print('=' * 130)
    print(f"{'전략':<18s}", end='')
    for sd in start_dates:
        print(f" | {sd:^28s}", end='')
    print()
    print(f"{'':18s}", end='')
    for _ in start_dates:
        print(f" | {'수익률':>8s} {'MDD':>8s} {'Sharpe':>7s}", end='')
    print()
    print('-' * 130)

    all_results = {}
    for entry, exit_ in combos:
        name = f'wTop{entry}/wTop{exit_}'
        print(f"{name:<18s}", end='')
        all_results[name] = {}
        for sd in start_dates:
            r = run_bt(trade_dates, all_dates, daily, entry, exit_, sd)
            if r:
                all_results[name][sd] = r
                print(f" | {r['cum']:>+7.1f}% {r['mdd']:>+7.1f}% {r['sharpe']:>7.2f}", end='')
            else:
                print(f" | {'N/A':>8s} {'N/A':>8s} {'N/A':>7s}", end='')
        print()

    # ── 전략별 평균/분산 ──
    print(f"\n{'=' * 90}")
    print(f"{'전략':<18s} {'평균수익':>8s} {'최소':>8s} {'최대':>8s} {'편차':>8s} {'평균MDD':>8s} {'평균Sharpe':>10s}")
    print('-' * 90)
    for entry, exit_ in combos:
        name = f'wTop{entry}/wTop{exit_}'
        cums = [r['cum'] for r in all_results[name].values()]
        mdds = [r['mdd'] for r in all_results[name].values()]
        sharpes = [r['sharpe'] for r in all_results[name].values()]
        avg_c = np.mean(cums)
        std_c = np.std(cums)
        avg_m = np.mean(mdds)
        avg_s = np.mean(sharpes)
        print(f"{name:<18s} {avg_c:>+7.1f}% {min(cums):>+7.1f}% {max(cums):>+7.1f}% {std_c:>7.1f}% {avg_m:>+7.1f}% {avg_s:>10.2f}")

    # ── 시작일별 첫날 매수 종목 ──
    print(f"\n{'=' * 90}")
    print(f"  시작일별 첫날 매수 종목 (wTop3 기준)")
    print(f"{'=' * 90}")
    for sd in start_dates:
        if sd not in [d for d in trade_dates]:
            print(f"  {sd}: 거래일 아님")
            continue
        stocks = daily[sd]['stocks']
        wranks = build_w_gap_ranks(sd, all_dates, daily)
        ranked = sorted(stocks.items(), key=lambda x: wranks.get(x[0], 999))
        top3 = []
        for tk, s in ranked[:3]:
            if s['min_seg'] >= 0:
                wg = compute_w_gap(tk, sd, all_dates, daily)
                top3.append(f"{tk}(wR{wranks[tk]}, wg{wg:+.1f}, ms{s['min_seg']:.1f}, ${s['price']:.1f})")
        print(f"  {sd}: {', '.join(top3)}")

    # ── 각 시작일별 상세 거래 (wTop3/wTop15만) ──
    print(f"\n{'=' * 90}")
    print(f"  wTop3/wTop15 시작일별 상세 거래")
    print(f"{'=' * 90}")
    for sd in start_dates:
        r = all_results.get('wTop3/wTop15', {}).get(sd)
        if not r:
            continue
        comp = r['comp']
        hold = r['hold']
        wins = sum(1 for t in comp if t['return'] > 0) if comp else 0
        print(f"\n  ── 시작일: {sd} → 수익 {r['cum']:+.1f}%, MDD {r['mdd']:+.1f}% ──")
        if comp:
            print(f"  완료 {len(comp)}건 (승률 {wins}/{len(comp)})")
        if hold:
            print(f"  보유중 {len(hold)}건")
        for t in sorted(r['trades'], key=lambda x: x['ed']):
            tag = '보유중' if t['reason'] == 'holding' else t['reason']
            print(f"    {t['ticker']:8s} {t['ed']}~{t['xd']:<11s} "
                  f"${t['ep']:>7.1f}>${t['xp']:>7.1f} {t['return']:>+7.1f}% {t['hold']:>3d}일 [{tag}]")


if __name__ == '__main__':
    main()
