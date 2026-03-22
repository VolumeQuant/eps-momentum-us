"""wTop3/wTop15 최종 백테스트 — 전체 DB 날짜, 매매 상세 로그"""
import sqlite3, sys, numpy as np
sys.stdout.reconfigure(encoding='utf-8')

DB = 'eps_momentum_data.db'


def load_data():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    # part2_rank 있는 날짜 (거래일)
    trade_dates = [r[0] for r in c.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    # adj_gap 있는 모든 날짜 (lookback용)
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
    """3일 가중 adj_gap"""
    idx = all_dates.index(date_str)
    g0 = daily[all_dates[idx]]['all_gaps'].get(ticker, 0)
    g1 = daily[all_dates[idx - 1]]['all_gaps'].get(ticker, 0) if idx >= 1 else 0
    g2 = daily[all_dates[idx - 2]]['all_gaps'].get(ticker, 0) if idx >= 2 else 0
    return g0 * 0.5 + g1 * 0.3 + g2 * 0.2


def build_w_gap_ranks(date_str, all_dates, daily):
    """해당 날짜 part2_rank 종목들의 w_gap 순위"""
    stocks = daily[date_str]['stocks']
    wgaps = []
    for tk in stocks:
        wg = compute_w_gap(tk, date_str, all_dates, daily)
        wgaps.append((tk, wg))
    wgaps.sort(key=lambda x: x[1])
    return {tk: rank + 1 for rank, (tk, _) in enumerate(wgaps)}


def main():
    trade_dates, all_dates, daily = load_data()
    print(f"전체 날짜: {all_dates[0]} ~ {all_dates[-1]} ({len(all_dates)}일)")
    print(f"거래일(part2_rank): {trade_dates[0]} ~ {trade_dates[-1]} ({len(trade_dates)}일)")
    print(f"\n전략: wTop3 진입(ms≥0%) / wTop15 이탈(ms<-2%) / -10% stop / Max 5슬롯")
    print(f"⚠️ {len(trade_dates)}일 데이터 — 참고용, 확정 판단 불가\n")

    # Parameters
    ENTRY_TOP = 3
    EXIT_TOP = 15
    MS_ENTRY = 0
    MS_EXIT = -2
    STOP_PCT = -10
    MAX_SLOTS = 5

    portfolio = {}
    trades = []
    daily_returns = []
    daily_log = []

    for d in trade_dates:
        stocks = daily[d]['stocks']
        all_prices = daily[d]['all_prices']
        wranks = build_w_gap_ranks(d, all_dates, daily)

        day_buys = []
        day_sells = []

        # ── Exit ──
        for tk in list(portfolio.keys()):
            entry = portfolio[tk]
            cp = (stocks.get(tk, {}).get('price')
                  or all_prices.get(tk)
                  or entry.get('lp', entry['ep']))
            pnl = (cp - entry['ep']) / entry['ep'] * 100

            should_exit, reason = False, ''
            # stop loss
            if pnl <= STOP_PCT:
                should_exit, reason = True, 'stop'
            # min_seg exit
            elif tk in stocks and stocks[tk]['min_seg'] < MS_EXIT:
                should_exit, reason = True, 'ms_exit'
            # rank exit
            elif tk not in stocks:
                should_exit, reason = True, 'rank_out'
            else:
                wr = wranks.get(tk, 999)
                if wr > EXIT_TOP:
                    should_exit, reason = True, 'wrank_exit'

            if should_exit:
                ret = (cp - entry['ep']) / entry['ep'] * 100
                trades.append({
                    'ticker': tk, 'return': ret,
                    'hold': trade_dates.index(d) - trade_dates.index(entry['ed']),
                    'reason': reason, 'ep': entry['ep'], 'xp': cp,
                    'ed': entry['ed'], 'xd': d
                })
                day_sells.append(f"{tk}({reason},{ret:+.1f}%)")
                portfolio.pop(tk)

        # ── Entry ──
        vac = MAX_SLOTS - len(portfolio)
        if vac > 0:
            ranked = sorted(stocks.items(), key=lambda x: wranks.get(x[0], 999))
            count = 0
            for tk, s in ranked[:ENTRY_TOP]:
                if count >= vac:
                    break
                if tk not in portfolio and s['min_seg'] >= MS_ENTRY:
                    portfolio[tk] = {
                        'ed': d, 'ep': s['price'], 'lp': s['price']
                    }
                    wg = compute_w_gap(tk, d, all_dates, daily)
                    day_buys.append(f"{tk}(wR{wranks[tk]},wg{wg:+.1f},ms{s['min_seg']:.1f})")
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
            dr = sum(drs) / len(drs) if drs else 0
            daily_returns.append(dr)
        else:
            dr = 0

        # Portfolio snapshot
        pf_str = []
        for tk in sorted(portfolio.keys()):
            cp = stocks.get(tk, {}).get('price') or all_prices.get(tk, portfolio[tk]['ep'])
            pnl = (cp - portfolio[tk]['ep']) / portfolio[tk]['ep'] * 100
            pf_str.append(f"{tk}({pnl:+.1f}%)")

        daily_log.append({
            'date': d, 'buys': day_buys, 'sells': day_sells,
            'portfolio': pf_str, 'dr': dr
        })

    # ── Daily Log ──
    print('=' * 100)
    print('  일별 매매 로그')
    print('=' * 100)
    for log in daily_log:
        d = log['date']
        parts = [d]
        if log['sells']:
            parts.append(f"매도: {', '.join(log['sells'])}")
        if log['buys']:
            parts.append(f"매수: {', '.join(log['buys'])}")
        parts.append(f"포트: [{', '.join(log['portfolio'])}]")
        if log['dr'] != 0:
            parts.append(f"일수익: {log['dr']*100:+.2f}%")
        print('  '.join(parts))

    # ── Unrealized ──
    for tk, info in portfolio.items():
        last = info.get('lp', info['ep'])
        ret = (last - info['ep']) / info['ep'] * 100
        trades.append({
            'ticker': tk, 'return': ret,
            'hold': trade_dates.index(trade_dates[-1]) - trade_dates.index(info['ed']),
            'reason': 'holding', 'ep': info['ep'], 'xp': last,
            'ed': info['ed'], 'xd': '~now'
        })

    # ── Trade List ──
    print(f"\n{'=' * 100}")
    print('  전체 매매 내역')
    print('=' * 100)
    print(f"{'티커':8s} {'진입일':12s} {'이탈일':12s} {'진입가':>8s} {'이탈가':>8s} {'수익률':>8s} {'보유일':>5s} {'사유':10s}")
    print('-' * 100)
    for t in sorted(trades, key=lambda x: x['ed']):
        tag = '보유중' if t['reason'] == 'holding' else t['reason']
        print(f"{t['ticker']:8s} {t['ed']:12s} {t['xd']:12s} "
              f"${t['ep']:>7.1f} ${t['xp']:>7.1f} {t['return']:>+7.1f}% {t['hold']:>4d}일 [{tag}]")

    # ── Summary ──
    arr = np.array(daily_returns) if daily_returns else np.array([0])
    cum = (np.prod(1 + arr) - 1) * 100
    ca = np.cumprod(1 + arr)
    pk = np.maximum.accumulate(ca)
    mdd = np.min((ca - pk) / pk) * 100
    sh = np.mean(arr) / np.std(arr) if np.std(arr) > 0 else 0
    wd = sum(1 for r in arr if r > 0)

    comp = [t for t in trades if t['reason'] != 'holding']
    hold = [t for t in trades if t['reason'] == 'holding']

    print(f"\n{'=' * 100}")
    print('  종합 성과')
    print('=' * 100)
    print(f"  누적 수익률:  {cum:+.1f}%")
    print(f"  MDD:          {mdd:+.1f}%")
    print(f"  Sharpe:       {sh:.2f}")
    print(f"  승률(일):     {wd}/{len(arr)} ({wd/len(arr)*100:.0f}%)")
    print(f"  거래일:       {len(trade_dates)}일")

    if comp:
        wins = sum(1 for t in comp if t['return'] > 0)
        avg_r = sum(t['return'] for t in comp) / len(comp)
        avg_h = sum(t['hold'] for t in comp) / len(comp)
        print(f"\n  완료 거래:    {len(comp)}건")
        print(f"  거래 승률:    {wins}/{len(comp)} ({wins/len(comp)*100:.0f}%)")
        print(f"  평균 수익:    {avg_r:+.1f}%")
        print(f"  평균 보유:    {avg_h:.1f}일")

        # 사유별 분석
        reasons = {}
        for t in comp:
            rr = t['reason']
            if rr not in reasons:
                reasons[rr] = []
            reasons[rr].append(t['return'])
        print(f"\n  이탈 사유별:")
        for k, v in reasons.items():
            avg = sum(v) / len(v)
            print(f"    {k:15s}: {len(v):3d}건, 평균 {avg:+.1f}%")

    if hold:
        avg_r = sum(t['return'] for t in hold) / len(hold)
        print(f"\n  보유중:       {len(hold)}건, 평균 미실현 {avg_r:+.1f}%")
        for t in hold:
            print(f"    {t['ticker']:8s} ${t['ep']:.1f} → ${t['xp']:.1f} ({t['return']:+.1f}%)")


if __name__ == '__main__':
    main()
