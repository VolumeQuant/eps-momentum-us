"""wTop 조합 × 모든 가능한 시작일 백테스트"""
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

    si = trade_dates.index(start_date)
    active_dates = trade_dates[si:]

    portfolio = {}
    trades = []
    daily_returns = []

    for d in active_dates:
        stocks = daily[d]['stocks']
        all_prices = daily[d]['all_prices']
        wranks = build_w_gap_ranks(d, all_dates, daily)

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
                trades.append({'return': ret})
                portfolio.pop(tk)

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

        for tk in portfolio:
            if tk in stocks:
                portfolio[tk]['lp'] = stocks[tk]['price']
            elif tk in all_prices:
                portfolio[tk]['lp'] = all_prices[tk]

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
        trades.append({'return': ret})

    arr = np.array(daily_returns) if daily_returns else np.array([0])
    cum = (np.prod(1 + arr) - 1) * 100
    ca = np.cumprod(1 + arr)
    pk = np.maximum.accumulate(ca)
    mdd = np.min((ca - pk) / pk) * 100
    sh = np.mean(arr) / np.std(arr) if np.std(arr) > 0 else 0
    wd = sum(1 for r in arr if r > 0)
    return {'cum': cum, 'mdd': mdd, 'sharpe': sh, 'days': len(active_dates)}


def main():
    trade_dates, all_dates, daily = load_data()
    print(f"거래일: {trade_dates[0]} ~ {trade_dates[-1]} ({len(trade_dates)}일)")
    print(f"공통 조건: ms_entry>=0%, ms_exit<-2%, stop -10%\n")

    combos = [
        (3, 15), (3, 20), (3, 25), (3, 30),
        (5, 15), (5, 20), (5, 25), (5, 30),
    ]

    # 최소 5거래일 이상 남아야 의미 있음
    min_days = 5
    max_start_idx = len(trade_dates) - min_days
    start_dates = trade_dates[:max_start_idx]

    print(f"시작일 후보: {len(start_dates)}개 ({start_dates[0]} ~ {start_dates[-1]})")
    print(f"최소 보유기간: {min_days}거래일\n")

    # ── 시작일별 상세 테이블 ──
    print('=' * 140)
    header = f"{'시작일':12s} {'남은일':>5s}"
    for e, x in combos:
        header += f" | T{e}/T{x:>2d}"
    print(header)
    print('-' * 140)

    combo_results = {f'wTop{e}/wTop{x}': [] for e, x in combos}

    for sd in start_dates:
        si = trade_dates.index(sd)
        remaining = len(trade_dates) - si
        line = f"{sd:12s} {remaining:>4d}일"
        for e, x in combos:
            r = run_bt(trade_dates, all_dates, daily, e, x, sd)
            combo_results[f'wTop{e}/wTop{x}'].append(r)
            line += f" | {r['cum']:>+6.1f}%"
        print(line)

    # ── 종합 통계 ──
    print(f"\n{'=' * 110}")
    print(f"{'전략':<18s} {'평균':>7s} {'중앙값':>7s} {'최소':>7s} {'최대':>7s} {'표준편차':>7s} "
          f"{'플러스':>7s} {'평균MDD':>8s} {'평균Sharpe':>10s}")
    print('-' * 110)

    for e, x in combos:
        name = f'wTop{e}/wTop{x}'
        cums = [r['cum'] for r in combo_results[name]]
        mdds = [r['mdd'] for r in combo_results[name]]
        sharpes = [r['sharpe'] for r in combo_results[name]]
        pos = sum(1 for c in cums if c > 0)
        print(f"{name:<18s} {np.mean(cums):>+6.1f}% {np.median(cums):>+6.1f}% "
              f"{min(cums):>+6.1f}% {max(cums):>+6.1f}% {np.std(cums):>6.1f}% "
              f"{pos:>3d}/{len(cums):<3d} {np.mean(mdds):>+7.1f}% {np.mean(sharpes):>10.2f}")

    # ── 구간별 분석: 초반(2/10~2/19) vs 후반(2/20~) ──
    mid_idx = len(start_dates) // 2
    early = start_dates[:mid_idx]
    late = start_dates[mid_idx:]

    print(f"\n{'=' * 110}")
    print(f"  구간별 평균 수익률")
    print(f"  초반 시작({early[0]}~{early[-1]}): 시장 상승기 포함")
    print(f"  후반 시작({late[0]}~{late[-1]}): 시장 하락기 포함")
    print(f"{'=' * 110}")
    print(f"{'전략':<18s} {'초반 평균':>9s} {'후반 평균':>9s} {'차이':>8s}")
    print('-' * 60)

    for e, x in combos:
        name = f'wTop{e}/wTop{x}'
        results = combo_results[name]
        early_avg = np.mean([results[i]['cum'] for i in range(len(early))])
        late_avg = np.mean([results[i + mid_idx]['cum'] for i in range(len(late))])
        diff = early_avg - late_avg
        print(f"{name:<18s} {early_avg:>+8.1f}% {late_avg:>+8.1f}% {diff:>+7.1f}%")


if __name__ == '__main__':
    main()
