"""w_gap 기반 전략 그리드 서치 — 진입/이탈/슬롯/ms 조합 전수 탐색"""
import sqlite3, sys, numpy as np
from itertools import product
sys.stdout.reconfigure(encoding='utf-8')

DB = 'eps_momentum_data.db'


def load_data():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    dates = [r[0] for r in c.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    daily = {}
    for d in dates:
        rows = c.execute('''
            SELECT ticker, part2_rank, price, adj_gap,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL
        ''', (d,)).fetchall()
        all_prices = {r[0]: r[1] for r in c.execute(
            'SELECT ticker, price FROM ntm_screening WHERE date=? AND price IS NOT NULL', (d,)
        ).fetchall()}
        all_gaps = {r[0]: r[1] for r in c.execute(
            'SELECT ticker, adj_gap FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL', (d,)
        ).fetchall()}
        stocks = {}
        for tk, p2r, price, ag, nc, n7, n30, n60, n90 in rows:
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append((a - b) / abs(b) * 100)
                else:
                    segs.append(0)
            stocks[tk] = {'price': price, 'adj_gap': ag, 'min_seg': min(segs)}
        daily[d] = {'stocks': stocks, 'all_prices': all_prices, 'all_gaps': all_gaps}
    conn.close()
    return dates, daily


def compute_w_gap(ticker, date_idx, dates, daily):
    g0 = daily[dates[date_idx]]['all_gaps'].get(ticker, 0)
    g1 = daily[dates[date_idx - 1]]['all_gaps'].get(ticker, 0) if date_idx >= 1 else 0
    g2 = daily[dates[date_idx - 2]]['all_gaps'].get(ticker, 0) if date_idx >= 2 else 0
    return g0 * 0.5 + g1 * 0.3 + g2 * 0.2


def build_w_gap_ranks(date_idx, dates, daily):
    stocks = daily[dates[date_idx]]['stocks']
    wgaps = []
    for tk in stocks:
        wg = compute_w_gap(tk, date_idx, dates, daily)
        wgaps.append((tk, wg))
    wgaps.sort(key=lambda x: x[1])
    return {tk: rank + 1 for rank, (tk, _) in enumerate(wgaps)}


def run_bt(dates, daily, mode, entry_p, exit_p, max_stocks, ms_entry, ms_exit, stop_pct):
    """
    mode: 'threshold' or 'rank'
    threshold mode: entry_p = w_gap entry threshold, exit_p = w_gap exit threshold (None=no gap exit)
    rank mode: entry_p = top N entry, exit_p = top N exit
    """
    portfolio = {}
    trades = []
    daily_returns = []

    for i, d in enumerate(dates):
        if i < 2:
            continue
        stocks = daily[d]['stocks']
        all_prices = daily[d]['all_prices']
        wranks = build_w_gap_ranks(i, dates, daily)

        # ── Exit ──
        for tk in list(portfolio.keys()):
            entry = portfolio[tk]
            cp = (stocks.get(tk, {}).get('price')
                  or all_prices.get(tk)
                  or entry.get('lp', entry['ep']))
            pnl = (cp - entry['ep']) / entry['ep'] * 100

            should_exit, reason = False, ''
            # stop loss
            if pnl <= stop_pct:
                should_exit, reason = True, 'stop'
            # min_seg exit
            elif tk in stocks and stocks[tk]['min_seg'] < ms_exit:
                should_exit, reason = True, 'ms_exit'
            elif mode == 'threshold' and exit_p is not None:
                wg = compute_w_gap(tk, i, dates, daily)
                if wg >= exit_p:
                    should_exit, reason = True, 'wgap_exit'
            elif mode == 'rank':
                if tk not in stocks:
                    should_exit, reason = True, 'rank_out'
                else:
                    wr = wranks.get(tk, 999)
                    if wr > exit_p:
                        should_exit, reason = True, 'rank_exit'

            if should_exit:
                ret = (cp - entry['ep']) / entry['ep'] * 100
                trades.append({
                    'ticker': tk, 'return': ret, 'hold': i - entry['ei'],
                    'reason': reason, 'ep': entry['ep'], 'xp': cp,
                    'ed': entry['ed'], 'xd': d
                })
                portfolio.pop(tk)

        # ── Entry ──
        vac = max_stocks - len(portfolio)
        if vac > 0:
            if mode == 'threshold':
                cands = []
                for tk, s in stocks.items():
                    if tk in portfolio:
                        continue
                    wg = compute_w_gap(tk, i, dates, daily)
                    if wg <= entry_p and s['min_seg'] >= ms_entry:
                        cands.append((tk, wg))
                cands.sort(key=lambda x: x[1])
                for tk, _ in cands[:vac]:
                    portfolio[tk] = {
                        'ed': d, 'ep': stocks[tk]['price'],
                        'ei': i, 'lp': stocks[tk]['price']
                    }
            elif mode == 'rank':
                ranked = sorted(stocks.items(), key=lambda x: wranks.get(x[0], 999))
                count = 0
                for tk, s in ranked[:entry_p]:
                    if count >= vac:
                        break
                    if tk not in portfolio and s['min_seg'] >= ms_entry:
                        portfolio[tk] = {
                            'ed': d, 'ep': s['price'],
                            'ei': i, 'lp': s['price']
                        }
                        count += 1

        # Update last price
        for tk in portfolio:
            if tk in stocks:
                portfolio[tk]['lp'] = stocks[tk]['price']
            elif tk in all_prices:
                portfolio[tk]['lp'] = all_prices[tk]

        # Daily return
        if i > 2 and portfolio:
            prev = dates[i - 1]
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
            'ticker': tk, 'return': ret, 'hold': len(dates) - 1 - info['ei'],
            'reason': 'holding', 'ep': info['ep'], 'xp': last,
            'ed': info['ed'], 'xd': '~now'
        })

    arr = np.array(daily_returns) if daily_returns else np.array([0])
    cum = (np.prod(1 + arr) - 1) * 100
    ca = np.cumprod(1 + arr)
    pk = np.maximum.accumulate(ca)
    mdd = np.min((ca - pk) / pk) * 100
    sh = np.mean(arr) / np.std(arr) if np.std(arr) > 0 else 0
    comp = [t for t in trades if t['reason'] != 'holding']
    hold = [t for t in trades if t['reason'] == 'holding']
    return {
        'cum': cum, 'mdd': mdd, 'sharpe': sh,
        'wd': sum(1 for r in arr if r > 0), 'td': len(arr),
        'trades': trades, 'comp': comp, 'hold': hold
    }


def main():
    dates, daily = load_data()
    print(f"데이터: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")
    print(f"⚠️ 21일 데이터 — 참고용, 확정 판단 불가\n")

    results = []

    # ── 1. Threshold 모드: w_gap 진입/이탈 임계값 그리드 ──
    wgap_entries = [-3, -4, -5, -6, -8]
    wgap_exits = [None, 0, 2, 4]
    ms_entries = [0, 1]
    ms_exits = [-2, 0]
    max_stocks_list = [3, 5]
    stop_pcts = [-10, -12]

    for we, wx, mse, msx, ms, sp in product(wgap_entries, wgap_exits, ms_entries, ms_exits, max_stocks_list, stop_pcts):
        wx_str = f'exit>={wx}' if wx is not None else 'no_exit'
        name = f'T wg<={we} {wx_str} ms>={mse} msx<{msx} M{ms} s{sp}'
        r = run_bt(dates, daily, 'threshold', we, wx, ms, mse, msx, sp)
        r['name'] = name
        r['mode'] = 'threshold'
        r['params'] = {'entry': we, 'exit': wx, 'ms_entry': mse, 'ms_exit': msx, 'max': ms, 'stop': sp}
        results.append(r)

    # ── 2. Rank 모드: wTop N 진입 / wTop M 이탈 그리드 ──
    rank_entries = [3, 5]
    rank_exits = [7, 10, 15, 20, 30]
    ms_entries_r = [-2, 0, 1]
    ms_exits_r = [-2, 0]

    for re_, rx, mse, msx in product(rank_entries, rank_exits, ms_entries_r, ms_exits_r):
        if rx <= re_:
            continue
        ms = re_ + 2  # max stocks = entry top + 2 buffer
        name = f'R wT{re_}/wT{rx} ms>={mse} msx<{msx}'
        r = run_bt(dates, daily, 'rank', re_, rx, ms, mse, msx, -10)
        r['name'] = name
        r['mode'] = 'rank'
        r['params'] = {'entry': re_, 'exit': rx, 'ms_entry': mse, 'ms_exit': msx, 'max': ms}
        results.append(r)

    # ── Sort by cumulative return ──
    results.sort(key=lambda x: x['cum'], reverse=True)

    # ── Top 20 ──
    print('=' * 115)
    print('  w_gap 기반 그리드 서치 결과 (상위 20)')
    print('=' * 115)
    print(f"{'#':>3s} {'전략':<48s} {'수익률':>8s} {'MDD':>8s} {'Sharpe':>7s} {'승률':>10s} {'완료':>4s} {'보유':>4s}")
    print('-' * 115)

    for i, r in enumerate(results[:20]):
        wr = f"{r['wd']}/{r['td']}"
        print(f"{i+1:>3d} {r['name']:<48s} {r['cum']:>+7.1f}% {r['mdd']:>+7.1f}% "
              f"{r['sharpe']:>7.2f} {wr:>10s} {len(r['comp']):>4d} {len(r['hold']):>4d}")

    # ── Bottom 5 ──
    print(f"\n{'하위 5':}")
    for r in results[-5:]:
        wr = f"{r['wd']}/{r['td']}"
        print(f"    {r['name']:<48s} {r['cum']:>+7.1f}% {r['mdd']:>+7.1f}% "
              f"{r['sharpe']:>7.2f} {wr:>10s} {len(r['comp']):>4d} {len(r['hold']):>4d}")

    # ── 상위 5개 상세 ──
    for i, r in enumerate(results[:5]):
        print(f"\n{'━' * 70}")
        print(f"  #{i+1} {r['name']}")
        print(f"  수익률 {r['cum']:+.1f}% | MDD {r['mdd']:+.1f}% | Sharpe {r['sharpe']:.2f}")
        print(f"{'━' * 70}")
        comp = r['comp']
        hold = r['hold']
        if comp:
            wins = sum(1 for t in comp if t['return'] > 0)
            avg_r = sum(t['return'] for t in comp) / len(comp)
            avg_h = sum(t['hold'] for t in comp) / len(comp)
            reasons = {}
            for t in comp:
                rr = t['reason']
                if rr not in reasons:
                    reasons[rr] = []
                reasons[rr].append(t['return'])
            reason_str = ' | '.join(
                f"{k}: {len(v)}건 {sum(v)/len(v):+.1f}%"
                for k, v in reasons.items()
            )
            print(f"  완료 {len(comp)}건 (승률 {wins}/{len(comp)}), 평균 {avg_r:+.1f}%, 보유 {avg_h:.1f}일")
            print(f"  사유: {reason_str}")
        if hold:
            print(f"  보유중 {len(hold)}건")
        for t in sorted(r['trades'], key=lambda x: x['ed']):
            tag = '보유' if t['reason'] == 'holding' else t['reason']
            print(f"  {t['ticker']:6s} {t['ed']}~{t['xd']:<11s} "
                  f"{t['ep']:>7.1f}>{t['xp']:>7.1f} {t['return']:>+6.1f}% "
                  f"{t['hold']:>2d}일 [{tag}]")

    # ── 패턴 분석 ──
    print(f"\n{'=' * 70}")
    print(f"  파라미터별 평균 수익률")
    print(f"{'=' * 70}")

    # Threshold mode analysis
    t_results = [r for r in results if r['mode'] == 'threshold']
    if t_results:
        print("\n[Threshold 모드]")
        for param, values in [
            ('entry', wgap_entries),
            ('exit', wgap_exits),
            ('ms_entry', ms_entries),
            ('ms_exit', ms_exits),
            ('max', max_stocks_list),
            ('stop', stop_pcts),
        ]:
            print(f"  {param}:")
            for v in values:
                subset = [r for r in t_results if r['params'].get(param) == v]
                if subset:
                    avg_cum = sum(r['cum'] for r in subset) / len(subset)
                    avg_mdd = sum(r['mdd'] for r in subset) / len(subset)
                    print(f"    {str(v):>6s}: 수익 {avg_cum:+.1f}%, MDD {avg_mdd:+.1f}% (n={len(subset)})")

    # Rank mode analysis
    r_results = [r for r in results if r['mode'] == 'rank']
    if r_results:
        print("\n[Rank 모드]")
        for param, values in [
            ('entry', rank_entries),
            ('exit', rank_exits),
            ('ms_entry', ms_entries_r),
            ('ms_exit', ms_exits_r),
        ]:
            print(f"  {param}:")
            for v in values:
                subset = [r for r in r_results if r['params'].get(param) == v]
                if subset:
                    avg_cum = sum(r['cum'] for r in subset) / len(subset)
                    avg_mdd = sum(r['mdd'] for r in subset) / len(subset)
                    print(f"    {str(v):>6s}: 수익 {avg_cum:+.1f}%, MDD {avg_mdd:+.1f}% (n={len(subset)})")


if __name__ == '__main__':
    main()
