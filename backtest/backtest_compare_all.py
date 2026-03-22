"""전략 비교 백테스트: raw adj_gap vs w_gap × threshold vs rank-based"""
import sqlite3, sys, numpy as np
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
            SELECT ticker, part2_rank, composite_rank, price, adj_gap,
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
        for tk, p2r, cr, price, ag, nc, n7, n30, n60, n90 in rows:
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append((a - b) / abs(b) * 100)
                else:
                    segs.append(0)
            stocks[tk] = {
                'price': price, 'adj_gap': ag, 'min_seg': min(segs),
                'part2_rank': p2r, 'composite_rank': cr
            }
        daily[d] = {'stocks': stocks, 'all_prices': all_prices, 'all_gaps': all_gaps}
    conn.close()
    return dates, daily


def compute_w_gap(ticker, date_idx, dates, daily):
    g0 = daily[dates[date_idx]]['all_gaps'].get(ticker, 0)
    g1 = daily[dates[date_idx - 1]]['all_gaps'].get(ticker, 0) if date_idx >= 1 else 0
    g2 = daily[dates[date_idx - 2]]['all_gaps'].get(ticker, 0) if date_idx >= 2 else 0
    return g0 * 0.5 + g1 * 0.3 + g2 * 0.2


def build_w_gap_ranks(date_idx, dates, daily):
    """w_gap 기준 순위 계산 — part2_rank 대체"""
    stocks = daily[dates[date_idx]]['stocks']
    wgaps = []
    for tk in stocks:
        wg = compute_w_gap(tk, date_idx, dates, daily)
        wgaps.append((tk, wg))
    wgaps.sort(key=lambda x: x[1])  # 음수(저평가) 우선
    return {tk: rank + 1 for rank, (tk, _) in enumerate(wgaps)}


def run_bt(dates, daily, name, entry_fn, exit_fn, max_stocks):
    portfolio = {}
    trades = []
    daily_returns = []
    for i, d in enumerate(dates):
        if i < 2:
            continue
        stocks = daily[d]['stocks']
        all_prices = daily[d]['all_prices']
        # Exit
        for tk in list(portfolio.keys()):
            entry = portfolio[tk]
            cp = (stocks.get(tk, {}).get('price')
                  or all_prices.get(tk)
                  or entry.get('lp', entry['ep']))
            should, reason = exit_fn(tk, i, dates, daily, stocks, entry, cp)
            if should:
                ret = (cp - entry['ep']) / entry['ep'] * 100
                trades.append({
                    'ticker': tk, 'return': ret, 'hold': i - entry['ei'],
                    'reason': reason, 'ep': entry['ep'], 'xp': cp,
                    'ed': entry['ed'], 'xd': d
                })
                portfolio.pop(tk)
        # Entry
        vac = max_stocks - len(portfolio)
        if vac > 0:
            cands = entry_fn(i, dates, daily, stocks, portfolio)
            for tk in cands[:vac]:
                if tk not in portfolio and tk in stocks:
                    portfolio[tk] = {
                        'ed': d, 'ep': stocks[tk]['price'],
                        'ei': i, 'lp': stocks[tk]['price']
                    }
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
        'name': name, 'cum': cum, 'mdd': mdd, 'sharpe': sh,
        'wd': sum(1 for r in arr if r > 0), 'td': len(arr),
        'trades': trades, 'comp': comp, 'hold': hold
    }


# ━━━ Entry/Exit: raw adj_gap 기반 ━━━

def e_raw_threshold(i, dates, daily, stocks, port):
    """raw adj_gap <= -4, ms >= 1"""
    c = [(tk, s['adj_gap']) for tk, s in stocks.items()
         if tk not in port and s['adj_gap'] is not None
         and s['adj_gap'] <= -4 and s['min_seg'] >= 1]
    c.sort(key=lambda x: x[1])
    return [tk for tk, _ in c]

def e_raw_top3(i, dates, daily, stocks, port):
    ranked = sorted(stocks.items(), key=lambda x: x[1]['part2_rank'])
    return [tk for tk, s in ranked[:3] if tk not in port and s['min_seg'] >= -2]

def e_raw_top5(i, dates, daily, stocks, port):
    ranked = sorted(stocks.items(), key=lambda x: x[1]['part2_rank'])
    return [tk for tk, s in ranked[:5] if tk not in port and s['min_seg'] >= -2]

def e_raw_top3_ms1(i, dates, daily, stocks, port):
    ranked = sorted(stocks.items(), key=lambda x: x[1]['part2_rank'])
    return [tk for tk, s in ranked[:3] if tk not in port and s['min_seg'] >= 1]

def x_ms0_stop(tk, i, dates, daily, stocks, entry, cp):
    """ms < 0% exit + -10% stop"""
    pnl = (cp - entry['ep']) / entry['ep'] * 100
    if pnl <= -10:
        return True, 'stop'
    if tk in stocks and stocks[tk]['min_seg'] < 0:
        return True, 'ms<0'
    return False, ''

def _x_rank(tk, stocks, limit):
    if tk not in stocks:
        return True, 'rank_out'
    if stocks[tk]['part2_rank'] > limit:
        return True, f'rank>{limit}'
    if stocks[tk]['min_seg'] < -2:
        return True, 'ms<-2'
    return False, ''

def x_raw_top7(tk, i, dates, daily, stocks, entry, cp):
    return _x_rank(tk, stocks, 7)

def x_raw_top15(tk, i, dates, daily, stocks, entry, cp):
    return _x_rank(tk, stocks, 15)

def x_raw_top30(tk, i, dates, daily, stocks, entry, cp):
    return _x_rank(tk, stocks, 30)


# ━━━ Entry/Exit: w_gap 기반 ━━━

def e_wgap_threshold(threshold):
    """w_gap <= threshold, ms >= 1"""
    def fn(i, dates, daily, stocks, port):
        c = []
        for tk, s in stocks.items():
            if tk in port:
                continue
            wg = compute_w_gap(tk, i, dates, daily)
            if wg <= threshold and s['min_seg'] >= 1:
                c.append((tk, wg))
        c.sort(key=lambda x: x[1])
        return [tk for tk, _ in c]
    return fn

def e_wgap_topN(n, ms_min=-2):
    """w_gap 순위 상위 N개 진입"""
    def fn(i, dates, daily, stocks, port):
        wranks = build_w_gap_ranks(i, dates, daily)
        ranked = sorted(stocks.items(), key=lambda x: wranks.get(x[0], 999))
        return [tk for tk, s in ranked[:n]
                if tk not in port and s['min_seg'] >= ms_min]
    return fn

def x_wgap_topN(limit):
    """w_gap 순위 > limit 이탈"""
    def fn(tk, i, dates, daily, stocks, entry, cp):
        if tk not in stocks:
            return True, 'rank_out'
        wranks = build_w_gap_ranks(i, dates, daily)
        wr = wranks.get(tk, 999)
        if wr > limit:
            return True, f'wrank>{limit}'
        if stocks[tk]['min_seg'] < -2:
            return True, 'ms<-2'
        return False, ''
    return fn

def x_wgap_exit(threshold):
    """w_gap >= threshold 이탈 + ms<0 + stop"""
    def fn(tk, i, dates, daily, stocks, entry, cp):
        pnl = (cp - entry['ep']) / entry['ep'] * 100
        if pnl <= -10:
            return True, 'stop'
        if tk in stocks and stocks[tk]['min_seg'] < 0:
            return True, 'ms<0'
        wg = compute_w_gap(tk, i, dates, daily)
        if wg >= threshold:
            return True, f'wgap>={threshold}'
        return False, ''
    return fn


def main():
    dates, daily = load_data()
    print(f"데이터: {dates[0]} ~ {dates[-1]} ({len(dates)}일)\n")

    strategies = [
        # ── raw adj_gap 기반 ──
        ('raw A. gap<=-4,ms>=1,Max3',         e_raw_threshold,         x_ms0_stop,          3),
        ('raw B. Top3/Top7 ms>=-2',           e_raw_top3,              x_raw_top7,          3),
        ('raw C. Top3/Top15 ms>=-2',          e_raw_top3,              x_raw_top15,         3),
        ('raw D. Top5/Top30 ms>=-2',          e_raw_top5,              x_raw_top30,         5),
        ('raw E. Top3/Top7 ms>=1',            e_raw_top3_ms1,          x_raw_top7,          3),
        # ── w_gap 기반 ──
        ('wgp A. wgap<=-4,ms>=1,Max3',       e_wgap_threshold(-4),    x_ms0_stop,          3),
        ('wgp B. wTop3/wTop7 ms>=-2',        e_wgap_topN(3, -2),      x_wgap_topN(7),      3),
        ('wgp C. wTop3/wTop15 ms>=-2',       e_wgap_topN(3, -2),      x_wgap_topN(15),     3),
        ('wgp D. wTop5/wTop30 ms>=-2',       e_wgap_topN(5, -2),      x_wgap_topN(30),     5),
        ('wgp E. wTop3/wTop7 ms>=1',         e_wgap_topN(3, 1),       x_wgap_topN(7),      3),
        # ── w_gap threshold + exit ──
        ('wgp F. wgap<=-6,exit>=+2,Max5',    e_wgap_threshold(-6),    x_wgap_exit(2),      5),
        ('wgp G. wgap<=-4,exit>=+2,Max3',    e_wgap_threshold(-4),    x_wgap_exit(2),      3),
    ]

    print('=' * 110)
    print('  raw adj_gap vs w_gap 전략 비교 백테스트')
    print('=' * 110)
    hdr = (f"{'전략':<38s} {'수익률':>8s} {'MDD':>8s} {'Sharpe':>7s} "
           f"{'승률(일)':>10s} {'완료':>5s} {'보유':>5s}")
    print(hdr)
    print('-' * 110)

    results = []
    for name, ef, xf, ms in strategies:
        r = run_bt(dates, daily, name, ef, xf, ms)
        results.append(r)
        wr = f"{r['wd']}/{r['td']}"
        line = (f"{name:<38s} {r['cum']:>+7.1f}% {r['mdd']:>+7.1f}% "
                f"{r['sharpe']:>7.2f} {wr:>10s} {len(r['comp']):>5d} {len(r['hold']):>5d}")
        print(line)
        # 구분선
        if name.startswith('raw E') or name.startswith('wgp E'):
            print('-' * 110)

    # 상위 3개 상세
    results.sort(key=lambda x: x['cum'], reverse=True)
    for r in results[:3]:
        print(f"\n{'━' * 70}")
        print(f"  {r['name']}  →  {r['cum']:+.1f}%, MDD {r['mdd']:+.1f}%, Sharpe {r['sharpe']:.2f}")
        print(f"{'━' * 70}")
        comp = r['comp']
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
                f"{k}: {len(v)}건 {sum(v) / len(v):+.1f}%"
                for k, v in reasons.items()
            )
            print(f"  완료 {len(comp)}건 (승률 {wins}/{len(comp)}), "
                  f"평균 {avg_r:+.1f}%, 보유 {avg_h:.1f}일")
            print(f"  사유: {reason_str}")
        hold = r['hold']
        if hold:
            print(f"  보유중 {len(hold)}건")
        print()
        for t in sorted(r['trades'], key=lambda x: x['ed']):
            tag = '보유' if t['reason'] == 'holding' else t['reason']
            print(f"  {t['ticker']:6s} {t['ed']}~{t['xd']:<11s} "
                  f"{t['ep']:>7.1f}>{t['xp']:>7.1f} {t['return']:>+6.1f}% "
                  f"{t['hold']:>2d}일 [{tag}]")


if __name__ == '__main__':
    main()
