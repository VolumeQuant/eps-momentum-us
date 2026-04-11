"""5가지 변형 백테스트: 각 개선안 효과 검증
Variant A: Baseline (E5/X12/S3, z-score)
Variant B: E3 단독 (E3/X12/S3, z-score)
Variant C: E3 + Conviction 강화 (이중확증 + 2.5x cap)
Variant D: E3 + Breakout Hold (이탈 유예 2일)
Variant E: E3 + Conviction 강화 + Breakout Hold (full stack)

기존 gridsearch_v73.py 기반.
모든 변형은 z-score 방식 사용 (롤백 후 baseline).
"""
import sqlite3
from collections import defaultdict
import numpy as np

DB_PATH = 'eps_momentum_data.db'


def load_full_data():
    """DB에서 전체 데이터 로드 — 가격, NTM, 애널리스트 정보 포함"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    data = {}
    for d in dates:
        rows = cursor.execute('''
            SELECT ticker, part2_rank, price, composite_rank,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   adj_gap, rev_up30, rev_down30, num_analysts,
                   ma60, ma120
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append((a - b) / abs(b) * 100)
                else:
                    segs.append(0)
            segs = [max(-100, min(100, s)) for s in segs]
            min_seg = min(segs) if segs else 0

            data[d][tk] = {
                'old_p2': r[1],
                'price': r[2],
                'comp_rank': r[3],
                'ntm_current': nc,
                'ntm_7d': n7,
                'ntm_30d': n30,
                'ntm_60d': n60,
                'ntm_90d': n90,
                'adj_gap': r[9],
                'rev_up30': r[10] or 0,
                'rev_down30': r[11] or 0,
                'num_analysts': r[12] or 0,
                'ma60': r[13],
                'ma120': r[14],
                'min_seg': min_seg,
            }
    conn.close()
    return dates, data


def apply_conviction_baseline(adj_gap, rev_up, num_analysts, ntm_current, ntm_90d):
    """기본 conviction (1~2x)"""
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if num_analysts and num_analysts > 0 else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    conviction = max(ratio, eps_floor)
    return adj_gap * (1 + conviction)


def apply_conviction_strong(adj_gap, rev_up, num_analysts, ntm_current, ntm_90d):
    """강화 conviction (1~2.5x, 이중 확증 시 tail bonus)"""
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if num_analysts and num_analysts > 0 else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 2.0)

    base = max(ratio, min(eps_floor, 1.0))
    tail_bonus = 0
    if ratio >= 0.5 and eps_floor >= 1.0:
        tail_bonus = min((eps_floor - 1.0) * 0.5, 0.5)
    conviction = base + tail_bonus
    return adj_gap * (1 + conviction)


def recompute_ranks(data, conviction_fn):
    """conviction 함수 바꿔서 composite_rank + part2_rank(z-score 가중) 재계산"""
    dates = sorted(data.keys())

    # 1. 일별 conviction adj_gap → composite_rank 재계산
    new_comp_rank = {}
    new_score_by_date = {}
    for d in dates:
        ticker_gaps = {}
        for tk, v in data[d].items():
            if v['min_seg'] < -2:  # min_seg<-2% 제외
                continue
            cg = conviction_fn(v['adj_gap'], v['rev_up30'], v['num_analysts'],
                               v['ntm_current'], v['ntm_90d'])
            if cg is not None:
                ticker_gaps[tk] = cg

        # composite_rank: 작을수록(음수) 좋음
        sorted_tks = sorted(ticker_gaps.items(), key=lambda x: x[1])
        new_comp_rank[d] = {tk: i + 1 for i, (tk, _) in enumerate(sorted_tks)}

        # z-score(30~100)로 변환 — 높을수록 좋음
        vals = list(ticker_gaps.values())
        if len(vals) >= 2:
            mean_v = np.mean(vals)
            std_v = np.std(vals)
            if std_v > 0:
                new_score_by_date[d] = {
                    tk: min(100.0, max(30.0, 65 + (-(v - mean_v) / std_v) * 15))
                    for tk, v in ticker_gaps.items()
                }
            else:
                new_score_by_date[d] = {tk: 65 for tk in ticker_gaps}
        else:
            new_score_by_date[d] = {tk: 65 for tk in ticker_gaps}

    # 2. 3일 가중 점수 → part2_rank
    new_p2 = {}
    for di, today in enumerate(dates):
        recent = []
        for j in range(di, max(di - 3, -1), -1):
            if new_score_by_date.get(dates[j]):
                recent.insert(0, dates[j])
            if len(recent) >= 3:
                break
        if not recent:
            continue

        weights = [0.2, 0.3, 0.5]
        if len(recent) == 2:
            weights = [0.4, 0.6]
        elif len(recent) == 1:
            weights = [1.0]

        eligible = list(new_score_by_date.get(today, {}).keys())

        # carry-forward
        def carry(tk, idx):
            for j in range(idx - 1, -1, -1):
                prev = new_score_by_date.get(recent[j], {}).get(tk)
                if prev is not None:
                    return prev
            return 30

        wgap_map = {}
        for tk in eligible:
            ws = 0
            for i, d in enumerate(recent):
                score = new_score_by_date.get(d, {}).get(tk)
                if score is None:
                    score = carry(tk, i)
                ws += score * weights[i]
            wgap_map[tk] = ws

        sorted_tks = sorted(eligible, key=lambda tk: -wgap_map.get(tk, -999))
        new_p2[today] = {tk: i + 1 for i, tk in enumerate(sorted_tks[:30])}

    return new_comp_rank, new_p2


def is_breakout_hold(data, dates, today_idx, ticker):
    """이탈 유예 조건 검사:
       1. 최근 20일 +25% 이상
       2. ntm_90d → ntm_current 순방향
       3. rev_up30/num_analysts >= 0.4
       4. price > MA60
    """
    if today_idx < 20:
        return False

    today = dates[today_idx]
    v = data[today].get(ticker)
    if not v:
        return False

    # 조건 1: 20일 가격 변화
    past = data.get(dates[today_idx - 20], {}).get(ticker)
    if not past or not past.get('price') or past['price'] <= 0:
        return False
    price_chg_20d = (v['price'] - past['price']) / past['price'] * 100
    if price_chg_20d < 25:
        return False

    # 조건 2: ntm_90d → ntm_current 상승
    if not v.get('ntm_90d') or v['ntm_90d'] <= 0:
        return False
    if v['ntm_current'] <= v['ntm_90d']:
        return False

    # 조건 3: 애널리스트 합의 상향
    if v['num_analysts'] < 1:
        return False
    rev_ratio = v['rev_up30'] / v['num_analysts']
    if rev_ratio < 0.4:
        return False

    # 조건 4: price > MA60
    if not v.get('ma60') or v['price'] <= v['ma60']:
        return False

    return True


def simulate(dates, data, p2_ranks, entry_top, exit_top, max_slots,
             use_breakout_hold=False, verbose=False):
    """변형 시뮬레이션
    p2_ranks: {date: {ticker: rank}} — 사용할 part2_rank 맵
    """
    portfolio = {}  # {ticker: {'entry_price': p, 'grace_days': 0}}
    daily_returns = []
    trades = []
    trade_log = []
    consecutive = defaultdict(int)
    breakout_holds_used = 0

    for di, today in enumerate(dates):
        if today not in data:
            continue
        today_data = data[today]
        rank_map = p2_ranks.get(today, {})

        # 연속 출현
        new_consecutive = defaultdict(int)
        for tk in rank_map:
            if rank_map[tk] <= 30:
                new_consecutive[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consecutive

        # 이탈 체크
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            price = today_data.get(tk, {}).get('price')

            should_exit = False
            exit_reason = ''

            if rank is None or rank > exit_top:
                should_exit = True
                exit_reason = 'rank'
            if min_seg < -2:
                should_exit = True
                exit_reason = 'min_seg'

            # 이탈 유예 체크 (rank 사유만, min_seg는 유예 안 함)
            if should_exit and exit_reason == 'rank' and use_breakout_hold:
                if is_breakout_hold(data, dates, di, tk):
                    grace = portfolio[tk].get('grace_days', 0)
                    if grace < 2:
                        portfolio[tk]['grace_days'] = grace + 1
                        breakout_holds_used += 1
                        should_exit = False  # 유예
                else:
                    pass

            if should_exit and price:
                entry_price = portfolio[tk]['entry_price']
                entry_date = portfolio[tk].get('entry_date', '?')
                ret = (price - entry_price) / entry_price * 100
                trades.append(ret)
                trade_log.append({
                    'ticker': tk, 'entry_date': entry_date, 'exit_date': today,
                    'entry_price': entry_price, 'exit_price': price,
                    'return': ret, 'reason': exit_reason,
                })
                exited.append(tk)
            elif not should_exit:
                portfolio[tk]['grace_days'] = portfolio[tk].get('grace_days', 0)

        for tk in exited:
            del portfolio[tk]

        # 진입
        vacancies = max_slots - len(portfolio)
        if vacancies > 0:
            candidates = []
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > entry_top:
                    break
                if tk in portfolio:
                    continue
                if consecutive.get(tk, 0) < 3:
                    continue
                min_seg = today_data.get(tk, {}).get('min_seg', 0)
                if min_seg < 0:
                    continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0:
                    candidates.append((tk, price))

            for tk, price in candidates[:vacancies]:
                portfolio[tk] = {'entry_price': price, 'grace_days': 0,
                                  'entry_date': today}

        # 일간 수익률
        if portfolio:
            day_ret = 0
            for tk in portfolio:
                price = today_data.get(tk, {}).get('price')
                if price and di > 0:
                    prev_data = data.get(dates[di - 1], {})
                    prev_price = prev_data.get(tk, {}).get('price')
                    if prev_price and prev_price > 0:
                        day_ret += (price - prev_price) / prev_price * 100
            day_ret /= len(portfolio)
            daily_returns.append(day_ret)
        else:
            daily_returns.append(0)

    cum_ret = 1.0
    peak = 1.0
    max_dd = 0
    for dr in daily_returns:
        cum_ret *= (1 + dr / 100)
        peak = max(peak, cum_ret)
        dd = (cum_ret - peak) / peak * 100
        max_dd = min(max_dd, dd)

    total_return = (cum_ret - 1) * 100
    n_trades = len(trades)
    win_rate = (sum(1 for t in trades if t > 0) / n_trades * 100
                if n_trades > 0 else 0)
    avg_trade = sum(trades) / n_trades if n_trades > 0 else 0
    best = max(trades) if trades else 0
    worst = min(trades) if trades else 0

    return {
        'total_return': round(total_return, 2),
        'max_dd': round(max_dd, 2),
        'n_trades': n_trades,
        'win_rate': round(win_rate, 1),
        'avg_trade': round(avg_trade, 2),
        'best_trade': round(best, 2),
        'worst_trade': round(worst, 2),
        'breakout_holds': breakout_holds_used,
        'trade_log': trade_log,
        'open_positions': list(portfolio.keys()),
    }


def main():
    print("Loading data...")
    dates, data = load_full_data()
    print(f"Period: {dates[0]} ~ {dates[-1]} ({len(dates)} days)")

    # Baseline part2_rank: DB의 기존 값 사용
    base_p2 = {}
    for d in dates:
        base_p2[d] = {tk: v['old_p2'] for tk, v in data[d].items()
                      if v.get('old_p2') is not None}

    # Conviction 강화로 재계산한 ranks
    print("Recomputing ranks with strong conviction...")
    _, strong_p2 = recompute_ranks(data, apply_conviction_strong)

    # 5 variants
    print("\nRunning 5 variants...\n")

    variants = [
        ('A: E5 baseline (현재)', base_p2, 5, 12, 3, False),
        ('B: E3 단독', base_p2, 3, 12, 3, False),
        ('C: E3 + Conviction 강화', strong_p2, 3, 12, 3, False),
        ('D: E3 + Breakout Hold', base_p2, 3, 12, 3, True),
        ('E: E3 + Conv + Hold (full)', strong_p2, 3, 12, 3, True),
    ]

    results = []
    for name, p2, e, x, s, hold in variants:
        r = simulate(dates, data, p2, e, x, s, use_breakout_hold=hold)
        r['name'] = name
        results.append(r)

    # 표 출력
    print(f"{'Variant':<32s} {'Return':>8s} {'MDD':>7s} {'Trades':>7s} {'WR':>5s} {'Best':>7s} {'Worst':>7s} {'Hold':>5s}")
    print('-' * 85)
    for r in results:
        print(f"{r['name']:<32s} {r['total_return']:+7.1f}% {r['max_dd']:+6.1f}% "
              f"{r['n_trades']:>7d} {r['win_rate']:>4.0f}% "
              f"{r['best_trade']:+6.1f}% {r['worst_trade']:+6.1f}% {r['breakout_holds']:>5d}")

    # 상대 비교
    print("\n=== A vs B/C/D/E ===")
    base_ret = results[0]['total_return']
    base_dd = results[0]['max_dd']
    for r in results[1:]:
        ret_diff = r['total_return'] - base_ret
        dd_diff = r['max_dd'] - base_dd
        print(f"  {r['name']:<32s}: {ret_diff:+6.2f}%p ret, {dd_diff:+6.2f}%p MDD")

    # 거래 상세 — A vs C 비교 (Conviction 강화 효과)
    print("\n=== Trade Log (Variant A baseline) ===")
    for t in results[0]['trade_log']:
        print(f"  {t['ticker']:6s} {t['entry_date']} -> {t['exit_date']} "
              f"${t['entry_price']:>7.1f} -> ${t['exit_price']:>7.1f} = {t['return']:+6.1f}% [{t['reason']}]")
    print(f"  Open positions: {results[0]['open_positions']}")

    print("\n=== Trade Log (Variant B - E3) ===")
    for t in results[1]['trade_log']:
        print(f"  {t['ticker']:6s} {t['entry_date']} -> {t['exit_date']} "
              f"${t['entry_price']:>7.1f} -> ${t['exit_price']:>7.1f} = {t['return']:+6.1f}% [{t['reason']}]")
    print(f"  Open positions: {results[1]['open_positions']}")

    print("\n=== Trade Log (Variant C - Conviction Strong) ===")
    for t in results[2]['trade_log']:
        print(f"  {t['ticker']:6s} {t['entry_date']} -> {t['exit_date']} "
              f"${t['entry_price']:>7.1f} -> ${t['exit_price']:>7.1f} = {t['return']:+6.1f}% [{t['reason']}]")
    print(f"  Open positions: {results[2]['open_positions']}")


if __name__ == '__main__':
    main()
