"""현행 전략 백테스트: wTop3/wTop15 (2/12~3/18, 3일 검증 시작일부터)

전략:
- 진입: w_gap 순위 Top3 + min_seg >= 0%
- 이탈: part2_rank > 15 / min_seg < -2% / -10% 손절
- 최대 3종목, 동일 비중
"""
import sqlite3
import sys
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = 'eps_momentum_data.db'


def calc_min_seg(nc, n7, n30, n60, n90):
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        if b and abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0)
    return min(segs)


def compute_w_gap(cursor, date_str, all_dates):
    """w_gap 계산 (3일 가중 adj_gap)"""
    di = all_dates.index(date_str)
    d0 = all_dates[di]
    d1 = all_dates[di - 1] if di >= 1 else None
    d2 = all_dates[di - 2] if di >= 2 else None

    gaps = {}
    for d in [d0, d1, d2]:
        if d:
            rows = cursor.execute(
                'SELECT ticker, adj_gap FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL', (d,)
            ).fetchall()
            gaps[d] = {r[0]: r[1] for r in rows}

    result = {}
    all_tickers = set()
    for d in [d0, d1, d2]:
        if d and d in gaps:
            all_tickers.update(gaps[d].keys())

    weights = [0.5, 0.3, 0.2]
    for tk in all_tickers:
        wg = 0
        wg += gaps.get(d0, {}).get(tk, 0) * weights[0]
        if d1:
            wg += gaps.get(d1, {}).get(tk, 0) * weights[1]
        if d2:
            wg += gaps.get(d2, {}).get(tk, 0) * weights[2]
        result[tk] = wg

    return result


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    all_dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    print(f'=== 현행 전략 백테스트: wTop3/wTop15 ===')
    print(f'데이터: {len(all_dates)}거래일 ({all_dates[0]} ~ {all_dates[-1]})')
    print(f'3일 검증 시작: {all_dates[2]}')
    print()

    # 각 날짜별 데이터 로드
    daily_data = {}
    for d in all_dates:
        rows = cursor.execute('''
            SELECT ticker, price, part2_rank, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, adj_gap
            FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL
        ''', (d,)).fetchall()
        daily_data[d] = {
            r[0]: {
                'price': r[1], 'part2_rank': r[2],
                'ntm_current': r[3], 'ntm_7d': r[4], 'ntm_30d': r[5],
                'ntm_60d': r[6], 'ntm_90d': r[7], 'adj_gap': r[8]
            } for r in rows
        }

    # 모든 종목 가격 (part2_rank 없는 종목도 포함)
    all_prices = {}
    for d in all_dates:
        rows = cursor.execute(
            'SELECT ticker, price FROM ntm_screening WHERE date=?', (d,)
        ).fetchall()
        all_prices[d] = {r[0]: r[1] for r in rows}

    # 백테스트: 2/12(3일 검증 가능일)부터 시작
    start_idx = 2  # all_dates[2] = 2/12
    portfolio = {}  # {ticker: {'entry_date': str, 'entry_price': float}}
    trade_log = []
    daily_returns = []

    for i in range(start_idx, len(all_dates)):
        date = all_dates[i]
        prev_date = all_dates[i - 1]
        data = daily_data[date]
        prices = all_prices[date]
        prev_prices = all_prices[prev_date]

        # w_gap 계산
        w_gap = compute_w_gap(cursor, date, all_dates)

        # 현재 part2_rank 종목 중 min_seg 계산
        ticker_min_seg = {}
        for tk, info in data.items():
            ms = calc_min_seg(
                info['ntm_current'], info['ntm_7d'], info['ntm_30d'],
                info['ntm_60d'], info['ntm_90d']
            )
            ticker_min_seg[tk] = ms

        # w_gap 기준 순위 (min_seg >= -2% 종목만)
        eligible = [(tk, w_gap.get(tk, 0)) for tk in data.keys() if ticker_min_seg.get(tk, 0) >= -2]
        eligible.sort(key=lambda x: x[1])
        wgap_rank = {tk: rank + 1 for rank, (tk, _) in enumerate(eligible)}

        # 이탈 체크
        exits = []
        for tk in list(portfolio.keys()):
            entry_price = portfolio[tk]['entry_price']
            cur_price = prices.get(tk)
            if cur_price is None:
                exits.append((tk, 'delisted'))
                continue

            rank = wgap_rank.get(tk)
            ms = ticker_min_seg.get(tk, 0)
            ret = (cur_price - entry_price) / entry_price * 100

            if rank is None or rank > 15:
                exits.append((tk, '순위밀림'))
            elif ms < -2:
                exits.append((tk, 'EPS↓'))
            elif ret <= -10:
                exits.append((tk, '손절'))

        for tk, reason in exits:
            cur_price = prices.get(tk, portfolio[tk]['entry_price'])
            ret = (cur_price - portfolio[tk]['entry_price']) / portfolio[tk]['entry_price'] * 100
            trade_log.append({
                'ticker': tk, 'entry_date': portfolio[tk]['entry_date'],
                'exit_date': date, 'entry_price': portfolio[tk]['entry_price'],
                'exit_price': cur_price, 'return': ret, 'reason': reason
            })
            del portfolio[tk]

        # 진입 (빈 슬롯 채우기)
        slots = 3 - len(portfolio)
        if slots > 0:
            # w_gap Top3 중 min_seg >= 0%이고 미보유
            candidates = []
            for tk, wg in eligible[:30]:  # Top30 내에서
                if tk in portfolio:
                    continue
                if wgap_rank.get(tk, 999) > 3:
                    continue
                if ticker_min_seg.get(tk, -999) < 0:
                    continue
                candidates.append(tk)

            for tk in candidates[:slots]:
                cur_price = prices.get(tk)
                if cur_price:
                    portfolio[tk] = {'entry_date': date, 'entry_price': cur_price}

        # 일간 수익률 계산 (보유 종목 평균)
        if portfolio:
            day_ret = 0
            count = 0
            for tk in portfolio:
                cur = prices.get(tk)
                prev = prev_prices.get(tk)
                if cur and prev and prev > 0:
                    day_ret += (cur - prev) / prev * 100
                    count += 1
            if count > 0:
                daily_returns.append(day_ret / count)
            else:
                daily_returns.append(0)
        else:
            daily_returns.append(0)

    conn.close()

    # 결과
    print(f'=== 거래 내역 ===')
    total_ret = 0
    winners = 0
    for t in trade_log:
        status = '✅' if t['return'] > 0 else '❌'
        print(f"  {status} {t['ticker']:6s} {t['entry_date']}→{t['exit_date']} "
              f"{t['entry_price']:.1f}→{t['exit_price']:.1f} {t['return']:+.1f}% [{t['reason']}]")
        total_ret += t['return']
        if t['return'] > 0:
            winners += 1

    # 미청산 포지션
    print(f'\n=== 미청산 포지션 ===')
    last_prices = all_prices[all_dates[-1]]
    unrealized_ret = 0
    for tk, pos in portfolio.items():
        cur = last_prices.get(tk, pos['entry_price'])
        ret = (cur - pos['entry_price']) / pos['entry_price'] * 100
        print(f"  {'✅' if ret > 0 else '❌'} {tk:6s} {pos['entry_date']}→(보유중) "
              f"{pos['entry_price']:.1f}→{cur:.1f} {ret:+.1f}%")
        unrealized_ret += ret

    # 누적 수익률
    cumulative = 0
    for r in daily_returns:
        cumulative += r

    # MDD
    peak = 0
    mdd = 0
    cum = 0
    for r in daily_returns:
        cum += r
        if cum > peak:
            peak = cum
        dd = cum - peak
        if dd < mdd:
            mdd = dd

    # SPY 비교
    print(f'\n=== 요약 ===')
    print(f'기간: {all_dates[start_idx]} ~ {all_dates[-1]} ({len(all_dates) - start_idx}거래일)')
    print(f'완료 거래: {len(trade_log)}건 (승률 {winners}/{len(trade_log)} = {winners/len(trade_log)*100:.0f}%)' if trade_log else '완료 거래: 0건')
    print(f'미청산: {len(portfolio)}종목')
    print(f'실현 수익: {total_ret:+.1f}% (평균 {total_ret/len(trade_log):+.1f}%)' if trade_log else '실현 수익: 0')
    print(f'미실현 수익: {unrealized_ret:+.1f}%')
    print(f'포트폴리오 누적: {cumulative:+.1f}%')
    print(f'MDD: {mdd:.1f}%')

    # 일별 보유 종목 출력
    print(f'\n=== 일별 보유 현황 ===')
    portfolio2 = {}
    for i in range(start_idx, len(all_dates)):
        date = all_dates[i]
        data = daily_data[date]
        prices = all_prices[date]
        w_gap = compute_w_gap(cursor if not conn else sqlite3.connect(DB_PATH).cursor(), date, all_dates)

        ticker_min_seg2 = {}
        for tk, info in data.items():
            ms = calc_min_seg(info['ntm_current'], info['ntm_7d'], info['ntm_30d'], info['ntm_60d'], info['ntm_90d'])
            ticker_min_seg2[tk] = ms

        eligible2 = [(tk, w_gap.get(tk, 0)) for tk in data.keys() if ticker_min_seg2.get(tk, 0) >= -2]
        eligible2.sort(key=lambda x: x[1])
        wgap_rank2 = {tk: rank + 1 for rank, (tk, _) in enumerate(eligible2)}

        # 이탈
        for tk in list(portfolio2.keys()):
            cur_price = prices.get(tk)
            if cur_price is None:
                del portfolio2[tk]; continue
            rank = wgap_rank2.get(tk)
            ms = ticker_min_seg2.get(tk, 0)
            ret = (cur_price - portfolio2[tk]['entry_price']) / portfolio2[tk]['entry_price'] * 100
            if (rank is None or rank > 15) or ms < -2 or ret <= -10:
                del portfolio2[tk]

        # 진입
        slots = 3 - len(portfolio2)
        if slots > 0:
            for tk, wg in eligible2[:30]:
                if tk in portfolio2: continue
                if wgap_rank2.get(tk, 999) > 3: continue
                if ticker_min_seg2.get(tk, -999) < 0: continue
                cur_price = prices.get(tk)
                if cur_price:
                    portfolio2[tk] = {'entry_date': date, 'entry_price': cur_price}
                    slots -= 1
                    if slots <= 0: break

        holdings = []
        for tk, pos in portfolio2.items():
            cur = prices.get(tk, pos['entry_price'])
            ret = (cur - pos['entry_price']) / pos['entry_price'] * 100
            holdings.append(f'{tk}({ret:+.1f}%)')
        print(f'  {date}: {", ".join(holdings) if holdings else "(없음)"}')


if __name__ == '__main__':
    main()
