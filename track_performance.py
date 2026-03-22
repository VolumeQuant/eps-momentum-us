"""실전 성과 추적기 — DB 3일 검증 시작일부터 복리 누적 수익률

DB에 저장된 전체 데이터(2/12~)를 사용하여:
1. 매일 시스템이 추천한 Top3 종목 + 역변동성 비중 결정
2. 일간 수익률을 복리로 누적 (재투자 가정)
3. SPY 대비 초과 수익 추적

Usage:
    python track_performance.py          # 전체 기간 리포트
    python track_performance.py detail   # 일별 상세 (종목명 포함)
"""
import math
import sqlite3
import sys
from pathlib import Path

import yfinance as yf

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = Path(__file__).parent / 'eps_momentum_data.db'


def calc_min_seg(nc, n7, n30, n60, n90):
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        if b and abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0)
    return min(segs)


def compute_w_gap(cursor, date_str, all_dates):
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
        wg = gaps.get(d0, {}).get(tk, 0) * weights[0]
        if d1:
            wg += gaps.get(d1, {}).get(tk, 0) * weights[1]
        if d2:
            wg += gaps.get(d2, {}).get(tk, 0) * weights[2]
        result[tk] = wg
    return result


def run_tracker():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    all_dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    # 일별 데이터 로드
    daily_data = {}
    for d in all_dates:
        rows = cursor.execute('''
            SELECT ticker, price, part2_rank, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL
        ''', (d,)).fetchall()
        daily_data[d] = {
            r[0]: {'price': r[1], 'part2_rank': r[2],
                   'nc': r[3], 'n7': r[4], 'n30': r[5], 'n60': r[6], 'n90': r[7]}
            for r in rows
        }

    # 전체 가격
    all_prices = {}
    for d in all_dates:
        rows = cursor.execute(
            'SELECT ticker, price FROM ntm_screening WHERE date=?', (d,)
        ).fetchall()
        all_prices[d] = {r[0]: r[1] for r in rows}

    # SPY 가격 로드
    spy_data = yf.download('SPY', start=all_dates[0], end=all_dates[-1],
                           auto_adjust=True, progress=False)
    spy_prices = {}
    for idx, row in spy_data.iterrows():
        ds = idx.strftime('%Y-%m-%d')
        spy_prices[ds] = float(row['Close']) if 'Close' in row.index else float(row.iloc[3])

    # 백테스트 시작 (3일 검증 시작일 = index 2)
    start_idx = 2
    portfolio = {}  # {ticker: {'entry_date', 'entry_price'}}
    trade_log = []
    daily_results = []  # [{date, tickers, weights, sys_ret, spy_ret, sys_nav, spy_nav}]

    sys_nav = 1.0  # 시스템 NAV (1.0 = 100%)
    spy_nav = 1.0  # SPY NAV

    for i in range(start_idx, len(all_dates)):
        date = all_dates[i]
        prev_date = all_dates[i - 1]
        data = daily_data[date]
        prices = all_prices[date]
        prev_prices = all_prices[prev_date]

        w_gap = compute_w_gap(cursor, date, all_dates)

        # min_seg 계산
        ticker_min_seg = {}
        for tk, info in data.items():
            ms = calc_min_seg(info['nc'], info['n7'], info['n30'], info['n60'], info['n90'])
            ticker_min_seg[tk] = ms

        # w_gap 순위 (min_seg >= -2% 종목만)
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
                'exit_date': date, 'return': ret, 'reason': reason
            })
            del portfolio[tk]

        # 진입
        slots = 3 - len(portfolio)
        if slots > 0:
            candidates = []
            for tk, wg in eligible[:30]:
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

        # 역변동성 비중 계산
        inv_vol_weights = {}
        if portfolio:
            vols = {}
            for tk in portfolio:
                # 최근 5일 변동성 계산 (DB 가격 사용)
                past_prices = []
                for j in range(max(0, i - 5), i + 1):
                    p = all_prices.get(all_dates[j], {}).get(tk)
                    if p:
                        past_prices.append(p)
                if len(past_prices) >= 3:
                    rets = [(past_prices[k] - past_prices[k-1]) / past_prices[k-1]
                            for k in range(1, len(past_prices))]
                    if len(rets) >= 2:
                        mean = sum(rets) / len(rets)
                        var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
                        vol = math.sqrt(var) if var > 0 else 0.01
                    else:
                        vol = 0.01
                else:
                    vol = 0.01
                vols[tk] = max(vol, 0.001)

            inv_vols = {tk: 1.0 / v for tk, v in vols.items()}
            total_inv = sum(inv_vols.values())
            inv_vol_weights = {tk: iv / total_inv for tk, iv in inv_vols.items()}

        # 일간 수익률 (역변동성 가중)
        day_ret = 0
        if portfolio and inv_vol_weights:
            for tk, w in inv_vol_weights.items():
                cur = prices.get(tk)
                prev = prev_prices.get(tk)
                if cur and prev and prev > 0:
                    day_ret += w * (cur - prev) / prev * 100

        # SPY 수익률
        spy_cur = spy_prices.get(date)
        spy_prev = spy_prices.get(prev_date)
        spy_ret = 0
        if spy_cur and spy_prev and spy_prev > 0:
            spy_ret = (spy_cur - spy_prev) / spy_prev * 100

        # 복리 NAV 업데이트
        sys_nav *= (1 + day_ret / 100)
        spy_nav *= (1 + spy_ret / 100)

        daily_results.append({
            'date': date,
            'tickers': list(portfolio.keys()),
            'weights': {tk: inv_vol_weights.get(tk, 0) * 100 for tk in portfolio},
            'sys_ret': day_ret,
            'spy_ret': spy_ret,
            'sys_nav': sys_nav,
            'spy_nav': spy_nav,
            'sys_cum': (sys_nav - 1) * 100,
            'spy_cum': (spy_nav - 1) * 100,
        })

    conn.close()
    return daily_results, trade_log, all_dates[start_idx:]


def print_report(daily_results, trade_log, detail=False):
    if not daily_results:
        print('데이터 없음')
        return

    first = daily_results[0]
    last = daily_results[-1]
    n_days = len(daily_results)

    print()
    print('=' * 90)
    print('  EPS Momentum 시스템 실전 성과 추적 (복리 재투자 가정)')
    print('=' * 90)
    print(f'  기간: {first["date"]} ~ {last["date"]} ({n_days}거래일)')
    print()

    # 일별 테이블
    if detail:
        print(f'  {"날짜":<12} {"시스템":>7} {"SPY":>7} {"시스템누적":>9} {"SPY누적":>8} {"초과":>7}  종목(비중)')
        print(f'  {"-"*85}')
        for r in daily_results:
            alpha = r['sys_cum'] - r['spy_cum']
            tickers_str = ' '.join(f'{tk}({r["weights"].get(tk, 0):.0f}%)' for tk in r['tickers'])
            if not tickers_str:
                tickers_str = '(빈 포트폴리오)'
            print(f'  {r["date"]:<12} {r["sys_ret"]:>+6.2f}% {r["spy_ret"]:>+6.2f}% '
                  f'{r["sys_cum"]:>+8.2f}% {r["spy_cum"]:>+7.2f}% {alpha:>+6.2f}%  {tickers_str}')
        print()
    else:
        # 주간 요약
        print(f'  {"날짜":<12} {"시스템일간":>9} {"SPY일간":>8} {"시스템누적":>9} {"SPY누적":>8} {"초과수익":>8}')
        print(f'  {"-"*60}')
        for r in daily_results:
            alpha = r['sys_cum'] - r['spy_cum']
            print(f'  {r["date"]:<12} {r["sys_ret"]:>+8.2f}% {r["spy_ret"]:>+7.2f}% '
                  f'{r["sys_cum"]:>+8.2f}% {r["spy_cum"]:>+7.2f}% {alpha:>+7.2f}%p')

    # 요약 통계
    sys_cum = last['sys_cum']
    spy_cum = last['spy_cum']
    alpha = sys_cum - spy_cum

    print()
    print('=' * 60)
    print('  종합 성과')
    print('=' * 60)
    print(f'  시스템 누적 수익률:   {sys_cum:>+.2f}%')
    print(f'  SPY 누적 수익률:      {spy_cum:>+.2f}%')
    print(f'  초과 수익 (알파):     {alpha:>+.2f}%p')
    print()

    # CAGR
    sys_cagr = (last['sys_nav'] ** (252 / n_days) - 1) * 100
    spy_cagr = (last['spy_nav'] ** (252 / n_days) - 1) * 100
    print(f'  시스템 CAGR (연환산): {sys_cagr:>+.1f}%')
    print(f'  SPY CAGR (연환산):    {spy_cagr:>+.1f}%')
    print()

    # MDD
    sys_peak = 1.0
    sys_mdd = 0
    spy_peak = 1.0
    spy_mdd = 0
    for r in daily_results:
        if r['sys_nav'] > sys_peak:
            sys_peak = r['sys_nav']
        dd = (r['sys_nav'] - sys_peak) / sys_peak * 100
        if dd < sys_mdd:
            sys_mdd = dd

        if r['spy_nav'] > spy_peak:
            spy_peak = r['spy_nav']
        dd2 = (r['spy_nav'] - spy_peak) / spy_peak * 100
        if dd2 < spy_mdd:
            spy_mdd = dd2

    print(f'  시스템 MDD:           {sys_mdd:.2f}%')
    print(f'  SPY MDD:              {spy_mdd:.2f}%')
    print()

    # 승률
    sys_wins = sum(1 for r in daily_results if r['sys_ret'] > 0)
    spy_wins = sum(1 for r in daily_results if r['spy_ret'] > 0)
    outperform = sum(1 for r in daily_results if r['sys_ret'] > r['spy_ret'])
    print(f'  시스템 일간 승률:     {sys_wins}/{n_days} ({sys_wins/n_days*100:.0f}%)')
    print(f'  SPY 일간 승률:        {spy_wins}/{n_days} ({spy_wins/n_days*100:.0f}%)')
    print(f'  SPY 초과 일수:        {outperform}/{n_days} ({outperform/n_days*100:.0f}%)')

    # 4억 시뮬레이션
    print()
    print('=' * 60)
    print('  4억 투자 시뮬레이션 (복리 재투자)')
    print('=' * 60)
    initial = 40000  # 만원 단위
    sys_final = initial * last['sys_nav']
    spy_final = initial * last['spy_nav']
    print(f'  시작 자산:            4억 원')
    print(f'  시스템 ({n_days}일 후):  {sys_final/10000:.1f}억 원 ({sys_cum:+.2f}%)')
    print(f'  SPY ({n_days}일 후):     {spy_final/10000:.1f}억 원 ({spy_cum:+.2f}%)')
    print()

    # 연환산 기준 7년 예측
    if sys_cagr > 0:
        proj_7y = 4 * ((1 + sys_cagr / 100) ** 7)
        print(f'  현재 CAGR({sys_cagr:+.1f}%) 유지 시 7년 후: {proj_7y:.1f}억 원')
    print('=' * 60)

    # 거래 내역
    if trade_log:
        print()
        print(f'  완료 거래 {len(trade_log)}건:')
        for t in trade_log:
            icon = '✅' if t['return'] > 0 else '❌'
            print(f'    {icon} {t["ticker"]:6s} {t["entry_date"]}→{t["exit_date"]} '
                  f'{t["return"]:+.1f}% [{t["reason"]}]')


def main():
    detail = len(sys.argv) > 1 and sys.argv[1].lower() == 'detail'

    print('데이터 로딩...')
    daily_results, trade_log, dates = run_tracker()
    print_report(daily_results, trade_log, detail=detail)


if __name__ == '__main__':
    main()
