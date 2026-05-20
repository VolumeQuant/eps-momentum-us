"""시스템 시작 ~ 5/19 동안 실제 진입한 종목들 buy-the-dip 분석

진입 시점에서 price < MA60 (= MA120 위 + MA60 아래 = buy-the-dip)인지 분류 후
보유 기간 수익률 측정.

가설: buy-the-dip 케이스가 alpha 만들었나?
"""
import sys
import sqlite3
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'


def load_data():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    # 종목별 모든 데이터
    daily = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price, ma60, ma120,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=?
        ''', (d,)).fetchall()
        daily[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else:
                    segs.append(0)
            daily[d][tk] = {
                'p2': r[1], 'price': r[2], 'ma60': r[3], 'ma120': r[4],
                'min_seg': min(segs) if segs else 0,
            }
    conn.close()
    return dates, daily


def simulate_with_logging(dates, daily,
                          entry=3, exit_=10, slots=3):
    """실제 production rule replay + 진입/이탈 로깅"""
    portfolio = {}
    consecutive = defaultdict(int)
    trades = []  # 매도 시 기록: (ticker, entry_date, entry_price, exit_date, exit_price, ret, days_held, dip_flag)

    for di, today in enumerate(dates):
        if today not in daily:
            continue
        today_data = daily[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consec = defaultdict(int)
        for tk in rank_map:
            new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        # 이탈 처리
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            row = today_data.get(tk, {})
            min_seg = row.get('min_seg', 0)
            price = row.get('price')
            if min_seg < -2 or rank is None or rank > exit_:
                exit_price = price
                if exit_price is None:
                    # price_full fallback (composite_rank IS NULL)
                    conn = sqlite3.connect(DB_PATH)
                    r = conn.cursor().execute(
                        'SELECT price FROM ntm_screening WHERE date=? AND ticker=?',
                        (today, tk)
                    ).fetchone()
                    conn.close()
                    if r and r[0]:
                        exit_price = r[0]
                if exit_price:
                    info = portfolio[tk]
                    ret = (exit_price - info['entry_price']) / info['entry_price'] * 100
                    days = di - info['entry_di']
                    trades.append({
                        'ticker': tk,
                        'entry_date': info['entry_date'],
                        'entry_price': info['entry_price'],
                        'entry_ma60': info['ma60'],
                        'entry_ma120': info['ma120'],
                        'entry_p2': info['entry_p2'],
                        'exit_date': today,
                        'exit_price': exit_price,
                        'exit_reason': 'min_seg' if min_seg < -2 else ('rank_NULL' if rank is None else f'rank>{exit_}'),
                        'ret': ret,
                        'days_held': days,
                        'dip_flag': info['dip_flag'],
                    })
                exited.append(tk)
        for tk in exited:
            del portfolio[tk]

        # 진입
        vacancies = slots - len(portfolio)
        if vacancies > 0:
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > entry or vacancies <= 0:
                    break
                if tk in portfolio:
                    continue
                if consecutive.get(tk, 0) < 3:
                    continue
                row = today_data.get(tk, {})
                min_seg = row.get('min_seg', 0)
                if min_seg < 0:
                    continue
                price = row.get('price')
                m60 = row.get('ma60')
                m120 = row.get('ma120')
                if price and price > 0:
                    # dip flag: price < MA60 (buy-the-dip)
                    dip = bool(m60 and price < m60)
                    portfolio[tk] = {
                        'entry_price': price,
                        'entry_date': today,
                        'entry_di': di,
                        'ma60': m60,
                        'ma120': m120,
                        'entry_p2': rank,
                        'dip_flag': dip,
                    }
                    vacancies -= 1

    # 보유 중 (아직 매도 안 한) 종목도 기록 (paper open position)
    final_d = dates[-1]
    for tk, info in portfolio.items():
        final_price = daily[final_d].get(tk, {}).get('price')
        if final_price:
            ret = (final_price - info['entry_price']) / info['entry_price'] * 100
            trades.append({
                'ticker': tk,
                'entry_date': info['entry_date'],
                'entry_price': info['entry_price'],
                'entry_ma60': info['ma60'],
                'entry_ma120': info['ma120'],
                'entry_p2': info['entry_p2'],
                'exit_date': final_d + ' (open)',
                'exit_price': final_price,
                'exit_reason': 'OPEN',
                'ret': ret,
                'days_held': len(dates) - info['entry_di'] - 1,
                'dip_flag': info['dip_flag'],
            })
    return trades


def main():
    dates, daily = load_data()
    print('=' * 100)
    print(f'시스템 진입 종목 분석 — {dates[0]} ~ {dates[-1]} ({len(dates)} 거래일)')
    print('=' * 100)

    trades = simulate_with_logging(dates, daily)

    # 분류
    dips = [t for t in trades if t['dip_flag']]
    non_dips = [t for t in trades if not t['dip_flag']]

    print(f'\n총 진입 케이스: {len(trades)}건')
    print(f'  buy-the-dip (price < MA60 at entry): {len(dips)}건')
    print(f'  normal (price ≥ MA60 at entry):     {len(non_dips)}건')

    def stats(group, label):
        if not group:
            print(f'  {label}: (없음)')
            return
        rets = [t['ret'] for t in group]
        days = [t['days_held'] for t in group]
        wins = sum(1 for r in rets if r > 0)
        avg_ret = sum(rets)/len(rets)
        max_ret = max(rets)
        min_ret = min(rets)
        avg_days = sum(days)/len(days)
        print(f'  {label}: 평균 {avg_ret:+.2f}%, win {wins}/{len(group)} ({wins/len(group)*100:.0f}%), '
              f'max {max_ret:+.1f}%, min {min_ret:+.1f}%, 평균 보유 {avg_days:.1f}일')

    print(f'\n=== 수익률 통계 ===')
    stats(dips, 'buy-the-dip')
    stats(non_dips, 'normal      ')
    stats(trades, 'all         ')

    # buy-the-dip 상세
    print(f'\n=== buy-the-dip 케이스 상세 ===')
    print(f'{"ticker":<8} {"entry":<12} {"exit":<22} {"p2":>3} {"price":>9} {"MA60":>9} {"dip%":>7} {"ret":>9} {"days":>5} {"reason":>12}')
    for t in sorted(dips, key=lambda x: -x['ret']):
        dip_pct = (t['entry_price']/t['entry_ma60'] - 1) * 100 if t['entry_ma60'] else 0
        marker = '★' if t['ret'] > 20 else ' '
        print(f' {marker}{t["ticker"]:<7} {t["entry_date"]:<12} {t["exit_date"]:<22} '
              f'{t["entry_p2"]:>3} {t["entry_price"]:>8.2f} {t["entry_ma60"] or 0:>8.2f} '
              f'{dip_pct:+6.1f}% {t["ret"]:+8.2f}% {t["days_held"]:>4} {t["exit_reason"]:>12}')

    # normal Top 10 by ret
    print(f'\n=== normal 케이스 Top 10 (수익률 순) ===')
    print(f'{"ticker":<8} {"entry":<12} {"exit":<22} {"p2":>3} {"price":>9} {"MA60":>9} {"diff%":>7} {"ret":>9} {"days":>5} {"reason":>12}')
    for t in sorted(non_dips, key=lambda x: -x['ret'])[:10]:
        diff = (t['entry_price']/t['entry_ma60'] - 1) * 100 if t['entry_ma60'] else 0
        marker = '★' if t['ret'] > 20 else ' '
        print(f' {marker}{t["ticker"]:<7} {t["entry_date"]:<12} {t["exit_date"]:<22} '
              f'{t["entry_p2"]:>3} {t["entry_price"]:>8.2f} {t["entry_ma60"] or 0:>8.2f} '
              f'{diff:+6.1f}% {t["ret"]:+8.2f}% {t["days_held"]:>4} {t["exit_reason"]:>12}')


if __name__ == '__main__':
    main()
