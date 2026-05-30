"""ma60_only vs current(MA120+MA60 fallback) trade-level diff

목적:
  paired BT 52/48 wins, avg -1.81%p, median +12.49%p — 통계적으로 동등.
  어디서 차이가 나는지 trade/일자/종목 단위로 분해.

분석 차원:
  A. 매수 후보 pool 크기 차이 (날짜별 eligible 종목 수 / part2_rank 종목 수)
  B. part2_rank Top 30 set diff (current에만 / ma60_only에만 있는 종목)
  C. Trade-level diff (single start_date 기준, 동일 시작일로 portfolio trajectory 비교)
  D. 종목별 entry/exit, hold, 수익률, 차이 기여
  E. 일자별 portfolio composition + daily return divergence
  F. MDD 발생 구간 비교

DB: research/ma_filter_dbs/current.db, ma60_only.db (이미 regenerate 완료)
"""
import sys
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
DB_CUR = ROOT / 'research' / 'ma_filter_dbs' / 'current.db'
DB_M60 = ROOT / 'research' / 'ma_filter_dbs' / 'ma60_only.db'

ENTRY_TOP = 3
EXIT_TOP = 10
MAX_SLOTS = 2  # v83.3


def load_db(db_path):
    """date → {ticker → row}"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening '
        'WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    data = {}
    price_full = defaultdict(dict)
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price, composite_rank,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   ma60, ma120
            FROM ntm_screening WHERE date=?
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else:
                    segs.append(0)
            data[d][tk] = {
                'p2': r[1], 'price': r[2], 'cr': r[3],
                'ma60': r[9], 'ma120': r[10],
                'min_seg': min(segs) if segs else 0,
            }
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'
    ):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def simulate(dates_all, data, price_full, start_date=None,
             entry=3, exit_=10, slots=2):
    """trade-level info 풍부히 기록"""
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    daily_returns = []
    daily_portfolio = []  # 일자별 보유 종목
    trades = []
    consecutive = defaultdict(int)
    if start_date:
        for d in dates_all:
            if d >= start_date:
                break
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    consecutive[tk] = consecutive.get(tk, 0) + 1

    for di, today in enumerate(dates):
        if today not in data:
            continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consec = defaultdict(int)
        for tk in rank_map:
            new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di-1]
            n = 0
            for tk in portfolio:
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                pr = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if p and pr and pr > 0:
                    day_ret += (p - pr) / pr * 100
                    n += 1
            if n > 0:
                day_ret /= n
        daily_returns.append(day_ret)
        daily_portfolio.append((today, list(portfolio.keys())))

        # 이탈
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            price = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            reason = None
            if min_seg < -2:
                reason = 'min_seg'
            elif rank is None or rank > exit_:
                reason = 'rank_exit'
            if reason and price:
                ep = portfolio[tk]['entry_price']
                ret = (price - ep) / ep * 100
                trades.append({
                    'ticker': tk, 'entry_date': portfolio[tk]['entry_date'],
                    'exit_date': today, 'entry_price': ep, 'exit_price': price,
                    'return': ret, 'reason': reason,
                    'hold_days': di - portfolio[tk]['entry_di'],
                })
                exited.append(tk)
            elif reason:
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
                min_seg = today_data.get(tk, {}).get('min_seg', 0)
                if min_seg < 0:
                    continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0:
                    portfolio[tk] = {
                        'entry_price': price, 'entry_date': today, 'entry_di': di,
                    }
                    vacancies -= 1

    # 마감 시 잔존 portfolio 정산
    if portfolio and dates:
        last = dates[-1]
        for tk, info in portfolio.items():
            price = data.get(last, {}).get(tk, {}).get('price') or price_full.get(last, {}).get(tk)
            if price:
                ep = info['entry_price']
                ret = (price - ep) / ep * 100
                trades.append({
                    'ticker': tk, 'entry_date': info['entry_date'],
                    'exit_date': last, 'entry_price': ep, 'exit_price': price,
                    'return': ret, 'reason': 'open',
                    'hold_days': len(dates) - 1 - info['entry_di'],
                })

    cum = 1.0; peak = 1.0; max_dd = 0; mdd_di = 0; peak_di = 0
    for i, r in enumerate(daily_returns):
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100
        if dd < max_dd:
            max_dd = dd
            mdd_di = i
        if cum == peak:
            peak_di = i
    return {
        'total_return': (cum-1)*100, 'max_dd': max_dd,
        'mdd_date': dates[mdd_di] if dates else None,
        'peak_date': dates[peak_di] if dates else None,
        'trades': trades,
        'daily_returns': daily_returns,
        'daily_portfolio': daily_portfolio,
        'dates': dates,
    }


def fmt_pct(x):
    return f'{x:+.2f}%'


def section(title):
    print()
    print('=' * 100)
    print(title)
    print('=' * 100)


def main():
    print('=' * 100)
    print('MA filter diff: current (MA120+MA60 fallback) vs ma60_only')
    print(f'params: entry={ENTRY_TOP}, exit={EXIT_TOP}, slots={MAX_SLOTS}')
    print('=' * 100)

    dates_c, data_c, px_c = load_db(DB_CUR)
    dates_m, data_m, px_m = load_db(DB_M60)
    assert dates_c == dates_m, 'date sets differ'
    DATES = dates_c
    print(f'\n거래일: {len(DATES)} ({DATES[0]} ~ {DATES[-1]})')

    # ========== A. Pool size per date ==========
    section('A. 날짜별 part2_rank 종목 수 (Top 30 진입 수)')
    pool_diff = []
    only_c_dates = []
    only_m_dates = []
    for d in DATES:
        nc = sum(1 for v in data_c[d].values() if v.get('p2') is not None)
        nm = sum(1 for v in data_m[d].values() if v.get('p2') is not None)
        pool_diff.append((d, nc, nm, nc - nm))
        # part2_rank 종목 set
        sc = {t for t, v in data_c[d].items() if v.get('p2') is not None}
        sm = {t for t, v in data_m[d].items() if v.get('p2') is not None}
        only_c = sc - sm
        only_m = sm - sc
        if only_c: only_c_dates.append((d, only_c))
        if only_m: only_m_dates.append((d, only_m))

    avg_c = sum(x[1] for x in pool_diff) / len(pool_diff)
    avg_m = sum(x[2] for x in pool_diff) / len(pool_diff)
    print(f'  평균 Top 30 진입 수 — current: {avg_c:.1f}, ma60_only: {avg_m:.1f}')
    print(f'  current에만 있는 종목 발생 일자: {len(only_c_dates)}/{len(DATES)}')
    print(f'  ma60_only에만 있는 종목 발생 일자: {len(only_m_dates)}/{len(DATES)}')

    # ========== B. set diff 종목 모음 ==========
    section('B. part2_rank Top 30 set diff — 누적 unique 종목')
    only_c_tickers = defaultdict(int)
    only_m_tickers = defaultdict(int)
    for _, s in only_c_dates:
        for t in s: only_c_tickers[t] += 1
    for _, s in only_m_dates:
        for t in s: only_m_tickers[t] += 1
    print(f'  current에만 (총 {len(only_c_tickers)} 종목, 상위 15):')
    for t, n in sorted(only_c_tickers.items(), key=lambda x: -x[1])[:15]:
        print(f'    {t}: {n}일')
    print(f'  ma60_only에만 (총 {len(only_m_tickers)} 종목, 상위 15):')
    for t, n in sorted(only_m_tickers.items(), key=lambda x: -x[1])[:15]:
        print(f'    {t}: {n}일')

    # ========== C. Multi-start trade-level diff ==========
    section('C. Multi-start (전체 dates 시작일 풀) trade-level diff')

    # 첫 시작일 + 1/4 + 1/2 + 3/4 + 최근 4 starts
    sample_starts = [
        DATES[0], DATES[len(DATES)//4], DATES[len(DATES)//2],
        DATES[3*len(DATES)//4],
    ]
    all_diffs = []
    for sd in sample_starts:
        rc = simulate(DATES, data_c, px_c, start_date=sd)
        rm = simulate(DATES, data_m, px_m, start_date=sd)
        lift = rm['total_return'] - rc['total_return']
        all_diffs.append({
            'start': sd, 'rc': rc, 'rm': rm, 'lift': lift,
        })
        print(f'  start={sd}: current {fmt_pct(rc["total_return"])} '
              f'mdd {rc["max_dd"]:+.2f}% | '
              f'ma60_only {fmt_pct(rm["total_return"])} mdd {rm["max_dd"]:+.2f}% '
              f'| lift {lift:+.2f}%p')

    # 가장 큰 차이 vs 가장 작은 차이 start 선택
    sorted_d = sorted(all_diffs, key=lambda x: x['lift'])
    pick_worst = sorted_d[0]  # ma60_only가 가장 진 시작일
    pick_best = sorted_d[-1]  # ma60_only가 가장 이긴 시작일

    # ========== D. ma60_only 우월 케이스 (worst-for-current) ==========
    section(f'D. ma60_only가 가장 우월한 시작일 ({pick_best["start"]}) — trade diff')
    rc, rm = pick_best['rc'], pick_best['rm']
    print(f'  current   total: {fmt_pct(rc["total_return"])} | mdd {rc["max_dd"]:+.2f}% '
          f'@ {rc["mdd_date"]} | trades {len(rc["trades"])}')
    print(f'  ma60_only total: {fmt_pct(rm["total_return"])} | mdd {rm["max_dd"]:+.2f}% '
          f'@ {rm["mdd_date"]} | trades {len(rm["trades"])}')
    tc_set = {(t['ticker'], t['entry_date']) for t in rc['trades']}
    tm_set = {(t['ticker'], t['entry_date']) for t in rm['trades']}
    only_c = [t for t in rc['trades'] if (t['ticker'], t['entry_date']) not in tm_set]
    only_m = [t for t in rm['trades'] if (t['ticker'], t['entry_date']) not in tc_set]
    common = [t for t in rc['trades'] if (t['ticker'], t['entry_date']) in tm_set]
    print(f'\n  공통 trade: {len(common)}, current만: {len(only_c)}, ma60_only만: {len(only_m)}')
    print(f'\n  current에만 있는 trade ({len(only_c)}, sorted by return):')
    print(f'  {"ticker":<8} {"entry":<10} {"exit":<10} {"hold":>5} {"return":>9} {"reason":<12}')
    for t in sorted(only_c, key=lambda x: x['return']):
        print(f'  {t["ticker"]:<8} {t["entry_date"]:<10} {t["exit_date"]:<10} '
              f'{t["hold_days"]:>5} {t["return"]:+8.2f}%  {t["reason"]:<12}')
    print(f'\n  ma60_only에만 있는 trade ({len(only_m)}, sorted by return):')
    print(f'  {"ticker":<8} {"entry":<10} {"exit":<10} {"hold":>5} {"return":>9} {"reason":<12}')
    for t in sorted(only_m, key=lambda x: x['return']):
        print(f'  {t["ticker"]:<8} {t["entry_date"]:<10} {t["exit_date"]:<10} '
              f'{t["hold_days"]:>5} {t["return"]:+8.2f}%  {t["reason"]:<12}')
    if only_c:
        print(f'\n  current-only 평균 return: {sum(t["return"] for t in only_c)/len(only_c):+.2f}%')
    if only_m:
        print(f'  ma60_only-only 평균 return: {sum(t["return"] for t in only_m)/len(only_m):+.2f}%')

    # ========== E. current 우월 케이스 (worst-for-ma60_only) ==========
    section(f'E. current가 가장 우월한 시작일 ({pick_worst["start"]}) — trade diff')
    rc, rm = pick_worst['rc'], pick_worst['rm']
    print(f'  current   total: {fmt_pct(rc["total_return"])} | mdd {rc["max_dd"]:+.2f}% '
          f'@ {rc["mdd_date"]} | trades {len(rc["trades"])}')
    print(f'  ma60_only total: {fmt_pct(rm["total_return"])} | mdd {rm["max_dd"]:+.2f}% '
          f'@ {rm["mdd_date"]} | trades {len(rm["trades"])}')
    tc_set = {(t['ticker'], t['entry_date']) for t in rc['trades']}
    tm_set = {(t['ticker'], t['entry_date']) for t in rm['trades']}
    only_c = [t for t in rc['trades'] if (t['ticker'], t['entry_date']) not in tm_set]
    only_m = [t for t in rm['trades'] if (t['ticker'], t['entry_date']) not in tc_set]
    print(f'\n  current에만 있는 trade ({len(only_c)}):')
    print(f'  {"ticker":<8} {"entry":<10} {"exit":<10} {"hold":>5} {"return":>9}')
    for t in sorted(only_c, key=lambda x: x['return']):
        print(f'  {t["ticker"]:<8} {t["entry_date"]:<10} {t["exit_date"]:<10} '
              f'{t["hold_days"]:>5} {t["return"]:+8.2f}%')
    print(f'\n  ma60_only에만 있는 trade ({len(only_m)}):')
    print(f'  {"ticker":<8} {"entry":<10} {"exit":<10} {"hold":>5} {"return":>9}')
    for t in sorted(only_m, key=lambda x: x['return']):
        print(f'  {t["ticker"]:<8} {t["entry_date"]:<10} {t["exit_date"]:<10} '
              f'{t["hold_days"]:>5} {t["return"]:+8.2f}%')
    if only_c:
        print(f'\n  current-only 평균 return: {sum(t["return"] for t in only_c)/len(only_c):+.2f}%')
    if only_m:
        print(f'  ma60_only-only 평균 return: {sum(t["return"] for t in only_m)/len(only_m):+.2f}%')

    # ========== F. 종목별 누적 기여 (전체 시작일 평균) ==========
    section('F. 전체 시작일 (5개 샘플) 종합 — 종목별 누적 평균 기여')
    contrib_c = defaultdict(list)
    contrib_m = defaultdict(list)
    for d in all_diffs:
        for t in d['rc']['trades']:
            contrib_c[t['ticker']].append(t['return'])
        for t in d['rm']['trades']:
            contrib_m[t['ticker']].append(t['return'])
    tickers_all = set(contrib_c) | set(contrib_m)
    diff_by_ticker = []
    for tk in tickers_all:
        ac = sum(contrib_c.get(tk, [])) / max(1, len(all_diffs))
        am = sum(contrib_m.get(tk, [])) / max(1, len(all_diffs))
        diff_by_ticker.append((tk, ac, am, am - ac,
                              len(contrib_c.get(tk, [])), len(contrib_m.get(tk, []))))
    print(f'  ma60_only 우월 종목 (top 10):')
    print(f'  {"ticker":<8} {"cur sum":>8} {"m60 sum":>8} {"diff":>8} {"#c":>3} {"#m":>3}')
    for row in sorted(diff_by_ticker, key=lambda x: -x[3])[:10]:
        print(f'  {row[0]:<8} {row[1]:+7.2f}% {row[2]:+7.2f}% {row[3]:+7.2f}%p {row[4]:>3} {row[5]:>3}')
    print(f'\n  current 우월 종목 (top 10):')
    for row in sorted(diff_by_ticker, key=lambda x: x[3])[:10]:
        print(f'  {row[0]:<8} {row[1]:+7.2f}% {row[2]:+7.2f}% {row[3]:+7.2f}%p {row[4]:>3} {row[5]:>3}')

    # ========== G. Trade stats overall ==========
    section('G. Trade 통계 (5개 샘플 합산)')
    all_c = [t for d in all_diffs for t in d['rc']['trades']]
    all_m = [t for d in all_diffs for t in d['rm']['trades']]
    def stats(lst):
        if not lst: return None
        rets = [t['return'] for t in lst]
        holds = [t['hold_days'] for t in lst]
        wins = sum(1 for r in rets if r > 0)
        return {
            'n': len(lst),
            'avg_ret': statistics.mean(rets),
            'med_ret': statistics.median(rets),
            'win_rate': wins / len(rets) * 100,
            'avg_hold': statistics.mean(holds),
            'avg_win': statistics.mean([r for r in rets if r > 0]) if wins else 0,
            'avg_loss': statistics.mean([r for r in rets if r <= 0]) if (len(rets)-wins) else 0,
            'max_win': max(rets),
            'max_loss': min(rets),
        }
    sc = stats(all_c)
    sm = stats(all_m)
    if sc and sm:
        rows = [
            ('n trades', sc['n'], sm['n']),
            ('avg return', f'{sc["avg_ret"]:+.2f}%', f'{sm["avg_ret"]:+.2f}%'),
            ('median return', f'{sc["med_ret"]:+.2f}%', f'{sm["med_ret"]:+.2f}%'),
            ('win rate', f'{sc["win_rate"]:.1f}%', f'{sm["win_rate"]:.1f}%'),
            ('avg hold (days)', f'{sc["avg_hold"]:.1f}', f'{sm["avg_hold"]:.1f}'),
            ('avg win', f'{sc["avg_win"]:+.2f}%', f'{sm["avg_win"]:+.2f}%'),
            ('avg loss', f'{sc["avg_loss"]:+.2f}%', f'{sm["avg_loss"]:+.2f}%'),
            ('max single win', f'{sc["max_win"]:+.2f}%', f'{sm["max_win"]:+.2f}%'),
            ('max single loss', f'{sc["max_loss"]:+.2f}%', f'{sm["max_loss"]:+.2f}%'),
        ]
        print(f'  {"metric":<22} {"current":>12} {"ma60_only":>12}')
        print('  ' + '-' * 50)
        for r in rows:
            print(f'  {r[0]:<22} {str(r[1]):>12} {str(r[2]):>12}')


if __name__ == '__main__':
    main()
