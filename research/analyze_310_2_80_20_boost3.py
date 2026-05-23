"""(3,10,2) 80/20 boost=3 — trade-by-trade + MDD 분석

전체 데이터 (5/22까지) 시작일=처음으로 보유 흐름 추적:
  - 각 진입/이탈 종목 + 시점 + 가격
  - 종목별 수익률 + 보유 기간
  - 1위/2위 종목 구분 (80% / 20%)
  - C2 여부 표시
  - 시간별 NAV 추적 (MDD 발생 시점 식별)

baseline (3,10,3) 균등과 비교.
"""
import sys
import sqlite3
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
LOOKBACK = 30


def load_all(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price, eps_chg_weighted,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
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
            data[d][tk] = {'p2': r[1], 'price': r[2], 'eps_w': r[3], 'min_seg': min(segs) if segs else 0}
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def get_price_30d(tk, today, dates_list, price_full):
    if today not in dates_list: return None
    di = dates_list.index(today)
    if di < LOOKBACK: return None
    past_d = dates_list[di - LOOKBACK]
    past_p = price_full.get(past_d, {}).get(tk)
    cur_p = price_full.get(today, {}).get(tk)
    if past_p and cur_p and past_p > 0:
        return (cur_p - past_p) / past_p * 100
    return None


def classify_c2(info, tk, today, dates_list, price_full):
    eps_w = info.get('eps_w')
    if eps_w is None: return False
    p30 = get_price_30d(tk, today, dates_list, price_full)
    if p30 is None: return False
    return eps_w > 0 and p30 < 0


def rerank(today, today_data, c2_boost, dates_list, price_full):
    if c2_boost == 0:
        return {tk: info.get('p2') for tk, info in today_data.items() if info.get('p2') is not None}, set()
    candidates = []
    c2_set = set()
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        is_c2 = classify_c2(info, tk, today, dates_list, price_full)
        if is_c2: c2_set.add(tk)
        score = (31 - p2) + (c2_boost if is_c2 else 0)
        candidates.append((-score, tk))
    candidates.sort()
    return {tk: i+1 for i, (_, tk) in enumerate(candidates)}, c2_set


def simulate_with_trace(dates, data, price_full, weights, entry, exit_, c2_boost):
    slots = len(weights)
    portfolio = {}
    consecutive = defaultdict(int)
    daily_returns = []
    nav_track = []
    trades = []  # 매도 시 기록

    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        new_ranks, c2_set = rerank(today, today_data, c2_boost, dates, price_full)
        new_consec = defaultdict(int)
        for tk, r in new_ranks.items():
            if r <= 30:
                new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di-1]
            for tk, info in portfolio.items():
                cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                prev_p = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if cur_p and prev_p and prev_p > 0:
                    w = weights[info['slot_idx']] / 100.0
                    day_ret += w * (cur_p - prev_p) / prev_p * 100
        daily_returns.append(day_ret)
        nav_track.append((today, day_ret))

        # Exit
        exited = []
        for tk in list(portfolio.keys()):
            rank = new_ranks.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            reason = None
            if min_seg < -2: reason = 'min_seg'
            elif rank is None: reason = 'rank_NULL'
            elif rank > exit_: reason = f'rank>{exit_}'
            if reason and cur_p:
                info = portfolio[tk]
                ret = (cur_p - info['entry_price']) / info['entry_price'] * 100
                trades.append({
                    'ticker': tk, 'slot_idx': info['slot_idx'], 'entry_date': info['entry_date'],
                    'exit_date': today, 'entry_price': info['entry_price'], 'exit_price': cur_p,
                    'ret': ret, 'days': di - info['entry_di'], 'reason': reason,
                    'was_c2': info.get('was_c2', False),
                })
                exited.append(tk)
        for tk in exited: del portfolio[tk]

        # Entry
        if len(portfolio) < slots:
            used_idx = {info['slot_idx'] for info in portfolio.values()}
            free_idx = sorted([i for i in range(slots) if i not in used_idx])
            cands = []
            for tk, new_r in sorted(new_ranks.items(), key=lambda x: x[1]):
                if new_r > entry: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                info = today_data.get(tk, {})
                min_seg = info.get('min_seg', 0)
                if min_seg < 0: continue
                price = info.get('price')
                if price and price > 0:
                    cands.append((tk, price))
            for slot_idx in free_idx:
                if not cands: break
                tk, price = cands.pop(0)
                portfolio[tk] = {
                    'entry_price': price, 'entry_date': today, 'entry_di': di,
                    'slot_idx': slot_idx, 'was_c2': (tk in c2_set),
                }

    # 최종 보유 종목들 (open)
    final_d = dates[-1]
    for tk, info in portfolio.items():
        final_price = data[final_d].get(tk, {}).get('price') or price_full.get(final_d, {}).get(tk)
        if final_price:
            ret = (final_price - info['entry_price']) / info['entry_price'] * 100
            trades.append({
                'ticker': tk, 'slot_idx': info['slot_idx'], 'entry_date': info['entry_date'],
                'exit_date': final_d + ' (open)', 'entry_price': info['entry_price'], 'exit_price': final_price,
                'ret': ret, 'days': len(dates) - info['entry_di'] - 1, 'reason': 'OPEN',
                'was_c2': info.get('was_c2', False),
            })

    # NAV 누적 + MDD
    cum = 1.0; peak = 1.0; max_dd = 0
    nav_series = []
    mdd_date = None
    for d, r in nav_track:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100
        if dd < max_dd:
            max_dd = dd; mdd_date = d
        nav_series.append((d, cum, dd))
    return {
        'total_return': (cum-1)*100, 'max_dd': max_dd, 'mdd_date': mdd_date,
        'trades': trades, 'nav_series': nav_series,
    }


def main():
    dates, data, price_full = load_all(DB_PATH)
    print('=' * 110)
    print(f'(3,10,2) 80/20 boost=3 — trade-by-trade + NAV 추적')
    print(f'기간: {dates[0]} ~ {dates[-1]} ({len(dates)} 거래일)')
    print('=' * 110)

    # 두 시나리오 비교
    scenarios = [
        ('baseline (3,10,3) 균등', [33,33,34], 3, 10, 0),
        ('(3,10,2) 80/20 boost=3', [80, 20], 3, 10, 3),
    ]
    results = {}
    for label, w, entry, exit_, b in scenarios:
        res = simulate_with_trace(dates, data, price_full, w, entry, exit_, b)
        results[label] = res
        print(f'\n[{label}]')
        print(f'  총 수익: {res["total_return"]:+.2f}%, MDD: {res["max_dd"]:+.2f}% ({res["mdd_date"]})')
        print(f'  Trades: {len(res["trades"])}건')

    # Trades detail (80/20 boost=3)
    print()
    print('=' * 110)
    print('(3,10,2) 80/20 boost=3 trades 상세')
    print('=' * 110)
    print(f'{"ticker":<8} {"slot":<5} {"weight":<7} {"C2?":<5} {"entry":<12} {"exit":<22} {"entry_p":>9} {"exit_p":>9} {"ret":>9} {"days":>5} {"reason":>12}')
    print('-' * 110)
    trades = results['(3,10,2) 80/20 boost=3']['trades']
    trades.sort(key=lambda t: t['entry_date'])
    weights = [80, 20]
    for t in trades:
        slot = t['slot_idx']
        wt = weights[slot]
        c2 = '★' if t['was_c2'] else ''
        print(f'{t["ticker"]:<8} {slot+1:<5} {wt}%     {c2:<5} {t["entry_date"]:<12} {t["exit_date"]:<22} '
              f'{t["entry_price"]:>8.2f} {t["exit_price"]:>8.2f} {t["ret"]:+8.2f}% {t["days"]:>4} {t["reason"]:>12}')

    # Slot/Case 통계
    print()
    print('=' * 110)
    print('slot/case별 통계 — (3,10,2) 80/20 boost=3')
    print('=' * 110)
    s1_rets = [t['ret'] for t in trades if t['slot_idx'] == 0]
    s2_rets = [t['ret'] for t in trades if t['slot_idx'] == 1]
    c2_rets = [t['ret'] for t in trades if t['was_c2']]
    c1_rets = [t['ret'] for t in trades if not t['was_c2']]
    print(f'  1위 슬롯 (80%): n={len(s1_rets)}, avg {sum(s1_rets)/max(1,len(s1_rets)):+.2f}%, max {max(s1_rets, default=0):+.2f}%, min {min(s1_rets, default=0):+.2f}%')
    print(f'  2위 슬롯 (20%): n={len(s2_rets)}, avg {sum(s2_rets)/max(1,len(s2_rets)):+.2f}%, max {max(s2_rets, default=0):+.2f}%, min {min(s2_rets, default=0):+.2f}%')
    print(f'  C2 (boost): n={len(c2_rets)}, avg {sum(c2_rets)/max(1,len(c2_rets)):+.2f}%')
    print(f'  C1: n={len(c1_rets)}, avg {sum(c1_rets)/max(1,len(c1_rets)):+.2f}%')

    # NAV 시계열 — MDD 발생 구간
    print()
    print('=' * 110)
    print('NAV 시계열 — MDD 진행 (drawdown -5%+ 구간만 표시)')
    print('=' * 110)
    nav = results['(3,10,2) 80/20 boost=3']['nav_series']
    print(f'{"date":<12} {"NAV":>8} {"DD":>8}')
    for d, n, dd in nav:
        if dd <= -5:
            print(f'{d:<12} {n:>7.4f} {dd:+7.2f}%')

    # baseline trades for comparison
    print()
    print('=' * 110)
    print('baseline (3,10,3) 균등 trades (비교용)')
    print('=' * 110)
    base_trades = results['baseline (3,10,3) 균등']['trades']
    base_trades.sort(key=lambda t: t['entry_date'])
    print(f'{"ticker":<8} {"slot":<5} {"entry":<12} {"exit":<22} {"ret":>9} {"days":>5} {"reason":>12}')
    for t in base_trades:
        print(f'{t["ticker"]:<8} {t["slot_idx"]+1:<5} {t["entry_date"]:<12} {t["exit_date"]:<22} '
              f'{t["ret"]:+8.2f}% {t["days"]:>4} {t["reason"]:>12}')


if __name__ == '__main__':
    main()
