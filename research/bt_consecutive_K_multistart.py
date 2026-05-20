"""K=3 vs K=4 12 multistart — random BT lift +1.96%p가 일관성 있는지 검증"""
import sys
import sqlite3
import statistics
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
MIN_HOLD_DAYS = 10


def load_all(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[3:8])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else:
                    segs.append(0)
            data[d][tk] = {'p2': r[1], 'price': r[2], 'min_seg': min(segs) if segs else 0}
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def simulate(dates_all, data, price_full, K, start_date=None,
             entry=3, exit_=10, slots=3):
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}; daily_returns = []
    consecutive = defaultdict(int)
    if start_date:
        for d in dates_all:
            if d >= start_date:
                break
            new_c = defaultdict(int)
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    new_c[tk] = consecutive.get(tk, 0) + 1
            consecutive = new_c
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
            prev_d = dates[di-1]; n = 0
            for tk in portfolio:
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                pr = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if p and pr and pr > 0:
                    day_ret += (p - pr) / pr * 100; n += 1
            if n > 0:
                day_ret /= n
        daily_returns.append(day_ret)
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2: exited.append(tk); continue
            if rank is None or rank > exit_: exited.append(tk); continue
        for tk in exited:
            del portfolio[tk]
        vacancies = slots - len(portfolio)
        if vacancies > 0:
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > entry or vacancies <= 0: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < K: continue
                min_seg = today_data.get(tk, {}).get('min_seg', 0)
                if min_seg < 0: continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0:
                    portfolio[tk] = {'entry_price': price}; vacancies -= 1
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def main():
    print('=' * 100)
    print('K=3 vs K=4 12 multistart 검증')
    print('=' * 100)
    dates, data, price_full = load_all(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    step = max(1, len(eligible) // 13)
    fixed_starts = [eligible[step * i] for i in range(1, 13)]
    print(f'12 시작일: {fixed_starts[0]} ~ {fixed_starts[-1]}\n')

    print(f'{"start_date":<12} {"K=3":>9} {"K=4":>9} {"lift":>8} {"mdd K=3":>10} {"mdd K=4":>10}')
    print('-' * 70)
    rets_3, rets_4, lifts = [], [], []
    for sd in fixed_starts:
        r3 = simulate(dates, data, price_full, 3, start_date=sd)
        r4 = simulate(dates, data, price_full, 4, start_date=sd)
        lift = r4['total_return'] - r3['total_return']
        marker = '+' if lift > 0 else '-' if lift < 0 else '='
        rets_3.append(r3['total_return']); rets_4.append(r4['total_return']); lifts.append(lift)
        print(f'{sd:<12} {r3["total_return"]:+8.2f}% {r4["total_return"]:+8.2f}% {marker}{lift:+7.2f}%p {r3["max_dd"]:+9.2f}% {r4["max_dd"]:+9.2f}%')

    print()
    avg_3 = sum(rets_3)/12; avg_4 = sum(rets_4)/12
    avg_l = sum(lifts)/12; wins = sum(1 for l in lifts if l > 0)
    print(f'K=3 평균: {avg_3:+.2f}% | K=4 평균: {avg_4:+.2f}% | 평균 lift: {avg_l:+.2f}%p | K=4 wins {wins}/12')


if __name__ == '__main__':
    main()
