"""옵션 B (점수 재계산) boost=3 12 multistart 검증"""
import sys
import sqlite3
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
MIN_HOLD_DAYS = 10
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
    candidates = []
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        is_c2 = classify_c2(info, tk, today, dates_list, price_full)
        score = (31 - p2) + (c2_boost if is_c2 else 0)
        candidates.append((-score, tk))
    candidates.sort()
    return {tk: i+1 for i, (_, tk) in enumerate(candidates)}


def simulate(dates_all, data, price_full, c2_boost, start_date=None,
             entry=3, exit_=10, slots=3):
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    consecutive = defaultdict(int)
    daily_returns = []
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            today_data = data.get(d, {})
            new_ranks = rerank(d, today_data, c2_boost, dates_all, price_full)
            new_c = defaultdict(int)
            for tk, r in new_ranks.items():
                if r <= 30:
                    new_c[tk] = consecutive.get(tk, 0) + 1
            consecutive = new_c
    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        new_ranks = rerank(today, today_data, c2_boost, dates_all, price_full)
        new_consec = defaultdict(int)
        for tk, r in new_ranks.items():
            if r <= 30:
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
            if n > 0: day_ret /= n
        daily_returns.append(day_ret)
        exited = []
        for tk in list(portfolio.keys()):
            rank = new_ranks.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2: exited.append(tk); continue
            if rank is None or rank > exit_: exited.append(tk); continue
        for tk in exited: del portfolio[tk]
        vacancies = slots - len(portfolio)
        if vacancies > 0:
            for tk, new_r in sorted(new_ranks.items(), key=lambda x: x[1]):
                if new_r > entry or vacancies <= 0: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                info = today_data.get(tk, {})
                min_seg = info.get('min_seg', 0)
                if min_seg < 0: continue
                price = info.get('price')
                if not (price and price > 0): continue
                portfolio[tk] = {'entry_price': price}
                vacancies -= 1
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def main():
    print('=' * 100)
    print('옵션 B (점수 재계산) boost=3 12 multistart')
    print('=' * 100)
    dates, data, price_full = load_all(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    step = max(1, len(eligible) // 13)
    fixed_starts = [eligible[step * i] for i in range(1, 13)]
    print(f'12 시작일: {fixed_starts[0]} ~ {fixed_starts[-1]}\n')
    print(f'{"start_date":<12} {"boost=0":>9} {"boost=3":>9} {"lift":>8} {"mdd 0":>8} {"mdd 3":>8}')
    print('-' * 70)
    rets_0, rets_3, lifts = [], [], []
    for sd in fixed_starts:
        r0 = simulate(dates, data, price_full, 0, start_date=sd)
        r3 = simulate(dates, data, price_full, 3, start_date=sd)
        lift = r3['total_return'] - r0['total_return']
        marker = '+' if lift > 0 else '-' if lift < 0 else '='
        rets_0.append(r0['total_return']); rets_3.append(r3['total_return']); lifts.append(lift)
        print(f'{sd:<12} {r0["total_return"]:+8.2f}% {r3["total_return"]:+8.2f}% {marker}{lift:+7.2f}%p {r0["max_dd"]:+7.2f}% {r3["max_dd"]:+7.2f}%')
    print()
    avg_0 = sum(rets_0)/12; avg_3 = sum(rets_3)/12
    avg_l = sum(lifts)/12; wins = sum(1 for l in lifts if l > 0)
    losses = sum(1 for l in lifts if l < 0)
    print(f'baseline 평균: {avg_0:+.2f}%')
    print(f'boost=3 평균:  {avg_3:+.2f}%')
    print(f'평균 lift:     {avg_l:+.2f}%p')
    print(f'boost=3 wins:  {wins}/12, losses {losses}/12')


if __name__ == '__main__':
    main()
