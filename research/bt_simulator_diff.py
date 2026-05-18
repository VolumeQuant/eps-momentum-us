"""OLD (bt_breakout_hold) vs NEW (bt_buy_ma120_sell_ma20) 시뮬레이터 차이 검증

목적:
  v81 적용 결정의 BT (bt_ma_filter_extended 류)가 OLD 방식이었다.
  새 BT (bt_buy_ma120_sell_ma20)는 NEW 방식이고 v81에 -7.59%p 평가.
  어느 simulator가 현실에 맞는가? — 매도일에 day_ret이 어떻게 잡히는지가 핵심.

설계:
  동일 DB (ext_current.db 또는 production), 동일 파라미터 (entry=3, exit=10, slots=3, MA20 매도 ON/OFF)
  OLD/NEW 두 방식으로 simulate, 매도일 daily_return 차이의 출처 추적.

핵심 차이:
  OLD: portfolio 보유 종목 중 today_data에 없는 것 (part2_rank NULL) → 그날 ret = 0,
       len(portfolio)로 나누기 → day_ret 희석/소실
  NEW: price_full fallback → NULL 종목도 real price로 ret 계산,
       실제 가격 잡힌 개수로 나누기 → 매도일 crash 포착

가설:
  MA20 매도 룰 켜진 경우 NEW가 OLD보다 매도일 daily_return을 더 negative하게 잡음.
  → OLD가 MA20 매도 전략의 알파를 인위적으로 부풀린다.
"""
import sys
import sqlite3
import random
import statistics
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'research' / 'ma_filter_dbs' / 'ext_current.db'

N_SEEDS = 50
SAMPLES = 3
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
            SELECT ticker, part2_rank, price, ma20,
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
            data[d][tk] = {
                'p2': r[1], 'price': r[2], 'ma20': r[3],
                'min_seg': min(segs) if segs else 0,
            }
    ma20_full = defaultdict(dict)
    for d, tk, ma20 in cur.execute(
        'SELECT date, ticker, ma20 FROM ntm_screening WHERE ma20 IS NOT NULL'
    ):
        ma20_full[d][tk] = ma20
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'
    ):
        price_full[d][tk] = px
    conn.close()
    return dates, data, ma20_full, price_full


def simulate(dates_all, data, ma20_full, price_full,
             entry, exit_, slots, use_ma20_exit, mode,
             start_date=None, capture_diff=False):
    """mode: 'OLD' or 'NEW'
    capture_diff: True면 매일 OLD/NEW day_ret 차이 추적용 metadata 리턴
    """
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    daily_returns = []
    diff_log = []  # [(date, old_ret, new_ret, missing_tickers)]
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

        # day_ret 계산 — mode별
        day_ret_old = 0
        day_ret_new = 0
        missing_tks = []
        if portfolio and di > 0:
            prev_d = dates[di-1]
            n_new = 0
            for tk in portfolio:
                # OLD 방식
                old_p = today_data.get(tk, {}).get('price')
                old_prev = data.get(prev_d, {}).get(tk, {}).get('price')
                if old_p and old_prev and old_prev > 0:
                    day_ret_old += (old_p - old_prev) / old_prev * 100
                else:
                    missing_tks.append(tk)
                # NEW 방식
                new_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                new_prev = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if new_p and new_prev and new_prev > 0:
                    day_ret_new += (new_p - new_prev) / new_prev * 100
                    n_new += 1
            day_ret_old /= len(portfolio)  # OLD: 무조건 보유 수로
            if n_new > 0:
                day_ret_new /= n_new

        if mode == 'OLD':
            daily_returns.append(day_ret_old)
        else:
            daily_returns.append(day_ret_new)

        if capture_diff and portfolio and missing_tks:
            diff_log.append({
                'date': today, 'old_ret': day_ret_old, 'new_ret': day_ret_new,
                'missing': missing_tks, 'pf_size': len(portfolio)
            })

        # 이탈 체크
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            price = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            ma20 = today_data.get(tk, {}).get('ma20') or ma20_full.get(today, {}).get(tk)
            if min_seg < -2:
                exited.append(tk); continue
            if rank is None or rank > exit_:
                exited.append(tk); continue
            if use_ma20_exit and price and ma20 and price < ma20:
                exited.append(tk); continue
        for tk in exited:
            del portfolio[tk]

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
                    portfolio[tk] = {'entry_price': price}
                    vacancies -= 1

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd,
            'diff_log': diff_log}


def main():
    print('=' * 100)
    print('OLD (bt_breakout_hold) vs NEW (bt_buy_ma120_sell_ma20) simulator 검증')
    print(f'DB: {DB_PATH.name}, N_SEEDS={N_SEEDS} × {SAMPLES} = {N_SEEDS*SAMPLES} sim')
    print('=' * 100)

    dates, data, ma20_full, price_full = load_all(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    # 2 시나리오 × 2 mode
    scenarios = [
        ('v80.10c (rank exit only)', False),
        ('v82a (rank + MA20 exit)',  True),
    ]
    results = {}
    for label, use_ma20 in scenarios:
        for mode in ['OLD', 'NEW']:
            key = f'{label} [{mode}]'
            rets = []
            seed_avgs = []
            for chosen in seed_starts:
                seed_rets = []
                for sd in chosen:
                    r = simulate(dates, data, ma20_full, price_full,
                                 3, 10, 3, use_ma20, mode, start_date=sd)
                    rets.append(r['total_return'])
                    seed_rets.append(r['total_return'])
                seed_avgs.append(sum(seed_rets)/len(seed_rets))
            results[key] = {'rets': rets, 'seed_avgs': seed_avgs}
            avg = sum(rets)/len(rets)
            print(f'  {key:<45} avg={avg:+7.2f}%')

    # paired: 같은 전략에서 OLD vs NEW 차이
    print()
    print('=' * 100)
    print('같은 전략 내부 OLD vs NEW 차이 (시뮬레이터 영향)')
    print('=' * 100)
    for label, _ in scenarios:
        old_avgs = results[f'{label} [OLD]']['seed_avgs']
        new_avgs = results[f'{label} [NEW]']['seed_avgs']
        diffs = [n - o for o, n in zip(old_avgs, new_avgs)]
        avg_d = sum(diffs)/len(diffs)
        wins = sum(1 for d in diffs if d > 0)
        print(f'  {label:<35} NEW-OLD avg={avg_d:+6.2f}%p  NEW>OLD {wins}/{N_SEEDS} seed')

    # paired: OLD에서 본 v82a vs v80.10c, NEW에서 본 v82a vs v80.10c
    print()
    print('=' * 100)
    print('각 모드에서의 전략간 비교 (v82a vs v80.10c)')
    print('=' * 100)
    for mode in ['OLD', 'NEW']:
        base = results[f'v80.10c (rank exit only) [{mode}]']['seed_avgs']
        new_ = results[f'v82a (rank + MA20 exit) [{mode}]']['seed_avgs']
        lifts = [b - a for a, b in zip(base, new_)]
        wins = sum(1 for l in lifts if l > 0)
        print(f'  [{mode}] v82a - v80.10c lift={sum(lifts)/len(lifts):+6.2f}%p  v82a wins {wins}/{N_SEEDS}')

    # diff_log 샘플: MA20 매도 모드에서 매도일 OLD/NEW gap이 큰 날짜 Top 5
    print()
    print('=' * 100)
    print('MA20 매도 시나리오에서 매도일 OLD/NEW gap이 큰 사례 (1 random sample)')
    print('=' * 100)
    random.seed(42)
    sd = random.choice(eligible)
    r = simulate(dates, data, ma20_full, price_full,
                 3, 10, 3, True, 'NEW', start_date=sd, capture_diff=True)
    gaps = [(d['date'], d['old_ret'], d['new_ret'], d['old_ret']-d['new_ret'], d['missing'])
            for d in r['diff_log']]
    gaps.sort(key=lambda x: abs(x[3]), reverse=True)
    print(f'  시작일: {sd}, 누락 케이스 {len(r["diff_log"])}건')
    print(f'  {"date":<12} {"old_ret":>8} {"new_ret":>8} {"gap (OLD-NEW)":>15} missing')
    for d, o, n, g, miss in gaps[:10]:
        print(f'  {d:<12} {o:+7.3f}% {n:+7.3f}% {g:+13.3f}%p  {miss}')


if __name__ == '__main__':
    main()
