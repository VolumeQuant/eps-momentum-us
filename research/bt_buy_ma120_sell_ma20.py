"""매수 MA120 + 매도 MA20 BT (사용자 직관)

설계:
  매수: part2_rank 풀 = MA120 통과 (ext_current.db 사용, 더 넓은 풀)
  매도: rank > exit_top OR min_seg < -2 OR price < MA20 (★ 추가)

비교:
  v80.10c production: MA120 매수 + (rank>10 매도)
  v81 현재:          MA20 매수 + (rank>10 매도, MA20 이탈 시 자동 트리거)
  v82 후보:          MA120 매수 + (rank>10 매도 + MA20 이탈 즉시 매도)
"""
import sys
import sqlite3
import random
import statistics
import time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'ma_filter_dbs'
DB_CURRENT = GRID / 'ext_current.db'  # MA120+fallback 매수 풀
DB_MA20 = GRID / 'ext_ma20.db'        # MA20 매수 풀

N_SEEDS = 500
SAMPLES = 5
MIN_HOLD_DAYS = 10


def load_data_with_ma20(db_path):
    """DB 로드 + 모든 ticker별 ma20 dict 함께 반환"""
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
    # MA20 값 — 모든 종목 (보유 중인데 part2_rank 빠진 경우 위해)
    ma20_full = defaultdict(dict)  # {date: {ticker: ma20}}
    for d, tk, ma20 in cur.execute(
        'SELECT date, ticker, ma20 FROM ntm_screening WHERE ma20 IS NOT NULL'
    ):
        ma20_full[d][tk] = ma20
    # price도 모든 일자
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'
    ):
        price_full[d][tk] = px
    conn.close()
    return dates, data, ma20_full, price_full


def simulate(dates_all, data, ma20_full, price_full,
             entry, exit_, slots, use_ma20_exit=True, start_date=None):
    """매수: rank<=entry, MA120 통과(이미 풀에 있음), min_seg>=0
       매도: rank>exit OR min_seg<-2 OR (use_ma20_exit AND price<MA20)
    """
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    daily_returns = []
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
        if portfolio:
            n = 0
            for tk in portfolio:
                # 가격 — data 또는 price_full에서
                price = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                if price and di > 0:
                    prev_d = dates[di-1]
                    prev = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                    if prev and prev > 0:
                        day_ret += (price - prev) / prev * 100
                        n += 1
            if n > 0:
                day_ret /= n
        daily_returns.append(day_ret)

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
            # MA20 이탈 트리거 (사용자 직관)
            if use_ma20_exit and price and ma20 and price < ma20:
                exited.append(tk); continue
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
                    portfolio[tk] = {'entry_price': price}
                    vacancies -= 1

    # cumulative + MDD
    cum = 1.0
    peak = 1.0
    max_dd = 0
    for r in daily_returns:
        cum *= (1 + r / 100)
        peak = max(peak, cum)
        dd = (cum - peak) / peak * 100
        max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd, 'daily_returns': daily_returns}


def run(db_path, entry, exit_, slots, use_ma20_exit, seed_starts):
    dates, data, ma20_full, price_full = load_data_with_ma20(db_path)
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        seed_rets = []
        for sd in chosen:
            r = simulate(dates, data, ma20_full, price_full,
                         entry, exit_, slots, use_ma20_exit, start_date=sd)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            seed_rets.append(r['total_return'])
        seed_avgs.append(sum(seed_rets) / len(seed_rets))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 100)
    print('매수 MA120 + 매도 MA20 BT (사용자 직관)')
    print(f'{N_SEEDS} seed × {SAMPLES} = {N_SEEDS*SAMPLES} sim/조합')
    print('=' * 100)

    # seed_starts (공통)
    dates_cur, _, _, _ = load_data_with_ma20(DB_CURRENT)
    eligible = dates_cur[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    candidates = [
        # (label, db, entry, exit, slots, use_ma20_exit)
        ('v80.10c production',     DB_CURRENT, 3, 10, 3, False),
        ('v81 current (MA20 매수)',  DB_MA20,    3, 10, 3, False),
        ('v82a: MA120 매수+MA20 매도', DB_CURRENT, 3, 10, 3, True),
        ('v82b: MA120 매수+MA20 매도 + entry 5', DB_CURRENT, 5, 10, 3, True),
        ('v82c: MA20 매수+MA20 매도 (이중)', DB_MA20, 3, 10, 3, True),
        ('v82d: MA120 매수+MA20 매도 (slots 5)', DB_CURRENT, 5, 10, 5, True),
    ]

    print()
    print(f'{"variant":<40} {"avg":>9} {"mdd":>8} {"ra":>7} {"sharpe":>7}')
    print('-' * 80)
    results = {}
    for label, db, e, x, s, ma20_exit in candidates:
        t0 = time.time()
        res = run(db, e, x, s, ma20_exit, seed_starts)
        avg = sum(res['rets'])/len(res['rets'])
        mdd = min(res['mdds'])
        ra = avg/abs(mdd) if mdd<0 else 0
        std = statistics.pstdev(res['rets'])
        sharpe = avg/std if std>0 else 0
        results[label] = res
        marker = '★' if 'production' in label else ('▲' if 'current' in label else '  ')
        print(f'  {marker} {label:<38} {avg:+8.2f}% {mdd:+7.2f}% {ra:+6.2f} {sharpe:+6.2f} [{time.time()-t0:.1f}s]')

    # paired vs v80.10c
    print()
    print('=' * 100)
    print('paired vs v80.10c production')
    print('=' * 100)
    base = results['v80.10c production']['seed_avgs']
    for label, *_ in candidates:
        if 'production' in label:
            continue
        new = results[label]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts) / len(lifts)
        verdict = '✓' if wins >= N_SEEDS * 0.9 else '◐' if wins >= N_SEEDS * 0.6 else '✗'
        print(f'  {label:<40} lift {avg_l:+7.2f}%p wins {wins}/{N_SEEDS} [{verdict}]')

    # paired vs v81 (현재 적용된 것)
    print()
    print('=' * 100)
    print('paired vs v81 (현재 적용된 MA20 매수)')
    print('=' * 100)
    base = results['v81 current (MA20 매수)']['seed_avgs']
    for label, *_ in candidates:
        if 'v81' in label or 'production' in label:
            continue
        new = results[label]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts) / len(lifts)
        verdict = '✓' if wins >= N_SEEDS * 0.9 else '◐' if wins >= N_SEEDS * 0.6 else '✗'
        print(f'  {label:<40} lift {avg_l:+7.2f}%p wins {wins}/{N_SEEDS} [{verdict}]')


if __name__ == '__main__':
    main()
