"""bt_ma_filter_extended 8변형 — NEW simulator 재검증

목적:
  OLD simulator 결과 (current/no_ma/ma20/ma50/ma100/ma120/ma150/ma200) 중
  어느 변형의 알파가 simulator 버그로 인한 환상이고 어느 것이 진짜인지 분리.

가설:
  - 매수 풀 강화 변형 (ma20/ma50/ma100/ma150/ma200) → 풀 회전 증가 → 버그 영향 큼
  - current vs ma120 vs ma150 vs ma200: ma120-200은 유사 (대부분 종목이 통과)
  - ma20 -27.57%p가 -7~+5%p 수준으로 축소 예상 (v81 검증 패턴)

데이터: research/ma_filter_dbs/ext_*.db (regenerate 결과 이미 있음)
방법: 100 seed × 3 starts paired, NEW simulator (price_full fallback)
"""
import sys
import sqlite3
import random
import statistics
import time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'ma_filter_dbs'

N_SEEDS = 100
SAMPLES = 3
MIN_HOLD_DAYS = 10

VARIANTS = [
    ('current',  GRID / 'ext_current.db'),
    ('no_ma',    GRID / 'ext_no_ma.db'),
    ('ma20',     GRID / 'ext_ma20.db'),
    ('ma50',     GRID / 'ext_ma50.db'),
    ('ma100',    GRID / 'ext_ma100.db'),
    ('ma120',    GRID / 'ext_ma120.db'),
    ('ma150',    GRID / 'ext_ma150.db'),
    ('ma200',    GRID / 'ext_ma200.db'),
]


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
            data[d][tk] = {
                'p2': r[1], 'price': r[2],
                'min_seg': min(segs) if segs else 0,
            }
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'
    ):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def simulate(dates_all, data, price_full, start_date=None,
             entry=3, exit_=10, slots=3):
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

        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2:
                exited.append(tk); continue
            if rank is None or rank > exit_:
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
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run(db_path, seed_starts):
    dates, data, price_full = load_all(db_path)
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 110)
    print(f'bt_ma_filter_extended 8변형 — NEW simulator 재검증')
    print(f'{N_SEEDS} seed × {SAMPLES} = {N_SEEDS*SAMPLES} sim/변형')
    print('=' * 110)

    t0 = time.time()
    # seed_starts (current DB 기준)
    dates_cur, _, _ = load_all(VARIANTS[0][1])
    eligible = dates_cur[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    print(f'\n[Load + seed_starts] {time.time()-t0:.1f}s')
    print(f'\n{"variant":<10} {"avg":>9} {"median":>9} {"worst MDD":>10} {"sharpe":>7} [{"time":>5}]')
    print('-' * 70)

    results = {}
    for name, db in VARIANTS:
        t1 = time.time()
        res = run(db, seed_starts)
        results[name] = res
        avg = sum(res['rets'])/len(res['rets'])
        med = sorted(res['rets'])[len(res['rets'])//2]
        worst_mdd = min(res['mdds'])
        std = statistics.pstdev(res['rets'])
        sharpe = avg/std if std > 0 else 0
        marker = '★' if name == 'current' else ' '
        print(f'{marker} {name:<8} {avg:+8.2f}% {med:+8.2f}% {worst_mdd:+9.2f}% {sharpe:+6.2f} [{time.time()-t1:5.1f}s]')

    print()
    print('=' * 110)
    print('paired vs current (production) — NEW simulator 기준')
    print('=' * 110)
    base = results['current']['seed_avgs']
    print(f'  {"variant":<10} {"avg lift":>10} {"worst":>10} {"best":>10} {"wins":>10} {"verdict":>15}')
    print('  ' + '-' * 80)
    for name, _ in VARIANTS:
        if name == 'current':
            continue
        new_ = results[name]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new_)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts)/len(lifts)
        worst = min(lifts); best = max(lifts)
        rate = wins / N_SEEDS * 100
        verdict = '✓✓ 명확 우월' if rate >= 90 else '✓ 우월' if rate >= 75 else '◐ 약함' if rate >= 60 else '~ 동등' if rate >= 40 else '✗ 열세'
        print(f'  {name:<10} {avg_l:+8.2f}%p {worst:+8.2f}%p {best:+8.2f}%p {wins:>5}/{N_SEEDS}  {verdict}')

    print()
    print('=' * 110)
    print('OLD vs NEW 결과 비교 (bt_ma_filter_extended.log의 OLD vs 이번 NEW)')
    print('=' * 110)
    print(f'  {"variant":<10} {"OLD lift":>10} {"NEW lift":>10} {"swing":>10}')
    print('  ' + '-' * 60)
    # bt_ma_filter_extended.log에서 OLD 결과 하드코딩
    OLD = {
        'no_ma':  +3.07,
        'ma20':  +27.57,
        'ma50':  +21.46,
        'ma100':  +3.18,
        'ma120':   0.00,
        'ma150':   0.00,
        'ma200':   0.00,
    }
    for name, _ in VARIANTS:
        if name == 'current':
            continue
        new_ = results[name]['seed_avgs']
        new_lift = sum(b - a for a, b in zip(base, new_))/len(base)
        old_lift = OLD.get(name, 0)
        swing = new_lift - old_lift
        print(f'  {name:<10} {old_lift:+8.2f}%p {new_lift:+8.2f}%p {swing:+8.2f}%p')

    print(f'\n총 소요: {time.time()-t0:.1f}s')


if __name__ == '__main__':
    main()
