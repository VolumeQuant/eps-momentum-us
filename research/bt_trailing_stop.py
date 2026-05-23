"""D: Trailing stop BT — 최고가 대비 -X% 자동 매도

현재: rank > 10 / min_seg < -2 매도 (사용자 -8% 안내는 수동)
검증: 시스템 자동 trailing stop 추가 시 효과

설계:
  - 진입 후 최고가 추적
  - 최고가 대비 -X% 도달 시 자동 매도
  - rank/min_seg 룰과 함께 작동 (먼저 발동되는 룰로 매도)

변형:
  baseline: trailing stop 없음 (현재 production)
  T8:  -8% trailing
  T10: -10% trailing
  T12: -12% trailing
  T15: -15% trailing
  T20: -20% trailing
"""
import sys
import sqlite3
import random
import statistics
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'

N_SEEDS = 500
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


def simulate(dates_all, data, price_full, trailing_pct, start_date=None,
             entry=3, exit_=10, slots=3):
    """trailing_pct: None or float (e.g., 0.08 for -8% trailing). None이면 baseline"""
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
            new_c = defaultdict(int)
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    new_c[tk] = consecutive.get(tk, 0) + 1
            consecutive = new_c
    for di, today in enumerate(dates):
        if today not in data: continue
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
            if n > 0: day_ret /= n
        daily_returns.append(day_ret)

        # 최고가 업데이트
        for tk, info in portfolio.items():
            cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            if cur_p:
                info['peak'] = max(info.get('peak', cur_p), cur_p)

        # Exit (rank, min_seg, trailing stop 모두 체크)
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            info = portfolio[tk]
            # 1. min_seg 매도
            if min_seg < -2: exited.append(tk); continue
            # 2. rank 매도
            if rank is None or rank > exit_: exited.append(tk); continue
            # 3. trailing stop 매도
            if trailing_pct and cur_p and info.get('peak'):
                drop_from_peak = (cur_p - info['peak']) / info['peak']
                if drop_from_peak <= -trailing_pct:
                    exited.append(tk); continue
        for tk in exited: del portfolio[tk]

        vacancies = slots - len(portfolio)
        if vacancies > 0:
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > entry or vacancies <= 0: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                min_seg = today_data.get(tk, {}).get('min_seg', 0)
                if min_seg < 0: continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0:
                    portfolio[tk] = {'entry_price': price, 'peak': price}
                    vacancies -= 1
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run(trailing_pct, dates, data, price_full, seed_starts):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, trailing_pct, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 100)
    print(f'D: Trailing stop BT')
    print(f'N_SEEDS={N_SEEDS} × {SAMPLES} = {N_SEEDS*SAMPLES} sim/config')
    print('=' * 100)
    dates, data, price_full = load_all(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    configs = [
        ('baseline (없음)', None),
        ('T6:  -6%', 0.06),
        ('T8:  -8%', 0.08),
        ('T10: -10%', 0.10),
        ('T12: -12%', 0.12),
        ('T15: -15%', 0.15),
        ('T20: -20%', 0.20),
        ('T25: -25%', 0.25),
    ]
    print()
    print(f'{"config":<20} {"avg":>9} {"med":>9} {"worst MDD":>10} {"avg MDD":>9} {"sharpe":>7}')
    print('-' * 75)
    results = {}
    for name, t in configs:
        res = run(t, dates, data, price_full, seed_starts)
        results[name] = res
        avg = sum(res['rets'])/len(res['rets'])
        med = sorted(res['rets'])[len(res['rets'])//2]
        worst_mdd = min(res['mdds'])
        avg_mdd = sum(res['mdds'])/len(res['mdds'])
        std = statistics.pstdev(res['rets'])
        sharpe = avg/std if std > 0 else 0
        marker = '★' if 'baseline' in name else '  '
        print(f'{marker} {name:<18} {avg:+8.2f}% {med:+8.2f}% {worst_mdd:+9.2f}% {avg_mdd:+8.2f}% {sharpe:+6.2f}')

    print()
    print('=' * 100)
    print('paired vs baseline')
    print('=' * 100)
    print(f'{"config":<20} {"avg lift":>10} {"worst":>10} {"best":>10} {"wins":>10} {"verdict":>15}')
    print('-' * 85)
    base = results['baseline (없음)']['seed_avgs']
    for name, _ in configs:
        if 'baseline' in name: continue
        new_ = results[name]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new_)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts)/len(lifts)
        worst = min(lifts); best = max(lifts)
        rate = wins / N_SEEDS * 100
        verdict = '✓✓ 우월' if rate >= 90 else '✓ 우월' if rate >= 75 else '◐ 약함' if rate >= 60 else '~ 동등' if rate >= 40 else '✗ 열세'
        print(f'  {name:<18} {avg_l:+8.2f}%p {worst:+8.2f}%p {best:+8.2f}%p {wins:>5}/{N_SEEDS}  {verdict}')


if __name__ == '__main__':
    main()
