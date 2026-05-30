"""최적 비중 grid BT — v83.3 90/10 vs 50/50 등

목적:
  사용자가 AEIS 변동성에 잠 못 잠. 단일 종목 90% 노출이 진짜 원인 가설 검증.
  비중 완화 시 (수익률 trade-off vs 변동성 감소)를 정량화.

simulator: 매일 part2_rank 기준 rebalance (그날 1위 종목 weights[0], 2위 weights[1] ...)
- max_slots = len(weights)
- 종목 수 < max_slots: weight normalize

variant grid:
  slot 1: 100/0 (가장 집중)
  slot 2: 100/0, 90/10, 80/20, 70/30, 60/40, 50/50
  slot 3: 50/30/20, 40/35/25, 33/33/33 (균등)

지표:
  - total_return avg/median
  - std (변동성)
  - MDD (max drawdown)
  - max_day_loss (단일 일자 최대 -%)
  - p5_day_loss (하위 5%ile 일일 수익률)
  - sharpe = avg / std
  - paired vs 90/10 baseline

100 seed × 3 starts paired.
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
DB_PATH = ROOT / 'eps_momentum_data.db'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 3
EXIT_TOP = 10


def load_data():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price, composite_rank,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
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
            data[d][tk] = {'p2': r[1], 'price': r[2], 'cr': r[3],
                          'min_seg': min(segs) if segs else 0}
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'
    ):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def simulate_weighted(dates_all, data, price_full, weights, start_date,
                      entry=3, exit_=10):
    """weights[i] = part2_rank i+1번째 종목 비중"""
    max_slots = len(weights)
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    portfolio = {}  # {tk: {entry_price, entry_date}}
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

        # weighted day_ret (포트폴리오 종목을 part2_rank 기준 정렬, weights 적용)
        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di-1]
            # rank 기준 정렬. rank 없으면 999 (꼴찌)
            sorted_pf = sorted(portfolio.keys(),
                              key=lambda t: rank_map.get(t, 999))
            n = len(sorted_pf)
            used_w = list(weights[:n])
            total_w = sum(used_w)
            if total_w > 0:
                used_w = [w/total_w for w in used_w]
                for i, tk in enumerate(sorted_pf):
                    p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                    pr = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                    if p and pr and pr > 0:
                        day_ret += used_w[i] * (p - pr) / pr * 100
        daily_returns.append(day_ret)

        # 이탈
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

        # 진입
        vacancies = max_slots - len(portfolio)
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
                    portfolio[tk] = {'entry_price': price, 'entry_date': today}
                    vacancies -= 1

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100
        max_dd = min(max_dd, dd)
    max_day_loss = min(daily_returns) if daily_returns else 0
    sorted_drs = sorted(daily_returns)
    p5_day = sorted_drs[len(sorted_drs)//20] if sorted_drs else 0  # 5%ile
    return {
        'total_return': (cum-1)*100, 'max_dd': max_dd,
        'max_day_loss': max_day_loss, 'p5_day': p5_day,
        'daily_returns': daily_returns,
    }


VARIANTS = [
    ('slot1_100',         [1.0]),
    ('slot2_100_0',       [1.0, 0.0]),   # = slot1과 동일이지만 entry 2개
    ('slot2_90_10',       [0.9, 0.1]),   # v83.3
    ('slot2_80_20',       [0.8, 0.2]),   # v83 초기
    ('slot2_70_30',       [0.7, 0.3]),   # v82
    ('slot2_60_40',       [0.6, 0.4]),
    ('slot2_50_50',       [0.5, 0.5]),
    ('slot3_50_30_20',    [0.5, 0.3, 0.2]),
    ('slot3_40_35_25',    [0.4, 0.35, 0.25]),
    ('slot3_equal',       [1/3, 1/3, 1/3]),  # v80.10c
]


def main():
    print('=' * 110)
    print('최적 비중 grid BT (v83.3 vs 비중 완화)')
    print(f'params: entry={ENTRY_TOP}, exit={EXIT_TOP}, slots=variable')
    print(f'seeds: {N_SEEDS} × {SAMPLES_PER_SEED} paired')
    print('=' * 110)

    dates, data, price_full = load_data()
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    all_results = {}
    for name, w in VARIANTS:
        t0 = time.time()
        rets, mdds, mdls, p5s, seed_avgs = [], [], [], [], []
        for chosen in seed_starts:
            sr = []
            for sd in chosen:
                r = simulate_weighted(dates, data, price_full, w, sd,
                                     entry=ENTRY_TOP, exit_=EXIT_TOP)
                rets.append(r['total_return'])
                mdds.append(r['max_dd'])
                mdls.append(r['max_day_loss'])
                p5s.append(r['p5_day'])
                sr.append(r['total_return'])
            seed_avgs.append(sum(sr)/len(sr))
        all_results[name] = {
            'rets': rets, 'mdds': mdds, 'mdls': mdls, 'p5s': p5s,
            'seed_avgs': seed_avgs,
            'weights': w,
        }
        avg = sum(rets)/len(rets)
        med = sorted(rets)[len(rets)//2]
        std = statistics.pstdev(rets)
        mdd_worst = min(mdds)
        mdl_worst = min(mdls)
        sharpe = avg/std if std > 0 else 0
        marker = ' ★' if name == 'slot2_90_10' else '  '
        print(f'{marker}{name:<20} {str(w):<25} avg={avg:+6.2f}% med={med:+6.2f}% '
              f'std={std:5.1f} mdd={mdd_worst:+6.2f}% maxday={mdl_worst:+5.2f}% '
              f'sharpe={sharpe:+.2f} [{time.time()-t0:.1f}s]')

    # paired vs v83.3 (slot2_90_10)
    print()
    print('=' * 110)
    print('paired vs slot2_90_10 (v83.3 production baseline)')
    print('=' * 110)
    base = all_results['slot2_90_10']['seed_avgs']
    print(f'  {"variant":<18} {"avg lift":>10} {"med lift":>10} {"min":>10} {"max":>10} '
          f'{"wins":>10} {"verdict":>10}')
    print('  ' + '-' * 95)
    for name, _ in VARIANTS:
        if name == 'slot2_90_10':
            continue
        new = all_results[name]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts) / len(lifts)
        med_l = statistics.median(lifts)
        verdict = ('✓✓ 우월' if wins >= 70
                   else '✓ 우월' if wins >= 60
                   else '~ 동등' if wins >= 40
                   else '✗ 열세' if wins >= 30
                   else '✗✗ 열세')
        print(f'  {name:<18} {avg_l:+8.2f}%p {med_l:+8.2f}%p {min(lifts):+8.2f}%p '
              f'{max(lifts):+8.2f}%p {wins:>5}/{N_SEEDS}  {verdict}')

    # 변동성 비교 표 (잠 자기 좋은 비중 찾기용)
    print()
    print('=' * 110)
    print('변동성 비교 (사용자 멘탈 안정 관점)')
    print('=' * 110)
    print(f'  {"variant":<20} {"avg ret":>9} {"std":>7} {"MDD":>8} {"max day":>9} {"5%ile day":>10} '
          f'{"sharpe":>7} {"수익/std":>9}')
    print('  ' + '-' * 100)
    for name, _ in VARIANTS:
        r = all_results[name]
        avg = sum(r['rets'])/len(r['rets'])
        std = statistics.pstdev(r['rets'])
        mdd = min(r['mdds'])
        mdl = min(r['mdls'])
        p5_avg = sum(r['p5s']) / len(r['p5s'])
        sharpe = avg/std if std > 0 else 0
        eff = avg / std if std > 0 else 0
        marker = ' ★' if name == 'slot2_90_10' else '  '
        print(f'{marker}{name:<18} {avg:+7.2f}% {std:>6.1f} {mdd:+7.2f}% '
              f'{mdl:+7.2f}% {p5_avg:+8.2f}% {sharpe:+6.2f} {eff:>+8.2f}')


if __name__ == '__main__':
    main()
