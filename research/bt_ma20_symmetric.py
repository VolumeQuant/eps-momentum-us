"""MA20 환경 — entry = slots 대칭 조합만 비교 (직관적)

(1,10,1): 단일 종목
(2,10,2): 2종목
(3,10,3): production 3종목
(4,10,4): 4종목 분산
(5,10,5): 5종목 분산
"""
import sys
import random
import statistics
import time
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import sqlite3
import bt_breakout_hold as bth

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'ma_filter_dbs'
DB_MA20 = GRID / 'ext_ma20.db'

N_SEEDS = 500
SAMPLES = 5
MIN_HOLD_DAYS = 10

CANDIDATES = [(1,10,1), (2,10,2), (3,10,3), (4,10,4), (5,10,5)]


def load_data_ban(banned):
    bth.DB_PATH = DB_MA20
    dates, data, price_series = bth.load_data_ext()
    for d in dates:
        for tk in list(data[d].keys()):
            if tk in banned:
                data[d][tk]['p2'] = None
                data[d][tk]['comp_rank'] = None
    return dates, data, price_series


def run(entry, exit_, slots, seed_starts, banned):
    dates, data, price_series = load_data_ban(banned)
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        seed_rets = []
        for sd in chosen:
            r = bth.simulate_hold(
                dates, data, price_series, hold_days=0,
                entry_top=entry, exit_top=exit_,
                max_slots=slots, start_date=sd
            )
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            seed_rets.append(r['total_return'])
        seed_avgs.append(sum(seed_rets) / len(seed_rets))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 100)
    print(f'MA20 환경 entry=slots 대칭 비교 ({N_SEEDS}seed × {SAMPLES} = {N_SEEDS*SAMPLES} sim)')
    print('=' * 100)

    bth.DB_PATH = DB_MA20
    dates, _, _ = bth.load_data_ext()
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    # super-winner Top 10
    conn = sqlite3.connect(DB_MA20)
    rows = conn.execute('''
        SELECT t1.ticker, (t2.price - t1.price) / t1.price * 100
        FROM ntm_screening t1
        JOIN ntm_screening t2 ON t1.ticker = t2.ticker
        WHERE t1.date = (SELECT MIN(date) FROM ntm_screening WHERE part2_rank IS NOT NULL)
          AND t2.date = (SELECT MAX(date) FROM ntm_screening)
          AND t1.price > 0 AND t2.price > 0
          AND (t2.price - t1.price) / t1.price > 0.5
        ORDER BY 2 DESC LIMIT 10
    ''').fetchall()
    conn.close()
    super_top10 = {r[0] for r in rows}

    scenarios = [
        ('전체', set()),
        ('SNDK 제외', {'SNDK'}),
        ('Top 10 super-winner 제외', super_top10),
    ]

    for label, banned in scenarios:
        print()
        print('=' * 100)
        print(f'시나리오: {label}')
        print('=' * 100)
        results = {}
        for entry, exit_, slots in CANDIDATES:
            spec = f'({entry},{exit_},{slots})'
            t0 = time.time()
            res = run(entry, exit_, slots, seed_starts, banned)
            avg = sum(res['rets'])/len(res['rets'])
            mdd = min(res['mdds'])
            ra = avg/abs(mdd) if mdd<0 else 0
            std = statistics.pstdev(res['rets'])
            sharpe = avg/std if std>0 else 0
            results[spec] = res
            marker = ' ★' if spec == '(3,10,3)' else '  '
            print(f'  [{time.time()-t0:>4.1f}s]{marker}{spec:<10} avg={avg:+6.2f}% '
                  f'mdd={mdd:+6.2f}% ra={ra:+5.2f} sharpe={sharpe:+.2f} std={std:.1f}')

        # paired vs (3,10,3)
        print(f'  vs (3,10,3) production paired:')
        base = results['(3,10,3)']['seed_avgs']
        for entry, exit_, slots in CANDIDATES:
            spec = f'({entry},{exit_},{slots})'
            if spec == '(3,10,3)':
                continue
            new = results[spec]['seed_avgs']
            lifts = [b - a for a, b in zip(base, new)]
            wins = sum(1 for l in lifts if l > 0)
            avg_l = sum(lifts) / len(lifts)
            print(f'    {spec}: lift {avg_l:+6.2f}%p, {wins}/{N_SEEDS} wins')


if __name__ == '__main__':
    main()
