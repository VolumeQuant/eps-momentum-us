"""w_gap weights — smooth/today_only 12시작일 multistart 재검증

random 100/0과 일관성 확인용. 기존 bt_wgap_weights.py가 만든 DB 재사용.
"""
import sys
import statistics
import time
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import bt_breakout_hold as bth

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'wgap_weight_dbs'

ENTRY_TOP = 3
EXIT_TOP = 10
MAX_SLOTS = 3
HOLD_DAYS = 0
N_STARTS = 12


def run_multistart(db_path):
    bth.DB_PATH = db_path
    dates, data, price_series = bth.load_data_ext()
    starts = dates[:N_STARTS]
    rets, mdds = [], []
    for sd in starts:
        r = bth.simulate_hold(
            dates, data, price_series, hold_days=HOLD_DAYS,
            entry_top=ENTRY_TOP, exit_top=EXIT_TOP,
            max_slots=MAX_SLOTS, start_date=sd
        )
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return starts, rets, mdds


def main():
    print('=' * 100)
    print('w_gap weights — 12시작일 multistart 재검증')
    print('=' * 100)

    db_cur = GRID / 'current_20_30_50.db'
    db_smooth = GRID / 'smooth_25_30_45.db'
    db_today = GRID / 'today_only_0_0_100.db'
    db_prop = GRID / 'proposed_10_20_70.db'

    print('\n[current 20/30/50] 12시작일...')
    starts_cur, rc, mc = run_multistart(db_cur)
    print('[smooth 25/30/45] 12시작일...')
    _, rs, ms = run_multistart(db_smooth)
    print('[today_only 0/0/100] 12시작일...')
    _, rt, mt = run_multistart(db_today)
    print('[proposed 10/20/70] 12시작일...')
    _, rp, mp = run_multistart(db_prop)

    print()
    print(f'{"start_date":<13} {"current":>9} {"proposed":>9} {"smooth":>9} {"today_only":>11} '
          f'{"lift_p":>10} {"lift_s":>10} {"lift_t":>10}')
    print('-' * 105)
    lifts_p, lifts_s, lifts_t = [], [], []
    for sd, c, p, s, t in zip(starts_cur, rc, rp, rs, rt):
        lp = p - c
        ls = s - c
        lt = t - c
        lifts_p.append(lp)
        lifts_s.append(ls)
        lifts_t.append(lt)
        print(f'  {sd:<11} {c:+8.2f}% {p:+8.2f}% {s:+8.2f}% {t:+10.2f}% '
              f'{lp:+9.2f}%p {ls:+9.2f}%p {lt:+9.2f}%p')

    print('-' * 105)
    print(f'{"avg":<13} {sum(rc)/len(rc):+8.2f}% {sum(rp)/len(rp):+8.2f}% '
          f'{sum(rs)/len(rs):+8.2f}% {sum(rt)/len(rt):+10.2f}% '
          f'{sum(lifts_p)/len(lifts_p):+9.2f}%p '
          f'{sum(lifts_s)/len(lifts_s):+9.2f}%p '
          f'{sum(lifts_t)/len(lifts_t):+9.2f}%p')

    wp = sum(1 for l in lifts_p if l > 0)
    ws = sum(1 for l in lifts_s if l > 0)
    wt = sum(1 for l in lifts_t if l > 0)
    print()
    print(f'paired wins:')
    print(f'  proposed 10/20/70 vs current: {wp}/{N_STARTS}')
    print(f'  smooth 25/30/45 vs current:   {ws}/{N_STARTS}')
    print(f'  today_only 100% vs current:   {wt}/{N_STARTS}')
    print()
    print(f'random 100seed × 3 (300 sim) 결과와 비교:')
    print(f'  proposed: random 60/36 wins, +0.24%p avg')
    print(f'  smooth:   random 100/0 wins, +13.31%p avg')
    print(f'  today:    random 100/0 wins, +13.43%p avg')


if __name__ == '__main__':
    main()
