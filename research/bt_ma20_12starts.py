"""MA20 vs current (production) — 12시작일 multistart 재검증

v80.10 검증 때 사용한 12시작일 고정 방법론으로 random 100seed BT (100/0) 재현 확인.
두 방법론 모두 일관되면 sample artifact 가능성 사실상 0.

방법:
  - 시작일: dates[0:12] (첫 12거래일에서 각각 시작)
  - 12개 paired 비교 (각 시작일 동일)
  - v80.10c production params: entry=3, exit=10, slots=3, hold=0
"""
import sys
import shutil
import sqlite3
import statistics
import time
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import pandas as pd
import daily_runner as dr
import bt_breakout_hold as bth
import bt_ma_filter_extended as ext

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'
PRICE_PARQUET = Path(__file__).parent / 'price_history_for_ma_bt.parquet'

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
    print('MA20 vs current — 12시작일 multistart 재검증')
    print(f'params: entry={ENTRY_TOP}, exit={EXIT_TOP}, slots={MAX_SLOTS}, hold={HOLD_DAYS}')
    print('=' * 100)

    # 기존 BT에서 만든 DB 재사용 (regenerate 결과 보존)
    db_current = GRID / 'ext_current.db'
    db_ma20 = GRID / 'ext_ma20.db'
    db_ma50 = GRID / 'ext_ma50.db'

    if not db_current.exists() or not db_ma20.exists():
        # 재생성
        print('\nDB 재생성 필요 — extended BT regenerate 호출')
        close_df = pd.read_parquet(PRICE_PARQUET)
        for name, period in [('current', None), ('ma20', 20), ('ma50', 50)]:
            db = GRID / f'ext_{name}.db'
            if db.exists():
                continue
            print(f'  [{name}] regenerate...')
            shutil.copy(DB_ORIGINAL, db)
            ma_df = close_df.rolling(window=period, min_periods=period).mean() if period else None
            ext.regenerate(db, name, ma_df)

    print('\n[current] 12시작일...')
    t0 = time.time()
    starts_cur, rets_cur, mdds_cur = run_multistart(db_current)
    print(f'  {time.time()-t0:.1f}s')

    print('[ma20] 12시작일...')
    t0 = time.time()
    starts_ma20, rets_ma20, mdds_ma20 = run_multistart(db_ma20)
    print(f'  {time.time()-t0:.1f}s')

    print('[ma50] 12시작일...')
    t0 = time.time()
    starts_ma50, rets_ma50, mdds_ma50 = run_multistart(db_ma50)
    print(f'  {time.time()-t0:.1f}s')

    # paired 비교 (시작일별)
    print()
    print('=' * 100)
    print(f'{"start_date":<14} {"current":>10} {"ma20":>10} {"ma50":>10} '
          f'{"lift_ma20":>11} {"lift_ma50":>11}')
    print('-' * 90)
    lifts_20, lifts_50 = [], []
    wins_20 = losses_20 = ties_20 = 0
    wins_50 = losses_50 = ties_50 = 0
    for sd, rc, r20, r50 in zip(starts_cur, rets_cur, rets_ma20, rets_ma50):
        l20 = r20 - rc
        l50 = r50 - rc
        lifts_20.append(l20)
        lifts_50.append(l50)
        if l20 > 0: wins_20 += 1
        elif l20 < 0: losses_20 += 1
        else: ties_20 += 1
        if l50 > 0: wins_50 += 1
        elif l50 < 0: losses_50 += 1
        else: ties_50 += 1
        print(f'  {sd:<12} {rc:+9.2f}% {r20:+9.2f}% {r50:+9.2f}% '
              f'{l20:+10.2f}%p {l50:+10.2f}%p')

    print('-' * 90)
    print(f'{"avg":<14} {sum(rets_cur)/len(rets_cur):+9.2f}% '
          f'{sum(rets_ma20)/len(rets_ma20):+9.2f}% '
          f'{sum(rets_ma50)/len(rets_ma50):+9.2f}% '
          f'{sum(lifts_20)/len(lifts_20):+10.2f}%p '
          f'{sum(lifts_50)/len(lifts_50):+10.2f}%p')
    print(f'{"min":<14} {min(rets_cur):+9.2f}% {min(rets_ma20):+9.2f}% {min(rets_ma50):+9.2f}% '
          f'{min(lifts_20):+10.2f}%p {min(lifts_50):+10.2f}%p')
    print(f'{"max":<14} {max(rets_cur):+9.2f}% {max(rets_ma20):+9.2f}% {max(rets_ma50):+9.2f}% '
          f'{max(lifts_20):+10.2f}%p {max(lifts_50):+10.2f}%p')
    print(f'{"worst MDD":<14} {min(mdds_cur):+9.2f}% {min(mdds_ma20):+9.2f}% {min(mdds_ma50):+9.2f}%')
    print()
    print(f'paired wins/losses/ties:')
    print(f'  ma20 vs current: {wins_20}/{losses_20}/{ties_20}')
    print(f'  ma50 vs current: {wins_50}/{losses_50}/{ties_50}')


if __name__ == '__main__':
    main()
