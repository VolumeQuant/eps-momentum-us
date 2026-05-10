"""v80.9 (production 이전) vs v80.10 (현재) 매수/매도 비교

backup DB(bak_pre_v80_10.db)와 현재 DB(eps_momentum_data.db)를 같은 룰로 BT.
첫 검증 완료 시점부터 현재까지 trade-by-trade 비교.
"""
import sys
import sqlite3
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB_OLD = ROOT / 'eps_momentum_data.bak_pre_v80_10.db'  # v80.9
DB_NEW = ROOT / 'eps_momentum_data.db'                  # v80.10


def run_bt(db_path, label):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    first_date = dates[0]
    # consecutive 3 검증 시작일 = 3번째 영업일
    result = bts2.simulate(dates, data, 3, 8, 3, start_date=first_date)
    return dates, data, result


def main():
    print('=' * 100)
    print('v80.9 (이전 production) vs v80.10 (현재) 비교')
    print('=' * 100)

    # 데이터 양쪽 동일 확인
    for db, label in [(DB_OLD, 'v80.9 backup'), (DB_NEW, 'v80.10 current')]:
        bts2.DB_PATH = str(db)
        dates, _ = bts2.load_data()
        print(f'  {label}: {dates[0]} ~ {dates[-1]} ({len(dates)}일)')

    print()
    print('=' * 100)
    print('단일 시작일 (첫 영업일부터, consecutive 3 검증 후 진입) BT')
    print('=' * 100)

    results = {}
    for db, label in [(DB_OLD, 'v80.9'), (DB_NEW, 'v80.10')]:
        dates, data, r = run_bt(db, label)
        results[label] = (dates, data, r)
        print(f'\n[{label}] 결과:')
        print(f'  누적 수익률: {r["total_return"]:+.2f}%')
        print(f'  MDD: {r["max_dd"]:+.2f}%')
        print(f'  거래 수: {len(r["trades"])}')

    # 트레이드 비교
    print()
    print('=' * 100)
    print('Trade-by-Trade 비교')
    print('=' * 100)

    trades_old = results['v80.9'][2]['trades']
    trades_new = results['v80.10'][2]['trades']

    print(f'\n[v80.9 trades ({len(trades_old)}개)]:')
    print(f'{"#":>3} {"진입일":<12} {"이탈일":<12} {"종목":<8} {"수익률":>9}')
    for i, t in enumerate(trades_old, 1):
        print(f'{i:>3} {t["entry_date"]:<12} {t["exit_date"]:<12} {t["ticker"]:<8} {t["return"]:+8.2f}%')

    print(f'\n[v80.10 trades ({len(trades_new)}개)]:')
    print(f'{"#":>3} {"진입일":<12} {"이탈일":<12} {"종목":<8} {"수익률":>9}')
    for i, t in enumerate(trades_new, 1):
        print(f'{i:>3} {t["entry_date"]:<12} {t["exit_date"]:<12} {t["ticker"]:<8} {t["return"]:+8.2f}%')

    # 매일 portfolio 비교
    print()
    print('=' * 100)
    print('일별 portfolio Top 3 비교 (5/5 ~ 5/8 최근)')
    print('=' * 100)

    print(f'\n{"날짜":<12} {"v80.9 Top 3":<35} {"v80.10 Top 3":<35} {"교체":<8}')
    print('-' * 100)
    recent_dates = sorted(set(results['v80.9'][0]) | set(results['v80.10'][0]))[-15:]
    for d in recent_dates:
        old_top = []
        new_top = []
        for label, key in [('v80.9', 'v80.9'), ('v80.10', 'v80.10')]:
            data = results[label][1].get(d, {})
            top = sorted([(tk, v['p2']) for tk, v in data.items() if v.get('p2') and v['p2'] <= 3],
                         key=lambda x: x[1])
            if label == 'v80.9':
                old_top = [tk for tk, _ in top]
            else:
                new_top = [tk for tk, _ in top]
        old_str = ', '.join(f'{tk}({i+1})' for i, tk in enumerate(old_top[:3]))
        new_str = ', '.join(f'{tk}({i+1})' for i, tk in enumerate(new_top[:3]))
        diff = len(set(old_top[:3]) ^ set(new_top[:3]))
        print(f'{d:<12} {old_str:<35} {new_str:<35} {diff}/3 changed')


if __name__ == '__main__':
    main()
