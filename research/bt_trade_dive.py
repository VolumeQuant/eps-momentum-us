"""baseline vs opt2 trade-by-trade 분석.

multistart simulate가 반환하는 trade 정보로 진짜 차이 추적.
trade = {ticker, entry_date, exit_date, entry_price, exit_price, return}
"""
import sys
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'sign_aware_dbs'


def get_trades(db_path, start_date='2026-02-10'):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    r = bts2.simulate(dates, data, 3, 8, 3, start_date=start_date)
    return r


def main():
    print('=' * 100)
    print('Trade-by-trade 분석 — 시작일 2/10 기준')
    print('=' * 100)

    base = get_trades(GRID / 'baseline.db')
    opt2 = get_trades(GRID / 'opt2.db')

    print(f'\nbaseline: 수익 {base["total_return"]:+.2f}%, MDD {base["max_dd"]:+.2f}%, '
          f'거래 {base["n_trades"]}, 미실현 보유 {base["n_open"]}')
    print(f'opt2:     수익 {opt2["total_return"]:+.2f}%, MDD {opt2["max_dd"]:+.2f}%, '
          f'거래 {opt2["n_trades"]}, 미실현 보유 {opt2["n_open"]}')

    # baseline trades
    print()
    print('=' * 100)
    print(f'baseline trades ({len(base["trades"])}건)')
    print('=' * 100)
    for t in sorted(base['trades'], key=lambda x: x['entry_date']):
        days = 0  # 보유일수 — entry_date~exit_date
        print(f'  {t["entry_date"]} → {t["exit_date"]} {t["ticker"]:<6} '
              f'${t["entry_price"]:>7.2f} → ${t["exit_price"]:>7.2f} = {t["return"]:+7.2f}%')

    # opt2 trades
    print()
    print('=' * 100)
    print(f'opt2 trades ({len(opt2["trades"])}건)')
    print('=' * 100)
    for t in sorted(opt2['trades'], key=lambda x: x['entry_date']):
        print(f'  {t["entry_date"]} → {t["exit_date"]} {t["ticker"]:<6} '
              f'${t["entry_price"]:>7.2f} → ${t["exit_price"]:>7.2f} = {t["return"]:+7.2f}%')

    # 비교
    print()
    print('=' * 100)
    print('차이 분석 — 같은 종목의 entry/exit 시점 차이')
    print('=' * 100)

    # 종목별 trade 매핑
    base_by_tk = defaultdict(list)
    for t in base['trades']:
        base_by_tk[t['ticker']].append(t)
    opt2_by_tk = defaultdict(list)
    for t in opt2['trades']:
        opt2_by_tk[t['ticker']].append(t)

    all_tk = set(base_by_tk.keys()) | set(opt2_by_tk.keys())
    only_base = set(base_by_tk.keys()) - set(opt2_by_tk.keys())
    only_opt2 = set(opt2_by_tk.keys()) - set(base_by_tk.keys())

    print(f'\nbaseline만 거래한 종목: {only_base}')
    for tk in only_base:
        for t in base_by_tk[tk]:
            print(f'  {tk}: {t["entry_date"]} → {t["exit_date"]} = {t["return"]:+.2f}%')

    print(f'\nopt2만 거래한 종목: {only_opt2}')
    for tk in only_opt2:
        for t in opt2_by_tk[tk]:
            print(f'  {tk}: {t["entry_date"]} → {t["exit_date"]} = {t["return"]:+.2f}%')

    print(f'\n공통 종목 — entry/exit 차이:')
    for tk in sorted(all_tk - only_base - only_opt2):
        b_trades = base_by_tk[tk]
        o_trades = opt2_by_tk[tk]
        if len(b_trades) != len(o_trades):
            print(f'  ⚠ {tk}: baseline {len(b_trades)}회 vs opt2 {len(o_trades)}회')
            continue
        for bt, ot in zip(b_trades, o_trades):
            if bt['entry_date'] != ot['entry_date'] or bt['exit_date'] != ot['exit_date']:
                print(f'  ⚠ {tk}: base {bt["entry_date"]}-{bt["exit_date"]} ({bt["return"]:+.1f}%)'
                      f' vs opt2 {ot["entry_date"]}-{ot["exit_date"]} ({ot["return"]:+.1f}%)')

    # 미실현 (현재 보유) 종목
    print()
    print(f'baseline 미실현 보유: {base["n_open"]}종목')
    print(f'opt2 미실현 보유: {opt2["n_open"]}종목')


if __name__ == '__main__':
    main()
