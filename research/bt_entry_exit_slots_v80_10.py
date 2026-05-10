"""v80.10에서 진입/이탈/슬롯 grid 재최적화

현재: entry=Top 3, exit=Top 8, slots=3
검증: long-tail 패러다임에서 다른 조합이 더 나은지

Grid:
  entry: 1, 2, 3, 5, 8
  exit: 5, 8, 10, 15, 20
  slots: 1, 2, 3, 5

multistart 12 시작일 평균
"""
import sys
import statistics
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB = ROOT / 'eps_momentum_data.db'  # v80.10

ENTRY_VALUES = [1, 2, 3, 5, 8]
EXIT_VALUES = [5, 8, 10, 15, 20]
SLOTS_VALUES = [1, 2, 3, 5]


def multistart(dates, data, entry, exit_top, slots, n_starts=12):
    rets, mdds = [], []
    for sd in dates[:n_starts]:
        r = bts2.simulate(dates, data, entry, exit_top, slots, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    avg = sum(rets) / len(rets)
    worst_mdd = min(mdds)
    risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
    return avg, worst_mdd, risk_adj


def main():
    print('=' * 100)
    print('v80.10 진입/이탈/슬롯 grid 재최적화 (12시작일)')
    print('=' * 100)
    print(f'Grid: entry ∈ {ENTRY_VALUES} × exit ∈ {EXIT_VALUES} × slots ∈ {SLOTS_VALUES}')
    print(f'총 조합: {len(ENTRY_VALUES) * len(EXIT_VALUES) * len(SLOTS_VALUES)}개')

    bts2.DB_PATH = str(DB)
    dates, data = bts2.load_data()
    print(f'데이터: {dates[0]} ~ {dates[-1]} ({len(dates)}일)\n')

    results = []
    for entry in ENTRY_VALUES:
        for exit_top in EXIT_VALUES:
            if exit_top < entry: continue
            for slots in SLOTS_VALUES:
                if slots > entry: continue
                avg, mdd, risk = multistart(dates, data, entry, exit_top, slots)
                marker = ' ★' if (entry, exit_top, slots) == (3, 8, 3) else ''
                results.append({
                    'entry': entry, 'exit': exit_top, 'slots': slots,
                    'avg': avg, 'mdd': mdd, 'risk': risk, 'marker': marker,
                })

    # 출력
    print(f'{"entry":>5} {"exit":>5} {"slots":>5} {"avg":>9} {"MDD":>8} {"risk":>6}  ')
    print('-' * 60)
    by_avg = sorted(results, key=lambda x: x['avg'], reverse=True)
    print('=== Top 10 by avg return ===')
    for r in by_avg[:10]:
        print(f'{r["entry"]:>5} {r["exit"]:>5} {r["slots"]:>5} {r["avg"]:+8.2f}% {r["mdd"]:+7.2f}% {r["risk"]:>5.2f}{r["marker"]}')

    print()
    print('=== Top 10 by risk_adj ===')
    by_risk = sorted(results, key=lambda x: x['risk'], reverse=True)
    for r in by_risk[:10]:
        print(f'{r["entry"]:>5} {r["exit"]:>5} {r["slots"]:>5} {r["avg"]:+8.2f}% {r["mdd"]:+7.2f}% {r["risk"]:>5.2f}{r["marker"]}')

    # 현재 production 위치
    prod = next((r for r in results if r['entry'] == 3 and r['exit'] == 8 and r['slots'] == 3), None)
    if prod:
        avg_rank = sorted(results, key=lambda x: x['avg'], reverse=True).index(prod) + 1
        risk_rank = sorted(results, key=lambda x: x['risk'], reverse=True).index(prod) + 1
        print()
        print(f'=== 현재 production (entry=3, exit=8, slots=3) ===')
        print(f'  avg={prod["avg"]:+.2f}% MDD={prod["mdd"]:+.2f}% risk={prod["risk"]:.2f}')
        print(f'  avg 순위: {avg_rank}/{len(results)}')
        print(f'  risk 순위: {risk_rank}/{len(results)}')

    # 차원별 효과
    print()
    print('=== 차원별 평균 ===')
    for axis in ['entry', 'exit', 'slots']:
        by = {}
        for r in results:
            by.setdefault(r[axis], []).append(r)
        print(f'\n{axis}:')
        for v in sorted(by):
            avgs = [x['avg'] for x in by[v]]
            risks = [x['risk'] for x in by[v]]
            print(f'  {v:>3}: n={len(by[v]):>2}, avg_ret={sum(avgs)/len(avgs):+.2f}%, avg_risk={sum(risks)/len(risks):.2f}')


if __name__ == '__main__':
    main()
