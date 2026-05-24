"""표준 지표 계산 — CAGR, Sharpe, Sortino, Calmar, MDD

simulate_hold가 일별 수익률을 반환하도록 wrapper 추가.
v81 (MA20) vs v80.10c (MA120+fallback) 비교 + 시스템 누적 수익률.

지표 정의:
  - Total Return: 65일 BT 누적 수익률 (%)
  - CAGR: (1 + total_return)^(252/65) - 1 — 연환산
  - MDD: max drawdown (음수, 더 작을수록 안전)
  - Sharpe: avg_daily_return / std_daily_return × sqrt(252)
  - Sortino: avg_daily_return / downside_std × sqrt(252)
  - Calmar: CAGR / |MDD|
"""
import sys
import sqlite3
import random
import math
import statistics
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'ma_filter_dbs'


def load_data_ext(db_path):
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
                    segs.append(max(-100, min(100, (a - b) / abs(b) * 100)))
                else:
                    segs.append(0)
            data[d][tk] = {'p2': r[1], 'price': r[2], 'min_seg': min(segs) if segs else 0}
    conn.close()
    return dates, data


def simulate_with_returns(dates_all, data, entry, exit_, slots, start_date=None):
    """simulate_hold + daily_returns 반환"""
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

        # daily return
        day_ret = 0
        if portfolio:
            n = 0
            for tk in portfolio:
                price = today_data.get(tk, {}).get('price')
                if price and di > 0:
                    prev = data.get(dates[di - 1], {}).get(tk, {}).get('price')
                    if prev and prev > 0:
                        day_ret += (price - prev) / prev * 100
                        n += 1
            if n > 0:
                day_ret /= n
        daily_returns.append(day_ret)

        # 이탈
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2 or rank is None or rank > exit_:
                exited.append(tk)
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

    return daily_returns


def compute_metrics(daily_returns):
    """daily_returns list (%) → 표준 지표"""
    if not daily_returns:
        return None
    n_days = len(daily_returns)
    # cumulative
    cum = 1.0
    peak = 1.0
    max_dd = 0
    for r in daily_returns:
        cum *= (1 + r / 100)
        peak = max(peak, cum)
        dd = (cum - peak) / peak * 100
        max_dd = min(max_dd, dd)
    total_return = (cum - 1) * 100  # %
    # CAGR: 연환산 (252 거래일/년)
    years = n_days / 252
    cagr = ((cum) ** (1 / years) - 1) * 100 if years > 0 else 0
    # Sharpe (daily ret, annualized)
    if len(daily_returns) > 1:
        avg = sum(daily_returns) / len(daily_returns)
        std = statistics.pstdev(daily_returns)
        sharpe = (avg / std) * math.sqrt(252) if std > 0 else 0
        # Sortino
        downside = [r for r in daily_returns if r < 0]
        if len(downside) > 1:
            down_std = math.sqrt(sum(r**2 for r in downside) / len(daily_returns))
            sortino = (avg / down_std) * math.sqrt(252) if down_std > 0 else 0
        else:
            sortino = float('inf') if avg > 0 else 0
    else:
        sharpe = sortino = 0
    # Calmar
    calmar = cagr / abs(max_dd) if max_dd < 0 else 0
    return {
        'total_return': total_return,
        'cagr': cagr,
        'mdd': max_dd,
        'sharpe': sharpe,
        'sortino': sortino,
        'calmar': calmar,
        'n_days': n_days,
    }


def main():
    print('=' * 100)
    print('표준 지표 계산 — v81 vs v80.10c (commodity 필터 fix 후)')
    print('=' * 100)

    db_current = GRID / 'ext_current.db'  # MA120+fallback (commodity fix)
    db_ma20 = GRID / 'ext_ma20.db'        # MA20 (commodity fix)

    # 전체 기간 단일 BT (시작일=첫날)
    print('\n[1] 전체 기간 단일 BT (시작 2026-02-12, 약 65거래일)')
    print('=' * 100)
    for name, db in [('v80.10c (MA120+fb)', db_current), ('v81 (MA20)', db_ma20)]:
        dates, data = load_data_ext(db)
        # production v80.10c: (3, 10, 3)
        rets = simulate_with_returns(dates, data, 3, 10, 3, start_date=dates[0])
        m = compute_metrics(rets)
        print(f'  {name}')
        print(f'    Total Return: {m["total_return"]:+7.2f}%  ({m["n_days"]}일)')
        print(f'    CAGR:         {m["cagr"]:+7.2f}%  (연환산)')
        print(f'    MDD:          {m["mdd"]:+7.2f}%')
        print(f'    Sharpe:       {m["sharpe"]:+7.2f}')
        print(f'    Sortino:      {m["sortino"]:+7.2f}')
        print(f'    Calmar:       {m["calmar"]:+7.2f}')
        print()

    # multistart 12시작일 평균
    print('[2] 12시작일 multistart 평균 (각 시작일에서 끝까지)')
    print('=' * 100)
    for name, db in [('v80.10c (MA120+fb)', db_current), ('v81 (MA20)', db_ma20)]:
        dates, data = load_data_ext(db)
        starts = dates[:12]
        all_metrics = []
        for sd in starts:
            rets = simulate_with_returns(dates, data, 3, 10, 3, start_date=sd)
            m = compute_metrics(rets)
            all_metrics.append(m)
        avg = {k: sum(m[k] for m in all_metrics) / len(all_metrics) for k in
               ('total_return', 'cagr', 'mdd', 'sharpe', 'sortino', 'calmar')}
        worst_mdd = min(m['mdd'] for m in all_metrics)
        print(f'  {name}')
        print(f'    Avg Total Return: {avg["total_return"]:+7.2f}%')
        print(f'    Avg CAGR:         {avg["cagr"]:+7.2f}%')
        print(f'    Avg MDD:          {avg["mdd"]:+7.2f}%  (worst {worst_mdd:+.2f}%)')
        print(f'    Avg Sharpe:       {avg["sharpe"]:+7.2f}')
        print(f'    Avg Sortino:      {avg["sortino"]:+7.2f}')
        print(f'    Avg Calmar:       {avg["calmar"]:+7.2f}')
        print()

    # random 100 seed × 3 (paired)
    print('[3] random 100 seed × 3 samples (300 sim) 평균')
    print('=' * 100)
    MIN_HOLD = 10

    for name, db in [('v80.10c (MA120+fb)', db_current), ('v81 (MA20)', db_ma20)]:
        dates, data = load_data_ext(db)
        eligible = dates[:-MIN_HOLD]
        seed_starts = []
        for seed_i in range(100):
            random.seed(seed_i)
            seed_starts.append(random.sample(eligible, 3))
        all_m = []
        for chosen in seed_starts:
            for sd in chosen:
                rets = simulate_with_returns(dates, data, 3, 10, 3, start_date=sd)
                m = compute_metrics(rets)
                all_m.append(m)
        # filter inf Sortino
        finite_sort = [m['sortino'] for m in all_m if not math.isinf(m['sortino'])]
        avg_sortino = sum(finite_sort)/len(finite_sort) if finite_sort else 0
        avg = {
            'total_return': sum(m['total_return'] for m in all_m) / len(all_m),
            'cagr': sum(m['cagr'] for m in all_m) / len(all_m),
            'mdd': sum(m['mdd'] for m in all_m) / len(all_m),
            'worst_mdd': min(m['mdd'] for m in all_m),
            'sharpe': sum(m['sharpe'] for m in all_m) / len(all_m),
            'sortino': avg_sortino,
            'calmar': sum(m['calmar'] for m in all_m) / len(all_m),
        }
        print(f'  {name}')
        print(f'    Avg Total Return: {avg["total_return"]:+7.2f}%')
        print(f'    Avg CAGR:         {avg["cagr"]:+7.2f}%')
        print(f'    Avg MDD:          {avg["mdd"]:+7.2f}%  (worst {avg["worst_mdd"]:+.2f}%)')
        print(f'    Avg Sharpe:       {avg["sharpe"]:+7.2f}')
        print(f'    Avg Sortino:      {avg["sortino"]:+7.2f}  (finite only)')
        print(f'    Avg Calmar:       {avg["calmar"]:+7.2f}')
        print()


if __name__ == '__main__':
    main()
