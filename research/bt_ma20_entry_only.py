"""MA20 매수 진입만 적용 (보유 종목은 rank로만 판단) — Option B BT

Option A (이전 BT): MA20 = entry filter + ranking filter
  → part2_rank에 MA20↓ 종목 없음 → rank NULL → rank>10 즉시 매도
  → 사실상 MA20 이탈 = 즉시 매도

Option B (이 BT): MA20 = entry filter only
  → part2_rank는 MA20 무관하게 모든 종목에 부여 (no_ma 기반)
  → 진입 시점에만 MA20 추가 체크
  → 보유 종목은 rank>10일 때만 매도 (MA20 무관)

비교:
  A: 이전 +27.57%p, MDD -10.35%, 100/0 wins
  B: 이번 BT
"""
import sys
import random
import statistics
import time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import sqlite3
import pandas as pd
import bt_breakout_hold as bth

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'ma_filter_dbs'
PRICE_PARQUET = Path(__file__).parent / 'price_history_for_ma_bt.parquet'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 3
EXIT_TOP = 10
MAX_SLOTS = 3


def load_data_with_ma20(db_path, ma20_df):
    """no_ma DB 사용 + ma20 dict 추가 부착"""
    bth.DB_PATH = db_path
    dates, data, price_series = bth.load_data_ext()

    # 각 (date, ticker)에 ma20 추가
    for d in dates:
        for tk in list(data[d].keys()):
            if tk in ma20_df.columns:
                # date 이전 (포함) 가장 가까운 거래일의 ma20
                col = ma20_df[tk]
                eligible = col.index[col.index <= d]
                if len(eligible) > 0:
                    val = col.loc[eligible[-1]]
                    data[d][tk]['ma20'] = float(val) if pd.notna(val) else None
                else:
                    data[d][tk]['ma20'] = None
            else:
                data[d][tk]['ma20'] = None
    return dates, data, price_series


def simulate_entry_only_ma20(dates_all, data, price_series, start_date=None):
    """매수 진입에만 MA20 적용, 매도는 rank>10/min_seg<-2"""
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    portfolio = {}
    daily_returns = []
    trades = []
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

        new_consecutive = defaultdict(int)
        for tk in rank_map:
            new_consecutive[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consecutive

        day_ret = 0
        if portfolio:
            for tk in portfolio:
                price = today_data.get(tk, {}).get('price')
                if price and di > 0:
                    prev = data.get(dates[di - 1], {}).get(tk, {}).get('price')
                    if prev and prev > 0:
                        day_ret += (price - prev) / prev * 100
            day_ret /= len(portfolio)
            daily_returns.append(day_ret)
        else:
            daily_returns.append(0)

        # 이탈 — rank>10 또는 min_seg<-2 (MA20 무관)
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            price = today_data.get(tk, {}).get('price')

            if min_seg < -2:
                if price:
                    ep = portfolio[tk]['entry_price']
                    trades.append({'ticker': tk, 'return': (price-ep)/ep*100, 'reason': 'min_seg'})
                exited.append(tk)
                continue

            out_top = (rank is None) or (rank > EXIT_TOP)
            if out_top:
                if price:
                    ep = portfolio[tk]['entry_price']
                    trades.append({'ticker': tk, 'return': (price-ep)/ep*100, 'reason': 'rank_exit'})
                exited.append(tk)

        for tk in exited:
            del portfolio[tk]

        # 진입 — Top 3 중 MA20 위 종목만
        vacancies = MAX_SLOTS - len(portfolio)
        if vacancies > 0:
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > ENTRY_TOP or vacancies <= 0:
                    break
                if tk in portfolio:
                    continue
                if consecutive.get(tk, 0) < 3:
                    continue
                min_seg = today_data.get(tk, {}).get('min_seg', 0)
                if min_seg < 0:
                    continue
                price = today_data.get(tk, {}).get('price')
                ma20 = today_data.get(tk, {}).get('ma20')
                # MA20 진입 체크 (Option B의 핵심)
                if not price or price <= 0:
                    continue
                if ma20 is None or price <= ma20:
                    continue
                portfolio[tk] = {'entry_price': price, 'entry_date': today}
                vacancies -= 1

    cum_ret = 1.0
    peak = 1.0
    max_dd = 0
    for dr_ in daily_returns:
        cum_ret *= (1 + dr_ / 100)
        peak = max(peak, cum_ret)
        dd = (cum_ret - peak) / peak * 100
        max_dd = min(max_dd, dd)

    return {
        'total_return': round((cum_ret - 1) * 100, 2),
        'max_dd': round(max_dd, 2),
        'n_trades': len(trades),
        'trades': trades,
    }


def run_bt_b(db_path, ma20_df):
    dates, data, price_series = load_data_with_ma20(db_path, ma20_df)
    if len(dates) <= MIN_HOLD_DAYS:
        return None
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        seed_rets = []
        for sd in chosen:
            r = simulate_entry_only_ma20(dates, data, price_series, start_date=sd)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            seed_rets.append(r['total_return'])
        seed_avgs.append(sum(seed_rets) / len(seed_rets))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def run_bt_baseline(db_path, hold=0):
    """baseline 또는 Option A (기존 simulate_hold 사용)"""
    bth.DB_PATH = db_path
    dates, data, price_series = bth.load_data_ext()
    if len(dates) <= MIN_HOLD_DAYS:
        return None
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        seed_rets = []
        for sd in chosen:
            r = bth.simulate_hold(
                dates, data, price_series, hold_days=hold,
                entry_top=ENTRY_TOP, exit_top=EXIT_TOP,
                max_slots=MAX_SLOTS, start_date=sd
            )
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            seed_rets.append(r['total_return'])
        seed_avgs.append(sum(seed_rets) / len(seed_rets))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 110)
    print('MA20 진입만 vs 진입+매도 — Option A vs B 비교')
    print(f'params: entry={ENTRY_TOP}, exit={EXIT_TOP}, slots={MAX_SLOTS}')
    print(f'seeds: {N_SEEDS} × {SAMPLES_PER_SEED} = {N_SEEDS*SAMPLES_PER_SEED} 시뮬/변형')
    print('=' * 110)

    close_df = pd.read_parquet(PRICE_PARQUET)
    ma20_df = close_df.rolling(window=20, min_periods=20).mean()

    db_baseline = GRID / 'ext_current.db'  # production: MA120+fallback
    db_a = GRID / 'ext_ma20.db'             # Option A: MA20 filter (rank에도 반영)
    db_no_ma = GRID / 'ext_no_ma.db'        # Option B base: no MA filter, full ranks

    print('\n[baseline] production (MA120 + MA60 fallback)...')
    t0 = time.time()
    res_base = run_bt_baseline(db_baseline)
    avg = sum(res_base['rets'])/len(res_base['rets'])
    mdd = min(res_base['mdds'])
    print(f'  {time.time()-t0:.1f}s | avg={avg:+.2f}% mdd={mdd:+.2f}%')

    print('\n[Option A] MA20 entry + 즉시 매도 (rank=NULL → rank>10 트리거)...')
    t0 = time.time()
    res_a = run_bt_baseline(db_a)
    avg = sum(res_a['rets'])/len(res_a['rets'])
    mdd = min(res_a['mdds'])
    print(f'  {time.time()-t0:.1f}s | avg={avg:+.2f}% mdd={mdd:+.2f}%')

    print('\n[Option B] MA20 entry only, 보유는 rank>10만 (no_ma rank pool)...')
    t0 = time.time()
    res_b = run_bt_b(db_no_ma, ma20_df)
    avg = sum(res_b['rets'])/len(res_b['rets'])
    mdd = min(res_b['mdds'])
    print(f'  {time.time()-t0:.1f}s | avg={avg:+.2f}% mdd={mdd:+.2f}%')

    print()
    print('=' * 110)
    print('비교 (300 시뮬)')
    print('=' * 110)
    print(f'{"variant":<22} {"avg":>9} {"median":>9} {"std":>6} '
          f'{"min":>9} {"max":>9} {"MDD":>8} {"ra":>7}')
    print('-' * 100)
    for name, res in [('baseline (MA120+FB)', res_base),
                       ('Option A (즉시 매도)', res_a),
                       ('Option B (보유 유지)', res_b)]:
        rets = sorted(res['rets'])
        n = len(rets)
        avg = sum(rets) / n
        med = rets[n // 2]
        std = statistics.pstdev(rets)
        mdd = min(res['mdds'])
        ra = avg / abs(mdd) if mdd < 0 else 0
        marker = ' ★' if 'baseline' in name else '  '
        print(f'{marker}{name:<20} {avg:+8.2f}% {med:+8.2f}% {std:>5.2f} '
              f'{min(rets):+8.2f}% {max(rets):+8.2f}% {mdd:+7.2f}% {ra:+6.2f}')

    print()
    print('=' * 110)
    print('baseline 대비 paired lift')
    print('=' * 110)
    base = res_base['seed_avgs']
    for name, res in [('Option A (즉시 매도)', res_a), ('Option B (보유 유지)', res_b)]:
        new = res['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        losses = sum(1 for l in lifts if l < 0)
        ties = sum(1 for l in lifts if l == 0)
        avg_lift = sum(lifts) / len(lifts)
        verdict = '✓ 우월' if wins >= 70 else '✗ 열세' if losses >= 70 else '~ 동등'
        print(f'  {name:<22} {avg_lift:+9.2f}%p min={min(lifts):+.2f}%p max={max(lifts):+.2f}%p '
              f'{wins}/{losses}/{ties}  {verdict}')

    # A vs B paired
    print()
    print('=' * 110)
    print('Option A vs Option B paired (어느 게 더 좋은가)')
    print('=' * 110)
    a = res_a['seed_avgs']
    b = res_b['seed_avgs']
    lifts = [bb - aa for aa, bb in zip(a, b)]
    wins_b = sum(1 for l in lifts if l > 0)
    losses_b = sum(1 for l in lifts if l < 0)
    ties_b = sum(1 for l in lifts if l == 0)
    avg_lift = sum(lifts) / len(lifts)
    print(f'  B vs A: avg_lift {avg_lift:+.2f}%p  min={min(lifts):+.2f}%p max={max(lifts):+.2f}%p')
    print(f'  paired: B 우월 {wins_b} / A 우월 {losses_b} / tie {ties_b}')


if __name__ == '__main__':
    main()
