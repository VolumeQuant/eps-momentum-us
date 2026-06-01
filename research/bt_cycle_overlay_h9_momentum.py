# -*- coding: utf-8 -*-
"""H9: Price momentum 추가 filter (KR v80.20 패턴 inspiration)
- 진입 후보 중 mom_X 음수면 reject, 다음 ✅로 슬라이드
- mom_10, mom_20 둘 다 시도
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10


def load_data():
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   adj_gap, score
            FROM ntm_screening WHERE date=?
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[3:8])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else: segs.append(0)
            data[d][tk] = {'p2': r[1], 'price': r[2],
                          'min_seg': min(segs) if segs else 0,
                          'adj_gap': r[8] or 0, 'score': r[9] or 0}
    # price_full + history per ticker (for momentum calculation)
    price_full = defaultdict(dict)
    ticker_dates = defaultdict(list)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL ORDER BY date'
    ):
        price_full[d][tk] = px
        ticker_dates[tk].append((d, px))
    conn.close()
    # Pre-compute mom_X for each (ticker, date)
    # mom_X = (price_today / price_X_trading_days_ago) - 1, but we use calendar lookup
    # 단순화: ticker별 정렬된 시계열에서 idx-X 사용
    mom_cache = defaultdict(dict)  # mom_cache[lookback][date][ticker] = mom value
    for lookback in [10, 20]:
        for tk, series in ticker_dates.items():
            # series = list of (date, price) 정렬됨
            for idx, (d, px) in enumerate(series):
                if idx < lookback or px is None or px <= 0:
                    continue
                px_prev = series[idx - lookback][1]
                if px_prev is None or px_prev <= 0:
                    continue
                mom = (px / px_prev) - 1
                mom_cache[lookback].setdefault(d, {})[tk] = mom
    return dates, data, price_full, mom_cache


def simulate(dates_all, data, price_full, mom_cache,
             min_mom_lookback=None,  # None or 10 or 20
             min_mom_threshold=0,  # mom > threshold만 통과
             entry=3, exit_=10,
             start_date=None):
    max_slots = 2
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    INIT_CAP = 100.0
    total_cash = INIT_CAP
    slot_holding = [None, None]
    daily_returns = []
    consecutive = defaultdict(int)
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    consecutive[tk] = consecutive.get(tk, 0) + 1

    prev_pv = INIT_CAP
    for di, today in enumerate(dates):
        if today not in data:
            daily_returns.append(0); continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}
        score_map = {tk: v['score'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consec = defaultdict(int)
        for tk in rank_map: new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        # PV
        pv_today = total_cash
        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, entry_price, _ = slot_holding[i]
            p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            if p is None: p = entry_price
            pv_today += shares * p
        if prev_pv > 0:
            daily_returns.append((pv_today - prev_pv) / prev_pv * 100)
        else: daily_returns.append(0)
        prev_pv = pv_today

        # 이탈
        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, entry_price, _ = slot_holding[i]
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            should_exit = False
            if min_seg < -2: should_exit = True
            elif rank is None or rank > exit_: should_exit = True
            if should_exit:
                exit_p = cur_p if cur_p else entry_price
                total_cash += shares * exit_p
                slot_holding[i] = None

        # 진입
        if slot_holding[0] is None and slot_holding[1] is None:
            # mom filter 적용
            today_mom = mom_cache.get(min_mom_lookback, {}).get(today, {}) if min_mom_lookback else {}
            cands = sorted(
                [(rank_map[tk], score_map.get(tk, 0), tk) for tk in rank_map
                 if rank_map[tk] <= entry
                 and (today_data[tk].get('min_seg') is None or today_data[tk]['min_seg'] >= 0)
                 and consecutive.get(tk, 0) >= 3
                 and today_data[tk].get('price')
                 and (min_mom_lookback is None or today_mom.get(tk, -1) > min_mom_threshold)],
                key=lambda x: x[0]
            )
            picked = cands[:max_slots]
            if len(picked) == 1:
                _, _, tk = picked[0]
                price = today_data[tk]['price']
                shares = total_cash / price
                slot_holding[0] = (tk, shares, price, today)
                total_cash = 0
            elif len(picked) >= 2:
                s1, s2 = picked[0][1], picked[1][1]
                gap = s1 - s2
                if gap >= 15: w = [1.0, 0.0]
                else: w = [0.5, 0.5]
                for i, (_, _, tk) in enumerate(picked[:2]):
                    if w[i] > 0:
                        price = today_data[tk]['price']
                        allocated = total_cash * w[i]
                        shares = allocated / price
                        slot_holding[i] = (tk, shares, price, today)
                used = sum(w[:2]) * total_cash
                total_cash = total_cash - used
        else:
            today_mom = mom_cache.get(min_mom_lookback, {}).get(today, {}) if min_mom_lookback else {}
            for i in range(max_slots):
                if slot_holding[i] is not None: continue
                if total_cash <= 0: continue
                cands = sorted(
                    [(rank_map[tk], tk) for tk in rank_map
                     if rank_map[tk] <= entry
                     and not any(h is not None and h[0] == tk for h in slot_holding)
                     and (today_data[tk].get('min_seg') is None or today_data[tk]['min_seg'] >= 0)
                     and consecutive.get(tk, 0) >= 3
                     and today_data[tk].get('price')
                     and (min_mom_lookback is None or today_mom.get(tk, -1) > min_mom_threshold)],
                    key=lambda x: x[0]
                )
                if cands:
                    p2_val, tk = cands[0]
                    price = today_data[tk]['price']
                    shares = total_cash / price
                    slot_holding[i] = (tk, shares, price, today)
                    total_cash = 0

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum - peak)/peak*100
        max_dd = min(max_dd, dd)
    return {
        'total_return': (cum-1)*100, 'max_dd': max_dd,
        'max_day_loss': min(daily_returns) if daily_returns else 0,
    }


VARIANTS = [
    ('baseline_v84',         {'min_mom_lookback': None}),
    ('H9_mom10>0',           {'min_mom_lookback': 10, 'min_mom_threshold': 0}),
    ('H9_mom10>0.02',        {'min_mom_lookback': 10, 'min_mom_threshold': 0.02}),
    ('H9_mom20>0',           {'min_mom_lookback': 20, 'min_mom_threshold': 0}),
    ('H9_mom20>0.05',        {'min_mom_lookback': 20, 'min_mom_threshold': 0.05}),
]


def main():
    print('='*100)
    print('H9: Price momentum filter (mom_X > threshold만 진입)')
    print('='*100)
    dates, data, price_full, mom_cache = load_data()
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    all_results = {}
    print(f'\n{"variant":<22}{"avg":>10}{"med":>10}{"std":>8}{"mdd":>10}{"sharpe":>10}')
    for name, kwargs in VARIANTS:
        rets, mdds, seed_avgs = [], [], []
        for chosen in seed_starts:
            sr = []
            for sd in chosen:
                r = simulate(dates, data, price_full, mom_cache, start_date=sd, **kwargs)
                rets.append(r['total_return']); mdds.append(r['max_dd'])
                sr.append(r['total_return'])
            seed_avgs.append(sum(sr)/len(sr))
        all_results[name] = {'seed_avgs': seed_avgs, 'rets': rets, 'mdds': mdds}
        avg = sum(rets)/len(rets); med = sorted(rets)[len(rets)//2]
        std = statistics.pstdev(rets); mdd = min(mdds)
        sharpe = avg/std if std > 0 else 0
        marker = ' ★' if name == 'baseline_v84' else '  '
        print(f'{marker}{name:<20}{avg:>+8.2f}%{med:>+8.2f}%{std:>8.1f}{mdd:>+8.2f}%{sharpe:>+8.2f}')

    print('\npaired vs baseline_v84:')
    print(f'  {"variant":<22}{"avg_lift":>10}{"wins":>10}')
    base = all_results['baseline_v84']['seed_avgs']
    for name, _ in VARIANTS:
        if name == 'baseline_v84': continue
        new = all_results[name]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        verdict = '✓✓ 우월' if wins >= 70 else '✓ 우월' if wins >= 60 else '~ 동등' if wins >= 40 else '✗ 열세'
        avg_l = sum(lifts)/len(lifts)
        print(f'  {name:<22}{avg_l:>+8.2f}%p{wins:>6}/{N_SEEDS}  {verdict}')


if __name__ == '__main__':
    main()
