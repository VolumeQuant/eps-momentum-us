# -*- coding: utf-8 -*-
"""v84 + H9 종합 robustness BT
- Baseline: v84 (part2_rank top 2 + 2step_t15 dynamic + dd_30_25는 DB의 part2_rank에 반영됨)
- v84+H9: H9 추가 (mom_20>X filter)
- robustness:
  - leave-one-winner-out (MU/SNDK/BE/AEIS 각각 + 동시)
  - period split (early vs late)
  - mom threshold sweep (0, 0.02, 0.05, 0.10)
  - 다른 layer 결합 (cash 20% + H9)
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
    # price_full + mom_20 precompute
    price_full = defaultdict(dict)
    ticker_series = defaultdict(list)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL ORDER BY date'
    ):
        price_full[d][tk] = px
        ticker_series[tk].append((d, px))
    conn.close()

    mom_20 = defaultdict(dict)
    for tk, series in ticker_series.items():
        for idx, (d, px) in enumerate(series):
            if idx < 20 or px is None or px <= 0: continue
            px_prev = series[idx - 20][1]
            if px_prev is None or px_prev <= 0: continue
            mom_20[d][tk] = (px / px_prev) - 1
    return dates, data, price_full, mom_20


def simulate(dates_all, data, price_full, mom_20,
             entry=3, exit_=10,
             use_h9=False, mom_threshold=0,
             cash_buffer=0,
             exclude=None,  # set of tickers to exclude
             start_date=None, end_date=None):
    max_slots = 2
    exclude = exclude or set()
    if start_date:
        dates = [d for d in dates_all if d >= start_date and (not end_date or d <= end_date)]
    else:
        dates = [d for d in dates_all if not end_date or d <= end_date]

    INIT_CAP = 100.0
    investable = INIT_CAP * (1 - cash_buffer)
    buffer = INIT_CAP * cash_buffer
    total_cash = investable
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
        today_mom = mom_20.get(today, {})
        new_consec = defaultdict(int)
        for tk in rank_map: new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        # PV
        pv_today = total_cash + buffer
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
            cands = sorted(
                [(rank_map[tk], score_map.get(tk, 0), tk) for tk in rank_map
                 if rank_map[tk] <= entry
                 and tk not in exclude
                 and (today_data[tk].get('min_seg') is None or today_data[tk]['min_seg'] >= 0)
                 and consecutive.get(tk, 0) >= 3
                 and today_data[tk].get('price')
                 and (not use_h9 or today_mom.get(tk, -1) > mom_threshold)],
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
            for i in range(max_slots):
                if slot_holding[i] is not None: continue
                if total_cash <= 0: continue
                cands = sorted(
                    [(rank_map[tk], tk) for tk in rank_map
                     if rank_map[tk] <= entry
                     and tk not in exclude
                     and not any(h is not None and h[0] == tk for h in slot_holding)
                     and (today_data[tk].get('min_seg') is None or today_data[tk]['min_seg'] >= 0)
                     and consecutive.get(tk, 0) >= 3
                     and today_data[tk].get('price')
                     and (not use_h9 or today_mom.get(tk, -1) > mom_threshold)],
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


def run_paired(dates, data, price_full, mom_20, **base_kwargs):
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, mom_20, start_date=sd, **base_kwargs)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def summary(name, res, base_seeds=None):
    rets = res['rets']; mdds = res['mdds']
    avg = sum(rets)/len(rets)
    med = sorted(rets)[len(rets)//2]
    std = statistics.pstdev(rets)
    mdd = min(mdds)
    sharpe = avg/std if std > 0 else 0
    cal = avg/abs(mdd) if mdd < 0 else 0
    line = f'{name:<28}{avg:>+8.2f}%{med:>+8.2f}%{std:>7.1f}{mdd:>+8.2f}%{sharpe:>+7.2f}{cal:>7.2f}'
    if base_seeds:
        new_seeds = res['seed_avgs']
        lifts = [b - a for a, b in zip(base_seeds, new_seeds)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts)/len(lifts)
        line += f'  lift {avg_l:+7.2f}%p  wins {wins:>3}/{N_SEEDS}'
    return line


def main():
    print('=' * 130)
    print('v84 + H9 종합 robustness BT')
    print('=' * 130)
    dates, data, price_full, mom_20 = load_data()
    print(f'dates: {len(dates)} ({dates[0]} ~ {dates[-1]})')
    print(f'header: {"name":<28}{"avg":>9}{"med":>9}{"std":>7}{"mdd":>9}{"Sh":>7}{"Cal":>7}  (lift vs v84)\n')

    # 1. Baseline + variants
    print('--- 1. Baseline + H9 variants (전체 기간) ---')
    base = run_paired(dates, data, price_full, mom_20)
    print(summary('baseline_v84', base))
    base_seeds = base['seed_avgs']

    h9_variants = [
        ('+H9 mom20>0',       {'use_h9': True, 'mom_threshold': 0}),
        ('+H9 mom20>0.02',    {'use_h9': True, 'mom_threshold': 0.02}),
        ('+H9 mom20>0.05',    {'use_h9': True, 'mom_threshold': 0.05}),
        ('+H9 mom20>0.10',    {'use_h9': True, 'mom_threshold': 0.10}),
    ]
    h9_results = {}
    for name, kw in h9_variants:
        r = run_paired(dates, data, price_full, mom_20, **kw)
        h9_results[name] = r
        print(summary(name, r, base_seeds))

    # 2. H9 + 다른 layer 결합
    print('\n--- 2. H9 + 다른 layer 결합 ---')
    combos = [
        ('+H9 mom20>0 + cash20%',  {'use_h9': True, 'mom_threshold': 0, 'cash_buffer': 0.20}),
        ('+H9 mom20>0 + cash10%',  {'use_h9': True, 'mom_threshold': 0, 'cash_buffer': 0.10}),
    ]
    for name, kw in combos:
        r = run_paired(dates, data, price_full, mom_20, **kw)
        print(summary(name, r, base_seeds))

    # 3. Leave-one-winner-out (MU, SNDK, BE, AEIS)
    print('\n--- 3. Leave-one-winner-out (v84 vs +H9 비교) ---')
    print(f'{"":<28}{"v84":>20}{"+H9 mom20>0":>20}  H9 lift vs v84')
    winners = [None, ['MU'], ['SNDK'], ['BE'], ['AEIS'], ['MU','SNDK'], ['MU','SNDK','BE','AEIS']]
    for ex in winners:
        ex_label = '+'.join(ex) if ex else 'NONE'
        exset = set(ex) if ex else set()
        base_ex = run_paired(dates, data, price_full, mom_20, exclude=exset)
        h9_ex = run_paired(dates, data, price_full, mom_20, exclude=exset, use_h9=True, mom_threshold=0)
        base_avg = sum(base_ex['rets'])/len(base_ex['rets'])
        h9_avg = sum(h9_ex['rets'])/len(h9_ex['rets'])
        base_mdd = min(base_ex['mdds'])
        h9_mdd = min(h9_ex['mdds'])
        lifts = [b - a for a, b in zip(base_ex['seed_avgs'], h9_ex['seed_avgs'])]
        wins = sum(1 for l in lifts if l > 0)
        print(f'  exclude {ex_label:<20}  v84 {base_avg:+7.1f}%/MDD{base_mdd:+6.1f}  +H9 {h9_avg:+7.1f}%/MDD{h9_mdd:+6.1f}  lift {sum(lifts)/len(lifts):+6.1f}%p  wins {wins}/{N_SEEDS}')

    # 4. Period split
    print('\n--- 4. 기간 분할 (early half vs late half) ---')
    mid_idx = len(dates) // 2
    mid_date = dates[mid_idx]
    print(f'  early: {dates[0]} ~ {dates[mid_idx-1]}  ({mid_idx}일)')
    print(f'  late:  {mid_date} ~ {dates[-1]}  ({len(dates)-mid_idx}일)')

    def period_paired(start, end_idx, **kwargs):
        eligible_starts = dates[start:end_idx-MIN_HOLD_DAYS]
        if len(eligible_starts) < SAMPLES_PER_SEED:
            return None
        seed_starts = []
        for seed_i in range(N_SEEDS):
            random.seed(seed_i)
            seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
        rets, mdds, seed_avgs = [], [], []
        end_date = dates[end_idx-1]
        for chosen in seed_starts:
            sr = []
            for sd in chosen:
                r = simulate(dates, data, price_full, mom_20, start_date=sd, end_date=end_date, **kwargs)
                rets.append(r['total_return']); mdds.append(r['max_dd'])
                sr.append(r['total_return'])
            seed_avgs.append(sum(sr)/len(sr))
        return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}

    for label, (s, e) in [('early', (0, mid_idx)), ('late', (mid_idx, len(dates)))]:
        base_p = period_paired(s, e)
        h9_p = period_paired(s, e, use_h9=True, mom_threshold=0)
        if base_p and h9_p:
            print(f'  {label}:')
            print(f'    {summary("v84", base_p)}')
            print(f'    {summary("v84+H9", h9_p, base_p["seed_avgs"])}')

    # 5. Final summary table
    print('\n' + '='*130)
    print('★ 최종 채택 권고 표')
    print('='*130)
    print(f'{"option":<28}{"return":>10}{"MDD":>10}{"Sharpe":>9}{"Calmar":>9}{"vs v84 lift":>15}{"wins":>10}')
    print('-' * 91)
    print(f'{"baseline v84":<28}{sum(base["rets"])/len(base["rets"]):+9.1f}%{min(base["mdds"]):+9.1f}%')
    for name, r in h9_results.items():
        avg = sum(r["rets"])/len(r["rets"])
        mdd = min(r["mdds"])
        std = statistics.pstdev(r["rets"])
        sh = avg/std if std>0 else 0
        cal = avg/abs(mdd) if mdd<0 else 0
        lifts = [b-a for a,b in zip(base_seeds, r['seed_avgs'])]
        wins = sum(1 for l in lifts if l>0)
        avg_l = sum(lifts)/len(lifts)
        print(f'{name:<28}{avg:+9.1f}%{mdd:+9.1f}%{sh:>9.2f}{cal:>9.2f}{avg_l:>13.1f}%p{wins:>7}/{N_SEEDS}')


if __name__ == '__main__':
    main()
