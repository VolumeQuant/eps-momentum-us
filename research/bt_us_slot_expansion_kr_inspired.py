# -*- coding: utf-8 -*-
"""US v84 slot 확대 + universe 필터 + fragility BT
KR 인사이트 적용:
1. Slot 3→5/7 확대 시 KR Cal +30% 발견 → US v84에 적용
2. Universe 필터 (S&P 500 only vs 동적 $5B+ 차이)
3. Fragility (Top 5 winner 제외)

entry_fixed simulator (production 정합), 100 seeds × 3 paired
"""
import sys, sqlite3, random, statistics, time, json
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
TI_PATH = ROOT / 'ticker_info_cache.json'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10


def load_data():
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price, market_cap,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   adj_gap, score, high30
            FROM ntm_screening WHERE date=?''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else: segs.append(0)
            data[d][tk] = {'p2': r[1], 'price': r[2], 'mcap': r[3] or 0,
                          'min_seg': min(segs) if segs else 0,
                          'adj_gap': r[9] or 0, 'score': r[10] or 0,
                          'high30': r[11]}
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def verified_cr(t, i, dates, data):
    for j in (i, i-1, i-2):
        if j < 0: return False
        info = data[dates[j]].get(t)
        if not info or info['p2'] is None or info['p2'] > 30: return False
    return True


def simulate(dates_all, data, price_full,
             slots=2, entry=3, exit_=10,
             weights_mode='2step_t15',
             min_mcap=None,  # filter: min market cap in USD
             exclude_tickers=None,
             dd_30_25_filter=True,
             start_idx=0):
    """v84 entry_fixed simulator with KR-inspired variants"""
    exclude = set(exclude_tickers or [])
    held = {}  # ticker -> (entry_date, entry_price, slot_idx)
    prev_held = None
    value = 1.0; peak = 1.0; mdd = 0.0
    daily_rets = []

    for i, d in enumerate(dates_all):
        if i < start_idx: continue
        if prev_held and i > start_idx:
            d_prev = dates_all[i-1]
            items = list(prev_held.keys())
            ranks = sorted(((t, data[d_prev].get(t, {}).get('p2', 999) or 999) for t in items), key=lambda x: x[1])
            n = len(items); ret = 0
            if n == 0: pass
            elif n == 1:
                t = items[0]
                pp = price_full[d_prev].get(t); pn = price_full[d].get(t, pp)
                ret = (pn/pp - 1) if (pp and pn) else 0
            else:
                # equal weight for slot 3+, 2step for slot 2
                if n == 2 and weights_mode == '2step_t15':
                    # determine weight from current d_prev scores (sticky from entry)
                    weights_map = {prev_held[t][2]: t for t in items}  # slot_idx -> ticker
                    # use slot_idx for weight assignment
                    slot_idx_to_w = prev_held[items[0]][3] if len(prev_held[items[0]]) >= 4 else None
                    # fallback: rank-based
                    if slot_idx_to_w is None:
                        # use the stored weight in prev_held value (slot_idx)
                        weights = []
                        for t in items:
                            sidx = prev_held[t][2]
                            stored_w = prev_held[t][3] if len(prev_held[t]) >= 4 else None
                            weights.append(stored_w if stored_w is not None else 0.5)
                        wsum = sum(weights) or 1
                        weights = [w/wsum for w in weights]
                        for t, w in zip(items, weights):
                            pp = price_full[d_prev].get(t); pn = price_full[d].get(t, pp)
                            if pp and pn: ret += w * (pn/pp - 1)
                    else:
                        for t in items:
                            sidx = prev_held[t][2]
                            stored_w = prev_held[t][3]
                            pp = price_full[d_prev].get(t); pn = price_full[d].get(t, pp)
                            if pp and pn: ret += stored_w * (pn/pp - 1)
                else:
                    # equal weight
                    w_each = 1.0 / n
                    for t in items:
                        pp = price_full[d_prev].get(t); pn = price_full[d].get(t, pp)
                        if pp and pn: ret += w_each * (pn/pp - 1)
            value *= (1 + ret)
            daily_rets.append(ret)
            peak = max(peak, value); mdd = max(mdd, (peak-value)/peak)

        dd = data[d]
        # Exits
        for t in list(held):
            info = dd.get(t)
            if info is None or (info['p2'] is not None and info['p2'] > exit_) or (info['min_seg'] is not None and info['min_seg'] < -2):
                del held[t]

        # Entries
        if len(held) < slots:
            cands = []
            for tk, info in dd.items():
                if info['p2'] is None or info['p2'] > entry: continue
                if tk in exclude or tk in held: continue
                if info['min_seg'] is not None and info['min_seg'] < 0: continue
                if not verified_cr(tk, i, dates_all, data): continue
                if not info['price']: continue
                # mcap filter
                if min_mcap is not None and info['mcap'] < min_mcap: continue
                # dd_30_25 filter
                if dd_30_25_filter and info['high30'] and info['price']:
                    drawdown = info['price'] / info['high30'] - 1
                    if drawdown < -0.25: continue
                cands.append((info['p2'], info['score'], tk))
            cands.sort(key=lambda x: x[0])

            # Weight assignment
            picked = cands[:slots]
            if len(picked) == 1:
                _, _, tk = picked[0]
                held[tk] = (d, dd[tk]['price'], 0, 1.0)
            elif len(picked) >= 2:
                if slots == 2 and weights_mode == '2step_t15':
                    s1, s2 = picked[0][1], picked[1][1]
                    gap = s1 - s2
                    if gap >= 15: w = [1.0, 0.0]
                    else: w = [0.5, 0.5]
                else:
                    # equal weight for slot 3+
                    w = [1.0/len(picked)] * len(picked)
                for i_slot, (_, _, tk) in enumerate(picked):
                    if w[i_slot] > 0:
                        held[tk] = (d, dd[tk]['price'], i_slot, w[i_slot])
        prev_held = dict(held)

    return {'value': value, 'mdd': mdd, 'daily_rets': daily_rets}


def metrics(r):
    cum = (r['value'] - 1) * 100
    mdd = r['mdd'] * 100
    if not r['daily_rets'] or len(r['daily_rets']) < 2:
        return cum, mdd, 0, 0
    import math
    mu = statistics.mean(r['daily_rets'])
    sigma = statistics.stdev(r['daily_rets']) if len(r['daily_rets']) > 1 else 1e-9
    sharpe = (mu*252)/(sigma*math.sqrt(252)) if sigma > 0 else 0
    cal = cum/100/r['mdd'] if r['mdd'] > 0 else 0
    return cum, mdd, sharpe, cal


def run_paired(dates, data, price_full, **kwargs):
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES_PER_SEED))
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            sd_idx = dates.index(sd)
            r = simulate(dates, data, price_full, start_idx=sd_idx, **kwargs)
            cum, mdd_p, sh, cal = metrics(r)
            rets.append(cum); mdds.append(mdd_p)
            sr.append(cum)
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 110)
    print('US v84 + KR 인사이트 적용 BT')
    print('=' * 110)
    dates, data, price_full = load_data()
    print(f'data: {len(dates)} dates, {dates[0]} ~ {dates[-1]}\n')

    # baseline
    base = run_paired(dates, data, price_full, slots=2, entry=3, exit_=10, weights_mode='2step_t15')
    print(f'{"variant":<28}{"avg":>10}{"med":>9}{"mdd":>10}{"sharpe":>9}{"cal":>8}  vs baseline')
    print('-' * 90)
    cum_b = sum(base['rets'])/len(base['rets'])
    mdd_b = min(base['mdds'])
    print(f'{"★baseline v84 (slot 2)":<28}{cum_b:>+9.1f}%{sorted(base["rets"])[len(base["rets"])//2]:>+8.1f}%{mdd_b:>+9.2f}%')

    base_seeds = base['seed_avgs']

    # 1. Slot 확대 (KR 인사이트)
    print('\n--- 1. Slot 확대 (KR: slot 3→5 Cal +30% 발견) ---')
    for slots in [3, 4, 5]:
        # entry 임계도 같이 올려야 (entry >= slots)
        r = run_paired(dates, data, price_full, slots=slots, entry=max(slots, 3), exit_=10, weights_mode='equal')
        cum = sum(r['rets'])/len(r['rets'])
        mdd = min(r['mdds'])
        lifts = [b-a for a, b in zip(base_seeds, r['seed_avgs'])]
        wins = sum(1 for l in lifts if l > 0)
        avg_lift = sum(lifts)/len(lifts)
        verdict = '✓✓' if wins>=70 else '✓' if wins>=60 else '~' if wins>=40 else '✗'
        med = sorted(r['rets'])[len(r['rets'])//2]
        print(f'  slot {slots} equal              {cum:>+9.1f}%{med:>+8.1f}%{mdd:>+9.2f}%   lift {avg_lift:+8.2f}%p  {wins:>3}/{N_SEEDS} {verdict}')

    # 2. Universe 필터 (시총 cap 변형)
    print('\n--- 2. Universe 시총 필터 ---')
    for mcap_b in [None, 5e9, 30e9, 100e9, 500e9]:
        r = run_paired(dates, data, price_full, slots=2, entry=3, exit_=10, min_mcap=mcap_b)
        cum = sum(r['rets'])/len(r['rets'])
        mdd = min(r['mdds'])
        lifts = [b-a for a, b in zip(base_seeds, r['seed_avgs'])]
        wins = sum(1 for l in lifts if l > 0)
        avg_lift = sum(lifts)/len(lifts)
        verdict = '✓✓' if wins>=70 else '✓' if wins>=60 else '~' if wins>=40 else '✗'
        label = '시총 무제한' if mcap_b is None else f'시총 {int(mcap_b/1e9)}B+'
        print(f'  {label:<24}{cum:>+9.1f}%   mdd {mdd:>+6.2f}%   lift {avg_lift:+8.2f}%p  {wins:>3}/{N_SEEDS} {verdict}')

    # 3. Fragility - Top 5 winner 제외
    print('\n--- 3. Fragility (KR: Top 5 제외 -29% reversion) ---')
    winners_to_exclude = [
        ('NONE',  []),
        ('MU 제외',  ['MU']),
        ('SNDK 제외', ['SNDK']),
        ('AEIS 제외', ['AEIS']),
        ('MU+SNDK', ['MU', 'SNDK']),
        ('Top4 (MU+SNDK+BE+AEIS)', ['MU', 'SNDK', 'BE', 'AEIS']),
        ('Top6', ['MU', 'SNDK', 'BE', 'AEIS', 'TER', 'KEYS']),
    ]
    for name, excl in winners_to_exclude:
        r = run_paired(dates, data, price_full, slots=2, entry=3, exit_=10, weights_mode='2step_t15', exclude_tickers=excl)
        cum = sum(r['rets'])/len(r['rets'])
        mdd = min(r['mdds'])
        if not excl:
            base_excl_cum = cum
            print(f'  {name:<28}{cum:>+9.1f}%   mdd {mdd:>+6.2f}%   (baseline)')
        else:
            diff = cum - base_excl_cum
            print(f'  {name:<28}{cum:>+9.1f}%   mdd {mdd:>+6.2f}%   diff {diff:+7.1f}%p')

    # 4. KR 인사이트: slot 5 + dd_30_25 결합
    print('\n--- 4. Slot 확대 + dd_30_25 결합 ---')
    for slots in [2, 3, 5]:
        for filter_name, dd_filter in [('with dd_30_25', True), ('no dd_30_25', False)]:
            r = run_paired(dates, data, price_full, slots=slots, entry=max(slots, 3),
                          exit_=10, weights_mode='equal' if slots > 2 else '2step_t15',
                          dd_30_25_filter=dd_filter)
            cum = sum(r['rets'])/len(r['rets'])
            mdd = min(r['mdds'])
            print(f'  slot {slots} {filter_name:<20}{cum:>+9.1f}%   mdd {mdd:>+6.2f}%')


if __name__ == '__main__':
    main()
