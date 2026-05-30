"""Dynamic weight sweet spot grid search

step_2 baseline (gap 10/30, weights 0.5/0.7/0.9) 주변 + continuous 변형 정밀 탐색.

Grid:
  Group A: threshold sensitivity (weights 0.5/0.7/0.9 고정)
    threshold_lo ∈ {5, 10, 15, 20}
    threshold_hi ∈ {20, 25, 30, 35}
  Group B: weight sensitivity (threshold 10/30 고정)
    weights ∈ baseline 주변
  Group C: continuous
    linear_N (anchor gap)
    sigmoid (center, width)
    power
  Group D: baseline 비교 (fixed_90_10, fixed_80_20)

두 환경 (MU+SNDK 포함/제외) 모두 평가, 평균 lift로 sweet spot 확정.
"""
import sys
import shutil
import sqlite3
import random
import statistics
import math
import time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')
import daily_runner as dr

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 3
EXIT_TOP = 10


def ma_pass_current(price, ma60, ma120):
    if price is None or price <= 0: return False
    if ma120 is not None: return price > ma120
    return ma60 is not None and price > ma60


def regenerate(test_db, exclude_set):
    conn = sqlite3.connect(test_db)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date').fetchall()]
    cur.execute('UPDATE ntm_screening SET composite_rank=NULL, part2_rank=NULL')
    conn.commit()
    for today in dates:
        rows = cur.execute('SELECT ticker, adj_score, adj_gap, eps_chg_weighted, price, ma60, ma120, ntm_current, ntm_90d, rev_growth, num_analysts, rev_up30, rev_down30, operating_margin, gross_margin, free_cashflow, roe FROM ntm_screening WHERE date=?', (today,)).fetchall()
        if not rows: continue
        eligible = []
        for r in rows:
            (tk, asc, ag, eps_w, px, m60, m120, nc, n90, rg, na, ru, rd, om, gm, fcf, roe) = r
            if tk in exclude_set: continue
            if asc is None or asc <= 9: continue
            if ag is None: continue
            if px is None or px < 10: continue
            if nc is None or nc <= 0: continue
            if eps_w is None or eps_w <= 0: continue
            if not ma_pass_current(px, m60, m120): continue
            if rg is None or rg < 0.10: continue
            if na is None or na < 3: continue
            if ru is None or ru < 3: continue
            total = (ru or 0) + (rd or 0)
            if total > 0 and (rd or 0) / total > 0.3: continue
            if om is not None and gm is not None and om < 0.10 and gm < 0.30: continue
            if om is not None and om < 0.05: continue
            if fcf is not None and roe is not None and fcf < 0 and roe < 0: continue
            eligible.append({
                'ticker': tk, 'adj_gap': ag, 'rev_up30': ru, 'num_analysts': na,
                'ntm_current': nc, 'ntm_90d': n90, 'rev_growth': rg, 'price': px,
            })
        def _min_seg(tk_row):
            r2 = cur.execute('SELECT ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d FROM ntm_screening WHERE date=? AND ticker=?', (today, tk_row['ticker'])).fetchone()
            if not r2 or any(x is None for x in r2): return 0
            nc, n7, n30, n60, n90 = r2
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b is not None and abs(b) > 0.01: segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else: segs.append(0)
            return min(segs)
        eligible = [e for e in eligible if _min_seg(e) >= -2]
        if not eligible: continue
        for e in eligible:
            e['_conv_gap'] = dr._apply_conviction(e['adj_gap'], e['rev_up30'], e['num_analysts'], e['ntm_current'], e['ntm_90d'], rev_growth=e['rev_growth'])
        eligible.sort(key=lambda e: e['_conv_gap'])
        for i, e in enumerate(eligible, 1):
            cur.execute('UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?', (i, today, e['ticker']))
        conn.commit()
        tickers = [e['ticker'] for e in eligible]
        wmap = dr._compute_w_gap_map(cur, today, tickers)
        sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        top30 = sorted_w[:30]
        for rk, tk in enumerate(top30, 1):
            cur.execute('UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?', (rk, today, tk))
        conn.commit()
    conn.close()


def precompute_scores(db_path):
    conn = sqlite3.connect(db_path); cur = conn.cursor()
    dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]
    out = {}
    for d in dates:
        rows = cur.execute('SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank LIMIT 5', (d,)).fetchall()
        tks = [r[0] for r in rows]
        if len(tks) < 2:
            out[d] = (100.0, 0.0, 100.0); continue
        wmap = dr._compute_w_gap_map(cur, d, tks)
        sorted_t = sorted(tks, key=lambda t: wmap.get(t, 0), reverse=True)
        top_w = wmap.get(sorted_t[0], 0)
        if top_w <= 0: out[d] = (100.0, 100.0, 0.0); continue
        s1 = 100.0; s2 = wmap.get(sorted_t[1], 0) / top_w * 100
        out[d] = (s1, s2, s1 - s2)
    conn.close()
    return out


def make_weight_fn(spec):
    """spec dict → function(gap, s1, s2) → [w1, w2]"""
    t = spec['type']
    if t == 'fixed':
        w = spec['weights']
        return lambda g, s1, s2: w
    if t == 'step_2':
        lo, hi = spec['lo'], spec['hi']
        wH, wM, wL = spec['wH'], spec['wM'], spec['wL']
        def fn(g, s1, s2):
            if g >= hi: return [wH, 1-wH]
            if g >= lo: return [wM, 1-wM]
            return [wL, 1-wL]
        return fn
    if t == 'linear':
        anchor = spec['anchor']
        def fn(g, s1, s2):
            x = min(g / anchor, 1.0)
            w1 = 0.5 + 0.5 * x
            return [w1, 1-w1]
        return fn
    if t == 'sigmoid':
        center, width = spec['center'], spec['width']
        def fn(g, s1, s2):
            x = 1 / (1 + math.exp(-(g - center) / width))
            w1 = 0.5 + 0.5 * x  # 0.5 ~ 1.0
            return [w1, 1-w1]
        return fn
    if t == 'power':
        anchor, p = spec['anchor'], spec['p']
        def fn(g, s1, s2):
            x = min(g / anchor, 1.0) ** p
            w1 = 0.5 + 0.5 * x
            return [w1, 1-w1]
        return fn
    if t == 'proportional':
        def fn(g, s1, s2):
            total = s1 + s2
            if total <= 0: return [1.0, 0.0]
            return [s1/total, s2/total]
        return fn


def load_data(db_path):
    conn = sqlite3.connect(db_path); cur = conn.cursor()
    dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]
    data = {}
    for d in dates:
        rows = cur.execute('SELECT ticker, part2_rank, price, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d FROM ntm_screening WHERE date=?', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[3:8])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01: segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else: segs.append(0)
            data[d][tk] = {'p2': r[1], 'price': r[2], 'min_seg': min(segs) if segs else 0}
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def simulate_dynamic(dates_all, data, price_full, scores, weight_fn, start_date,
                     max_slots=2, entry=3, exit_=10):
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    INIT_CAP = 100.0
    total_cash = INIT_CAP
    slot_cash = [0.0] * max_slots
    slot_holding = [None] * max_slots
    current_weights = None
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
        new_consec = defaultdict(int)
        for tk in rank_map: new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        pv_today = total_cash
        for i in range(max_slots):
            if slot_holding[i] is None:
                pv_today += slot_cash[i]
            else:
                tk, shares, _, _ = slot_holding[i]
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                if p is None: p = slot_holding[i][2]
                pv_today += shares * p
        if prev_pv > 0: daily_returns.append((pv_today - prev_pv) / prev_pv * 100)
        else: daily_returns.append(0)
        prev_pv = pv_today

        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, entry_price, _ = slot_holding[i]
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2 or rank is None or rank > exit_:
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                slot_cash[i] = shares * (p if p else entry_price)
                slot_holding[i] = None

        if all(h is None for h in slot_holding):
            total_cash += sum(slot_cash)
            slot_cash = [0.0] * max_slots
            current_weights = None

        for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
            if rank > entry: break
            if any(h is not None and h[0] == tk for h in slot_holding): continue
            if consecutive.get(tk, 0) < 3: continue
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < 0: continue
            price = today_data.get(tk, {}).get('price')
            if not price or price <= 0: continue

            if current_weights is None and total_cash > 0:
                if today in scores:
                    s1, s2, gap = scores[today]
                    current_weights = weight_fn(gap, s1, s2)
                else:
                    current_weights = weight_fn(30, 100, 70)
                slot_cash = [w * total_cash for w in current_weights]
                total_cash = 0

            free = next((i for i in range(max_slots) if slot_holding[i] is None), None)
            if free is None: break
            if slot_cash[free] <= 0: continue
            shares = slot_cash[free] / price
            slot_holding[free] = (tk, shares, price, today)
            slot_cash[free] = 0

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100
        max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd,
            'max_day_loss': min(daily_returns) if daily_returns else 0}


# Grid 정의 (~28 변형)
VARIANTS = []

# Group 0: baselines
VARIANTS += [
    ('fixed_90_10',    {'type': 'fixed', 'weights': [0.9, 0.1]}),  # v83.3
    ('fixed_80_20',    {'type': 'fixed', 'weights': [0.8, 0.2]}),
    ('fixed_70_30',    {'type': 'fixed', 'weights': [0.7, 0.3]}),
    ('fixed_50_50',    {'type': 'fixed', 'weights': [0.5, 0.5]}),
]
# Group A: step_2 threshold grid (weights 0.9/0.7/0.5 고정)
for lo in [5, 10, 15, 20]:
    for hi in [20, 25, 30, 35]:
        if hi <= lo: continue
        VARIANTS.append((f'step_t{lo}_{hi}',
                        {'type': 'step_2', 'lo': lo, 'hi': hi, 'wH': 0.9, 'wM': 0.7, 'wL': 0.5}))
# Group B: weight sensitivity (threshold 10/30 고정)
VARIANTS += [
    ('step_w85_65_50', {'type': 'step_2', 'lo': 10, 'hi': 30, 'wH': 0.85, 'wM': 0.65, 'wL': 0.5}),
    ('step_w95_70_50', {'type': 'step_2', 'lo': 10, 'hi': 30, 'wH': 0.95, 'wM': 0.7, 'wL': 0.5}),
    ('step_w90_70_55', {'type': 'step_2', 'lo': 10, 'hi': 30, 'wH': 0.9, 'wM': 0.7, 'wL': 0.55}),
    ('step_w90_65_50', {'type': 'step_2', 'lo': 10, 'hi': 30, 'wH': 0.9, 'wM': 0.65, 'wL': 0.5}),
    ('step_w100_70_50', {'type': 'step_2', 'lo': 10, 'hi': 30, 'wH': 1.0, 'wM': 0.7, 'wL': 0.5}),
    ('step_w90_75_55', {'type': 'step_2', 'lo': 10, 'hi': 30, 'wH': 0.9, 'wM': 0.75, 'wL': 0.55}),
]
# Group C: continuous
VARIANTS += [
    ('linear_20',      {'type': 'linear', 'anchor': 20}),
    ('linear_30',      {'type': 'linear', 'anchor': 30}),
    ('linear_40',      {'type': 'linear', 'anchor': 40}),
    ('sigmoid_15_5',   {'type': 'sigmoid', 'center': 15, 'width': 5}),
    ('sigmoid_20_5',   {'type': 'sigmoid', 'center': 20, 'width': 5}),
    ('power_30_0.5',   {'type': 'power', 'anchor': 30, 'p': 0.5}),
    ('power_30_2.0',   {'type': 'power', 'anchor': 30, 'p': 2.0}),
    ('proportional',   {'type': 'proportional'}),
]

print(f'총 {len(VARIANTS)} 변형')


def run_env(exclude_set, env_name):
    db = GRID / f'sweet_{env_name}.db'
    if not db.exists() or env_name == 'force':
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, exclude_set)
    scores = precompute_scores(db)
    dates, data, price_full = load_data(db)
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    results = {}
    for name, spec in VARIANTS:
        t0 = time.time()
        weight_fn = make_weight_fn(spec)
        rets, mdds, seed_avgs = [], [], []
        for chosen in seed_starts:
            sr = []
            for sd in chosen:
                r = simulate_dynamic(dates, data, price_full, scores, weight_fn, sd,
                                    max_slots=2, entry=ENTRY_TOP, exit_=EXIT_TOP)
                rets.append(r['total_return'])
                mdds.append(r['max_dd'])
                sr.append(r['total_return'])
            seed_avgs.append(sum(sr)/len(sr))
        avg = sum(rets)/len(rets); med = sorted(rets)[len(rets)//2]
        std = statistics.pstdev(rets); mdd = min(mdds); sharpe = avg/std if std > 0 else 0
        results[name] = {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs,
                         'avg': avg, 'med': med, 'std': std, 'mdd': mdd, 'sharpe': sharpe}
    return results


def main():
    print('=' * 110)
    print(f'Dynamic Weight Sweet Spot Grid Search ({len(VARIANTS)} 변형 × 2환경)')
    print('=' * 110)

    print('\n[환경 1: incl] regenerate + BT...')
    t0 = time.time()
    res_incl = run_env(set(), 'incl')
    print(f'  done {time.time()-t0:.1f}s')

    print('\n[환경 2: excl] regenerate + BT...')
    t0 = time.time()
    res_excl = run_env({'MU', 'SNDK'}, 'excl')
    print(f'  done {time.time()-t0:.1f}s')

    # 종합 표
    print()
    print('=' * 110)
    print('종합 결과 — 두 환경 paired lift vs fixed_90_10')
    print('=' * 110)
    base_i = res_incl['fixed_90_10']['seed_avgs']
    base_e = res_excl['fixed_90_10']['seed_avgs']
    print(f'{"variant":<22} {"i_avg":>7} {"i_mdd":>7} {"i_lift":>8} {"i_win":>6} | '
          f'{"e_avg":>7} {"e_mdd":>7} {"e_lift":>8} {"e_win":>6} | {"평균lift":>9}')
    print('-' * 110)

    rows = []
    for name, _ in VARIANTS:
        ri = res_incl[name]; re = res_excl[name]
        li = [b - a for a, b in zip(base_i, ri['seed_avgs'])]
        le = [b - a for a, b in zip(base_e, re['seed_avgs'])]
        avg_li = sum(li)/len(li); avg_le = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0); we = sum(1 for x in le if x > 0)
        avg_both = (avg_li + avg_le) / 2
        rows.append((name, ri['avg'], ri['mdd'], avg_li, wi,
                    re['avg'], re['mdd'], avg_le, we, avg_both))

    # 평균 lift 기준 정렬
    rows.sort(key=lambda x: -x[9])
    for r in rows:
        marker = ' ★' if r[0] == 'fixed_90_10' else '  '
        print(f'{marker}{r[0]:<20} {r[1]:+6.1f}% {r[2]:+6.1f}% {r[3]:+7.2f}%p {r[4]:>4}/100 | '
              f'{r[5]:+6.1f}% {r[6]:+6.1f}% {r[7]:+7.2f}%p {r[8]:>4}/100 | {r[9]:+7.2f}%p')

    # 상위 5 sweet spot
    print()
    print('=' * 110)
    print('★ Sweet Spot Top 5 (평균 lift 기준)')
    print('=' * 110)
    for i, r in enumerate(rows[:5], 1):
        spec = dict([(k, v) for n, s in VARIANTS for k, v in s.items() if n == r[0]])
        # spec 찾기 (간단)
        spec_str = next(s for n, s in VARIANTS if n == r[0])
        print(f'{i}. {r[0]:<22} 평균 lift {r[9]:+.2f}%p '
              f'(incl {r[3]:+.2f}%p {r[4]}/100, excl {r[7]:+.2f}%p {r[8]}/100)')
        print(f'   spec: {spec_str}')
        print(f'   incl: avg={r[1]:+.1f}% mdd={r[2]:+.1f}%, excl: avg={r[5]:+.1f}% mdd={r[6]:+.1f}%')


if __name__ == '__main__':
    main()
