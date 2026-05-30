"""맹점 검증 — production 개선 방향 명백히 찾기

검증 1: simulator 일관성
  - hybrid simulator (cash 슬롯별 독립 + dynamic weights) 구현
  - linear_20 vs fixed_90_10이 dynamic simulator와 hybrid에서 일관 우월인지

검증 2: anchor sensitivity
  - linear_{12, 15, 17, 18, 20, 22, 25, 28, 30, 35} grid
  - sweet spot이 plateau인지 sharp peak인지

검증 3: multi-outlier sweep
  - exclude sets: {}, {MU}, {SNDK}, {MU,SNDK}, {MU,SNDK,BE}, {MU,SNDK,BE,FORM}
  - linear_20 vs fixed_90_10 paired lift 추적
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
import bt_dynamic_sweetspot as sw

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 3
EXIT_TOP = 10


def simulate_hybrid(dates_all, data, price_full, scores, weight_fn, start_date,
                    max_slots=2, entry=3, exit_=10):
    """Hybrid: cash 슬롯별 독립 (entry_fixed) + dynamic weights (sweetspot)

    - 시작 시점에 cash 한 덩어리 (total_cash)
    - 첫 종목 진입 시점에 weight_fn으로 weights 결정 → slot_cash 분배
    - 이후 슬롯 매도 → 같은 슬롯 cash로 환원 (다른 슬롯과 무관)
    - 슬롯 1만 비어도 다음 진입은 슬롯 1 cash로 (weights 재결정 X)
    - 모든 슬롯 비면 cash 통합 → 다음 진입 시 weights 재결정
    """
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

        # PV
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

        # 이탈
        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, entry_price, _ = slot_holding[i]
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2 or rank is None or rank > exit_:
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                slot_cash[i] = shares * (p if p else entry_price)
                slot_holding[i] = None

        # 모든 슬롯 비면: cash 통합, weights 리셋 (다음 첫 진입 시 재결정)
        if all(h is None for h in slot_holding):
            total_cash += sum(slot_cash)
            slot_cash = [0.0] * max_slots
            current_weights = None

        # 진입
        for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
            if rank > entry: break
            if any(h is not None and h[0] == tk for h in slot_holding): continue
            if consecutive.get(tk, 0) < 3: continue
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < 0: continue
            price = today_data.get(tk, {}).get('price')
            if not price or price <= 0: continue

            # 첫 진입 시 weights 결정 (total_cash > 0 = 통합된 상태)
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


def run_paired(db_path, weight_fns, sim_fn=None):
    """주어진 DB에 대해 다양한 weight 함수로 paired BT"""
    if sim_fn is None: sim_fn = simulate_hybrid
    scores = sw.precompute_scores(db_path)
    dates, data, price_full = sw.load_data(db_path)
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    results = {}
    for name, wfn in weight_fns:
        rets, mdds, seed_avgs = [], [], []
        for chosen in seed_starts:
            sr = []
            for sd in chosen:
                r = sim_fn(dates, data, price_full, scores, wfn, sd,
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


def regenerate(db, exclude_set):
    """current 필터로 regenerate"""
    conn = sqlite3.connect(db); cur = conn.cursor()
    dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date').fetchall()]
    cur.execute('UPDATE ntm_screening SET composite_rank=NULL, part2_rank=NULL'); conn.commit()
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
            # current filter
            if m120 is not None:
                if not (px > m120): continue
            else:
                if not (m60 is not None and px > m60): continue
            if rg is None or rg < 0.10: continue
            if na is None or na < 3: continue
            if ru is None or ru < 3: continue
            total = (ru or 0) + (rd or 0)
            if total > 0 and (rd or 0) / total > 0.3: continue
            if om is not None and gm is not None and om < 0.10 and gm < 0.30: continue
            if om is not None and om < 0.05: continue
            if fcf is not None and roe is not None and fcf < 0 and roe < 0: continue
            eligible.append({'ticker': tk, 'adj_gap': ag, 'rev_up30': ru, 'num_analysts': na,
                            'ntm_current': nc, 'ntm_90d': n90, 'rev_growth': rg, 'price': px})
        def _min_seg(tr):
            r2 = cur.execute('SELECT ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d FROM ntm_screening WHERE date=? AND ticker=?', (today, tr['ticker'])).fetchone()
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


def main():
    print('=' * 110)
    print('맹점 검증 — production 개선 방향 명백히 찾기')
    print('=' * 110)

    # =========================================================
    # 검증 1: simulator 일관성 (dynamic vs hybrid)
    # =========================================================
    print('\n' + '=' * 110)
    print('★ 검증 1: simulator 일관성 — dynamic vs hybrid')
    print('=' * 110)
    db = GRID / 'v_incl.db'
    if not (db).exists():
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, set())
    weight_fns = [
        ('fixed_90_10',     sw.make_weight_fn({'type': 'fixed', 'weights': [0.9, 0.1]})),
        ('fixed_80_20',     sw.make_weight_fn({'type': 'fixed', 'weights': [0.8, 0.2]})),
        ('fixed_50_50',     sw.make_weight_fn({'type': 'fixed', 'weights': [0.5, 0.5]})),
        ('linear_20',       sw.make_weight_fn({'type': 'linear', 'anchor': 20})),
        ('step_100_70_50',  sw.make_weight_fn({'type': 'step_2', 'lo': 10, 'hi': 30, 'wH': 1.0, 'wM': 0.7, 'wL': 0.5})),
        ('step_90_70_50',   sw.make_weight_fn({'type': 'step_2', 'lo': 10, 'hi': 30, 'wH': 0.9, 'wM': 0.7, 'wL': 0.5})),
    ]
    res_dynamic = run_paired(db, weight_fns, sim_fn=sw.simulate_dynamic)
    res_hybrid = run_paired(db, weight_fns, sim_fn=simulate_hybrid)

    print(f'{"variant":<20} | {"dyn avg":>9} {"dyn lift":>9} {"dyn wins":>9} | {"hyb avg":>9} {"hyb lift":>9} {"hyb wins":>9}')
    print('-' * 110)
    base_d = res_dynamic['fixed_90_10']['seed_avgs']
    base_h = res_hybrid['fixed_90_10']['seed_avgs']
    for name, _ in weight_fns:
        rd = res_dynamic[name]; rh = res_hybrid[name]
        ld = [b-a for a,b in zip(base_d, rd['seed_avgs'])]
        lh = [b-a for a,b in zip(base_h, rh['seed_avgs'])]
        wd = sum(1 for x in ld if x > 0.01); wh = sum(1 for x in lh if x > 0.01)
        avgld = sum(ld)/len(ld); avglh = sum(lh)/len(lh)
        marker = ' ★' if name == 'fixed_90_10' else '  '
        print(f'{marker}{name:<18} | {rd["avg"]:+8.2f}% {avgld:+7.2f}%p {wd:>5}/100 | '
              f'{rh["avg"]:+8.2f}% {avglh:+7.2f}%p {wh:>5}/100')

    # 결론: 두 simulator에서 linear_20 우월성 일관성
    print()
    print(f'  → linear_20 paired lift:')
    print(f'    dynamic sim:  {sum([b-a for a,b in zip(base_d, res_dynamic["linear_20"]["seed_avgs"])])/100:+.2f}%p')
    print(f'    hybrid sim:   {sum([b-a for a,b in zip(base_h, res_hybrid["linear_20"]["seed_avgs"])])/100:+.2f}%p')

    # =========================================================
    # 검증 2: anchor sensitivity (linear_N grid)
    # =========================================================
    print('\n' + '=' * 110)
    print('★ 검증 2: anchor sensitivity — linear_{12, 15, 17, 18, 20, 22, 25, 28, 30, 35}')
    print('=' * 110)
    anchors = [12, 15, 17, 18, 20, 22, 25, 28, 30, 35, 40]
    anchor_fns = [
        ('fixed_90_10', sw.make_weight_fn({'type': 'fixed', 'weights': [0.9, 0.1]})),
    ]
    for a in anchors:
        anchor_fns.append((f'linear_{a}', sw.make_weight_fn({'type': 'linear', 'anchor': a})))

    res_anchor_d = run_paired(db, anchor_fns, sim_fn=sw.simulate_dynamic)
    res_anchor_h = run_paired(db, anchor_fns, sim_fn=simulate_hybrid)

    print(f'{"anchor":<14} | {"dyn avg":>9} {"dyn lift":>9} {"dyn wins":>9} | {"hyb avg":>9} {"hyb lift":>9} {"hyb wins":>9}')
    print('-' * 110)
    base_ad = res_anchor_d['fixed_90_10']['seed_avgs']
    base_ah = res_anchor_h['fixed_90_10']['seed_avgs']
    for name, _ in anchor_fns:
        if name == 'fixed_90_10': continue
        rd = res_anchor_d[name]; rh = res_anchor_h[name]
        ld = [b-a for a,b in zip(base_ad, rd['seed_avgs'])]
        lh = [b-a for a,b in zip(base_ah, rh['seed_avgs'])]
        wd = sum(1 for x in ld if x > 0.01); wh = sum(1 for x in lh if x > 0.01)
        avgld = sum(ld)/len(ld); avglh = sum(lh)/len(lh)
        print(f'{name:<14} | {rd["avg"]:+8.2f}% {avgld:+7.2f}%p {wd:>5}/100 | '
              f'{rh["avg"]:+8.2f}% {avglh:+7.2f}%p {wh:>5}/100')

    # =========================================================
    # 검증 3: multi-outlier sweep
    # =========================================================
    print('\n' + '=' * 110)
    print('★ 검증 3: multi-outlier sweep — linear_20 vs fixed_90_10 (hybrid)')
    print('=' * 110)
    outlier_sets = [
        ('none', set()),
        ('MU', {'MU'}),
        ('SNDK', {'SNDK'}),
        ('MU+SNDK', {'MU', 'SNDK'}),
        ('MU+SNDK+BE', {'MU', 'SNDK', 'BE'}),
        ('MU+SNDK+BE+FORM', {'MU', 'SNDK', 'BE', 'FORM'}),
        ('+LITE+TTMI', {'MU', 'SNDK', 'BE', 'FORM', 'LITE', 'TTMI'}),
    ]
    print(f'{"exclude set":<22} | {"f90 avg":>8} {"lin20 avg":>9} {"lift":>9} {"wins":>9} | {"verdict":>10}')
    print('-' * 90)
    weight_fns_simple = [
        ('fixed_90_10', sw.make_weight_fn({'type': 'fixed', 'weights': [0.9, 0.1]})),
        ('linear_20',   sw.make_weight_fn({'type': 'linear', 'anchor': 20})),
    ]
    for env_name, excl in outlier_sets:
        db_e = GRID / f'v_{env_name.replace("+","_")}.db'
        shutil.copy(DB_ORIGINAL, db_e)
        regenerate(db_e, excl)
        res = run_paired(db_e, weight_fns_simple, sim_fn=simulate_hybrid)
        base = res['fixed_90_10']['seed_avgs']
        new = res['linear_20']['seed_avgs']
        lifts = [b-a for a,b in zip(base, new)]
        avg_l = sum(lifts)/len(lifts)
        wins = sum(1 for x in lifts if x > 0.01)
        verdict = ('✓✓ 우월' if wins >= 70 else '✓ 우월' if wins >= 60
                   else '~ 동등' if wins >= 40
                   else '✗ 열세' if wins >= 30 else '✗✗ 열세')
        print(f'{env_name:<22} | {res["fixed_90_10"]["avg"]:+7.2f}% {res["linear_20"]["avg"]:+8.2f}% '
              f'{avg_l:+7.2f}%p {wins:>5}/100 | {verdict}')


if __name__ == '__main__':
    main()
