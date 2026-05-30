"""dd_30_20 최종 검증 — anchor sensitivity + 2step_t15 결합 시너지

검증 1: anchor sensitivity (overfitting 확인)
  dd_30_-15, -18, -20, -22, -25, -30
  + dd_20_-X, dd_45_-X (window sensitivity)

검증 2: dd_30_20 + 2step_t15 결합 시너지
  4조합: current/dd_30_20 × fixed_90_10/2step_t15
  두 환경 (incl/excl)

검증 3: trade-level diff (dd_30_20 단독 vs current)
  어떤 종목이 컷되는지, 어떤 종목이 새로 들어오는지
"""
import sys
import shutil
import sqlite3
import random
import statistics
import time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')
import daily_runner as dr
import bt_dynamic_sweetspot as sw
import bt_verify_blindspots as vb

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 2
EXIT_TOP = 10


def precompute_highs(db_path, windows):
    """여러 window에 대한 high precompute"""
    conn = sqlite3.connect(db_path); cur = conn.cursor()
    rows = cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL ORDER BY ticker, date').fetchall()
    by_tk = defaultdict(list)
    for tk, d, px in rows:
        by_tk[tk].append((d, px))
    out = {w: {} for w in windows}
    for tk, lst in by_tk.items():
        prices = [p for _, p in lst]
        dates_ = [d for d, _ in lst]
        for i, d in enumerate(dates_):
            for w in windows:
                if i + 1 >= w:
                    out[w][(d, tk)] = max(prices[i+1-w:i+1])
                elif i + 1 >= 10:
                    out[w][(d, tk)] = max(prices[:i+1])
    conn.close()
    return out


def ma_pass(price, ma60, ma120, highs_map, variant):
    if price is None or price <= 0: return False
    # current 기본
    if ma120 is not None:
        if not (price > ma120): return False
    else:
        if not (ma60 is not None and price > ma60): return False

    if variant == 'current':
        return True

    # variant 형식: 'dd_{window}_{threshold}'
    if variant.startswith('dd_'):
        parts = variant.split('_')
        w = int(parts[1])
        th = int(parts[2])
        high = highs_map.get(w, {}).get('_KEY_')  # placeholder
        return None  # handled in caller


def regenerate(db, variant_spec, exclude_set, highs_map):
    """variant_spec: ('current', None) or ('dd', window, threshold)"""
    conn = sqlite3.connect(db); cur = conn.cursor()
    dates_ = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date').fetchall()]
    cur.execute('UPDATE ntm_screening SET composite_rank=NULL, part2_rank=NULL'); conn.commit()
    for today in dates_:
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
            # current 기본
            if m120 is not None:
                if not (px > m120): continue
            else:
                if not (m60 is not None and px > m60): continue
            # variant 추가 필터
            if variant_spec[0] == 'dd':
                _, w, th = variant_spec
                high = highs_map.get(w, {}).get((today, tk))
                if high is not None:
                    dd = (px - high) / high * 100
                    if dd <= -th: continue
            # 'current'는 추가 필터 없음
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


def run_bt(db_path, weight_fn):
    scores = sw.precompute_scores(db_path)
    dates_, data, price_full = sw.load_data(db_path)
    eligible_starts = dates_[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = vb.simulate_hybrid(dates_, data, price_full, scores, weight_fn, sd,
                                  max_slots=2, entry=ENTRY_TOP, exit_=EXIT_TOP)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    avg = sum(rets)/len(rets); med = sorted(rets)[len(rets)//2]
    std = statistics.pstdev(rets); mdd = min(mdds); sharpe = avg/std if std > 0 else 0
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs,
            'avg': avg, 'med': med, 'std': std, 'mdd': mdd, 'sharpe': sharpe}


def main():
    print('=' * 110)
    print('★ dd_30_20 최종 검증 — anchor sensitivity + 2step_t15 결합 시너지')
    print('=' * 110)

    windows = [20, 30, 45, 60]
    print(f'\n[step 0] highs precompute (windows: {windows})...')
    t0 = time.time()
    highs_map = precompute_highs(DB_ORIGINAL, windows)
    print(f'  done {time.time()-t0:.1f}s')

    fixed_90 = sw.make_weight_fn({'type': 'fixed', 'weights': [0.9, 0.1]})
    step_2 = sw.make_weight_fn({'type': 'step_2', 'lo': 0, 'hi': 15, 'wH': 1.0, 'wM': 0.5, 'wL': 0.5})

    # ============================================================
    # 검증 1: dd_30 anchor sensitivity + window sensitivity
    # ============================================================
    print('\n' + '=' * 110)
    print('★ 검증 1: dd anchor + window sensitivity')
    print('=' * 110)

    grid_variants = [
        ('current', ('current', None)),
        ('dd_20_15', ('dd', 20, 15)),
        ('dd_20_20', ('dd', 20, 20)),
        ('dd_30_15', ('dd', 30, 15)),
        ('dd_30_18', ('dd', 30, 18)),
        ('dd_30_20', ('dd', 30, 20)),
        ('dd_30_22', ('dd', 30, 22)),
        ('dd_30_25', ('dd', 30, 25)),
        ('dd_30_30', ('dd', 30, 30)),
        ('dd_45_20', ('dd', 45, 20)),
        ('dd_45_25', ('dd', 45, 25)),
        ('dd_60_25', ('dd', 60, 25)),
        ('dd_60_30', ('dd', 60, 30)),
    ]

    results_grid = {'incl': {}, 'excl': {}}
    for env_name, excl in [('incl', set()), ('excl', {'MU', 'SNDK'})]:
        print(f'\n[{env_name}]')
        for name, spec in grid_variants:
            db = GRID / f'dd_grid_{env_name}_{name}.db'
            t0 = time.time()
            shutil.copy(DB_ORIGINAL, db)
            regenerate(db, spec, excl, highs_map)
            res = run_bt(db, fixed_90)
            results_grid[env_name][name] = res
            marker = ' ★' if name == 'dd_30_20' else '  '
            print(f'{marker}{name:<14} avg={res["avg"]:+6.2f}% mdd={res["mdd"]:+6.2f}% [{time.time()-t0:.1f}s]')

    print()
    print('=' * 110)
    print('★ anchor grid 종합')
    print('=' * 110)
    base_i = results_grid['incl']['current']['seed_avgs']
    base_e = results_grid['excl']['current']['seed_avgs']
    rows = []
    for name, _ in grid_variants:
        ri = results_grid['incl'][name]; re = results_grid['excl'][name]
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        rows.append((name, ri['avg'], avgi, wi, re['avg'], avge, we, (avgi+avge)/2))
    rows.sort(key=lambda x: -x[7])
    print(f'  {"variant":<14} {"i_avg":>8} {"i_lift":>8} {"i_win":>7} | {"e_avg":>8} {"e_lift":>8} {"e_win":>7} | {"avg_lift":>9}')
    print('  ' + '-' * 90)
    for r in rows:
        marker = ' ★' if r[0] == 'dd_30_20' else ' ☆' if r[0] == 'current' else '  '
        print(f'{marker}{r[0]:<12} {r[1]:+7.2f}% {r[2]:+7.2f}%p {r[3]:>4}/100 | '
              f'{r[4]:+7.2f}% {r[5]:+7.2f}%p {r[6]:>4}/100 | {r[7]:+7.2f}%p')

    # ============================================================
    # 검증 2: 결합 시너지 (dd_30_20 + 2step_t15)
    # ============================================================
    print()
    print('=' * 110)
    print('★ 검증 2: 결합 시너지 — 4조합')
    print('=' * 110)

    combo_results = {}
    for env_name, excl in [('incl', set()), ('excl', {'MU', 'SNDK'})]:
        print(f'\n[{env_name}]')
        for filter_name, spec in [('current', ('current', None)), ('dd_30_20', ('dd', 30, 20))]:
            db = GRID / f'syn_{env_name}_{filter_name}.db'
            if filter_name == 'current':
                # 이미 검증 1에서 만들었을 수도
                shutil.copy(DB_ORIGINAL, db)
                regenerate(db, spec, excl, highs_map)
            else:
                shutil.copy(DB_ORIGINAL, db)
                regenerate(db, spec, excl, highs_map)
            for w_name, w_fn in [('fixed_90_10', fixed_90), ('2step_t15', step_2)]:
                key = f'{env_name}|{filter_name}|{w_name}'
                t0 = time.time()
                res = run_bt(db, w_fn)
                combo_results[key] = res
                print(f'  {filter_name:<12} + {w_name:<14} avg={res["avg"]:+6.2f}% mdd={res["mdd"]:+6.2f}% sharpe={res["sharpe"]:+.2f} [{time.time()-t0:.1f}s]')

    # 매트릭스
    print()
    print('=' * 110)
    print('★ 결합 매트릭스 — paired lift vs baseline (current + 90/10 = v83.3 현행)')
    print('=' * 110)
    for env in ['incl', 'excl']:
        base = combo_results[f'{env}|current|fixed_90_10']['seed_avgs']
        print(f'\n[{env}]')
        print(f'  {"조합":<40} {"avg":>8} {"mdd":>8} {"sharpe":>7} | {"lift":>9} {"wins":>8}')
        print('  ' + '-' * 90)
        for label, key_suffix in [
            ('A. baseline (현행 v83.3: current + 90/10)', f'current|fixed_90_10'),
            ('B. + dd_30_20 단독', f'dd_30_20|fixed_90_10'),
            ('C. + 2step_t15 단독', f'current|2step_t15'),
            ('D. + 둘 다 결합', f'dd_30_20|2step_t15'),
        ]:
            r = combo_results[f'{env}|{key_suffix}']
            new = r['seed_avgs']
            lifts = [b-a for a,b in zip(base, new)]
            avg_l = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0.01)
            marker = ' ★' if 'baseline' in label else '  '
            print(f'{marker}{label:<40} {r["avg"]:+7.2f}% {r["mdd"]:+7.2f}% {r["sharpe"]:+6.2f} | {avg_l:+7.2f}%p {wins:>4}/100')

    print()
    print('=' * 110)
    print('★ 두 환경 평균 — 어느 조합이 진짜 최강?')
    print('=' * 110)
    base_i = combo_results['incl|current|fixed_90_10']['seed_avgs']
    base_e = combo_results['excl|current|fixed_90_10']['seed_avgs']
    print(f'  {"조합":<40} {"i_lift":>8} {"i_win":>7} {"e_lift":>8} {"e_win":>7} {"avg":>9}')
    print('  ' + '-' * 90)
    for label, key_suffix in [
        ('A. 현행 (90/10 + current)',           f'current|fixed_90_10'),
        ('B. dd_30_20 단독',                    f'dd_30_20|fixed_90_10'),
        ('C. 2step_t15 단독',                   f'current|2step_t15'),
        ('D. dd_30_20 + 2step_t15 (둘 다)',     f'dd_30_20|2step_t15'),
    ]:
        ri = combo_results[f'incl|{key_suffix}']
        re = combo_results[f'excl|{key_suffix}']
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        avg_both = (avgi + avge) / 2
        print(f'  {label:<40} {avgi:+7.2f}%p {wi:>4}/100 {avge:+7.2f}%p {we:>4}/100 {avg_both:+7.2f}%p')


if __name__ == '__main__':
    main()
