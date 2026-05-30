"""최종 결합 BT — dd60_30 (진입 필터) × 2step_t15_100_50 (weight)

4조합:
  A. current 필터 + fixed_90_10 weight (= v83.3 production baseline)
  B. dd60_30 + fixed_90_10
  C. current + 2step_t15_100_50
  D. dd60_30 + 2step_t15_100_50

2환경: MU+SNDK 포함 / 제외
총 8 BT.
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

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 3
EXIT_TOP = 10


def precompute_high60(db_path):
    conn = sqlite3.connect(db_path); cur = conn.cursor()
    rows = cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL ORDER BY ticker, date').fetchall()
    by_tk = defaultdict(list)
    for tk, d, px in rows:
        by_tk[tk].append((d, px))
    high60 = {}
    for tk, lst in by_tk.items():
        prices = [p for _, p in lst]
        dates = [d for d, _ in lst]
        for i, d in enumerate(dates):
            if i + 1 >= 60:
                high60[(d, tk)] = max(prices[i+1-60:i+1])
            elif i + 1 >= 20:
                high60[(d, tk)] = max(prices[:i+1])
    conn.close()
    return high60


def ma_pass(price, ma60, ma120, high60, variant):
    if price is None or price <= 0: return False
    # current 기본 필터 (모든 변형 공통)
    if ma120 is not None:
        if not (price > ma120): return False
    else:
        if not (ma60 is not None and price > ma60): return False
    if variant == 'current':
        return True
    if variant == 'dd60_30':
        if high60 is None: return True
        dd = (price - high60) / high60 * 100
        return dd > -30
    raise ValueError(variant)


def regenerate(db, filter_variant, exclude_set, high60_map):
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
            h60 = high60_map.get((today, tk))
            if not ma_pass(px, m60, m120, h60, filter_variant): continue
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


def weight_2step_t15(gap, s1, s2):
    if gap >= 15: return [1.0, 0.0]
    return [0.5, 0.5]


def weight_fixed_90_10(gap, s1, s2):
    return [0.9, 0.1]


def run_bt(db_path, weight_fn):
    scores = sw.precompute_scores(db_path)
    dates, data, price_full = sw.load_data(db_path)
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
    rets, mdds, mdls, seed_avgs = [], [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = sw.simulate_dynamic(dates, data, price_full, scores, weight_fn, sd,
                                  max_slots=2, entry=ENTRY_TOP, exit_=EXIT_TOP)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            mdls.append(r['max_day_loss'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    avg = sum(rets)/len(rets); med = sorted(rets)[len(rets)//2]
    std = statistics.pstdev(rets); mdd = min(mdds); mdl = min(mdls)
    sharpe = avg/std if std > 0 else 0
    return {'rets': rets, 'mdds': mdds, 'mdls': mdls, 'seed_avgs': seed_avgs,
            'avg': avg, 'med': med, 'std': std, 'mdd': mdd, 'mdl': mdl, 'sharpe': sharpe}


def main():
    print('=' * 110)
    print('★ 최종 결합 BT — 4조합 × 2환경')
    print('  필터:   current vs dd60_30 (60일 -30% drawdown 제외)')
    print('  weight: fixed_90_10 (v83.3) vs 2step_t15_100_50 (gap≥15 100/0, else 50/50)')
    print(f'  환경:   MU+SNDK 포함 (incl) / 제외 (excl)')
    print(f'  seeds:  {N_SEEDS} × {SAMPLES_PER_SEED} paired')
    print('=' * 110)

    print('\n[step 0] 60일 high precompute...')
    high60 = precompute_high60(DB_ORIGINAL)
    print(f'  done ({len(high60)} entries)')

    weights_list = [
        ('fixed_90_10',  weight_fixed_90_10),
        ('2step_t15',    weight_2step_t15),
    ]

    results = {}
    for env_name, excl in [('incl_MU+SNDK포함', set()), ('excl_MU+SNDK제외', {'MU', 'SNDK'})]:
        for ma_var in ['current', 'dd60_30']:
            db = GRID / f'fin_{env_name.split("_")[0]}_{ma_var}.db'
            print(f'\n[{env_name} | {ma_var}] DB 생성...')
            t0 = time.time()
            shutil.copy(DB_ORIGINAL, db)
            regenerate(db, ma_var, excl, high60)
            print(f'  regen {time.time()-t0:.1f}s')
            for wname, wfn in weights_list:
                key = f'{env_name}|{ma_var}|{wname}'
                t1 = time.time()
                res = run_bt(db, wfn)
                results[key] = res
                print(f'  {wname:<12} avg={res["avg"]:+6.2f}% med={res["med"]:+6.2f}% '
                      f'std={res["std"]:5.1f} mdd={res["mdd"]:+6.2f}% maxday={res["mdl"]:+5.2f}% '
                      f'sharpe={res["sharpe"]:+.2f} [{time.time()-t1:.1f}s]')

    # 종합 매트릭스
    print()
    print('=' * 110)
    print('★ 최종 매트릭스 — paired lift vs production baseline (incl + current + fixed_90_10)')
    print('=' * 110)
    base_key = 'incl_MU+SNDK포함|current|fixed_90_10'
    base = results[base_key]['seed_avgs']

    rows = [
        ('A. baseline (현행 v83.3)',                     'incl_MU+SNDK포함|current|fixed_90_10'),
        ('B. + 진입필터 dd60_30 만',                    'incl_MU+SNDK포함|dd60_30|fixed_90_10'),
        ('C. + dynamic weight 2step_t15 만',            'incl_MU+SNDK포함|current|2step_t15'),
        ('D. + 둘 다 (dd60_30 + 2step_t15)',           'incl_MU+SNDK포함|dd60_30|2step_t15'),
        ('────────────────────────────────', None),
        ('E. excl: baseline (current + 90_10)',          'excl_MU+SNDK제외|current|fixed_90_10'),
        ('F. excl: + dd60_30 만',                        'excl_MU+SNDK제외|dd60_30|fixed_90_10'),
        ('G. excl: + 2step_t15 만',                      'excl_MU+SNDK제외|current|2step_t15'),
        ('H. excl: + 둘 다',                             'excl_MU+SNDK제외|dd60_30|2step_t15'),
    ]
    print(f'  {"조합":<42} {"avg":>8} {"std":>6} {"MDD":>7} {"maxday":>7} | {"lift":>10} {"wins":>10}')
    print('  ' + '-' * 100)
    for label, key in rows:
        if key is None:
            print('  ' + '-' * 100)
            continue
        r = results[key]
        new = r['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        avg_l = sum(lifts)/len(lifts)
        wins = sum(1 for l in lifts if l > 0.01)
        marker = ' ★' if 'baseline (현행' in label else '  '
        print(f'{marker}{label:<40} {r["avg"]:+7.2f}% {r["std"]:>5.1f} {r["mdd"]:+6.2f}% '
              f'{r["mdl"]:+6.2f}% | {avg_l:+8.2f}%p {wins:>5}/{N_SEEDS}')

    # 효과 분해 (incl)
    print()
    print('=' * 110)
    print('★ 효과 분해 (MU+SNDK 포함 환경)')
    print('=' * 110)
    base_i = results['incl_MU+SNDK포함|current|fixed_90_10']['seed_avgs']
    for label, key in [
        ('dd60_30 진입필터 단독',           'incl_MU+SNDK포함|dd60_30|fixed_90_10'),
        ('2step_t15 dynamic weight 단독',   'incl_MU+SNDK포함|current|2step_t15'),
        ('둘 다 결합',                      'incl_MU+SNDK포함|dd60_30|2step_t15'),
    ]:
        new = results[key]['seed_avgs']
        lifts = [b - a for a, b in zip(base_i, new)]
        wins = sum(1 for l in lifts if l > 0.01)
        avg_l = sum(lifts)/len(lifts); med_l = statistics.median(lifts)
        print(f'  {label:<40} lift {avg_l:+7.2f}%p (median {med_l:+.2f}%p) wins {wins}/{N_SEEDS}')

    # 효과 분해 (excl)
    print()
    print('=' * 110)
    print('★ 효과 분해 (MU+SNDK 제외 환경, robust 검증)')
    print('=' * 110)
    base_e = results['excl_MU+SNDK제외|current|fixed_90_10']['seed_avgs']
    for label, key in [
        ('dd60_30 진입필터 단독',           'excl_MU+SNDK제외|dd60_30|fixed_90_10'),
        ('2step_t15 dynamic weight 단독',   'excl_MU+SNDK제외|current|2step_t15'),
        ('둘 다 결합',                      'excl_MU+SNDK제외|dd60_30|2step_t15'),
    ]:
        new = results[key]['seed_avgs']
        lifts = [b - a for a, b in zip(base_e, new)]
        wins = sum(1 for l in lifts if l > 0.01)
        avg_l = sum(lifts)/len(lifts); med_l = statistics.median(lifts)
        print(f'  {label:<40} lift {avg_l:+7.2f}%p (median {med_l:+.2f}%p) wins {wins}/{N_SEEDS}')


if __name__ == '__main__':
    main()
