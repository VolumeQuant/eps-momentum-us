"""두 발견 정합성 검증 — 같은 simulator로 4조합 + outlier 의존성

조합:
  A. current 필터 + fixed_90_10 weight (= v83.3 production baseline)
  B. ma120_and_60 필터 + fixed_90_10
  C. current 필터 + linear_20
  D. ma120_and_60 필터 + linear_20

두 환경 (MU+SNDK 포함/제외) × 4조합 = 8 BT. 모두 동일 dynamic simulator.
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


def ma_pass(price, ma60, ma120, variant):
    if price is None or price <= 0: return False
    if variant == 'current':
        if ma120 is not None: return price > ma120
        return ma60 is not None and price > ma60
    if variant == 'ma120_and_60':
        if ma120 is not None:
            ok120 = price > ma120
            ok60 = ma60 is not None and price > ma60
            return ok120 and ok60
        return ma60 is not None and price > ma60
    raise ValueError(variant)


def regenerate(db, exclude_set, ma_variant):
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
            if not ma_pass(px, m60, m120, ma_variant): continue
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
    print('두 발견 정합성 검증 — 4조합 × 2환경 = 8 BT (동일 dynamic simulator)')
    print(f'  필터:   current vs ma120_and_60')
    print(f'  weight: fixed_90_10 (v83.3) vs linear_20')
    print(f'  환경:   MU+SNDK 포함 vs 제외')
    print('=' * 110)

    weight_fixed = sw.make_weight_fn({'type': 'fixed', 'weights': [0.9, 0.1]})
    weight_linear = sw.make_weight_fn({'type': 'linear', 'anchor': 20})

    results = {}
    for env_name, excl in [('incl', set()), ('excl', {'MU', 'SNDK'})]:
        for ma_var in ['current', 'ma120_and_60']:
            db = GRID / f'comb_{env_name}_{ma_var}.db'
            print(f'\n[{env_name} | {ma_var}] DB 생성...')
            t0 = time.time()
            shutil.copy(DB_ORIGINAL, db)
            regenerate(db, excl, ma_var)
            print(f'  regen {time.time()-t0:.1f}s')

            scores = sw.precompute_scores(db)
            dates, data, price_full = sw.load_data(db)
            eligible_starts = dates[:-MIN_HOLD_DAYS]
            seed_starts = []
            for seed_i in range(N_SEEDS):
                random.seed(seed_i)
                seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

            for wname, wfn in [('fixed_90_10', weight_fixed), ('linear_20', weight_linear)]:
                key = f'{env_name}_{ma_var}_{wname}'
                t1 = time.time()
                rets, mdds, seed_avgs = [], [], []
                for chosen in seed_starts:
                    sr = []
                    for sd in chosen:
                        r = sw.simulate_dynamic(dates, data, price_full, scores, wfn, sd,
                                              max_slots=2, entry=ENTRY_TOP, exit_=EXIT_TOP)
                        rets.append(r['total_return'])
                        mdds.append(r['max_dd'])
                        sr.append(r['total_return'])
                    seed_avgs.append(sum(sr)/len(sr))
                avg = sum(rets)/len(rets)
                med = sorted(rets)[len(rets)//2]
                std = statistics.pstdev(rets)
                mdd = min(mdds)
                sharpe = avg/std if std > 0 else 0
                results[key] = {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs,
                               'avg': avg, 'med': med, 'std': std, 'mdd': mdd, 'sharpe': sharpe}
                print(f'  {wname:<12} avg={avg:+6.2f}% med={med:+6.2f}% std={std:.1f} '
                      f'mdd={mdd:+6.2f}% sharpe={sharpe:+.2f} [{time.time()-t1:.1f}s]')

    # 종합 — paired vs production (incl_current_fixed_90_10)
    print()
    print('=' * 110)
    print('★ paired vs production baseline (incl + current + fixed_90_10)')
    print('=' * 110)
    base = results['incl_current_fixed_90_10']['seed_avgs']
    rows = [
        ('A. baseline (incl+current+90_10)',    'incl_current_fixed_90_10'),
        ('B. incl+ma120_and_60+90_10',           'incl_ma120_and_60_fixed_90_10'),
        ('C. incl+current+linear_20',            'incl_current_linear_20'),
        ('D. incl+ma120_and_60+linear_20',       'incl_ma120_and_60_linear_20'),
        ('E. excl+current+90_10',                'excl_current_fixed_90_10'),
        ('F. excl+ma120_and_60+90_10',           'excl_ma120_and_60_fixed_90_10'),
        ('G. excl+current+linear_20',            'excl_current_linear_20'),
        ('H. excl+ma120_and_60+linear_20',       'excl_ma120_and_60_linear_20'),
    ]
    print(f'  {"조합":<40} {"avg":>8} {"std":>6} {"MDD":>7} {"sharpe":>7} | {"lift":>9} {"wins":>10}')
    print('  ' + '-' * 100)
    for label, key in rows:
        r = results[key]
        new = r['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        avg_l = sum(lifts)/len(lifts) if any(abs(l)>0.01 for l in lifts) else 0
        wins = sum(1 for l in lifts if l > 0.01)
        is_base = (key == 'incl_current_fixed_90_10')
        marker = ' ★' if is_base else '  '
        print(f'{marker}{label:<38} {r["avg"]:+7.2f}% {r["std"]:>5.1f} {r["mdd"]:+6.2f}% '
              f'{r["sharpe"]:+6.2f} | {avg_l:+7.2f}%p {wins:>5}/{N_SEEDS}')

    # 핵심 효과 분해
    print()
    print('=' * 110)
    print('효과 분해 (incl 환경)')
    print('=' * 110)
    base_i = results['incl_current_fixed_90_10']['seed_avgs']
    for label, key in [
        ('진입 필터 단독 (current→ma120_and_60, 90/10 고정)',  'incl_ma120_and_60_fixed_90_10'),
        ('dynamic weight 단독 (current 고정, 90/10→linear_20)', 'incl_current_linear_20'),
        ('둘 다 결합',                                          'incl_ma120_and_60_linear_20'),
    ]:
        new = results[key]['seed_avgs']
        lifts = [b - a for a, b in zip(base_i, new)]
        wins = sum(1 for l in lifts if l > 0.01)
        avg_l = sum(lifts)/len(lifts)
        med_l = statistics.median(lifts)
        print(f'  {label:<55} lift {avg_l:+7.2f}%p (med {med_l:+.2f}%p) wins {wins}/{N_SEEDS}')

    print()
    print('=' * 110)
    print('효과 분해 (excl 환경, outlier 의존성 검증)')
    print('=' * 110)
    base_e = results['excl_current_fixed_90_10']['seed_avgs']
    for label, key in [
        ('진입 필터 단독',                         'excl_ma120_and_60_fixed_90_10'),
        ('dynamic weight 단독',                    'excl_current_linear_20'),
        ('둘 다 결합',                             'excl_ma120_and_60_linear_20'),
    ]:
        new = results[key]['seed_avgs']
        lifts = [b - a for a, b in zip(base_e, new)]
        wins = sum(1 for l in lifts if l > 0.01)
        avg_l = sum(lifts)/len(lifts)
        med_l = statistics.median(lifts)
        print(f'  {label:<55} lift {avg_l:+7.2f}%p (med {med_l:+.2f}%p) wins {wins}/{N_SEEDS}')


if __name__ == '__main__':
    main()
