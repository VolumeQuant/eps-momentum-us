"""dd_30_25 최종 검증 — anchor sensitivity + 2step_t15 결합 시너지

검증 1: anchor sensitivity (24, 25, 26, 27, 28) - sweet spot 정확화
검증 2: dd_30_25 + 2step_t15 4조합 결합 시너지
"""
import sys
import shutil
import sqlite3
import random
import statistics
import time
import pandas as pd
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')
import daily_runner as dr
import bt_dynamic_sweetspot as sw
import bt_verify_blindspots as vb
import bt_dd30_real as bdr

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 2
EXIT_TOP = 10


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
    print('★ dd_30_25 최종 검증 — anchor sensitivity + 2step_t15 결합')
    print('=' * 110)

    print('\n[step 0] yfinance 200d cache load + high30 compute...')
    closes = bdr.fetch_yfinance_200d()
    conn = sqlite3.connect(DB_ORIGINAL); cur = conn.cursor()
    db_dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date').fetchall()]
    conn.close()
    t0 = time.time()
    high30 = bdr.compute_high30_real(closes, db_dates)
    print(f'  done {time.time()-t0:.1f}s')

    fixed_90 = sw.make_weight_fn({'type': 'fixed', 'weights': [0.9, 0.1]})
    step_2 = sw.make_weight_fn({'type': 'step_2', 'lo': 0, 'hi': 15, 'wH': 1.0, 'wM': 0.5, 'wL': 0.5})

    # ============================================================
    # 검증 1: anchor sensitivity 정밀
    # ============================================================
    print('\n' + '=' * 110)
    print('★ 검증 1: dd_30 anchor sensitivity 정밀 (23~28)')
    print('=' * 110)

    thresholds_anchor = [0, 23, 24, 25, 26, 27, 28]
    results_anchor = {'incl': {}, 'excl': {}}
    for env_name, excl in [('incl', set()), ('excl', {'MU', 'SNDK'})]:
        print(f'\n[{env_name}]')
        for th in thresholds_anchor:
            name = 'current' if th == 0 else f'dd_30_{th}'
            db = GRID / f'anchor_{env_name}_{name}.db'
            t0 = time.time()
            shutil.copy(DB_ORIGINAL, db)
            bdr.regenerate(db, th, excl, high30)
            res = run_bt(db, fixed_90)
            results_anchor[env_name][name] = res
            marker = ' ★' if th == 25 else '  '
            print(f'{marker}{name:<14} avg={res["avg"]:+6.2f}% mdd={res["mdd"]:+6.2f}% sharpe={res["sharpe"]:+.2f} [{time.time()-t0:.1f}s]')

    # 정리
    print()
    print('=' * 110)
    print('★ anchor sensitivity 정밀 종합')
    print('=' * 110)
    base_i = results_anchor['incl']['current']['seed_avgs']
    base_e = results_anchor['excl']['current']['seed_avgs']
    rows = []
    for th in thresholds_anchor:
        name = 'current' if th == 0 else f'dd_30_{th}'
        ri = results_anchor['incl'][name]; re = results_anchor['excl'][name]
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        rows.append((name, ri['avg'], ri['mdd'], avgi, wi, re['avg'], re['mdd'], avge, we, (avgi+avge)/2))
    rows.sort(key=lambda x: -x[9])
    print(f'  {"variant":<14} {"i_avg":>8} {"i_mdd":>7} {"i_lift":>8} {"i_win":>7} | {"e_avg":>8} {"e_mdd":>7} {"e_lift":>8} {"e_win":>7} | {"avg_lift":>9}')
    print('  ' + '-' * 110)
    for r in rows:
        marker = ' ★' if r[0] == 'dd_30_25' else ' ☆' if r[0] == 'current' else '  '
        print(f'{marker}{r[0]:<12} {r[1]:+7.2f}% {r[2]:+6.2f}% {r[3]:+7.2f}%p {r[4]:>4}/100 | '
              f'{r[5]:+7.2f}% {r[6]:+6.2f}% {r[7]:+7.2f}%p {r[8]:>4}/100 | {r[9]:+7.2f}%p')

    # ============================================================
    # 검증 2: 결합 시너지 (dd_30_25 + 2step_t15)
    # ============================================================
    print()
    print('=' * 110)
    print('★ 검증 2: 결합 시너지 — dd_30_25 + 2step_t15 4조합')
    print('=' * 110)

    combo_results = {}
    for env_name, excl in [('incl', set()), ('excl', {'MU', 'SNDK'})]:
        print(f'\n[{env_name}]')
        for filter_th, fname in [(0, 'current'), (25, 'dd_30_25')]:
            db = GRID / f'syn25_{env_name}_{fname}.db'
            shutil.copy(DB_ORIGINAL, db)
            bdr.regenerate(db, filter_th, excl, high30)
            for w_name, w_fn in [('fixed_90_10', fixed_90), ('2step_t15', step_2)]:
                key = f'{env_name}|{fname}|{w_name}'
                t0 = time.time()
                res = run_bt(db, w_fn)
                combo_results[key] = res
                print(f'  {fname:<12} + {w_name:<14} avg={res["avg"]:+6.2f}% mdd={res["mdd"]:+6.2f}% sharpe={res["sharpe"]:+.2f} [{time.time()-t0:.1f}s]')

    # 매트릭스
    print()
    print('=' * 110)
    print('★ 결합 매트릭스 — paired lift vs baseline (current + 90/10 = 현행 v83.3)')
    print('=' * 110)
    for env in ['incl', 'excl']:
        base = combo_results[f'{env}|current|fixed_90_10']['seed_avgs']
        print(f'\n[{env}]')
        print(f'  {"조합":<40} {"avg":>8} {"mdd":>8} {"sharpe":>7} | {"lift":>9} {"wins":>8}')
        print('  ' + '-' * 90)
        for label, key_suffix in [
            ('A. baseline (현행 v83.3: current + 90/10)', f'current|fixed_90_10'),
            ('B. + dd_30_25 단독',                       f'dd_30_25|fixed_90_10'),
            ('C. + 2step_t15 단독',                      f'current|2step_t15'),
            ('D. + 둘 다 결합',                          f'dd_30_25|2step_t15'),
        ]:
            r = combo_results[f'{env}|{key_suffix}']
            new = r['seed_avgs']
            lifts = [b-a for a,b in zip(base, new)]
            avg_l = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0.01)
            marker = ' ★' if 'baseline' in label else '  '
            print(f'{marker}{label:<40} {r["avg"]:+7.2f}% {r["mdd"]:+7.2f}% {r["sharpe"]:+6.2f} | {avg_l:+7.2f}%p {wins:>4}/100')

    print()
    print('=' * 110)
    print('★ 두 환경 평균 — 최강 조합')
    print('=' * 110)
    base_i = combo_results['incl|current|fixed_90_10']['seed_avgs']
    base_e = combo_results['excl|current|fixed_90_10']['seed_avgs']
    print(f'  {"조합":<40} {"i_lift":>8} {"i_win":>7} {"e_lift":>8} {"e_win":>7} {"avg":>9}')
    print('  ' + '-' * 90)
    rows_c = []
    for label, key_suffix in [
        ('A. 현행 (90/10 + current)',            f'current|fixed_90_10'),
        ('B. dd_30_25 단독',                     f'dd_30_25|fixed_90_10'),
        ('C. 2step_t15 단독',                    f'current|2step_t15'),
        ('D. dd_30_25 + 2step_t15 (둘 다)',     f'dd_30_25|2step_t15'),
    ]:
        ri = combo_results[f'incl|{key_suffix}']
        re = combo_results[f'excl|{key_suffix}']
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        avg_both = (avgi + avge) / 2
        rows_c.append((label, avgi, wi, avge, we, avg_both))
        print(f'  {label:<40} {avgi:+7.2f}%p {wi:>4}/100 {avge:+7.2f}%p {we:>4}/100 {avg_both:+7.2f}%p')

    print()
    print('=' * 110)
    print('★ 최종 robust 우월 (양 환경 wins ≥ 60)')
    print('=' * 110)
    for label, avgi, wi, avge, we, avg_both in rows_c:
        if 'baseline' in label.lower() or 'A.' in label: continue
        if wi >= 60 and we >= 60:
            print(f'  ✓✓ {label}: incl +{avgi:.2f}%p ({wi}/100), excl +{avge:.2f}%p ({we}/100), 평균 +{avg_both:.2f}%p')


if __name__ == '__main__':
    main()
