"""사이드 질문 BT — MA60 strict + dd_30_25 결합 검증

가설: MA60 strict는 어제 검증에서 -7.38%p 손실 (incl), case 2 종목 컷.
      dd_30_25는 +8.73%p robust 우월.
      결합 시 MA60의 손실이 더 클까, dd_30_25의 이득이 더 클까?
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


def regenerate(db, variant, exclude_set, high30):
    """variant: 'current', 'ma60_only', 'ma60_dd30_25', 'dd30_25', 'current_dd30_25'"""
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

            # MA 필터
            if variant == 'current' or variant == 'current_dd30_25':
                # MA120 우선, NULL이면 MA60 fallback
                if m120 is not None:
                    if not (px > m120): continue
                else:
                    if not (m60 is not None and px > m60): continue
            elif variant == 'ma60_only' or variant == 'ma60_dd30_25':
                # MA60 strict
                if not (m60 is not None and px > m60): continue

            # dd_30_25 필터 (조합에만 적용)
            if variant in ('current_dd30_25', 'ma60_dd30_25', 'dd30_25_only'):
                high = high30.get((today, tk))
                if high is not None:
                    dd = (px - high) / high * 100
                    if dd <= -25: continue

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
    print('=' * 100)
    print('★ 사이드 BT — MA60 + dd_30_25 결합')
    print('=' * 100)

    closes = bdr.fetch_yfinance_200d()
    conn = sqlite3.connect(DB_ORIGINAL); cur = conn.cursor()
    db_dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date').fetchall()]
    conn.close()
    high30 = bdr.compute_high30_real(closes, db_dates)

    fixed_90 = sw.make_weight_fn({'type': 'fixed', 'weights': [0.9, 0.1]})

    variants = ['current', 'ma60_only', 'current_dd30_25', 'ma60_dd30_25']
    results = {'incl': {}, 'excl': {}}
    for env_name, excl in [('incl', set()), ('excl', {'MU', 'SNDK'})]:
        print(f'\n[{env_name}]')
        for v in variants:
            db = GRID / f'sq_{env_name}_{v}.db'
            t0 = time.time()
            shutil.copy(DB_ORIGINAL, db)
            regenerate(db, v, excl, high30)
            res = run_bt(db, fixed_90)
            results[env_name][v] = res
            marker = ' ★' if v == 'current' else '  '
            print(f'{marker}{v:<22} avg={res["avg"]:+6.2f}% mdd={res["mdd"]:+6.2f}% sharpe={res["sharpe"]:+.2f} [{time.time()-t0:.1f}s]')

    print()
    print('=' * 100)
    print('paired lift vs current (현행)')
    print('=' * 100)
    base_i = results['incl']['current']['seed_avgs']
    base_e = results['excl']['current']['seed_avgs']
    print(f'  {"variant":<22} {"i_avg":>8} {"i_lift":>9} {"i_win":>7} | {"e_avg":>8} {"e_lift":>9} {"e_win":>7}')
    print('  ' + '-' * 90)
    for v in variants:
        ri = results['incl'][v]; re = results['excl'][v]
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        marker = ' ☆' if v == 'current' else '  '
        print(f'{marker}{v:<22} {ri["avg"]:+7.2f}% {avgi:+7.2f}%p {wi:>4}/100 | '
              f'{re["avg"]:+7.2f}% {avge:+7.2f}%p {we:>4}/100')


if __name__ == '__main__':
    main()
