"""최적 비중 BT — MU + SNDK 제외 (알파 핵심 종목 의존성 검증)

배경:
  진입 시점 고정 BT에서 90/10이 최적 결론.
  단, MU/SNDK가 v83 알파 핵심 종목 — 이 두 종목 제외 시에도 90/10이 우월한지 검증.

방법:
  1. DB 복제 후 EXCLUDE = {MU, SNDK}로 part2_rank 재계산
  2. simulate_entry_fixed 그대로 사용
  3. 비중 grid BT (slot1 ~ slot2_50_50)
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

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 3
EXIT_TOP = 10

EXCLUDE = {'MU', 'SNDK'}


def ma_pass_current(price, ma60, ma120):
    if price is None or price <= 0:
        return False
    if ma120 is not None:
        return price > ma120
    return ma60 is not None and price > ma60


def regenerate_exclude(test_db, exclude_set):
    conn = sqlite3.connect(test_db)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening ORDER BY date'
    ).fetchall()]
    cur.execute('UPDATE ntm_screening SET composite_rank=NULL, part2_rank=NULL')
    conn.commit()

    for today in dates:
        rows = cur.execute('''
            SELECT ticker, adj_score, adj_gap, eps_chg_weighted, price, ma60, ma120,
                   ntm_current, ntm_90d, rev_growth, num_analysts, rev_up30, rev_down30,
                   operating_margin, gross_margin, free_cashflow, roe
            FROM ntm_screening WHERE date=?
        ''', (today,)).fetchall()
        if not rows:
            continue

        eligible = []
        for r in rows:
            (tk, asc, ag, eps_w, px, m60, m120,
             nc, n90, rg, na, ru, rd, om, gm, fcf, roe) = r
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
            r2 = cur.execute(
                'SELECT ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d '
                'FROM ntm_screening WHERE date=? AND ticker=?',
                (today, tk_row['ticker'])
            ).fetchone()
            if not r2 or any(x is None for x in r2): return 0
            nc, n7, n30, n60, n90 = r2
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b is not None and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a - b) / abs(b) * 100)))
                else:
                    segs.append(0)
            return min(segs)
        eligible = [e for e in eligible if _min_seg(e) >= -2]
        if not eligible: continue

        for e in eligible:
            e['_conv_gap'] = dr._apply_conviction(
                e['adj_gap'], e['rev_up30'], e['num_analysts'],
                e['ntm_current'], e['ntm_90d'], rev_growth=e['rev_growth']
            )
        eligible.sort(key=lambda e: e['_conv_gap'])

        for i, e in enumerate(eligible, 1):
            cur.execute('UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                       (i, today, e['ticker']))
        conn.commit()

        tickers = [e['ticker'] for e in eligible]
        wmap = dr._compute_w_gap_map(cur, today, tickers)
        sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        top30 = sorted_w[:30]
        for rk, tk in enumerate(top30, 1):
            cur.execute('UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                       (rk, today, tk))
        conn.commit()
    conn.close()


def load_data(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
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
                else:
                    segs.append(0)
            data[d][tk] = {'p2': r[1], 'price': r[2],
                          'min_seg': min(segs) if segs else 0}
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'
    ):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def simulate_entry_fixed(dates_all, data, price_full, weights, start_date,
                         entry=3, exit_=10):
    max_slots = len(weights)
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    INIT_CAP = 100.0
    slot_cash = [w * INIT_CAP for w in weights]
    slot_holding = [None] * max_slots
    daily_returns = []
    consecutive = defaultdict(int)
    if start_date:
        for d in dates_all:
            if d >= start_date:
                break
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    consecutive[tk] = consecutive.get(tk, 0) + 1
    prev_pv = INIT_CAP

    for di, today in enumerate(dates):
        if today not in data:
            daily_returns.append(0)
            continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consec = defaultdict(int)
        for tk in rank_map:
            new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        pv_today = 0
        for i in range(max_slots):
            if slot_holding[i] is None:
                pv_today += slot_cash[i]
            else:
                tk, shares, _, _ = slot_holding[i]
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                if p is None: p = slot_holding[i][2]
                pv_today += shares * p
        if prev_pv > 0:
            daily_returns.append((pv_today - prev_pv) / prev_pv * 100)
        else:
            daily_returns.append(0)
        prev_pv = pv_today

        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, entry_price, entry_date = slot_holding[i]
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2 or rank is None or rank > exit_:
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                slot_cash[i] = shares * (p if p else entry_price)
                slot_holding[i] = None

        for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
            if rank > entry: break
            if any(h is not None and h[0] == tk for h in slot_holding): continue
            if consecutive.get(tk, 0) < 3: continue
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < 0: continue
            price = today_data.get(tk, {}).get('price')
            if not price or price <= 0: continue
            free = next((i for i in range(max_slots) if slot_holding[i] is None), None)
            if free is None: break
            shares = slot_cash[free] / price
            slot_holding[free] = (tk, shares, price, today)
            slot_cash[free] = 0

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100
        max_dd = min(max_dd, dd)
    max_day_loss = min(daily_returns) if daily_returns else 0
    return {'total_return': (cum-1)*100, 'max_dd': max_dd,
            'max_day_loss': max_day_loss}


VARIANTS = [
    ('slot1_100',    [1.0]),
    ('slot2_90_10',  [0.9, 0.1]),
    ('slot2_80_20',  [0.8, 0.2]),
    ('slot2_70_30',  [0.7, 0.3]),
    ('slot2_60_40',  [0.6, 0.4]),
    ('slot2_50_50',  [0.5, 0.5]),
    ('slot3_equal',  [1/3, 1/3, 1/3]),
]


def main():
    print('=' * 110)
    print(f'최적 비중 BT — EXCLUDE {EXCLUDE} (알파 핵심 의존성 검증)')
    print('=' * 110)

    db = GRID / 'wgt_no_mu_sndk.db'
    print(f'\n[step 1] DB 복제 + regenerate (EXCLUDE={EXCLUDE})')
    t0 = time.time()
    shutil.copy(DB_ORIGINAL, db)
    regenerate_exclude(db, EXCLUDE)
    print(f'  done: {time.time()-t0:.1f}s')

    dates, data, price_full = load_data(db)
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    print(f'\n[step 2] 비중 grid BT ({N_SEEDS}×{SAMPLES_PER_SEED})')
    all_results = {}
    for name, w in VARIANTS:
        t0 = time.time()
        rets, mdds, mdls, seed_avgs = [], [], [], []
        for chosen in seed_starts:
            sr = []
            for sd in chosen:
                r = simulate_entry_fixed(dates, data, price_full, w, sd,
                                        entry=ENTRY_TOP, exit_=EXIT_TOP)
                rets.append(r['total_return'])
                mdds.append(r['max_dd'])
                mdls.append(r['max_day_loss'])
                sr.append(r['total_return'])
            seed_avgs.append(sum(sr)/len(sr))
        all_results[name] = {'rets': rets, 'mdds': mdds, 'mdls': mdls,
                             'seed_avgs': seed_avgs, 'weights': w}
        avg = sum(rets)/len(rets)
        med = sorted(rets)[len(rets)//2]
        std = statistics.pstdev(rets)
        mdd = min(mdds); mdl = min(mdls)
        sharpe = avg/std if std > 0 else 0
        marker = ' ★' if name == 'slot2_90_10' else '  '
        print(f'{marker}{name:<18} {str(w):<25} avg={avg:+6.2f}% med={med:+6.2f}% '
              f'std={std:5.1f} mdd={mdd:+6.2f}% maxday={mdl:+5.2f}% sharpe={sharpe:+.2f} [{time.time()-t0:.1f}s]')

    print()
    print('=' * 110)
    print('paired vs slot2_90_10 (MU+SNDK 제외)')
    print('=' * 110)
    base = all_results['slot2_90_10']['seed_avgs']
    print(f'  {"variant":<18} {"avg lift":>10} {"med lift":>10} {"min":>10} {"max":>10} '
          f'{"wins":>10} {"verdict":>10}')
    print('  ' + '-' * 95)
    for name, _ in VARIANTS:
        if name == 'slot2_90_10': continue
        new = all_results[name]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts) / len(lifts)
        med_l = statistics.median(lifts)
        verdict = ('✓✓ 우월' if wins >= 70 else '✓ 우월' if wins >= 60
                   else '~ 동등' if wins >= 40
                   else '✗ 열세' if wins >= 30 else '✗✗ 열세')
        print(f'  {name:<18} {avg_l:+8.2f}%p {med_l:+8.2f}%p {min(lifts):+8.2f}%p '
              f'{max(lifts):+8.2f}%p {wins:>5}/{N_SEEDS}  {verdict}')

    # MU/SNDK 포함 BT (직전 결과)와 비교
    print()
    print('=' * 110)
    print('비교: MU+SNDK 포함 vs 제외')
    print('=' * 110)
    INCL = {
        'slot1_100':    (122.73, -4.23),
        'slot2_90_10':  (126.96,  0.00),
        'slot2_80_20':  (123.66, -3.30),
        'slot2_70_30':  (120.37, -6.59),
        'slot2_60_40':  (117.07, -9.89),
        'slot2_50_50':  (113.78, -13.18),
        'slot3_equal':   (96.60, -30.36),
    }
    print(f'  {"variant":<18} {"incl avg":>10} {"excl avg":>10} {"avg Δ":>8} '
          f'| {"incl lift":>10} {"excl lift":>10} {"lift swing":>10}')
    print('  ' + '-' * 100)
    base_excl = all_results['slot2_90_10']['seed_avgs']
    for name, _ in VARIANTS:
        r = all_results[name]
        excl_avg = sum(r['rets'])/len(r['rets'])
        incl_avg, incl_lift = INCL.get(name, (0, 0))
        new = all_results[name]['seed_avgs']
        lifts = [b - a for a, b in zip(base_excl, new)]
        excl_lift = sum(lifts)/len(lifts) if name != 'slot2_90_10' else 0.0
        swing = excl_lift - incl_lift
        marker = ' ★' if name == 'slot2_90_10' else '  '
        print(f'{marker}{name:<16} {incl_avg:+8.2f}% {excl_avg:+8.2f}% {excl_avg-incl_avg:+7.2f}%p '
              f'| {incl_lift:+8.2f}%p {excl_lift:+8.2f}%p {swing:+8.2f}%p')


if __name__ == '__main__':
    main()
