"""MA 필터 새 후보 BT — current 개선 탐색

지금까지 대화 인사이트 종합 기반 변형:
  A. current: MA120 + MA60 fallback (baseline, v83.3 production)
  B. ma60_or_ma120: 둘 중 하나 통과 (case 2 + case 3 모두 통과 — 가장 관대)
  C. ma120_slope_up_10: current + MA120 10d slope > 0 (장기 추세 상승 중인 것만)
  D. ma120_slope_up_20: current + MA120 20d slope > 0 (더 보수적)
  E. ma120_slope_up_10_relaxed: current + MA120 10d slope > -1% (약한 하락 허용)
  F. eps_escape: current + rev_up30 ≥ 5 강한 EPS 종목엔 MA60 fallback 항상 적용 (MA120 strict 우회)

100×3 paired BT (v83.3 params, slot 2, entry 3, exit 10, hold 0)
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
import bt_breakout_hold as bth

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'
GRID.mkdir(exist_ok=True)

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10

# v83.3 production params
ENTRY_TOP = 3
EXIT_TOP = 10
MAX_SLOTS = 2
HOLD_DAYS = 0


def get_ma_history(cur, ticker, today, n_days_back):
    """N일전 ma120 가져오기"""
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE date <= ? ORDER BY date DESC',
        (today,)
    ).fetchmany(n_days_back + 1)]
    if len(dates) < n_days_back + 1:
        return None
    past_date = dates[-1]
    row = cur.execute(
        'SELECT ma120 FROM ntm_screening WHERE date=? AND ticker=?',
        (past_date, ticker)
    ).fetchone()
    return row[0] if row and row[0] else None


def ma_pass(cur, today, tk, price, ma60, ma120, rev_up30, variant):
    """변형별 MA 필터"""
    if price is None or price <= 0:
        return False

    if variant == 'current':
        if ma120 is not None:
            return price > ma120
        return ma60 is not None and price > ma60

    if variant == 'ma60_or_ma120':
        ok60 = ma60 is not None and price > ma60
        ok120 = ma120 is not None and price > ma120
        return ok60 or ok120

    if variant in ('ma120_slope_up_10', 'ma120_slope_up_20',
                   'ma120_slope_up_10_relaxed'):
        # current 통과 우선
        if ma120 is not None:
            if not (price > ma120):
                return False
        else:
            return ma60 is not None and price > ma60  # NULL fallback은 그대로

        # slope 조건 추가
        n = 20 if variant == 'ma120_slope_up_20' else 10
        ma120_past = get_ma_history(cur, tk, today, n)
        if ma120_past is None or ma120_past <= 0:
            return True  # 데이터 부족이면 통과
        slope_pct = (ma120 - ma120_past) / ma120_past * 100
        if variant == 'ma120_slope_up_10_relaxed':
            return slope_pct > -1.0
        return slope_pct > 0

    if variant == 'eps_escape':
        # rev_up30 ≥ 5: 강한 EPS — MA60 fallback 항상 OK (case 3 통과)
        # rev_up30 < 5: 약한 EPS — current 그대로
        strong_eps = (rev_up30 or 0) >= 5
        if strong_eps:
            # MA60 OR MA120 (둘 중 하나)
            ok60 = ma60 is not None and price > ma60
            ok120 = ma120 is not None and price > ma120
            return ok60 or ok120
        else:
            # current 동작
            if ma120 is not None:
                return price > ma120
            return ma60 is not None and price > ma60

    raise ValueError(f'unknown variant: {variant}')


def regenerate(test_db, variant):
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

            if asc is None or asc <= 9: continue
            if ag is None: continue
            if px is None or px < 10: continue
            if nc is None or nc <= 0: continue
            if eps_w is None or eps_w <= 0: continue
            if not ma_pass(cur, today, tk, px, m60, m120, ru, variant): continue
            if rg is None: continue
            if rg < 0.10: continue
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
            cur.execute(
                'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                (i, today, e['ticker'])
            )
        conn.commit()

        tickers = [e['ticker'] for e in eligible]
        wmap = dr._compute_w_gap_map(cur, today, tickers)
        sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        top30 = sorted_w[:30]
        for rk, tk in enumerate(top30, 1):
            cur.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rk, today, tk)
            )
        conn.commit()

    conn.close()


def run_bt(db_path, seed_starts):
    bth.DB_PATH = db_path
    dates, data, price_series = bth.load_data_ext()
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = bth.simulate_hold(
                dates, data, price_series, hold_days=HOLD_DAYS,
                entry_top=ENTRY_TOP, exit_top=EXIT_TOP,
                max_slots=MAX_SLOTS, start_date=sd
            )
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr) / len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


VARIANTS = [
    'current',
    'ma60_or_ma120',
    'ma120_slope_up_10',
    'ma120_slope_up_20',
    'ma120_slope_up_10_relaxed',
    'eps_escape',
]


def main():
    print('=' * 110)
    print('MA 필터 새 후보 BT — current 개선 탐색')
    print(f'params: entry={ENTRY_TOP}, exit={EXIT_TOP}, slots={MAX_SLOTS} (v83.3)')
    print(f'seeds: {N_SEEDS} × {SAMPLES_PER_SEED} = {N_SEEDS*SAMPLES_PER_SEED} 시뮬/변형')
    print('=' * 110)

    # 공통 seed_starts 생성 (current DB 기준)
    cur_db_tmp = GRID / 'alt_current.db'
    shutil.copy(DB_ORIGINAL, cur_db_tmp)
    regenerate(cur_db_tmp, 'current')
    bth.DB_PATH = cur_db_tmp
    dates_cur, _, _ = bth.load_data_ext()
    eligible_starts = dates_cur[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    all_results = {}
    for variant in VARIANTS:
        db = GRID / f'alt_{variant}.db'
        print(f'\n[{variant}] DB regenerate + BT...')
        t0 = time.time()
        if variant != 'current':  # current는 이미 위에서 생성
            shutil.copy(DB_ORIGINAL, db)
            regenerate(db, variant)
        print(f'  regenerate: {time.time()-t0:.1f}s')

        t1 = time.time()
        res = run_bt(db, seed_starts)
        all_results[variant] = res
        avg = sum(res['rets']) / len(res['rets'])
        med = sorted(res['rets'])[len(res['rets']) // 2]
        worst_mdd = min(res['mdds'])
        std = statistics.pstdev(res['rets'])
        sharpe = avg/std if std > 0 else 0
        print(f'  BT: {time.time()-t1:.1f}s | avg={avg:+6.2f}% med={med:+6.2f}% '
              f'mdd={worst_mdd:+6.2f}% sharpe={sharpe:+.2f}')

    # 분포
    print()
    print('=' * 110)
    print(f'결과 분포 ({N_SEEDS * SAMPLES_PER_SEED}개 시뮬/변형)')
    print('=' * 110)
    print(f'{"variant":<30} {"avg":>9} {"median":>9} {"std":>6} '
          f'{"p25":>9} {"p75":>9} {"min":>9} {"max":>9} {"MDD":>8}')
    print('-' * 110)
    for v in VARIANTS:
        if v not in all_results: continue
        r = all_results[v]
        rets = sorted(r['rets'])
        n = len(rets)
        avg = sum(rets) / n
        med = rets[n // 2]
        std = statistics.pstdev(rets)
        p25 = rets[n // 4]; p75 = rets[3*n // 4]
        mdd = min(r['mdds'])
        marker = ' ★' if v == 'current' else '  '
        print(f'{marker}{v:<28} {avg:+8.2f}% {med:+8.2f}% {std:>5.2f} '
              f'{p25:+8.2f}% {p75:+8.2f}% {min(rets):+8.2f}% {max(rets):+8.2f}% '
              f'{mdd:+7.2f}%')

    # paired vs current
    print()
    print('=' * 110)
    print('paired vs current (production baseline) — seed별 같은 시작일')
    print('=' * 110)
    base = all_results['current']['seed_avgs']
    print(f'  {"variant":<30} {"avg lift":>10} {"med lift":>10} {"min":>10} {"max":>10} '
          f'{"wins":>10} {"verdict":>15}')
    print('  ' + '-' * 100)
    for v in VARIANTS:
        if v == 'current' or v not in all_results: continue
        new = all_results[v]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts) / len(lifts)
        med_l = statistics.median(lifts)
        verdict = ('✓✓ 명확 우월' if wins >= 70
                   else '✓ 우월' if wins >= 60
                   else '~ 동등' if wins >= 40
                   else '✗ 열세' if wins >= 30
                   else '✗✗ 명확 열세')
        print(f'  {v:<30} {avg_l:+8.2f}%p {med_l:+8.2f}%p {min(lifts):+8.2f}%p '
              f'{max(lifts):+8.2f}%p {wins:>5}/{N_SEEDS}  {verdict}')


if __name__ == '__main__':
    main()
