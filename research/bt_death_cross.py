"""데드크로스 필터 변형 BT — current + MA20/60, MA60/120 데드크로스 차단

배경:
  사용자 제안 — 가격 vs MA가 아닌 MA 간 관계 (데드크로스)로 추세 약화 종목 차단.
  AEIS 5/29 케이스: MA20<MA60 (단기 데드) 발생, MA60>MA120 (중기 골든) 유지.

변형:
  A. current (baseline) — MA120 + MA60 fallback
  B. + MA20 > MA60 강제 (단기 데드크로스 제외)
  C. + MA60 > MA120 강제 (중기 데드크로스 제외)
  D. + 둘 다 (가장 엄격)
  E. + MA20 > MA120 강제 (장기 골든만 요구)

MA20은 DB에 없음 — ntm_screening 가격 history (73일)로 일자별 계산.
첫 19일은 MA20 NULL → 통과 처리 (cold start 보호).

100×3 paired BT (v83.3 params, slot 2)
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

ENTRY_TOP = 3
EXIT_TOP = 10
MAX_SLOTS = 2
HOLD_DAYS = 0


def precompute_ma20(cur):
    """일자별 종목별 MA20 미리 계산 — {(date, ticker): ma20}"""
    print('  MA20 precompute...', end=' ', flush=True)
    t0 = time.time()
    # 모든 가격 데이터 한 번에 로드
    rows = cur.execute('''
        SELECT ticker, date, price FROM ntm_screening
        WHERE price IS NOT NULL
        ORDER BY ticker, date
    ''').fetchall()
    # ticker별 date list / price list
    by_tk = defaultdict(list)
    for tk, d, px in rows:
        by_tk[tk].append((d, px))

    ma20 = {}
    for tk, lst in by_tk.items():
        prices = [p for _, p in lst]
        dates = [d for d, _ in lst]
        for i, d in enumerate(dates):
            if i + 1 < 20:
                continue  # 데이터 부족 — NULL
            window = prices[i+1-20:i+1]
            ma20[(d, tk)] = sum(window) / 20
    print(f'{time.time()-t0:.1f}s ({len(ma20)} entries)')
    return ma20


def ma_pass(price, ma20, ma60, ma120, variant):
    if price is None or price <= 0:
        return False

    # current filter (모든 변형 공통)
    if ma120 is not None:
        if not (price > ma120):
            return False
    else:
        # MA60 fallback
        if not (ma60 is not None and price > ma60):
            return False

    # 변형별 추가 조건
    if variant == 'current':
        return True

    if variant == 'golden_short':  # MA20 > MA60 강제
        if ma20 is None or ma60 is None:
            return True  # 데이터 부족 통과
        return ma20 > ma60

    if variant == 'golden_mid':  # MA60 > MA120 강제
        if ma60 is None or ma120 is None:
            return True
        return ma60 > ma120

    if variant == 'golden_both':  # MA20>MA60 AND MA60>MA120
        ok_short = (ma20 is None or ma60 is None) or (ma20 > ma60)
        ok_mid = (ma60 is None or ma120 is None) or (ma60 > ma120)
        return ok_short and ok_mid

    if variant == 'golden_long':  # MA20 > MA120 강제
        if ma20 is None or ma120 is None:
            return True
        return ma20 > ma120

    raise ValueError(f'unknown: {variant}')


def regenerate(test_db, variant, ma20_map):
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
            m20 = ma20_map.get((today, tk))
            if not ma_pass(px, m20, m60, m120, variant): continue
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
    'golden_short',   # MA20 > MA60
    'golden_mid',     # MA60 > MA120
    'golden_both',    # 둘 다
    'golden_long',    # MA20 > MA120
]


def main():
    print('=' * 110)
    print('데드크로스 필터 BT — current + MA cross 조건')
    print(f'params: entry={ENTRY_TOP}, exit={EXIT_TOP}, slots={MAX_SLOTS} (v83.3)')
    print(f'seeds: {N_SEEDS} × {SAMPLES_PER_SEED}')
    print('=' * 110)

    # MA20 precompute (원본 DB에서 한 번)
    print('\n[step 0] MA20 precompute')
    src_conn = sqlite3.connect(DB_ORIGINAL)
    ma20_map = precompute_ma20(src_conn.cursor())
    src_conn.close()

    # seed_starts (current DB 기준)
    cur_db = GRID / 'dc_current.db'
    print(f'\n[step 1] current DB 생성')
    shutil.copy(DB_ORIGINAL, cur_db)
    regenerate(cur_db, 'current', ma20_map)
    bth.DB_PATH = cur_db
    dates_cur, _, _ = bth.load_data_ext()
    eligible_starts = dates_cur[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    all_results = {}
    for variant in VARIANTS:
        db = GRID / f'dc_{variant}.db'
        print(f'\n[{variant}] regenerate + BT...')
        t0 = time.time()
        if variant != 'current':
            shutil.copy(DB_ORIGINAL, db)
            regenerate(db, variant, ma20_map)
        print(f'  regenerate: {time.time()-t0:.1f}s')

        t1 = time.time()
        res = run_bt(db, seed_starts)
        all_results[variant] = res
        avg = sum(res['rets']) / len(res['rets'])
        med = sorted(res['rets'])[len(res['rets']) // 2]
        worst_mdd = min(res['mdds'])
        std = statistics.pstdev(res['rets'])
        print(f'  BT: {time.time()-t1:.1f}s | avg={avg:+6.2f}% med={med:+6.2f}% '
              f'mdd={worst_mdd:+6.2f}% std={std:.1f}')

    # 분포
    print()
    print('=' * 110)
    print(f'결과 분포 ({N_SEEDS * SAMPLES_PER_SEED}개 시뮬/변형)')
    print('=' * 110)
    print(f'{"variant":<18} {"avg":>9} {"median":>9} {"std":>6} '
          f'{"p25":>9} {"p75":>9} {"min":>9} {"max":>9} {"MDD":>8}')
    print('-' * 100)
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
        print(f'{marker}{v:<16} {avg:+8.2f}% {med:+8.2f}% {std:>5.2f} '
              f'{p25:+8.2f}% {p75:+8.2f}% {min(rets):+8.2f}% {max(rets):+8.2f}% '
              f'{mdd:+7.2f}%')

    # paired
    print()
    print('=' * 110)
    print('paired vs current')
    print('=' * 110)
    base = all_results['current']['seed_avgs']
    print(f'  {"variant":<18} {"avg lift":>10} {"med lift":>10} {"min":>10} {"max":>10} '
          f'{"wins":>8} {"verdict":>15}')
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
        print(f'  {v:<18} {avg_l:+8.2f}%p {med_l:+8.2f}%p {min(lifts):+8.2f}%p '
              f'{max(lifts):+8.2f}%p {wins:>5}/{N_SEEDS}  {verdict}')

    # 각 변형의 Top 3 entry zone 변화 분석
    print()
    print('=' * 110)
    print('Top 3 entry zone 차이 (current 대비 누적 종목 세트 diff)')
    print('=' * 110)
    cc_db = GRID / 'dc_current.db'
    cc = sqlite3.connect(cc_db).cursor()
    dates_db = [r[0] for r in cc.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    for v in VARIANTS:
        if v == 'current': continue
        vc = sqlite3.connect(GRID / f'dc_{v}.db').cursor()
        only_c, only_v, same = 0, 0, 0
        only_c30, only_v30, same30 = 0, 0, 0
        for d in dates_db:
            sc = set(r[0] for r in cc.execute(
                'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank <= 3', (d,)
            ).fetchall())
            sv = set(r[0] for r in vc.execute(
                'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank <= 3', (d,)
            ).fetchall())
            only_c += len(sc - sv); only_v += len(sv - sc); same += len(sc & sv)
            sc30 = set(r[0] for r in cc.execute(
                'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,)
            ).fetchall())
            sv30 = set(r[0] for r in vc.execute(
                'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,)
            ).fetchall())
            only_c30 += len(sc30 - sv30); only_v30 += len(sv30 - sc30); same30 += len(sc30 & sv30)
        print(f'  {v:<18}  Top3: same {same:>3} cur-only {only_c:>3} {v}-only {only_v:>3} | '
              f'Top30: same {same30:>4} cur-only {only_c30:>3} {v}-only {only_v30:>3}')


if __name__ == '__main__':
    main()
