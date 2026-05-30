"""MA120 AND MA60 진입 필터 BT (v83.3 slot 2)

current vs user_proposal:
  current      : price > MA120 (NULL이면 MA60 fallback)
  ma120_and_60 : price > MA120 AND price > MA60 (NULL이면 MA60 fallback)
                = case 2 (MA120 위, MA60 아래) 종목 제외

AEIS 같은 case 2 종목을 컷하는 효과.
SNDK/MU 같은 MA120 NULL 신규 종목은 두 변형 모두 MA60 fallback 적용 (같음).
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

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 3
EXIT_TOP = 10
MAX_SLOTS = 2  # v83.3


def ma_pass(price, ma60, ma120, variant):
    if price is None or price <= 0: return False
    if variant == 'current':
        # production: MA120 우선, NULL이면 MA60 fallback
        if ma120 is not None:
            return price > ma120
        return ma60 is not None and price > ma60
    if variant == 'ma120_and_60':
        # MA120 있으면 둘 다 필수. NULL이면 MA60 fallback (신규 상장 보호)
        if ma120 is not None:
            ok120 = price > ma120
            ok60 = ma60 is not None and price > ma60
            return ok120 and ok60
        return ma60 is not None and price > ma60
    raise ValueError(variant)


def regenerate(db, variant):
    conn = sqlite3.connect(db); cur = conn.cursor()
    dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date').fetchall()]
    cur.execute('UPDATE ntm_screening SET composite_rank=NULL, part2_rank=NULL'); conn.commit()
    for today in dates:
        rows = cur.execute('SELECT ticker, adj_score, adj_gap, eps_chg_weighted, price, ma60, ma120, ntm_current, ntm_90d, rev_growth, num_analysts, rev_up30, rev_down30, operating_margin, gross_margin, free_cashflow, roe FROM ntm_screening WHERE date=?', (today,)).fetchall()
        if not rows: continue
        eligible = []
        for r in rows:
            (tk, asc, ag, eps_w, px, m60, m120, nc, n90, rg, na, ru, rd, om, gm, fcf, roe) = r
            if asc is None or asc <= 9: continue
            if ag is None: continue
            if px is None or px < 10: continue
            if nc is None or nc <= 0: continue
            if eps_w is None or eps_w <= 0: continue
            if not ma_pass(px, m60, m120, variant): continue
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


def run_bt(db_path, seed_starts):
    bth.DB_PATH = db_path
    dates, data, price_series = bth.load_data_ext()
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = bth.simulate_hold(dates, data, price_series, hold_days=0,
                                  entry_top=ENTRY_TOP, exit_top=EXIT_TOP,
                                  max_slots=MAX_SLOTS, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd']); sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 100)
    print('MA120 + MA60 (둘 다 통과 필수) vs current')
    print(f'params: slot {MAX_SLOTS}, entry={ENTRY_TOP}, exit={EXIT_TOP} (v83.3)')
    print('=' * 100)

    all_results = {}
    for variant in ['current', 'ma120_and_60']:
        db = GRID / f'and_{variant}.db'
        print(f'\n[{variant}] regenerate + BT...')
        t0 = time.time()
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, variant)
        print(f'  regen {time.time()-t0:.1f}s')
        if variant == 'current':
            bth.DB_PATH = db
            dates_c, _, _ = bth.load_data_ext()
            eligible_starts = dates_c[:-MIN_HOLD_DAYS]
            seed_starts = []
            for seed_i in range(N_SEEDS):
                random.seed(seed_i)
                seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
        t1 = time.time()
        res = run_bt(db, seed_starts)
        all_results[variant] = res
        avg = sum(res['rets'])/len(res['rets'])
        med = sorted(res['rets'])[len(res['rets'])//2]
        std = statistics.pstdev(res['rets'])
        mdd = min(res['mdds'])
        print(f'  BT {time.time()-t1:.1f}s | avg={avg:+6.2f}% med={med:+6.2f}% std={std:.1f} mdd={mdd:+6.2f}%')

    # paired
    base = all_results['current']['seed_avgs']
    new = all_results['ma120_and_60']['seed_avgs']
    lifts = [b - a for a, b in zip(base, new)]
    wins = sum(1 for l in lifts if l > 0)
    avg_l = sum(lifts) / len(lifts); med_l = statistics.median(lifts)
    print(f'\n  paired (ma120_and_60 - current):')
    print(f'    avg lift: {avg_l:+.2f}%p')
    print(f'    med lift: {med_l:+.2f}%p')
    print(f'    min: {min(lifts):+.2f}%p, max: {max(lifts):+.2f}%p')
    print(f'    wins: {wins}/{N_SEEDS}')

    # AEIS 같은 케이스가 제외되는지 확인
    print()
    print('=' * 100)
    print('AEIS-style 종목 (Top 3 진입한 case 2: MA120 위 MA60 아래) 분석')
    print('=' * 100)
    cc = sqlite3.connect(GRID / 'and_current.db').cursor()
    ac = sqlite3.connect(GRID / 'and_ma120_and_60.db').cursor()
    dates_db = [r[0] for r in cc.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]
    only_c_top3 = defaultdict(int)
    only_a_top3 = defaultdict(int)
    for d in dates_db:
        sc = set(r[0] for r in cc.execute('SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank <= 3', (d,)).fetchall())
        sa = set(r[0] for r in ac.execute('SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank <= 3', (d,)).fetchall())
        for t in sc - sa: only_c_top3[t] += 1
        for t in sa - sc: only_a_top3[t] += 1
    print(f'\n  current Top 3에만 있는 종목 (= ma120_and_60에서 컷됨):')
    for t, n in sorted(only_c_top3.items(), key=lambda x: -x[1])[:10]:
        print(f'    {t}: {n}일')
    print(f'  ma120_and_60 Top 3에만 있는 종목 (= 새로 진입):')
    for t, n in sorted(only_a_top3.items(), key=lambda x: -x[1])[:10]:
        print(f'    {t}: {n}일')


if __name__ == '__main__':
    main()
