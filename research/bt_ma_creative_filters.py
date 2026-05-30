"""MA 필터 창의적 변형 검증 — 사용자 아이디어 + 다양한 조합

변형:
  current      : MA120 + MA60 fallback (baseline)
  not_inverse  : 역배열(MA20<MA60<MA120) 제외 — 사용자 아이디어
  ma_aligned   : 정배열만 (MA20>MA60>MA120 AND price>MA20)
  price_above_3ma : price가 MA20+MA60+MA120 모두 위
  dd60_30      : 60일 high 대비 -30%+ drawdown 제외
  dd60_20      : 60일 high 대비 -20%+ drawdown 제외
  ma_converge  : |MA20-MA60|/MA60 < 5% (MA 수렴 종목만)
  ma20_above   : price > MA20만 (MA120/60 무관)

두 환경 (incl/excl MU+SNDK) × dynamic simulator + fixed_90_10 weight.
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


def precompute_extras(db_path):
    """MA20, 60일 high precompute"""
    conn = sqlite3.connect(db_path); cur = conn.cursor()
    rows = cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL ORDER BY ticker, date').fetchall()
    by_tk = defaultdict(list)
    for tk, d, px in rows:
        by_tk[tk].append((d, px))
    ma20 = {}
    high60 = {}
    for tk, lst in by_tk.items():
        prices = [p for _, p in lst]
        dates = [d for d, _ in lst]
        for i, d in enumerate(dates):
            if i + 1 >= 20:
                ma20[(d, tk)] = sum(prices[i+1-20:i+1]) / 20
            if i + 1 >= 60:
                high60[(d, tk)] = max(prices[i+1-60:i+1])
            elif i + 1 >= 20:
                high60[(d, tk)] = max(prices[:i+1])  # 부족하면 그동안 max
    conn.close()
    return ma20, high60


def ma_pass(price, ma60, ma120, ma20, high60, variant):
    if price is None or price <= 0: return False

    # current baseline 모든 변형 공통 (MA120 + MA60 fallback)
    if ma120 is not None:
        if not (price > ma120): return False
    else:
        if not (ma60 is not None and price > ma60): return False

    # 변형별 추가 조건
    if variant == 'current':
        return True

    if variant == 'not_inverse':
        # 역배열(MA20 < MA60 < MA120) 제외
        if ma20 is not None and ma60 is not None and ma120 is not None:
            if ma20 < ma60 < ma120:
                return False
        return True

    if variant == 'ma_aligned':
        # 정배열만: price > MA20 > MA60 > MA120 모두 만족
        if ma20 is None or ma60 is None or ma120 is None:
            return True  # 데이터 부족 통과 (cold start 보호)
        return price > ma20 > ma60 > ma120

    if variant == 'price_above_3ma':
        # price가 3개 MA 모두 위
        if ma20 is None: return True
        ok20 = price > ma20
        ok60 = ma60 is not None and price > ma60
        ok120 = ma120 is not None and price > ma120
        return ok20 and ok60 and ok120

    if variant == 'dd60_30':
        # 60일 high 대비 -30%+ drawdown 제외
        if high60 is None: return True
        dd = (price - high60) / high60 * 100
        return dd > -30

    if variant == 'dd60_20':
        if high60 is None: return True
        dd = (price - high60) / high60 * 100
        return dd > -20

    if variant == 'ma_converge':
        # |MA20-MA60|/MA60 < 5% (수렴)
        if ma20 is None or ma60 is None or ma60 <= 0: return True
        return abs(ma20 - ma60) / ma60 < 0.05

    if variant == 'ma20_above':
        # price > MA20 (current 위에 추가 조건)
        if ma20 is None: return True
        return price > ma20

    raise ValueError(variant)


def regenerate(db, variant, exclude_set, ma20_map, high60_map):
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
            m20 = ma20_map.get((today, tk))
            h60 = high60_map.get((today, tk))
            if not ma_pass(px, m60, m120, m20, h60, variant): continue
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
    dates, data, price_full = sw.load_data(db_path)
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = sw.simulate_dynamic(dates, data, price_full, scores, weight_fn, sd,
                                  max_slots=2, entry=ENTRY_TOP, exit_=EXIT_TOP)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    avg = sum(rets)/len(rets); med = sorted(rets)[len(rets)//2]
    std = statistics.pstdev(rets); mdd = min(mdds); sharpe = avg/std if std > 0 else 0
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs,
            'avg': avg, 'med': med, 'std': std, 'mdd': mdd, 'sharpe': sharpe}


VARIANTS = [
    'current',
    'not_inverse',
    'ma_aligned',
    'price_above_3ma',
    'dd60_30',
    'dd60_20',
    'ma_converge',
    'ma20_above',
]


def main():
    print('=' * 110)
    print('MA 필터 창의적 변형 검증')
    print(f'simulator: dynamic + fixed_90_10 weight (v83.3 환경)')
    print('=' * 110)

    print('\n[step 0] MA20 + 60일 high precompute...')
    t0 = time.time()
    ma20_map, high60_map = precompute_extras(DB_ORIGINAL)
    print(f'  done {time.time()-t0:.1f}s (ma20: {len(ma20_map)}, high60: {len(high60_map)})')

    fixed_90 = sw.make_weight_fn({'type': 'fixed', 'weights': [0.9, 0.1]})

    for env_name, excl in [('incl', set()), ('excl', {'MU', 'SNDK'})]:
        print(f'\n{"="*110}')
        print(f'환경: {env_name} (EXCLUDE={excl or "none"})')
        print('=' * 110)

        results = {}
        for variant in VARIANTS:
            db = GRID / f'cre_{env_name}_{variant}.db'
            t0 = time.time()
            shutil.copy(DB_ORIGINAL, db)
            regenerate(db, variant, excl, ma20_map, high60_map)
            res = run_bt(db, fixed_90)
            results[variant] = res
            marker = ' ★' if variant == 'current' else '  '
            print(f'{marker}{variant:<18} avg={res["avg"]:+6.2f}% med={res["med"]:+6.2f}% '
                  f'std={res["std"]:5.1f} mdd={res["mdd"]:+6.2f}% sharpe={res["sharpe"]:+.2f} [{time.time()-t0:.1f}s]')

        # paired vs current
        base = results['current']['seed_avgs']
        print(f'\n  paired vs current:')
        print(f'  {"variant":<18} {"avg lift":>10} {"med lift":>10} {"min":>10} {"max":>10} {"wins":>10} {"verdict":>12}')
        print('  ' + '-' * 95)
        for variant in VARIANTS:
            if variant == 'current': continue
            new = results[variant]['seed_avgs']
            lifts = [b - a for a, b in zip(base, new)]
            wins = sum(1 for l in lifts if l > 0.01)
            avg_l = sum(lifts)/len(lifts); med_l = statistics.median(lifts)
            verdict = ('✓✓ 우월' if wins >= 70 else '✓ 우월' if wins >= 60
                       else '~ 동등' if wins >= 40
                       else '✗ 열세' if wins >= 30 else '✗✗ 열세')
            print(f'  {variant:<18} {avg_l:+8.2f}%p {med_l:+8.2f}%p {min(lifts):+8.2f}%p '
                  f'{max(lifts):+8.2f}%p {wins:>5}/{N_SEEDS}  {verdict}')

        # 환경별 결과 저장
        if env_name == 'incl':
            res_incl = results
        else:
            res_excl = results

    # 종합 robust 비교 (두 환경 평균 lift)
    print()
    print('=' * 110)
    print('★ 두 환경 평균 lift (robust 평가)')
    print('=' * 110)
    base_i = res_incl['current']['seed_avgs']
    base_e = res_excl['current']['seed_avgs']
    rows = []
    for variant in VARIANTS:
        if variant == 'current': continue
        li = [b - a for a, b in zip(base_i, res_incl[variant]['seed_avgs'])]
        le = [b - a for a, b in zip(base_e, res_excl[variant]['seed_avgs'])]
        avg_i = sum(li)/len(li); avg_e = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        avg_both = (avg_i + avg_e) / 2
        rows.append((variant, avg_i, wi, avg_e, we, avg_both))
    rows.sort(key=lambda x: -x[5])
    print(f'  {"variant":<18} {"incl lift":>10} {"incl wins":>10} {"excl lift":>10} {"excl wins":>10} {"평균":>9}')
    print('  ' + '-' * 80)
    for r in rows:
        print(f'  {r[0]:<18} {r[1]:+8.2f}%p {r[2]:>4}/100 {r[3]:+8.2f}%p {r[4]:>4}/100 {r[5]:+7.2f}%p')


if __name__ == '__main__':
    main()
