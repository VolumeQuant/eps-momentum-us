"""MA/추세 필터 새 차원 검증 — 이전 검증 안 한 영역

이전 검증한 변형 (다 보류 또는 거부):
  current, ma60_only, ma120_strict, ma60_and_ma120, ma60_or_ma120, no_ma,
  ma120_and_60, dd60_30, dd60_20, ma20_above, golden_short/mid/both/long,
  ma_aligned, price_above_3ma, ma_converge, ma120_slope_up_10/20, eps_escape

새 차원 후보 (이번 검증):
  1. low_vol_3      : 60일 일간 std < 3%/day (저변동성만, AEIS 3.84% 차단)
  2. low_vol_2      : < 2%/day (더 엄격, 안정주만)
  3. dd_30_15       : 30일 high -15% 이상 빠진 종목 제외 (단기 약세 차단)
  4. dd_30_20       : 30일 high -20% drawdown
  5. ma120_x1_3     : price > MA120 × 1.3 시 제외 (과매수 차단)
  6. uptrend_strict : price > MA20 > MA60 > MA120 (정배열 + price 위)
  7. combined       : low_vol_3 + dd_30_15 (저변동성 + 단기 약세 차단)
  8. dd_combined    : dd60_30 + dd_30_15 (이전 발견 + 단기 결합)

production 환경: entry=2, exit=10, slot=2, entry_fixed simulator
weight: fixed_90_10 (baseline 통일)
두 환경: incl / excl (MU+SNDK 제외)
"""
import sys
import shutil
import sqlite3
import random
import statistics
import math
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


def precompute_extras(db_path):
    """MA20, 60일 high/std, 30일 high precompute"""
    conn = sqlite3.connect(db_path); cur = conn.cursor()
    rows = cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL ORDER BY ticker, date').fetchall()
    by_tk = defaultdict(list)
    for tk, d, px in rows:
        by_tk[tk].append((d, px))
    ma20 = {}
    high60 = {}
    high30 = {}
    vol60 = {}  # 일간 returns std (60일)
    for tk, lst in by_tk.items():
        prices = [p for _, p in lst]
        dates_ = [d for d, _ in lst]
        # daily returns
        rets = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                rets.append((prices[i] - prices[i-1]) / prices[i-1] * 100)
            else:
                rets.append(0)
        for i, d in enumerate(dates_):
            if i + 1 >= 20: ma20[(d, tk)] = sum(prices[i+1-20:i+1]) / 20
            if i + 1 >= 30: high30[(d, tk)] = max(prices[i+1-30:i+1])
            if i + 1 >= 60:
                high60[(d, tk)] = max(prices[i+1-60:i+1])
                if i >= 60:
                    window_rets = rets[i-60:i]
                    if len(window_rets) >= 2:
                        vol60[(d, tk)] = statistics.pstdev(window_rets)
            elif i + 1 >= 20:
                high60[(d, tk)] = max(prices[:i+1])
                if i + 1 >= 20 and len(rets[:i]) >= 2:
                    vol60[(d, tk)] = statistics.pstdev(rets[:i])
    conn.close()
    return {'ma20': ma20, 'high60': high60, 'high30': high30, 'vol60': vol60}


def ma_pass(price, ma60, ma120, extras, variant):
    if price is None or price <= 0: return False

    # current 기본 필터 (모든 변형 공통)
    if ma120 is not None:
        if not (price > ma120): return False
    else:
        if not (ma60 is not None and price > ma60): return False

    ma20 = extras.get('ma20')
    high60 = extras.get('high60')
    high30 = extras.get('high30')
    vol60 = extras.get('vol60')

    if variant == 'current':
        return True

    if variant == 'low_vol_3':
        if vol60 is None: return True
        return vol60 < 3.0

    if variant == 'low_vol_2_5':
        if vol60 is None: return True
        return vol60 < 2.5

    if variant == 'low_vol_2':
        if vol60 is None: return True
        return vol60 < 2.0

    if variant == 'dd_30_20':
        if high30 is None: return True
        dd = (price - high30) / high30 * 100
        return dd > -20

    if variant == 'dd_30_15':
        if high30 is None: return True
        dd = (price - high30) / high30 * 100
        return dd > -15

    if variant == 'dd_30_10':
        if high30 is None: return True
        dd = (price - high30) / high30 * 100
        return dd > -10

    if variant == 'ma120_x1_3':
        if ma120 is None: return True
        return price < ma120 * 1.3

    if variant == 'ma120_x1_5':
        if ma120 is None: return True
        return price < ma120 * 1.5

    if variant == 'uptrend_strict':
        if ma20 is None or ma60 is None or ma120 is None: return True
        return price > ma20 > ma60 > ma120

    if variant == 'combined_lv3_dd15':
        # low_vol_3 + dd_30_15
        if vol60 is not None and vol60 >= 3.0: return False
        if high30 is not None:
            dd = (price - high30) / high30 * 100
            if dd <= -15: return False
        return True

    if variant == 'combined_lv3_dd60_30':
        # low_vol_3 + dd60_30 (이전 발견 결합)
        if vol60 is not None and vol60 >= 3.0: return False
        if high60 is not None:
            dd = (price - high60) / high60 * 100
            if dd <= -30: return False
        return True

    if variant == 'combined_dd30_15_dd60_30':
        if high30 is not None:
            dd = (price - high30) / high30 * 100
            if dd <= -15: return False
        if high60 is not None:
            dd = (price - high60) / high60 * 100
            if dd <= -30: return False
        return True

    raise ValueError(variant)


def regenerate(db, variant, exclude_set, extras_maps):
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
            extras = {
                'ma20': extras_maps['ma20'].get((today, tk)),
                'high60': extras_maps['high60'].get((today, tk)),
                'high30': extras_maps['high30'].get((today, tk)),
                'vol60': extras_maps['vol60'].get((today, tk)),
            }
            if not ma_pass(px, m60, m120, extras, variant): continue
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


VARIANTS = [
    'current',
    'low_vol_3',
    'low_vol_2_5',
    'low_vol_2',
    'dd_30_20',
    'dd_30_15',
    'dd_30_10',
    'ma120_x1_3',
    'ma120_x1_5',
    'uptrend_strict',
    'combined_lv3_dd15',
    'combined_lv3_dd60_30',
    'combined_dd30_15_dd60_30',
]


def main():
    print('=' * 110)
    print('★ MA/추세 필터 새 차원 검증')
    print(f'  entry={ENTRY_TOP}, exit={EXIT_TOP}, slot=2, weight=fixed_90_10')
    print(f'  entry_fixed simulator (사용자 의도 일치)')
    print('=' * 110)

    print('\n[step 0] MA20 + 60일 high/std + 30일 high precompute...')
    t0 = time.time()
    extras_maps = precompute_extras(DB_ORIGINAL)
    print(f'  done {time.time()-t0:.1f}s (vol60: {len(extras_maps["vol60"])} entries)')

    # AEIS 변동성 확인
    aeis_vols = [(d, v) for (d, tk), v in extras_maps['vol60'].items() if tk == 'AEIS']
    if aeis_vols:
        avg_aeis_vol = sum(v for _, v in aeis_vols) / len(aeis_vols)
        print(f'  AEIS 평균 60일 일간 std: {avg_aeis_vol:.2f}%/day')

    fixed_90 = sw.make_weight_fn({'type': 'fixed', 'weights': [0.9, 0.1]})
    results = {'incl': {}, 'excl': {}}

    for env_name, excl in [('incl', set()), ('excl', {'MU', 'SNDK'})]:
        print(f'\n[{env_name}]')
        for variant in VARIANTS:
            db = GRID / f'nm_{env_name}_{variant}.db'
            t0 = time.time()
            shutil.copy(DB_ORIGINAL, db)
            regenerate(db, variant, excl, extras_maps)
            res = run_bt(db, fixed_90)
            results[env_name][variant] = res
            marker = ' ★' if variant == 'current' else '  '
            print(f'{marker}{variant:<28} avg={res["avg"]:+6.2f}% mdd={res["mdd"]:+6.2f}% sharpe={res["sharpe"]:+.2f} [{time.time()-t0:.1f}s]')

    # 종합
    print()
    print('=' * 110)
    print('★ 종합 (lift vs current, 양 환경 robust 평가)')
    print('=' * 110)
    base_i = results['incl']['current']['seed_avgs']
    base_e = results['excl']['current']['seed_avgs']
    rows = []
    print(f'  {"필터":<28} {"i_avg":>8} {"i_mdd":>8} {"i_lift":>8} {"i_win":>7} | {"e_avg":>8} {"e_mdd":>8} {"e_lift":>8} {"e_win":>7} | {"avg_lift":>9}')
    print('  ' + '-' * 110)
    for variant in VARIANTS:
        ri = results['incl'][variant]; re = results['excl'][variant]
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        avg_both = (avgi + avge) / 2
        rows.append((variant, ri['avg'], ri['mdd'], avgi, wi, re['avg'], re['mdd'], avge, we, avg_both))

    rows.sort(key=lambda x: -x[9])
    for r in rows:
        marker = ' ★' if r[0] == 'current' else '  '
        print(f'{marker}{r[0]:<26} {r[1]:+7.2f}% {r[2]:+7.2f}% {r[3]:+7.2f}%p {r[4]:>4}/100 | '
              f'{r[5]:+7.2f}% {r[6]:+7.2f}% {r[7]:+7.2f}%p {r[8]:>4}/100 | {r[9]:+7.2f}%p')

    print()
    print('=' * 110)
    print('★ Robust 우월 (두 환경 모두 wins ≥ 60)')
    print('=' * 110)
    for r in rows:
        if r[4] >= 60 and r[8] >= 60:
            print(f'  ✓✓ {r[0]:<28} incl +{r[3]:.2f}%p ({r[4]}/100), excl +{r[7]:.2f}%p ({r[8]}/100)')


if __name__ == '__main__':
    main()
