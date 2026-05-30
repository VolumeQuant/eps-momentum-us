"""마스터 정리 BT — production 정확 환경에서 비중 + MA 필터 통합 비교

환경 통일:
  - entry=2, exit=10, slot=2 (v83.3 production 정확)
  - entry_fixed simulator (사용자 명시: "한번 1위 90%로 샀으면 유지")
  - 100×3 paired
  - 2환경: MU+SNDK 포함(incl) / 제외(excl)

비교 대상:
  매수 비중: slot1, 90/10(baseline), 80/20, 70/30, 60/40, 50/50, 2step_t15
  MA 필터: current(baseline), ma60_only, ma120_strict, ma60_and_ma120, ma120_and_60,
           ma60_or_ma120, no_ma, dd60_30, dd60_20, ma20_above
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
import bt_verify_blindspots as vb  # hybrid simulator (entry_fixed)

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 2  # ← production 정확
EXIT_TOP = 10
MAX_SLOTS = 2


def precompute_extras(db_path):
    conn = sqlite3.connect(db_path); cur = conn.cursor()
    rows = cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL ORDER BY ticker, date').fetchall()
    by_tk = defaultdict(list)
    for tk, d, px in rows:
        by_tk[tk].append((d, px))
    ma20, high60 = {}, {}
    for tk, lst in by_tk.items():
        prices = [p for _, p in lst]
        dates_ = [d for d, _ in lst]
        for i, d in enumerate(dates_):
            if i + 1 >= 20: ma20[(d, tk)] = sum(prices[i+1-20:i+1]) / 20
            if i + 1 >= 60: high60[(d, tk)] = max(prices[i+1-60:i+1])
            elif i + 1 >= 20: high60[(d, tk)] = max(prices[:i+1])
    conn.close()
    return ma20, high60


def ma_pass(price, ma60, ma120, ma20, high60, variant):
    if price is None or price <= 0: return False
    if variant == 'no_ma':
        return True
    if variant == 'ma60_only':
        return ma60 is not None and price > ma60
    if variant == 'ma120_strict':
        return ma120 is not None and price > ma120
    if variant == 'ma60_and_ma120':
        ok60 = ma60 is not None and price > ma60
        ok120 = ma120 is not None and price > ma120
        return ok60 and ok120
    if variant == 'ma60_or_ma120':
        ok60 = ma60 is not None and price > ma60
        ok120 = ma120 is not None and price > ma120
        return ok60 or ok120
    if variant == 'current':
        if ma120 is not None: return price > ma120
        return ma60 is not None and price > ma60
    if variant == 'ma120_and_60':
        # MA120 있으면 둘 다, NULL이면 MA60 fallback
        if ma120 is not None:
            ok120 = price > ma120
            ok60 = ma60 is not None and price > ma60
            return ok120 and ok60
        return ma60 is not None and price > ma60
    if variant == 'dd60_30':
        # current + 60일 -30% drawdown 제외
        if ma120 is not None:
            if not (price > ma120): return False
        else:
            if not (ma60 is not None and price > ma60): return False
        if high60 is None: return True
        return (price - high60) / high60 * 100 > -30
    if variant == 'dd60_20':
        if ma120 is not None:
            if not (price > ma120): return False
        else:
            if not (ma60 is not None and price > ma60): return False
        if high60 is None: return True
        return (price - high60) / high60 * 100 > -20
    if variant == 'ma20_above':
        if ma120 is not None:
            if not (price > ma120): return False
        else:
            if not (ma60 is not None and price > ma60): return False
        if ma20 is None: return True
        return price > ma20
    raise ValueError(variant)


def regenerate(db, variant, exclude_set, ma20_map, high60_map):
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


def run_bt(db_path, weight_fn, max_slots=2):
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
                                  max_slots=max_slots, entry=ENTRY_TOP, exit_=EXIT_TOP)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    avg = sum(rets)/len(rets); med = sorted(rets)[len(rets)//2]
    std = statistics.pstdev(rets); mdd = min(mdds); sharpe = avg/std if std > 0 else 0
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs,
            'avg': avg, 'med': med, 'std': std, 'mdd': mdd, 'sharpe': sharpe}


# ===== 1. 매수 비중 변형 (current 필터 + 비중 변경) =====
WEIGHT_VARIANTS = [
    ('slot1_100only',    sw.make_weight_fn({'type': 'fixed', 'weights': [1.0]}),  1),
    ('★90/10 (v83.3)',   sw.make_weight_fn({'type': 'fixed', 'weights': [0.9, 0.1]}), 2),
    ('80/20',            sw.make_weight_fn({'type': 'fixed', 'weights': [0.8, 0.2]}), 2),
    ('70/30',            sw.make_weight_fn({'type': 'fixed', 'weights': [0.7, 0.3]}), 2),
    ('60/40',            sw.make_weight_fn({'type': 'fixed', 'weights': [0.6, 0.4]}), 2),
    ('50/50',            sw.make_weight_fn({'type': 'fixed', 'weights': [0.5, 0.5]}), 2),
    ('2step_t15_100_50', sw.make_weight_fn({'type': 'step_2', 'lo': 0, 'hi': 15, 'wH': 1.0, 'wM': 0.5, 'wL': 0.5}), 2),
]

# ===== 2. MA 필터 변형 (90/10 비중 고정) =====
MA_VARIANTS = [
    '★current',
    'ma60_only',
    'ma120_strict',
    'ma60_and_ma120',
    'ma120_and_60',
    'ma60_or_ma120',
    'no_ma',
    'dd60_30',
    'dd60_20',
    'ma20_above',
]


def main():
    print('=' * 110)
    print('★ 마스터 정리 BT — production 정확 환경')
    print(f'  entry={ENTRY_TOP}, exit={EXIT_TOP}, slot={MAX_SLOTS} (v83.3)')
    print(f'  entry_fixed simulator (한 번 매수하면 비중 고정)')
    print(f'  100 seed × 3 starts paired')
    print('=' * 110)

    print('\n[step 0] MA20 + 60일 high precompute...')
    ma20_map, high60_map = precompute_extras(DB_ORIGINAL)

    # ============================================================
    # 1. 매수 비중 (current 필터 + 비중 변형)
    # ============================================================
    print('\n' + '=' * 110)
    print('★ Part 1: 매수 비중 비교 (current 필터 고정, 비중만 변경)')
    print('=' * 110)

    results_w = {'incl': {}, 'excl': {}}
    for env_name, excl in [('incl', set()), ('excl', {'MU', 'SNDK'})]:
        print(f'\n[{env_name}] current 필터 DB 생성...')
        db = GRID / f'master_w_{env_name}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, 'current', excl, ma20_map, high60_map)
        for name, wfn, slots in WEIGHT_VARIANTS:
            t0 = time.time()
            res = run_bt(db, wfn, max_slots=slots)
            results_w[env_name][name] = res
            print(f'  {name:<22} avg={res["avg"]:+6.2f}% mdd={res["mdd"]:+6.2f}% sharpe={res["sharpe"]:+.2f} [{time.time()-t0:.1f}s]')

    # 비중 종합 표
    print()
    print('=' * 110)
    print('비중 종합 (lift = baseline 90/10 대비, paired wins / 100)')
    print('=' * 110)
    base_i = results_w['incl']['★90/10 (v83.3)']['seed_avgs']
    base_e = results_w['excl']['★90/10 (v83.3)']['seed_avgs']
    print(f'{"비중":<22} {"i_avg":>9} {"i_mdd":>8} {"i_lift":>9} {"i_win":>7} | {"e_avg":>9} {"e_mdd":>8} {"e_lift":>9} {"e_win":>7}')
    print('-' * 110)
    for name, _, _ in WEIGHT_VARIANTS:
        ri = results_w['incl'][name]; re = results_w['excl'][name]
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        print(f'{name:<22} {ri["avg"]:+8.2f}% {ri["mdd"]:+7.2f}% {avgi:+7.2f}%p {wi:>4}/100 | '
              f'{re["avg"]:+8.2f}% {re["mdd"]:+7.2f}% {avge:+7.2f}%p {we:>4}/100')

    # ============================================================
    # 2. MA 필터 (90/10 비중 고정)
    # ============================================================
    print('\n' + '=' * 110)
    print('★ Part 2: MA 필터 비교 (90/10 비중 고정, 필터만 변경)')
    print('=' * 110)

    fixed_90 = sw.make_weight_fn({'type': 'fixed', 'weights': [0.9, 0.1]})
    results_m = {'incl': {}, 'excl': {}}
    for env_name, excl in [('incl', set()), ('excl', {'MU', 'SNDK'})]:
        print(f'\n[{env_name}]')
        for variant in MA_VARIANTS:
            v_clean = variant.lstrip('★')
            db = GRID / f'master_m_{env_name}_{v_clean}.db'
            t0 = time.time()
            shutil.copy(DB_ORIGINAL, db)
            regenerate(db, v_clean, excl, ma20_map, high60_map)
            res = run_bt(db, fixed_90, max_slots=2)
            results_m[env_name][variant] = res
            print(f'  {variant:<22} avg={res["avg"]:+6.2f}% mdd={res["mdd"]:+6.2f}% sharpe={res["sharpe"]:+.2f} [{time.time()-t0:.1f}s]')

    # MA 필터 종합 표
    print()
    print('=' * 110)
    print('MA 필터 종합 (lift = baseline current 대비)')
    print('=' * 110)
    base_i = results_m['incl']['★current']['seed_avgs']
    base_e = results_m['excl']['★current']['seed_avgs']
    print(f'{"필터":<22} {"i_avg":>9} {"i_mdd":>8} {"i_lift":>9} {"i_win":>7} | {"e_avg":>9} {"e_mdd":>8} {"e_lift":>9} {"e_win":>7}')
    print('-' * 110)
    for variant in MA_VARIANTS:
        ri = results_m['incl'][variant]; re = results_m['excl'][variant]
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        print(f'{variant:<22} {ri["avg"]:+8.2f}% {ri["mdd"]:+7.2f}% {avgi:+7.2f}%p {wi:>4}/100 | '
              f'{re["avg"]:+8.2f}% {re["mdd"]:+7.2f}% {avge:+7.2f}%p {we:>4}/100')

    # ============================================================
    # 최종 종합 판정
    # ============================================================
    print()
    print('=' * 110)
    print('★ 최종 판정 — 두 환경 robust 우월 = 채택 권고')
    print('=' * 110)
    print('\n[비중 후보]')
    for name, _, _ in WEIGHT_VARIANTS:
        if '★' in name: continue
        ri = results_w['incl'][name]; re = results_w['excl'][name]
        li = [b-a for a,b in zip(results_w['incl']['★90/10 (v83.3)']['seed_avgs'], ri['seed_avgs'])]
        le = [b-a for a,b in zip(results_w['excl']['★90/10 (v83.3)']['seed_avgs'], re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        is_robust_up = wi >= 60 and we >= 60
        is_robust_down = wi <= 40 and we <= 40
        verdict = '✓✓ 채택' if is_robust_up else '✗✗ 거부' if is_robust_down else '~ 보류'
        print(f'  {name:<22} incl {avgi:+6.2f}%p ({wi}/100) excl {avge:+6.2f}%p ({we}/100) → {verdict}')

    print('\n[MA 필터 후보]')
    for variant in MA_VARIANTS:
        if '★' in variant: continue
        ri = results_m['incl'][variant]; re = results_m['excl'][variant]
        li = [b-a for a,b in zip(results_m['incl']['★current']['seed_avgs'], ri['seed_avgs'])]
        le = [b-a for a,b in zip(results_m['excl']['★current']['seed_avgs'], re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        is_robust_up = wi >= 60 and we >= 60
        is_robust_down = wi <= 40 and we <= 40
        verdict = '✓✓ 채택' if is_robust_up else '✗✗ 거부' if is_robust_down else '~ 보류'
        print(f'  {variant:<22} incl {avgi:+6.2f}%p ({wi}/100) excl {avge:+6.2f}%p ({we}/100) → {verdict}')


if __name__ == '__main__':
    main()
