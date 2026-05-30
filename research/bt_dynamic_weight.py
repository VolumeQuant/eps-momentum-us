"""Dynamic weight BT — 1-2위 score 격차에 따라 진입 시점 비중 결정

사용자 제안: 정적 90/10이 아니라 매 portfolio 시작 시점의 1-2위 격차 보고 dynamic.

Dynamic rule 후보:
  fixed_90_10:        baseline (v83.3 정적)
  step_3:             gap≥30 → [0.9,0.1] / gap≥15 → [0.75,0.25] / else [0.6,0.4]
  step_2:             gap≥30 → [0.9,0.1] / gap≥10 → [0.7,0.3] / else [0.5,0.5]
  proportional:       weight_i = score_i / (score_1 + score_2)
  ratio_half:         weight_2 = ratio × 0.5 (제한)

Simulator:
  - 슬롯 1 첫 진입 시점에 그 시점 1-2위 score 격차로 weights 결정
  - portfolio 차있는 동안 weights 고정
  - 모든 슬롯 매도 (portfolio 비면) → 다음 진입 시 weights 재결정

MU+SNDK 포함 / 제외 두 환경 모두 BT.
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


def ma_pass_current(price, ma60, ma120):
    if price is None or price <= 0: return False
    if ma120 is not None: return price > ma120
    return ma60 is not None and price > ma60


def regenerate(test_db, exclude_set):
    conn = sqlite3.connect(test_db)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date').fetchall()]
    cur.execute('UPDATE ntm_screening SET composite_rank=NULL, part2_rank=NULL')
    conn.commit()
    for today in dates:
        rows = cur.execute('''
            SELECT ticker, adj_score, adj_gap, eps_chg_weighted, price, ma60, ma120,
                   ntm_current, ntm_90d, rev_growth, num_analysts, rev_up30, rev_down30,
                   operating_margin, gross_margin, free_cashflow, roe
            FROM ntm_screening WHERE date=?
        ''', (today,)).fetchall()
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
            r2 = cur.execute('SELECT ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d FROM ntm_screening WHERE date=? AND ticker=?', (today, tk_row['ticker'])).fetchone()
            if not r2 or any(x is None for x in r2): return 0
            nc, n7, n30, n60, n90 = r2
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b is not None and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
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


def precompute_scores(db_path):
    """일자별 (top_1_ticker, top_2_ticker, score_1=100, score_2, gap) precompute"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]
    out = {}
    for d in dates:
        rows = cur.execute('SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank LIMIT 5', (d,)).fetchall()
        tks = [r[0] for r in rows]
        if len(tks) < 2:
            out[d] = (tks[0] if tks else None, None, 100.0, 0.0, 100.0)
            continue
        wmap = dr._compute_w_gap_map(cur, d, tks)
        sorted_t = sorted(tks, key=lambda t: wmap.get(t, 0), reverse=True)
        top_w = wmap.get(sorted_t[0], 0)
        if top_w <= 0:
            out[d] = (sorted_t[0], sorted_t[1], 100.0, 100.0, 0.0)
            continue
        s1 = 100.0
        s2 = wmap.get(sorted_t[1], 0) / top_w * 100
        gap = s1 - s2
        out[d] = (sorted_t[0], sorted_t[1], s1, s2, gap)
    conn.close()
    return out


def get_dynamic_weights(rule, gap, s1, s2):
    if rule == 'fixed_90_10':
        return [0.9, 0.1]
    if rule == 'fixed_80_20':
        return [0.8, 0.2]
    if rule == 'step_3':
        if gap >= 30: return [0.9, 0.1]
        if gap >= 15: return [0.75, 0.25]
        return [0.6, 0.4]
    if rule == 'step_2':
        if gap >= 30: return [0.9, 0.1]
        if gap >= 10: return [0.7, 0.3]
        return [0.5, 0.5]
    if rule == 'proportional':
        total = s1 + s2
        if total <= 0: return [1.0, 0.0]
        return [s1/total, s2/total]
    if rule == 'ratio_half':
        ratio = s2 / s1 if s1 > 0 else 0
        w2 = min(0.5, ratio * 0.5)
        return [1 - w2, w2]
    raise ValueError(f'unknown rule: {rule}')


def load_data(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]
    data = {}
    for d in dates:
        rows = cur.execute('SELECT ticker, part2_rank, price, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d FROM ntm_screening WHERE date=?', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[3:8])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01: segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else: segs.append(0)
            data[d][tk] = {'p2': r[1], 'price': r[2], 'min_seg': min(segs) if segs else 0}
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def simulate_dynamic(dates_all, data, price_full, scores, rule, start_date,
                     max_slots=2, entry=3, exit_=10):
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    INIT_CAP = 100.0
    total_cash = INIT_CAP
    slot_cash = [0.0] * max_slots
    slot_holding = [None] * max_slots
    current_weights = None  # 첫 진입 시 결정
    daily_returns = []
    consecutive = defaultdict(int)
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    consecutive[tk] = consecutive.get(tk, 0) + 1
    prev_pv = INIT_CAP

    for di, today in enumerate(dates):
        if today not in data:
            daily_returns.append(0); continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consec = defaultdict(int)
        for tk in rank_map: new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        # PV
        pv_today = total_cash
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

        # 이탈 (각 슬롯 매도 → slot_cash 환원)
        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, entry_price, _ = slot_holding[i]
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2 or rank is None or rank > exit_:
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                slot_cash[i] = shares * (p if p else entry_price)
                slot_holding[i] = None

        # 모든 슬롯 비면 cash 통합, weights 리셋
        if all(h is None for h in slot_holding):
            total_cash += sum(slot_cash)
            slot_cash = [0.0] * max_slots
            current_weights = None

        # 진입
        for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
            if rank > entry: break
            if any(h is not None and h[0] == tk for h in slot_holding): continue
            if consecutive.get(tk, 0) < 3: continue
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < 0: continue
            price = today_data.get(tk, {}).get('price')
            if not price or price <= 0: continue

            # 이번 entry가 첫 entry (portfolio 비어있다가 채우는)면 weights 결정
            if current_weights is None and total_cash > 0:
                if today in scores:
                    _, _, s1, s2, gap = scores[today]
                    current_weights = get_dynamic_weights(rule, gap, s1, s2)
                else:
                    current_weights = get_dynamic_weights(rule, 30, 100, 70)
                # cash 분배
                slot_cash = [w * total_cash for w in current_weights]
                total_cash = 0

            free = next((i for i in range(max_slots) if slot_holding[i] is None), None)
            if free is None: break
            if slot_cash[free] <= 0: continue  # weight 0 슬롯은 진입 안 함
            shares = slot_cash[free] / price
            slot_holding[free] = (tk, shares, price, today)
            slot_cash[free] = 0

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100
        max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd,
            'max_day_loss': min(daily_returns) if daily_returns else 0}


RULES = ['fixed_90_10', 'fixed_80_20', 'step_3', 'step_2', 'proportional', 'ratio_half']


def run_environment(exclude_set, env_name):
    print('=' * 110)
    print(f'환경: {env_name} (EXCLUDE={exclude_set or "none"})')
    print('=' * 110)

    db = GRID / f'dyn_{env_name}.db'
    shutil.copy(DB_ORIGINAL, db)
    regenerate(db, exclude_set)
    scores = precompute_scores(db)
    dates, data, price_full = load_data(db)
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    all_results = {}
    for rule in RULES:
        t0 = time.time()
        rets, mdds, mdls, seed_avgs = [], [], [], []
        for chosen in seed_starts:
            sr = []
            for sd in chosen:
                r = simulate_dynamic(dates, data, price_full, scores, rule, sd,
                                    max_slots=2, entry=ENTRY_TOP, exit_=EXIT_TOP)
                rets.append(r['total_return'])
                mdds.append(r['max_dd'])
                mdls.append(r['max_day_loss'])
                sr.append(r['total_return'])
            seed_avgs.append(sum(sr)/len(sr))
        all_results[rule] = {'rets': rets, 'mdds': mdds, 'mdls': mdls, 'seed_avgs': seed_avgs}
        avg = sum(rets)/len(rets)
        med = sorted(rets)[len(rets)//2]
        std = statistics.pstdev(rets)
        mdd = min(mdds); mdl = min(mdls)
        sharpe = avg/std if std > 0 else 0
        marker = ' ★' if rule == 'fixed_90_10' else '  '
        print(f'{marker}{rule:<20} avg={avg:+6.2f}% med={med:+6.2f}% std={std:5.1f} '
              f'mdd={mdd:+6.2f}% maxday={mdl:+5.2f}% sharpe={sharpe:+.2f} [{time.time()-t0:.1f}s]')

    base = all_results['fixed_90_10']['seed_avgs']
    print(f'\n  paired vs fixed_90_10 (v83.3):')
    print(f'  {"rule":<20} {"avg lift":>10} {"med lift":>10} {"min":>10} {"max":>10} {"wins":>8} {"verdict":>10}')
    print('  ' + '-' * 90)
    for rule in RULES:
        if rule == 'fixed_90_10': continue
        new = all_results[rule]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts) / len(lifts)
        med_l = statistics.median(lifts)
        verdict = ('✓✓ 우월' if wins >= 70 else '✓ 우월' if wins >= 60
                   else '~ 동등' if wins >= 40
                   else '✗ 열세' if wins >= 30 else '✗✗ 열세')
        print(f'  {rule:<20} {avg_l:+8.2f}%p {med_l:+8.2f}%p {min(lifts):+8.2f}%p '
              f'{max(lifts):+8.2f}%p {wins:>5}/{N_SEEDS}  {verdict}')
    return all_results


def main():
    print('=' * 110)
    print('Dynamic weight BT — 1-2위 score 격차 기반')
    print(f'simulator: 진입 시점 score 격차로 weights 결정, portfolio 비면 재결정')
    print(f'seeds: {N_SEEDS}×{SAMPLES_PER_SEED}')
    print('=' * 110)

    print('\n### 환경 1: MU+SNDK 포함 (현행 환경) ###')
    res_incl = run_environment(set(), 'incl')

    print('\n### 환경 2: MU+SNDK 제외 (outlier 의존 검증) ###')
    res_excl = run_environment({'MU', 'SNDK'}, 'excl')

    # 종합 비교
    print()
    print('=' * 110)
    print('종합 — 두 환경에서 fixed_90_10 대비 dynamic 룰의 paired lift')
    print('=' * 110)
    print(f'  {"rule":<20} {"incl lift":>10} {"excl lift":>10} {"평균":>9}')
    print('  ' + '-' * 60)
    base_incl = res_incl['fixed_90_10']['seed_avgs']
    base_excl = res_excl['fixed_90_10']['seed_avgs']
    for rule in RULES:
        if rule == 'fixed_90_10': continue
        lifts_i = [b - a for a, b in zip(base_incl, res_incl[rule]['seed_avgs'])]
        lifts_e = [b - a for a, b in zip(base_excl, res_excl[rule]['seed_avgs'])]
        avg_i = sum(lifts_i) / len(lifts_i)
        avg_e = sum(lifts_e) / len(lifts_e)
        avg_both = (avg_i + avg_e) / 2
        print(f'  {rule:<20} {avg_i:+8.2f}%p {avg_e:+8.2f}%p {avg_both:+7.2f}%p')


if __name__ == '__main__':
    main()
