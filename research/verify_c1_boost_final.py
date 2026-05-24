"""C1 boost=5 + (2,10,2) 70/30 — 만족 기준 다층 검증

만족 기준:
  - random 500: wins ≥ 95%, lift ≥ +30%p
  - 12 multistart: 12/12 양수, M min ≥ +10%p
  - Simulator: trade DB 일치
  - 약세장 sub-period: 5개 중 4개 우월
  - 거래비용 1%: 알파 ≥ +25%p
  - 임계값 sensitivity: boost 4/5/6 일관 양수
  - 종목 빈도: SNDK 진입 정상

통과 → 적용, 실패 → 보수 옵션
"""
import sys
import sqlite3
import random
import statistics
import time
from pathlib import Path
from collections import defaultdict, Counter

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'

N_SEEDS = 500
SAMPLES = 3
MIN_HOLD_DAYS = 10
LOOKBACK = 30

sys.path.insert(0, str(ROOT))


def load_all_with_score(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    import daily_runner as dr
    score_cache = {}
    for d in dates:
        try:
            _, sm = dr._build_score_100_map(d)
            score_cache[d] = sm
        except Exception:
            score_cache[d] = {}
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price, eps_chg_weighted,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else:
                    segs.append(0)
            data[d][tk] = {
                'p2': r[1], 'price': r[2], 'eps_w': r[3],
                'min_seg': min(segs) if segs else 0,
                'score_100': score_cache.get(d, {}).get(tk, 0),
            }
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def get_price_30d(tk, today, dates_list, price_full):
    if today not in dates_list: return None
    di = dates_list.index(today)
    if di < LOOKBACK: return None
    past_d = dates_list[di - LOOKBACK]
    past_p = price_full.get(past_d, {}).get(tk)
    cur_p = price_full.get(today, {}).get(tk)
    if past_p and cur_p and past_p > 0:
        return (cur_p - past_p) / past_p * 100
    return None


def is_c1(info, tk, today, dates_list, price_full):
    eps_w = info.get('eps_w')
    if eps_w is None or eps_w <= 0: return False
    p30 = get_price_30d(tk, today, dates_list, price_full)
    if p30 is None: return False
    return p30 > 0


def rerank(today, today_data, c1_boost, dates_list, price_full):
    if c1_boost == 0:
        return {tk: info.get('p2') for tk, info in today_data.items() if info.get('p2') is not None}
    candidates = []
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        is_c1_now = is_c1(info, tk, today, dates_list, price_full)
        score = (31 - p2) + (c1_boost if is_c1_now else 0)
        candidates.append((-score, tk))
    candidates.sort()
    return {tk: i+1 for i, (_, tk) in enumerate(candidates)}


def simulate(dates_all, data, price_full, weights, entry, exit_, c1_boost,
             start_date=None, end_date=None, cost_pct=0.0, log_trades=False):
    slots = len(weights)
    if start_date or end_date:
        dates = [d for d in dates_all if (not start_date or d >= start_date) and (not end_date or d <= end_date)]
    else:
        dates = dates_all
    portfolio = {}
    consecutive = defaultdict(int)
    daily_returns = []
    trades_log = []
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            today_data = data.get(d, {})
            new_ranks = rerank(d, today_data, c1_boost, dates_all, price_full)
            new_c = defaultdict(int)
            for tk, r in new_ranks.items():
                if r <= 30:
                    new_c[tk] = consecutive.get(tk, 0) + 1
            consecutive = new_c
    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        new_ranks = rerank(today, today_data, c1_boost, dates_all, price_full)
        new_consec = defaultdict(int)
        for tk, r in new_ranks.items():
            if r <= 30:
                new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec
        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di-1]
            for tk, info in portfolio.items():
                cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                prev_p = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if cur_p and prev_p and prev_p > 0:
                    w = info['weight'] / 100.0
                    day_ret += w * (cur_p - prev_p) / prev_p * 100
        daily_returns.append(day_ret)
        exited = []
        for tk in list(portfolio.keys()):
            rank = new_ranks.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2 or rank is None or rank > exit_:
                cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                if log_trades and cur_p:
                    info = portfolio[tk]
                    ret = (cur_p - info['entry_price']) / info['entry_price'] * 100
                    trades_log.append({'tk': tk, 'entry_d': info['entry_d'], 'exit_d': today,
                                       'entry_p': info['entry_price'], 'exit_p': cur_p, 'ret': ret,
                                       'weight': info['weight']})
                # cost deduction
                if cost_pct > 0:
                    daily_returns[-1] -= cost_pct * 100 * portfolio[tk]['weight'] / 100
                exited.append(tk)
        for tk in exited: del portfolio[tk]
        if len(portfolio) < slots:
            cands = []
            for tk, new_r in sorted(new_ranks.items(), key=lambda x: x[1]):
                if new_r > entry: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                info = today_data.get(tk, {})
                min_seg = info.get('min_seg', 0)
                if min_seg < 0: continue
                price = info.get('price')
                score = info.get('score_100', 0)
                if price and price > 0:
                    cands.append((tk, price, score))
            free_slots = slots - len(portfolio)
            new_entries = cands[:free_slots]
            if new_entries:
                existing = [(tk, today_data.get(tk, {}).get('score_100', 0), 'old', info['entry_price'], info['entry_d'])
                            for tk, info in portfolio.items()]
                all_in_pf = existing + [(tk, sc, 'new', price, today) for tk, price, sc in new_entries]
                all_in_pf.sort(key=lambda x: -x[1])
                new_weights = weights
                new_portfolio = {}
                for i, (tk, sc, typ, price, ed) in enumerate(all_in_pf):
                    w = new_weights[i] if i < len(new_weights) else 0
                    ep = portfolio[tk]['entry_price'] if typ == 'old' else price
                    new_portfolio[tk] = {'entry_price': ep, 'weight': w, 'entry_d': ed}
                    if log_trades and typ == 'new':
                        # 진입 비용
                        if cost_pct > 0:
                            daily_returns[-1] -= cost_pct * 100 * w / 100
                portfolio = new_portfolio
    # 보유 중 종목 (open positions)
    if log_trades:
        for tk, info in portfolio.items():
            final_p = data[dates[-1]].get(tk, {}).get('price') or price_full.get(dates[-1], {}).get(tk)
            if final_p:
                ret = (final_p - info['entry_price']) / info['entry_price'] * 100
                trades_log.append({'tk': tk, 'entry_d': info['entry_d'], 'exit_d': dates[-1] + ' (open)',
                                   'entry_p': info['entry_price'], 'exit_p': final_p, 'ret': ret,
                                   'weight': info['weight']})
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd, 'trades': trades_log}


def run_random(weights, entry, exit_, c1_boost, dates, data, price_full, seed_starts, cost_pct=0.0):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, weights, entry, exit_, c1_boost,
                         start_date=sd, cost_pct=cost_pct)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 110)
    print('(2,10,2) 70/30 + C1 boost=5 — 만족 기준 다층 검증')
    print('=' * 110)
    t0 = time.time()
    dates, data, price_full = load_all_with_score(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))
    step = max(1, len(eligible) // 13)
    fixed_starts = [eligible[step * i] for i in range(1, 13)]

    NEW = ([70, 30], 30, 10, 5)  # 후보
    BASE = ([33, 33, 34], 30, 10, 0)  # baseline

    print('\n[Test 1] Random 500 paired')
    print('-' * 70)
    base_r = run_random(*BASE, dates, data, price_full, seed_starts)
    new_r = run_random(*NEW, dates, data, price_full, seed_starts)
    base_avg = sum(base_r['rets'])/len(base_r['rets'])
    new_avg = sum(new_r['rets'])/len(new_r['rets'])
    lifts = [b - a for a, b in zip(base_r['seed_avgs'], new_r['seed_avgs'])]
    avg_lift = sum(lifts)/len(lifts)
    wins = sum(1 for l in lifts if l > 0)
    test1 = wins >= 475 and avg_lift >= 30
    print(f'  baseline: {base_avg:+.2f}%, new: {new_avg:+.2f}%')
    print(f'  lift: {avg_lift:+.2f}%p, wins: {wins}/{N_SEEDS}')
    print(f'  통과 기준: wins >= 475 (95%), lift >= +30%p')
    print(f'  결과: {"✓ PASS" if test1 else "✗ FAIL"}')

    print('\n[Test 2] 12 Multistart')
    print('-' * 70)
    base_m = [simulate(dates, data, price_full, *BASE, start_date=sd)['total_return'] for sd in fixed_starts]
    new_m = [simulate(dates, data, price_full, *NEW, start_date=sd)['total_return'] for sd in fixed_starts]
    m_lifts = [b - a for a, b in zip(base_m, new_m)]
    m_avg = sum(m_lifts)/12
    m_wins = sum(1 for l in m_lifts if l > 0)
    m_min = min(m_lifts)
    test2 = m_wins == 12 and m_min >= 10
    print(f'  multistart lifts: avg {m_avg:+.2f}%p, wins {m_wins}/12, min {m_min:+.2f}%p, max {max(m_lifts):+.2f}%p')
    print(f'  통과 기준: wins == 12, m_min >= +10%p')
    print(f'  결과: {"✓ PASS" if test2 else "✗ FAIL"}')

    print('\n[Test 3] Simulator 정확성 — 처음부터 전체 시뮬')
    print('-' * 70)
    r_full = simulate(dates, data, price_full, *NEW, log_trades=True)
    # DB 가격 vs simulator 가격 일치 확인
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    sim_ok = 0; sim_fail = 0
    for t in r_full['trades']:
        tk = t['tk']
        ed = t['entry_d']
        if ' (open)' in str(t['exit_d']):
            continue
        # DB에서 entry/exit price 확인
        rep = cur.execute("SELECT price FROM ntm_screening WHERE date=? AND ticker=?", (ed, tk)).fetchone()
        rxp = cur.execute("SELECT price FROM ntm_screening WHERE date=? AND ticker=?", (t['exit_d'], tk)).fetchone()
        if rep and rxp and rep[0] and rxp[0]:
            if abs(rep[0] - t['entry_p']) < 0.01 and abs(rxp[0] - t['exit_p']) < 0.01:
                sim_ok += 1
            else:
                sim_fail += 1
    conn.close()
    n_closed = sim_ok + sim_fail
    test3 = sim_fail == 0
    print(f'  closed trades: {n_closed}, OK {sim_ok}, fail {sim_fail}')
    print(f'  결과: {"✓ PASS" if test3 else "✗ FAIL"}')

    print('\n[Test 4] 약세장 sub-period stress')
    print('-' * 70)
    periods = [
        ('Early Stress (2/13~3/13)',  '2026-02-13', '2026-03-13'),
        ('Crisis (3/13~4/10)',        '2026-03-13', '2026-04-10'),
        ('Recovery (4/10~5/8)',       '2026-04-10', '2026-05-08'),
        ('Late (5/8~5/22)',           '2026-05-08', '2026-05-22'),
        ('Stress Only (3/20~4/4)',    '2026-03-20', '2026-04-04'),
    ]
    period_wins = 0
    for pname, sd, ed in periods:
        r_b = simulate(dates, data, price_full, *BASE, start_date=sd, end_date=ed)
        r_n = simulate(dates, data, price_full, *NEW, start_date=sd, end_date=ed)
        lift = r_n['total_return'] - r_b['total_return']
        marker = '+' if lift > 0 else '-'
        if lift > 0: period_wins += 1
        print(f'  {pname:<30} base {r_b["total_return"]:+7.2f}% | new {r_n["total_return"]:+7.2f}% | {marker}{lift:+.2f}%p')
    test4 = period_wins >= 4
    print(f'  통과 기준: 5개 중 4개 이상 우월')
    print(f'  결과: {period_wins}/5 → {"✓ PASS" if test4 else "✗ FAIL"}')

    print('\n[Test 5] 거래비용 1% stress')
    print('-' * 70)
    base_cost_r = run_random(*BASE, dates, data, price_full, seed_starts, cost_pct=0.01)
    new_cost_r = run_random(*NEW, dates, data, price_full, seed_starts, cost_pct=0.01)
    base_cost_avg = sum(base_cost_r['rets'])/len(base_cost_r['rets'])
    new_cost_avg = sum(new_cost_r['rets'])/len(new_cost_r['rets'])
    cost_lift = new_cost_avg - base_cost_avg
    test5 = cost_lift >= 25
    print(f'  baseline w/ 1% cost: {base_cost_avg:+.2f}%')
    print(f'  new w/ 1% cost: {new_cost_avg:+.2f}%')
    print(f'  lift w/ cost: {cost_lift:+.2f}%p (기준 +25%p)')
    print(f'  결과: {"✓ PASS" if test5 else "✗ FAIL"}')

    print('\n[Test 6] C1 boost 임계값 sensitivity (boost 3/4/5/6)')
    print('-' * 70)
    boost_results = {}
    for b in [3, 4, 5, 6]:
        r = run_random([70, 30], 30, 10, b, dates, data, price_full, seed_starts)
        avg = sum(r['rets'])/len(r['rets'])
        lifts = [b_ - a for a, b_ in zip(base_r['seed_avgs'], r['seed_avgs'])]
        avg_lift = sum(lifts)/len(lifts)
        boost_results[b] = avg_lift
        print(f'  boost={b}: lift {avg_lift:+.2f}%p')
    test6 = all(boost_results[b] > 20 for b in [3, 4, 5, 6])
    print(f'  통과 기준: boost 3/4/5/6 모두 lift > +20%p')
    print(f'  결과: {"✓ PASS" if test6 else "✗ FAIL"}')

    print('\n[Test 7] 종목 빈도 — SNDK 진입 정상')
    print('-' * 70)
    counter = Counter()
    for sd in fixed_starts:
        r = simulate(dates, data, price_full, *NEW, start_date=sd, log_trades=True)
        for t in r['trades']:
            counter[t['tk']] += 1
    print(f'  Top 10 종목 진입 빈도:')
    for tk, cnt in counter.most_common(10):
        marker = ' ★' if tk == 'SNDK' else ''
        print(f'    {tk}: {cnt}회{marker}')
    sndk_cnt = counter.get('SNDK', 0)
    test7 = sndk_cnt >= 5
    print(f'  통과 기준: SNDK >= 5회 진입')
    print(f'  결과: SNDK {sndk_cnt}회 → {"✓ PASS" if test7 else "✗ FAIL"}')

    # 종합
    tests = [test1, test2, test3, test4, test5, test6, test7]
    test_names = [
        'Random 500 paired',
        '12 multistart',
        'Simulator 정확성',
        '약세장 sub-period',
        '거래비용 1% stress',
        'boost 임계값 sensitivity',
        '종목 빈도 SNDK',
    ]
    print()
    print('=' * 110)
    print('만족 기준 종합')
    print('=' * 110)
    for name, ok in zip(test_names, tests):
        print(f'  {"✓" if ok else "✗"} {name}')
    n_pass = sum(tests)
    print(f'\n  {n_pass}/7 통과')
    overall = n_pass == 7
    print(f'\n  ★ 최종 결과: {"PASS - 적용 진행" if overall else "FAIL - 보수 옵션 적용"} ★')
    print(f'\n총 소요: {time.time()-t0:.1f}s')


if __name__ == '__main__':
    main()
