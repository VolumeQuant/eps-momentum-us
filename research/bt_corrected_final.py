"""정확한 BT 재측정 — 점수 기반 weight 재배정 + 모든 후보 paired 비교

이전 BT 버그: 슬롯 idx 기반 weight (점수 무관) → 점수 2위가 슬롯 0 점유 시 80% 받음
새 모델: 신규 진입 시 portfolio 점수 정렬, 점수 1위 = weights[0]

모든 후보 한 번에:
  baseline (3,10,3) 균등 [33,33,34]
  A2 (3,10,3) [50,30,20] (안전 후보)
  fixed_80_20 (2,10,2) (기존 챔피언)
  fixed_70_30 (2,10,2)
  fixed_60_40 (2,10,2)
  fixed_50_50 (2,10,2)
  score_ratio (2,10,2) — 점수 비율
  power_2 (2,10,2) — 점수 제곱 비율
  power_1.5 (2,10,2) — 점수^1.5
  dynamic_5 (2,10,2) — 차이 >5: 80/20, else 50/50
  dynamic_10 (2,10,2) — 차이 >10: 80/20, else 50/50

각각 random 500 paired + 12 multistart.
"""
import sys
import sqlite3
import random
import statistics
import time
from pathlib import Path
from collections import defaultdict

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
    print('[Init] score_100 캐싱...')
    import daily_runner as dr
    score_cache = {}
    for d in dates:
        try:
            _, score_map = dr._build_score_100_map(d)
            score_cache[d] = score_map
        except Exception:
            score_cache[d] = {}
    print(f'  {len(score_cache)}일 캐시 완료')
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


def classify_c2(info, tk, today, dates_list, price_full):
    eps_w = info.get('eps_w')
    if eps_w is None: return False
    p30 = get_price_30d(tk, today, dates_list, price_full)
    if p30 is None: return False
    return eps_w > 0 and p30 < 0


def rerank(today, today_data, c2_boost, dates_list, price_full):
    candidates = []
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        is_c2 = classify_c2(info, tk, today, dates_list, price_full)
        score = (31 - p2) + (c2_boost if is_c2 else 0)
        candidates.append((-score, tk))
    candidates.sort()
    return {tk: i+1 for i, (_, tk) in enumerate(candidates)}


def calc_weights(strategy, scores, slots):
    """strategy + scores list → weights list (길이 = slots, 합 100)"""
    n = len(scores)
    if n == 0:
        return [0] * slots
    if n == 1:
        return [100] + [0] * (slots - 1)

    if strategy == 'baseline_equal_3':
        base = 100 // slots
        rem = 100 - base * slots
        return [base + (1 if i < rem else 0) for i in range(slots)]
    elif strategy == 'A2_3slot':
        # [50, 30, 20]
        return [50, 30, 20] if slots == 3 else [100] + [0] * (slots-1)
    elif strategy == 'fixed_80_20':
        return [80, 20]
    elif strategy == 'fixed_70_30':
        return [70, 30]
    elif strategy == 'fixed_60_40':
        return [60, 40]
    elif strategy == 'fixed_50_50':
        return [50, 50]
    elif strategy == 'score_ratio':
        # 점수 비율
        total = sum(scores[:slots])
        if total <= 0:
            base = 100 // slots
            return [base] * slots
        return [s/total*100 for s in scores[:slots]]
    elif strategy == 'power_2':
        # 점수 제곱 비율
        sq = [s**2 for s in scores[:slots]]
        total = sum(sq)
        if total <= 0: return [50, 50]
        return [s/total*100 for s in sq]
    elif strategy == 'power_1_5':
        pw = [s**1.5 for s in scores[:slots]]
        total = sum(pw)
        if total <= 0: return [50, 50]
        return [s/total*100 for s in pw]
    elif strategy == 'dynamic_5':
        if n >= 2 and scores[0] - scores[1] > 5:
            return [80, 20]
        return [50, 50]
    elif strategy == 'dynamic_10':
        if n >= 2 and scores[0] - scores[1] > 10:
            return [80, 20]
        return [50, 50]
    return [50, 50]


def simulate(dates_all, data, price_full, strategy, slots, entry, exit_, c2_boost, start_date=None):
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}  # {tk: {'entry_price', 'weight', 'score'}}
    consecutive = defaultdict(int)
    daily_returns = []
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            today_data = data.get(d, {})
            new_ranks = rerank(d, today_data, c2_boost, dates_all, price_full)
            new_c = defaultdict(int)
            for tk, r in new_ranks.items():
                if r <= 30:
                    new_c[tk] = consecutive.get(tk, 0) + 1
            consecutive = new_c
    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        new_ranks = rerank(today, today_data, c2_boost, dates_all, price_full)
        new_consec = defaultdict(int)
        for tk, r in new_ranks.items():
            if r <= 30:
                new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        # Day return
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

        # Exit
        exited = []
        for tk in list(portfolio.keys()):
            rank = new_ranks.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2: exited.append(tk); continue
            if rank is None or rank > exit_: exited.append(tk); continue
        for tk in exited:
            del portfolio[tk]

        # Entry
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
                # 기존 + 신규 종목 모두 점수 정렬, weight 재배정
                existing = [(tk, info.get('score', 0), 'old', info.get('entry_price'))
                            for tk, info in portfolio.items()]
                # score는 보유 종목들도 오늘 점수로 재평가
                existing = [(tk, today_data.get(tk, {}).get('score_100', 0), 'old', info['entry_price'])
                            for tk, info in portfolio.items()]
                all_in_pf = existing + [(tk, sc, 'new', price) for tk, price, sc in new_entries]
                all_in_pf.sort(key=lambda x: -x[1])  # 점수 내림차순
                scores = [sc for _, sc, _, _ in all_in_pf]
                weights = calc_weights(strategy, scores, slots)
                # weight 재배정
                new_portfolio = {}
                for i, (tk, sc, typ, price) in enumerate(all_in_pf):
                    w = weights[i] if i < len(weights) else 0
                    if typ == 'old':
                        ep = portfolio[tk]['entry_price']
                    else:
                        ep = price
                    new_portfolio[tk] = {'entry_price': ep, 'weight': w, 'score': sc}
                portfolio = new_portfolio

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run_random(strategy, slots, entry, exit_, c2_boost, dates, data, price_full, seed_starts):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, strategy, slots, entry, exit_, c2_boost, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def run_multi(strategy, slots, entry, exit_, c2_boost, dates, data, price_full, fixed_starts):
    rets, mdds = [], []
    for sd in fixed_starts:
        r = simulate(dates, data, price_full, strategy, slots, entry, exit_, c2_boost, start_date=sd)
        rets.append(r['total_return']); mdds.append(r['max_dd'])
    return {'rets': rets, 'mdds': mdds}


def main():
    print('=' * 110)
    print('★ 정확한 BT 재측정 — 점수 기반 weight 재배정')
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

    # configs: (label, strategy, slots, entry, exit_, c2_boost)
    configs = [
        ('baseline (3,10,3) 균등',          'baseline_equal_3', 3, 30, 10, 0),
        ('A2 (3,10,3) 50/30/20',           'A2_3slot',        3, 30, 10, 0),
        ('A2 + C2 boost=3',                 'A2_3slot',        3, 30, 10, 3),
        ('(2,10,2) 80/20 b=3 [기존 champ]', 'fixed_80_20',     2, 30, 10, 3),
        ('(2,10,2) 70/30 b=3',             'fixed_70_30',     2, 30, 10, 3),
        ('(2,10,2) 60/40 b=3',             'fixed_60_40',     2, 30, 10, 3),
        ('(2,10,2) 50/50 b=3',             'fixed_50_50',     2, 30, 10, 3),
        ('(2,10,2) score_ratio b=3',       'score_ratio',     2, 30, 10, 3),
        ('(2,10,2) power_1.5 b=3',         'power_1_5',       2, 30, 10, 3),
        ('(2,10,2) power_2 b=3',           'power_2',         2, 30, 10, 3),
        ('(2,10,2) dynamic_5 b=3',         'dynamic_5',       2, 30, 10, 3),
        ('(2,10,2) dynamic_10 b=3',        'dynamic_10',      2, 30, 10, 3),
        # boost 없이 weighting만
        ('(2,10,2) score_ratio b=0',       'score_ratio',     2, 30, 10, 0),
        ('(2,10,2) power_2 b=0',           'power_2',         2, 30, 10, 0),
    ]
    print()
    print(f'{"config":<37} {"R avg":>9} {"R wins":>10} {"M avg":>9} {"M wins":>8} {"M min":>9} {"MDD":>9}')
    print('-' * 100)
    results = {}
    for label, strat, slots, entry, exit_, b in configs:
        t1 = time.time()
        r_random = run_random(strat, slots, entry, exit_, b, dates, data, price_full, seed_starts)
        r_multi = run_multi(strat, slots, entry, exit_, b, dates, data, price_full, fixed_starts)
        results[label] = {'random': r_random, 'multi': r_multi}
        r_avg = sum(r_random['rets'])/len(r_random['rets'])
        m_avg = sum(r_multi['rets'])/12
        worst_mdd = min(r_random['mdds'])
        m_min = min(r_multi['rets'])
        baseline_avg = results.get('baseline (3,10,3) 균등', {}).get('random', {}).get('seed_avgs', [])
        baseline_multi = results.get('baseline (3,10,3) 균등', {}).get('multi', {}).get('rets', [])
        if baseline_avg and 'baseline' not in label:
            r_lifts = [b_ - a for a, b_ in zip(baseline_avg, r_random['seed_avgs'])]
            r_wins = sum(1 for l in r_lifts if l > 0)
            m_lifts = [b_ - a for a, b_ in zip(baseline_multi, r_multi['rets'])]
            m_wins = sum(1 for l in m_lifts if l > 0)
            r_wstr = f'{r_wins}/{N_SEEDS}'
            m_wstr = f'{m_wins}/12'
        else:
            r_wstr = '★'
            m_wstr = '★'
        marker = '★' if 'champ' in label else '  '
        print(f'{marker} {label:<35} {r_avg:+7.2f}% {r_wstr:>10} {m_avg:+7.2f}% {m_wstr:>8} {m_min:+7.2f}% {worst_mdd:+8.2f}% [{time.time()-t1:.0f}s]')

    # Paired vs baseline
    print()
    print('=' * 110)
    print('paired vs baseline (3,10,3) 균등 — 정확 알파')
    print('=' * 110)
    base_r = results['baseline (3,10,3) 균등']['random']['seed_avgs']
    base_m = results['baseline (3,10,3) 균등']['multi']['rets']
    rankings = []
    for label in results:
        if 'baseline' in label: continue
        new_r = results[label]['random']['seed_avgs']
        new_m = results[label]['multi']['rets']
        r_lifts = [b - a for a, b in zip(base_r, new_r)]
        m_lifts = [b - a for a, b in zip(base_m, new_m)]
        r_lift = sum(r_lifts)/len(r_lifts)
        m_lift = sum(m_lifts)/12
        r_wins = sum(1 for l in r_lifts if l > 0)
        m_wins = sum(1 for l in m_lifts if l > 0)
        m_min = min(m_lifts); m_max = max(m_lifts)
        worst_mdd = min(results[label]['random']['mdds'])
        rankings.append((r_lift, label, r_wins, m_lift, m_wins, m_min, m_max, worst_mdd))
    rankings.sort(reverse=True)
    print(f'{"rank":<5} {"config":<37} {"R lift":>10} {"R wins":>9} {"M lift":>10} {"M wins":>8} {"M min":>10} {"MDD":>9}')
    print('-' * 110)
    for i, (r_lift, label, r_wins, m_lift, m_wins, m_min, m_max, mdd) in enumerate(rankings, 1):
        print(f'{i:>4} {label:<37} {r_lift:+8.2f}%p {r_wins:>4}/{N_SEEDS} {m_lift:+8.2f}%p {m_wins:>4}/12 {m_min:+8.2f}%p {mdd:+8.2f}%')

    print(f'\n총 소요: {time.time()-t0:.1f}s')


if __name__ == '__main__':
    main()
