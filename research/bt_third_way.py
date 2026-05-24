"""제 3의 방안 — boost 비대칭 회피 메커니즘 측정

후보:
- 후보 1: Slot 0 점수 1위 lock + Slot 1만 C1 boost
- 후보 2: min_seg 강도 boost (대칭, C1/C2 무관)
- 후보 3: rev_up30 합의 boost (대칭)
- 후보 4: Score 격차 기반 동적 비중 (boost 없음)
- 후보 5: C2 boost (사용자 제안)
- 후보 6: 두 슈퍼위너 보호 — C1 또는 C2 강한 종목 boost
- 후보 7: 모든 EPS↑ 종목에 균등 boost (사실상 boost 없음과 동일하지만 검증)

24 multistart로 worst case + random 500 + lift 측정.
"""
import sys
import sqlite3
import random
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


def load_all(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    import daily_runner as dr
    score_cache = {}
    wgap_cache = {}
    for d in dates:
        try:
            wm, sm = dr._build_score_100_map(d)
            score_cache[d] = sm
            wgap_cache[d] = wm
        except Exception:
            score_cache[d] = {}
            wgap_cache[d] = {}
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price, eps_chg_weighted,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   rev_up30, num_analysts
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
                'w_gap': wgap_cache.get(d, {}).get(tk, 0),
                'rev_up30': r[9] or 0,
                'num_analysts': r[10] or 1,
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


def classify(info, tk, today, dates_list, price_full):
    """Returns 'C1' (EPS↑+가격↑), 'C2' (EPS↑+가격↓), 'C3' (EPS↓+가격↑), 'C4' (EPS↓+가격↓), None"""
    eps_w = info.get('eps_w')
    if eps_w is None: return None
    p30 = get_price_30d(tk, today, dates_list, price_full)
    if p30 is None: return None
    if eps_w > 0 and p30 > 0: return 'C1'
    if eps_w > 0 and p30 < 0: return 'C2'
    if eps_w < 0 and p30 > 0: return 'C3'
    if eps_w < 0 and p30 < 0: return 'C4'
    return None


def rerank(today, today_data, mode, params, dates_list, price_full):
    """Reranking by mode.

    mode:
      'baseline' - p2 그대로
      'c1_boost' - C1에 +boost
      'c2_boost' - C2에 +boost
      'minseg_boost' - min_seg ≥ threshold 종목에 +boost
      'revup_boost' - rev_up30/num_analysts ≥ threshold → +boost
      'either_boost' - C1 또는 C2 강한 종목 (조건 만족) → +boost
    """
    if mode == 'baseline':
        return {tk: info.get('p2') for tk, info in today_data.items() if info.get('p2') is not None}
    candidates = []
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        boost = 0
        if mode == 'c1_boost':
            cls = classify(info, tk, today, dates_list, price_full)
            if cls == 'C1': boost = params['boost']
        elif mode == 'c2_boost':
            cls = classify(info, tk, today, dates_list, price_full)
            if cls == 'C2': boost = params['boost']
        elif mode == 'minseg_boost':
            ms = info.get('min_seg', 0)
            if ms >= params['threshold']: boost = params['boost']
        elif mode == 'revup_boost':
            ru, na = info.get('rev_up30', 0), max(1, info.get('num_analysts', 1))
            if ru / na >= params['threshold']: boost = params['boost']
        elif mode == 'either_boost':
            cls = classify(info, tk, today, dates_list, price_full)
            if cls in ('C1', 'C2'):
                ms = info.get('min_seg', 0)
                if ms >= params['threshold']: boost = params['boost']
        score = (31 - p2) + boost
        candidates.append((-score, tk))
    candidates.sort()
    return {tk: i+1 for i, (_, tk) in enumerate(candidates)}


def simulate(dates_all, data, price_full, weights, entry, exit_, mode, params,
             dynamic_weight=False, slot0_lock=False, start_date=None):
    slots = len(weights)
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    consecutive = defaultdict(int)
    daily_returns = []
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            today_data = data.get(d, {})
            new_ranks = rerank(d, today_data, mode, params, dates_all, price_full)
            new_c = defaultdict(int)
            for tk, r in new_ranks.items():
                if r <= 30:
                    new_c[tk] = consecutive.get(tk, 0) + 1
            consecutive = new_c
    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        # rerank applied to entry order (and exit판단도 동일)
        if slot0_lock:
            # slot 0 = score 1위 (boost 무시), 나머지는 rerank로
            baseline_ranks = {tk: info.get('p2') for tk, info in today_data.items() if info.get('p2') is not None}
            new_ranks_for_entry = rerank(today, today_data, mode, params, dates_all, price_full)
            new_ranks_for_exit = baseline_ranks  # exit 판단은 baseline p2 기준 (안전)
        else:
            new_ranks_for_entry = rerank(today, today_data, mode, params, dates_all, price_full)
            new_ranks_for_exit = new_ranks_for_entry
        new_consec = defaultdict(int)
        for tk, r in new_ranks_for_entry.items():
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
            rank = new_ranks_for_exit.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2: exited.append(tk); continue
            if rank is None or rank > exit_: exited.append(tk); continue
        for tk in exited: del portfolio[tk]
        # Entry — 빈 슬롯
        used_slots = {info['slot_idx'] for info in portfolio.values()}
        free_slots = sorted([i for i in range(slots) if i not in used_slots])

        if slot0_lock:
            # Slot 0이 비어있으면 baseline 1위 잡고, slot 1은 boost rerank로
            slot0_cands_base = []
            for tk, new_r in sorted(({tk: info.get('p2') for tk, info in today_data.items() if info.get('p2') is not None}).items(), key=lambda x: x[1]):
                if new_r > entry: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                info = today_data.get(tk, {})
                if info.get('min_seg', 0) < 0: continue
                price = info.get('price')
                if price and price > 0:
                    slot0_cands_base.append((tk, price))
            slot1_cands = []
            for tk, new_r in sorted(new_ranks_for_entry.items(), key=lambda x: x[1]):
                if new_r > entry: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                info = today_data.get(tk, {})
                if info.get('min_seg', 0) < 0: continue
                price = info.get('price')
                if price and price > 0:
                    slot1_cands.append((tk, price))
            for slot_idx in free_slots:
                if slot_idx == 0:
                    if not slot0_cands_base: continue
                    tk, price = slot0_cands_base.pop(0)
                    # slot 1 후보에서도 제거
                    slot1_cands = [(t, p) for t, p in slot1_cands if t != tk]
                else:
                    if not slot1_cands: continue
                    tk, price = slot1_cands.pop(0)
                portfolio[tk] = {'entry_price': price, 'slot_idx': slot_idx, 'weight': weights[slot_idx]}
        else:
            cands = []
            for tk, new_r in sorted(new_ranks_for_entry.items(), key=lambda x: x[1]):
                if new_r > entry: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                info = today_data.get(tk, {})
                if info.get('min_seg', 0) < 0: continue
                price = info.get('price')
                if price and price > 0:
                    cands.append((tk, price))

            if dynamic_weight and free_slots == list(range(slots)) and len(cands) >= 2:
                # 격차 기반 weight: 1위 w_gap vs 2위 w_gap
                tk1, _ = cands[0]; tk2, _ = cands[1]
                wg1 = abs(today_data.get(tk1, {}).get('w_gap', 0))
                wg2 = abs(today_data.get(tk2, {}).get('w_gap', 0))
                ratio = wg1 / wg2 if wg2 > 0 else 1
                if ratio >= 1.5:
                    dyn_w = [85, 15]
                elif ratio >= 1.25:
                    dyn_w = [75, 25]
                else:
                    dyn_w = [65, 35]
                actual_weights = dyn_w
            else:
                actual_weights = weights

            for slot_idx in free_slots:
                if not cands: break
                tk, price = cands.pop(0)
                portfolio[tk] = {'entry_price': price, 'slot_idx': slot_idx,
                                 'weight': actual_weights[slot_idx] if dynamic_weight else weights[slot_idx]}
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run_random(spec, dates, data, price_full, seed_starts):
    seed_avgs, mdds = [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, **spec, start_date=sd)
            sr.append(r['total_return']); mdds.append(r['max_dd'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'seed_avgs': seed_avgs, 'mdds': mdds}


def main():
    print('=' * 130)
    print('제 3의 방안 — boost 비대칭 회피 메커니즘 측정')
    print('=' * 130)
    dates, data, price_full = load_all(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))
    step12 = max(1, len(eligible) // 13)
    starts12 = [eligible[step12 * i] for i in range(1, 13)]
    step24 = max(1, len(eligible) // 25)
    starts24 = [eligible[step24 * i] for i in range(1, 25)]

    BASE_SPEC = {'weights': [33,34,33], 'entry': 30, 'exit_': 10,
                 'mode': 'baseline', 'params': {}}
    base_r = run_random(BASE_SPEC, dates, data, price_full, seed_starts)
    base_24 = [simulate(dates, data, price_full, **BASE_SPEC, start_date=sd)['total_return'] for sd in starts24]
    base_12 = [simulate(dates, data, price_full, **BASE_SPEC, start_date=sd)['total_return'] for sd in starts12]

    cands = [
        # 참고
        ('A: 70/30 b=0 [v82 현재]',     {'weights':[70,30],'entry':30,'exit_':10,'mode':'baseline','params':{}}),
        ('B: 80/20 + C1 b=5',          {'weights':[80,20],'entry':30,'exit_':10,'mode':'c1_boost','params':{'boost':5}}),
        # 사용자 제안 — C2 boost
        ('70/30 + C2 b=3',             {'weights':[70,30],'entry':30,'exit_':10,'mode':'c2_boost','params':{'boost':3}}),
        ('70/30 + C2 b=5',             {'weights':[70,30],'entry':30,'exit_':10,'mode':'c2_boost','params':{'boost':5}}),
        ('80/20 + C2 b=3',             {'weights':[80,20],'entry':30,'exit_':10,'mode':'c2_boost','params':{'boost':3}}),
        ('80/20 + C2 b=5',             {'weights':[80,20],'entry':30,'exit_':10,'mode':'c2_boost','params':{'boost':5}}),
        # 후보 1 — slot 0 lock + slot 1 boost
        ('70/30 + Slot0Lock + S1 C1 b=5', {'weights':[70,30],'entry':30,'exit_':10,'mode':'c1_boost','params':{'boost':5},'slot0_lock':True}),
        ('80/20 + Slot0Lock + S1 C1 b=5', {'weights':[80,20],'entry':30,'exit_':10,'mode':'c1_boost','params':{'boost':5},'slot0_lock':True}),
        ('80/20 + Slot0Lock + S1 C2 b=5', {'weights':[80,20],'entry':30,'exit_':10,'mode':'c2_boost','params':{'boost':5},'slot0_lock':True}),
        # 후보 2 — min_seg 강도 boost (대칭)
        ('70/30 + minseg≥1 b=3',       {'weights':[70,30],'entry':30,'exit_':10,'mode':'minseg_boost','params':{'threshold':1.0,'boost':3}}),
        ('70/30 + minseg≥1 b=5',       {'weights':[70,30],'entry':30,'exit_':10,'mode':'minseg_boost','params':{'threshold':1.0,'boost':5}}),
        ('80/20 + minseg≥1 b=5',       {'weights':[80,20],'entry':30,'exit_':10,'mode':'minseg_boost','params':{'threshold':1.0,'boost':5}}),
        ('80/20 + minseg≥2 b=5',       {'weights':[80,20],'entry':30,'exit_':10,'mode':'minseg_boost','params':{'threshold':2.0,'boost':5}}),
        # 후보 3 — rev_up30 합의 boost
        ('70/30 + revup≥0.5 b=5',      {'weights':[70,30],'entry':30,'exit_':10,'mode':'revup_boost','params':{'threshold':0.5,'boost':5}}),
        ('80/20 + revup≥0.5 b=5',      {'weights':[80,20],'entry':30,'exit_':10,'mode':'revup_boost','params':{'threshold':0.5,'boost':5}}),
        ('80/20 + revup≥0.4 b=5',      {'weights':[80,20],'entry':30,'exit_':10,'mode':'revup_boost','params':{'threshold':0.4,'boost':5}}),
        # 후보 4 — Score 격차 기반 dynamic weight (boost 없음)
        ('Dynamic Weight (격차 기반)',   {'weights':[70,30],'entry':30,'exit_':10,'mode':'baseline','params':{},'dynamic_weight':True}),
        # 후보 6 — C1 or C2 모두 boost (min_seg + 가격 방향 무관)
        ('80/20 + C1orC2 ms≥1 b=5',    {'weights':[80,20],'entry':30,'exit_':10,'mode':'either_boost','params':{'threshold':1.0,'boost':5}}),
        ('80/20 + C1orC2 ms≥0 b=5',    {'weights':[80,20],'entry':30,'exit_':10,'mode':'either_boost','params':{'threshold':0,'boost':5}}),
    ]

    print()
    print(f'{"config":<38} {"R lift":>10} {"R wins":>10} {"M12 lift":>10} {"M12 min":>10} {"M24 lift":>10} {"M24 wins":>11} {"M24 min":>11} {"MDD":>8}')
    print('-' * 130)
    for label, spec in cands:
        r_random = run_random(spec, dates, data, price_full, seed_starts)
        m12 = [simulate(dates, data, price_full, **spec, start_date=sd)['total_return'] for sd in starts12]
        m24 = [simulate(dates, data, price_full, **spec, start_date=sd)['total_return'] for sd in starts24]
        r_lifts = [b_ - a for a, b_ in zip(base_r['seed_avgs'], r_random['seed_avgs'])]
        r_lift = sum(r_lifts)/len(r_lifts)
        r_wins = sum(1 for l in r_lifts if l > 0)
        m12_lifts = [b_ - a for a, b_ in zip(base_12, m12)]
        m12_lift = sum(m12_lifts)/12; m12_min = min(m12_lifts)
        m24_lifts = [b_ - a for a, b_ in zip(base_24, m24)]
        m24_lift = sum(m24_lifts)/24; m24_wins = sum(1 for l in m24_lifts if l > 0); m24_min = min(m24_lifts)
        worst_mdd = min(r_random['mdds'])
        print(f'{label:<38} {r_lift:+8.2f}%p {r_wins:>5}/500 {m12_lift:+8.2f}%p {m12_min:+8.2f}%p {m24_lift:+8.2f}%p {m24_wins:>5}/24 {m24_min:+9.2f}%p {worst_mdd:+7.2f}%')


if __name__ == '__main__':
    main()
