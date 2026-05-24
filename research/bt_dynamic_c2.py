"""C2 b=3 + Dynamic Weight 측정 — 1위 신호 강도에 따른 비중 동적 조정

격차 = |w_gap_1위| / |w_gap_2위| (또는 w_gap 차이)

3가지 dynamic 모드:
- mode A: 2단계 (ratio ≥ 1.2 → 80/20, else 70/30) — 가장 단순
- mode B: 3단계 (≥1.4 → 85/15, ≥1.2 → 75/25, else 65/35) — 격차 비례
- mode C: 4단계 (≥1.5 → 85/15, ≥1.3 → 80/20, ≥1.15 → 70/30, else 60/40)

또 측정:
- 절대값 격차 (w_gap_1 - w_gap_2 = absolute spread) 기반
- score_100 격차 기반 (score_100[2위] 값 — 1위는 100 고정)
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
    score_cache = {}; wgap_cache = {}
    for d in dates:
        try:
            wm, sm = dr._build_score_100_map(d)
            score_cache[d] = sm; wgap_cache[d] = wm
        except Exception:
            score_cache[d] = {}; wgap_cache[d] = {}
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
                'w_gap': wgap_cache.get(d, {}).get(tk, 0),
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


def is_c2(info, tk, today, dates_list, price_full):
    eps_w = info.get('eps_w')
    if eps_w is None or eps_w <= 0: return False
    p30 = get_price_30d(tk, today, dates_list, price_full)
    if p30 is None: return False
    return p30 < 0


def rerank(today, today_data, c2_boost, dates_list, price_full):
    if c2_boost == 0:
        return {tk: info.get('p2') for tk, info in today_data.items() if info.get('p2') is not None}
    candidates = []
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        is_c2_now = is_c2(info, tk, today, dates_list, price_full)
        score = (31 - p2) + (c2_boost if is_c2_now else 0)
        candidates.append((-score, tk))
    candidates.sort()
    return {tk: i+1 for i, (_, tk) in enumerate(candidates)}


def get_dynamic_weights(today_data, tk1, tk2, mode):
    """1위, 2위 종목의 w_gap/score_100 격차 기반 비중 계산"""
    wg1 = abs(today_data.get(tk1, {}).get('w_gap', 0))
    wg2 = abs(today_data.get(tk2, {}).get('w_gap', 0))
    ratio = wg1 / wg2 if wg2 > 0 else 999
    if mode == 'A':  # 2단계
        if ratio >= 1.2: return [80, 20]
        else: return [70, 30]
    elif mode == 'B':  # 3단계
        if ratio >= 1.4: return [85, 15]
        elif ratio >= 1.2: return [75, 25]
        else: return [65, 35]
    elif mode == 'C':  # 4단계
        if ratio >= 1.5: return [85, 15]
        elif ratio >= 1.3: return [80, 20]
        elif ratio >= 1.15: return [70, 30]
        else: return [60, 40]
    elif mode == 'D':  # 점수 차이 기반 (score_100[2위] < 70이면 분산)
        s2 = today_data.get(tk2, {}).get('score_100', 100)
        if s2 < 70: return [80, 20]
        elif s2 < 85: return [75, 25]
        else: return [65, 35]
    elif mode == 'fixed_80_20': return [80, 20]
    elif mode == 'fixed_70_30': return [70, 30]
    elif mode == 'fixed_75_25': return [75, 25]
    return [70, 30]


def simulate(dates_all, data, price_full, default_weights, entry, exit_, c2_boost,
             dynamic_mode, start_date=None):
    slots = len(default_weights)
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
            if min_seg < -2: exited.append(tk); continue
            if rank is None or rank > exit_: exited.append(tk); continue
        for tk in exited: del portfolio[tk]
        used_slots = {info['slot_idx'] for info in portfolio.values()}
        free_slots = sorted([i for i in range(slots) if i not in used_slots])
        cands = []
        for tk, new_r in sorted(new_ranks.items(), key=lambda x: x[1]):
            if new_r > entry: break
            if tk in portfolio: continue
            if consecutive.get(tk, 0) < 3: continue
            info = today_data.get(tk, {})
            if info.get('min_seg', 0) < 0: continue
            price = info.get('price')
            if price and price > 0:
                cands.append((tk, price))
        # Dynamic weight 결정 — 2슬롯 + 양 슬롯 모두 빈 경우만 적용
        if slots == 2 and not dynamic_mode.startswith('fixed') and free_slots == list(range(slots)) and len(cands) >= 2:
            tk1, _ = cands[0]; tk2, _ = cands[1]
            weights = get_dynamic_weights(today_data, tk1, tk2, dynamic_mode)
        else:
            weights = default_weights
        for slot_idx in free_slots:
            if not cands: break
            tk, price = cands.pop(0)
            portfolio[tk] = {'entry_price': price, 'slot_idx': slot_idx, 'weight': weights[slot_idx]}
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def main():
    print('=' * 130)
    print('C2 b=3 + Dynamic Weight 측정 — 1위 신호 강도 비례 비중')
    print('=' * 130)
    dates, data, price_full = load_all(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))
    step24 = max(1, len(eligible) // 25)
    starts24 = [eligible[step24 * i] for i in range(1, 25)]

    BASE_W = [33,34,33]
    BASE_DEF = (BASE_W, 30, 10, 0, 'fixed_70_30')  # baseline은 dynamic 안 씀, 균등
    # baseline simulator (균등)
    def sim_base(sd):
        return simulate(dates, data, price_full, BASE_W, 30, 10, 0, 'fixed_70_30', start_date=sd)['total_return']
    base_seed_avgs = []
    for chosen in seed_starts:
        sr = [sim_base(sd) for sd in chosen]
        base_seed_avgs.append(sum(sr)/len(sr))
    base_24 = [sim_base(sd) for sd in starts24]

    cands = [
        # 참고 (고정)
        ('A: 70/30 b=0 [v82]',            [70,30], 30, 10, 0, 'fixed_70_30'),
        ('Fixed 80/20 + C2 b=3 [★best]',  [80,20], 30, 10, 3, 'fixed_80_20'),
        ('Fixed 70/30 + C2 b=3',           [70,30], 30, 10, 3, 'fixed_70_30'),
        ('Fixed 75/25 + C2 b=3',           [75,25], 30, 10, 3, 'fixed_75_25'),
        # Dynamic
        ('Dynamic A (2단계 80/70) + C2 b=3', [70,30], 30, 10, 3, 'A'),
        ('Dynamic B (3단계 85/75/65) + C2 b=3', [70,30], 30, 10, 3, 'B'),
        ('Dynamic C (4단계 85/80/70/60) + C2 b=3', [70,30], 30, 10, 3, 'C'),
        ('Dynamic D (score_100[2위] 기반) + C2 b=3', [70,30], 30, 10, 3, 'D'),
    ]

    print()
    print(f'{"config":<46} {"R lift":>10} {"R wins":>10} {"M24 lift":>10} {"M24 wins":>11} {"M24 min":>11} {"M24 max":>11} {"MDD":>8}')
    print('-' * 130)
    for label, w, e, x, b, dm in cands:
        seed_avgs, mdds = [], []
        for chosen in seed_starts:
            sr = []
            for sd in chosen:
                r = simulate(dates, data, price_full, w, e, x, b, dm, start_date=sd)
                sr.append(r['total_return']); mdds.append(r['max_dd'])
            seed_avgs.append(sum(sr)/len(sr))
        m24 = [simulate(dates, data, price_full, w, e, x, b, dm, start_date=sd)['total_return'] for sd in starts24]
        r_lifts = [b_ - a for a, b_ in zip(base_seed_avgs, seed_avgs)]
        r_lift = sum(r_lifts)/len(r_lifts)
        r_wins = sum(1 for l in r_lifts if l > 0)
        m24_lifts = [b_ - a for a, b_ in zip(base_24, m24)]
        m24_lift = sum(m24_lifts)/24
        m24_wins = sum(1 for l in m24_lifts if l > 0)
        m24_min = min(m24_lifts); m24_max = max(m24_lifts)
        worst_mdd = min(mdds)
        print(f'{label:<46} {r_lift:+8.2f}%p {r_wins:>5}/500 {m24_lift:+8.2f}%p {m24_wins:>5}/24 {m24_min:+9.2f}%p {m24_max:+9.2f}%p {worst_mdd:+7.2f}%')

    # 현재 (5/22) 1위/2위 격차 보기
    print()
    print('=' * 130)
    print('현재 최근 5일 1위/2위 w_gap 격차 — 사용자 우려 검증')
    print('=' * 130)
    recent = dates[-10:]
    print(f'{"date":<12} {"1위":<7} {"w_gap_1":>9} {"score_1":>8} {"2위":<7} {"w_gap_2":>9} {"score_2":>8} {"ratio":>8} {"동적 비중":>15}')
    for d in recent:
        ranked = sorted(
            [(tk, info.get('w_gap', 0), info.get('score_100', 0)) for tk, info in data.get(d, {}).items() if info.get('p2') and info.get('p2') <= 5],
            key=lambda x: data[d][x[0]]['p2']
        )
        if len(ranked) < 2: continue
        tk1, wg1, s1 = ranked[0]; tk2, wg2, s2 = ranked[1]
        ratio = abs(wg1)/abs(wg2) if abs(wg2) > 0 else 999
        dyn_w = get_dynamic_weights(data[d], tk1, tk2, 'C')
        print(f'{d:<12} {tk1:<7} {wg1:+8.4f} {s1:>7.1f} {tk2:<7} {wg2:+8.4f} {s2:>7.1f} {ratio:>7.2f} {str(dyn_w):>15}')


if __name__ == '__main__':
    main()
