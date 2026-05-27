"""C2 boost EPS-aware 그리드 — binary boost 부당성 fix 검증

배경: v83 C2 boost는 binary (+3 고정). eps_chg_weighted > 0 이기만 하면
MU(eps_w~70)든 BWXT(eps_w 2.82)든 동일 +3 → 20% 비중 영역으로 점프.
사용자 지적: 약한 EPS인데 dip 하나로 MU와 같은 boost = 부당.

후보:
- no_boost      : C2 boost 없음 (80/20)
- v83_binary    : C2면 +3 (현 production)
- gate T        : C2 AND eps_w >= T 일 때만 +3   (T = 8 / 10 / 12)
- prop SCALE    : C2면 +3 * min(eps_w/SCALE, 1)   (SCALE = 10 / 15 / 20)

전부 80/20 고정 (한 번에 하나만 — boost 메커니즘만 변경).

★ 핵심 수정: 베이스라인 순위를 DB part2_rank(이미 v83 boost 적용됨)가 아니라
  raw w_gap(_compute_w_gap_map, boost 미적용)에서 재구성 → 이중 boost 차단.

측정: random 500 lift / M12 / M24 / M24 min / MDD / 4-2 worst / MU·약한C2 진단.
lift 분모 = equal-weight 3슬롯 [33,34,33] (bt_third_way와 동일 → v83 +48%p 교차검증).
"""
import sys
import sqlite3
import random
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
sys.path.insert(0, str(ROOT))

N_SEEDS = 500
SAMPLES = 3
MIN_HOLD_DAYS = 10
LOOKBACK = 30
WEAK_EPS = 8.0  # 진단용: eps_w < 8 = 약한 C2


def load_all(db_path):
    import daily_runner as dr
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    price_full = defaultdict(dict)
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px

    def price_30d(tk, today):
        if today not in dates:
            return None
        di = dates.index(today)
        if di < LOOKBACK:
            return None
        pd = dates[di - LOOKBACK]
        pp = price_full.get(pd, {}).get(tk)
        cp = price_full.get(today, {}).get(tk)
        if pp and cp and pp > 0:
            return (cp - pp) / pp * 100
        return None

    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, price, eps_chg_weighted,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        tks = [r[0] for r in rows]
        # ★ 깨끗한 raw w_gap (boost 미적용 본연 conviction)
        wmap = dr._compute_w_gap_map(cur, d, tks)
        # raw w_gap 내림차순 = 깨끗한 베이스라인 순위
        base_order = sorted(tks, key=lambda t: -wmap.get(t, 0))
        base_rank = {t: i + 1 for i, t in enumerate(base_order)}
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[3:8])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a - b) / abs(b) * 100)))
                else:
                    segs.append(0)
            eps_w = r[2]
            p30 = price_30d(tk, d)
            is_c2 = (eps_w is not None and eps_w > 0 and p30 is not None and p30 < 0)
            data[d][tk] = {
                'base_rank': base_rank[tk],
                'price': r[1],
                'eps_w': eps_w if eps_w is not None else 0.0,
                'min_seg': min(segs) if segs else 0,
                'is_c2': is_c2,
                'p30': p30,
            }
    conn.close()
    return dates, data, price_full


def rerank(today_data, mode, params):
    """깨끗한 base_rank + boost → 새 순위. 큰 score 먼저."""
    N = len(today_data)
    cands = []
    for tk, info in today_data.items():
        br = info['base_rank']
        boost = 0.0
        if info['is_c2']:
            ew = info['eps_w']
            if mode == 'baseline':
                boost = 0.0
            elif mode == 'binary':
                boost = params['boost']
            elif mode == 'gate':
                boost = params['boost'] if ew >= params['T'] else 0.0
            elif mode == 'prop':
                boost = params['boost'] * min(ew / params['SCALE'], 1.0)
        score = (N + 1 - br) + boost
        cands.append((-score, br, tk))
    cands.sort()  # score 큰 순, 동점이면 base_rank 작은 순
    return {tk: i + 1 for i, (_, _, tk) in enumerate(cands)}


def simulate(dates_all, data, price_full, weights, entry, exit_, mode, params, start_date=None):
    slots = len(weights)
    dates = [d for d in dates_all if (not start_date or d >= start_date)]
    portfolio = {}
    consecutive = defaultdict(int)
    daily_returns = []
    # warm-up consecutive
    if start_date:
        for d in dates_all:
            if d >= start_date:
                break
            nr = rerank(data.get(d, {}), mode, params)
            nc = defaultdict(int)
            for tk, r in nr.items():
                if r <= 30:
                    nc[tk] = consecutive.get(tk, 0) + 1
            consecutive = nc
    for di, today in enumerate(dates):
        if today not in data:
            continue
        td = data[today]
        nr = rerank(td, mode, params)
        nc = defaultdict(int)
        for tk, r in nr.items():
            if r <= 30:
                nc[tk] = consecutive.get(tk, 0) + 1
        consecutive = nc
        # day return (어제 포트폴리오 기준)
        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di - 1]
            for tk, pinfo in portfolio.items():
                cur_p = td.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                prev_p = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if cur_p and prev_p and prev_p > 0:
                    day_ret += (pinfo['weight'] / 100.0) * (cur_p - prev_p) / prev_p * 100
        daily_returns.append(day_ret)
        # exit
        for tk in list(portfolio.keys()):
            rank = nr.get(tk)
            ms = td.get(tk, {}).get('min_seg', 0)
            if ms < -2 or rank is None or rank > exit_:
                del portfolio[tk]
        # entry — 빈 슬롯 채우기 (슬라이드)
        used = {p['slot_idx'] for p in portfolio.values()}
        free = sorted(i for i in range(slots) if i not in used)
        cands = []
        for tk, r in sorted(nr.items(), key=lambda x: x[1]):
            if r > entry:
                break
            if tk in portfolio:
                continue
            if consecutive.get(tk, 0) < 3:
                continue
            info = td.get(tk, {})
            if info.get('min_seg', 0) < 0:
                continue
            px = info.get('price')
            if px and px > 0:
                cands.append((tk, px))
        for slot_idx in free:
            if not cands:
                break
            tk, px = cands.pop(0)
            portfolio[tk] = {'entry_price': px, 'slot_idx': slot_idx, 'weight': weights[slot_idx]}
    cum = 1.0
    peak = 1.0
    max_dd = 0
    for r in daily_returns:
        cum *= (1 + r / 100)
        peak = max(peak, cum)
        max_dd = min(max_dd, (cum - peak) / peak * 100)
    return {'total_return': (cum - 1) * 100, 'max_dd': max_dd}


def run_random(spec, dates, data, price_full, seed_starts):
    seed_avgs, mdds = [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, start_date=sd, **spec)
            sr.append(r['total_return'])
            mdds.append(r['max_dd'])
        seed_avgs.append(sum(sr) / len(sr))
    return {'seed_avgs': seed_avgs, 'mdds': mdds}


def diagnose(dates, data):
    """각 변형이 weak-C2를 억제하고 MU(강한 C2)는 유지하는지 진단."""
    variants = [
        ('binary',  {'mode': 'binary', 'params': {'boost': 3}}),
        ('gate8',   {'mode': 'gate', 'params': {'boost': 3, 'T': 8}}),
        ('gate10',  {'mode': 'gate', 'params': {'boost': 3, 'T': 10}}),
        ('gate12',  {'mode': 'gate', 'params': {'boost': 3, 'T': 12}}),
        ('prop10',  {'mode': 'prop', 'params': {'boost': 3, 'SCALE': 10}}),
        ('prop15',  {'mode': 'prop', 'params': {'boost': 3, 'SCALE': 15}}),
        ('prop20',  {'mode': 'prop', 'params': {'boost': 3, 'SCALE': 20}}),
    ]
    print('\n=== 진단: C2 boost로 Top2 진입 케이스 (binary 기준) → 각 변형서 유지/억제 ===')
    print(f'{"date":<12}{"tk":<6}{"eps_w":>7}{"dip":>8}  | ' + ' '.join(f'{v[0]:>7}' for v in variants))
    for d in dates:
        td = data[d]
        base = rerank(td, 'baseline', {})
        binr = rerank(td, 'binary', {'boost': 3})
        # binary가 Top2로 올린 종목
        for tk, info in td.items():
            if not info['is_c2']:
                continue
            if binr.get(tk, 99) <= 2 and base.get(tk, 99) > binr.get(tk, 99):
                row = f'{d:<12}{tk:<6}{info["eps_w"]:>7.1f}{info["p30"]:>+8.1f}  | '
                cells = []
                for _, spec in variants:
                    rr = rerank(td, spec['mode'], spec['params'])
                    cells.append(f'{rr.get(tk, 99):>7}')
                print(row + ' '.join(cells))


def main():
    print('=' * 140)
    print('C2 boost EPS-aware 그리드 — binary 부당성 fix (raw w_gap 베이스라인, 이중 boost 차단)')
    print('=' * 140)
    dates, data, price_full = load_all(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]
    print(f'days={len(dates)} eligible={len(eligible)} {dates[0]}~{dates[-1]}')

    seed_starts = []
    for s in range(N_SEEDS):
        random.seed(s)
        seed_starts.append(random.sample(eligible, SAMPLES))
    step12 = max(1, len(eligible) // 13)
    starts12 = [eligible[step12 * i] for i in range(1, 13)]
    step24 = max(1, len(eligible) // 25)
    starts24 = [eligible[step24 * i] for i in range(1, 25)]
    # 4/2 worst (v83 강조) — eligible 내 가장 가까운 날
    start_42 = next((d for d in eligible if d >= '2026-04-02'), eligible[-1])

    # lift 분모: equal-weight 3슬롯 baseline (bt_third_way 동일)
    BASE = {'weights': [33, 34, 33], 'entry': 30, 'exit_': 10, 'mode': 'baseline', 'params': {}}
    base_r = run_random(BASE, dates, data, price_full, seed_starts)
    base_24 = [simulate(dates, data, price_full, start_date=sd, **BASE)['total_return'] for sd in starts24]
    base_12 = [simulate(dates, data, price_full, start_date=sd, **BASE)['total_return'] for sd in starts12]

    W = [80, 20]
    cands = [
        ('80/20 no_boost (v82형)',  {'weights': W, 'entry': 30, 'exit_': 10, 'mode': 'baseline', 'params': {}}),
        ('80/20 binary b=3 [v83]',  {'weights': W, 'entry': 30, 'exit_': 10, 'mode': 'binary', 'params': {'boost': 3}}),
        ('80/20 gate T=8 b=3',      {'weights': W, 'entry': 30, 'exit_': 10, 'mode': 'gate', 'params': {'boost': 3, 'T': 8}}),
        ('80/20 gate T=10 b=3',     {'weights': W, 'entry': 30, 'exit_': 10, 'mode': 'gate', 'params': {'boost': 3, 'T': 10}}),
        ('80/20 gate T=12 b=3',     {'weights': W, 'entry': 30, 'exit_': 10, 'mode': 'gate', 'params': {'boost': 3, 'T': 12}}),
        ('80/20 prop SCALE=10',     {'weights': W, 'entry': 30, 'exit_': 10, 'mode': 'prop', 'params': {'boost': 3, 'SCALE': 10}}),
        ('80/20 prop SCALE=15',     {'weights': W, 'entry': 30, 'exit_': 10, 'mode': 'prop', 'params': {'boost': 3, 'SCALE': 15}}),
        ('80/20 prop SCALE=20',     {'weights': W, 'entry': 30, 'exit_': 10, 'mode': 'prop', 'params': {'boost': 3, 'SCALE': 20}}),
    ]

    print()
    hdr = (f'{"config":<26}{"R lift":>10}{"R wins":>9}{"M12 lift":>10}{"M12 min":>10}'
           f'{"M24 lift":>10}{"M24 wins":>10}{"M24 min":>10}{"MDD":>9}{"4/2 ret":>10}')
    print(hdr)
    print('-' * 140)
    for label, spec in cands:
        rr = run_random(spec, dates, data, price_full, seed_starts)
        m12 = [simulate(dates, data, price_full, start_date=sd, **spec)['total_return'] for sd in starts12]
        m24 = [simulate(dates, data, price_full, start_date=sd, **spec)['total_return'] for sd in starts24]
        r42 = simulate(dates, data, price_full, start_date=start_42, **spec)['total_return']
        b42 = simulate(dates, data, price_full, start_date=start_42, **BASE)['total_return']
        r_lifts = [b - a for a, b in zip(base_r['seed_avgs'], rr['seed_avgs'])]
        r_lift = sum(r_lifts) / len(r_lifts)
        r_wins = sum(1 for l in r_lifts if l > 0)
        m12_l = [b - a for a, b in zip(base_12, m12)]
        m24_l = [b - a for a, b in zip(base_24, m24)]
        print(f'{label:<26}{r_lift:>+8.2f}%p{r_wins:>6}/500{sum(m12_l)/12:>+8.2f}%p'
              f'{min(m12_l):>+8.2f}%p{sum(m24_l)/24:>+8.2f}%p{sum(1 for l in m24_l if l>0):>7}/24'
              f'{min(m24_l):>+8.2f}%p{min(rr["mdds"]):>+8.2f}%{(r42-b42):>+8.2f}%p')

    diagnose(dates, data)


if __name__ == '__main__':
    main()
