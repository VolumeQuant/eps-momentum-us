"""C1 boost robustness + 80/20 weight robustness — single-stock 의존성 검증

Part A: C1 boost (EPS↑ + 가격 30d↑) vs no_boost. 슈퍼위너 제외 시 edge 유지?
  - C1 dominant winner = SNDK (가격↑). MU(C2)는 C1 boost 안 받음.
  - 우주: 전체 / MU 제외 / SNDK 제외
Part B: 비중 80/20 vs 50/50 vs 70/30 (boost 없음). 80/20 우위가 single-stock 탓?
  - 우주: 전체 / MU 제외 / SNDK 제외

self-contained (외부 rerank import 안 함).
"""
import sys
import random
from pathlib import Path
from collections import defaultdict
import sqlite3

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
import daily_runner as dr

DB_PATH = ROOT / 'eps_momentum_data.db'
N_SEEDS = 500
SAMPLES = 3
MIN_HOLD = 10
LOOKBACK = 30


def load_raw():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px

    def p30(tk, today):
        if today not in dates:
            return None
        di = dates.index(today)
        if di < LOOKBACK:
            return None
        pp = price_full.get(dates[di - LOOKBACK], {}).get(tk)
        cp = price_full.get(today, {}).get(tk)
        return (cp - pp) / pp * 100 if (pp and cp and pp > 0) else None

    raw = {}
    for d in dates:
        rows = cur.execute('''SELECT ticker, price, eps_chg_weighted,
                ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL''', (d,)).fetchall()
        tks = [r[0] for r in rows]
        wmap = dr._compute_w_gap_map(cur, d, tks)
        raw[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[3:8])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                segs.append(max(-100, min(100, (a - b) / abs(b) * 100)) if (b and abs(b) > 0.01) else 0)
            ew = r[2] if r[2] is not None else 0.0
            pp = p30(tk, d)
            raw[d][tk] = {
                'wgap': wmap.get(tk, 0), 'price': r[1], 'eps_w': ew,
                'min_seg': min(segs) if segs else 0,
                'is_c1': (ew > 0 and pp is not None and pp > 0),
                'is_c2': (ew > 0 and pp is not None and pp < 0),
            }
    conn.close()
    return dates, raw, price_full


def build_data(raw, exclude=frozenset()):
    data = {}
    for d, day in raw.items():
        tks = [t for t in day if t not in exclude]
        order = sorted(tks, key=lambda t: -day[t]['wgap'])
        br = {t: i + 1 for i, t in enumerate(order)}
        data[d] = {t: {**day[t], 'base_rank': br[t]} for t in tks}
    return data


def rerank(td, boost_field, boost):
    N = len(td)
    cands = []
    for tk, info in td.items():
        b = boost if (boost and boost_field and info.get(boost_field)) else 0
        cands.append((-((N + 1 - info['base_rank']) + b), info['base_rank'], tk))
    cands.sort()
    return {tk: i + 1 for i, (_, _, tk) in enumerate(cands)}


def simulate(dates_all, data, price_full, weights, boost_field=None, boost=0,
             entry=30, exit_=10, start_date=None):
    slots = len(weights)
    dates = [d for d in dates_all if (not start_date or d >= start_date)]
    portfolio = {}
    consec = defaultdict(int)
    drs = []
    if start_date:
        for d in dates_all:
            if d >= start_date:
                break
            nr = rerank(data.get(d, {}), boost_field, boost)
            nc = defaultdict(int)
            for tk, r in nr.items():
                if r <= 30:
                    nc[tk] = consec.get(tk, 0) + 1
            consec = nc
    for di, today in enumerate(dates):
        if today not in data:
            continue
        td = data[today]
        nr = rerank(td, boost_field, boost)
        nc = defaultdict(int)
        for tk, r in nr.items():
            if r <= 30:
                nc[tk] = consec.get(tk, 0) + 1
        consec = nc
        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di - 1]
            for tk, pi in portfolio.items():
                cp = td.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                pp = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if cp and pp and pp > 0:
                    day_ret += (pi['weight'] / 100.0) * (cp - pp) / pp * 100
        drs.append(day_ret)
        for tk in list(portfolio.keys()):
            rank = nr.get(tk)
            if td.get(tk, {}).get('min_seg', 0) < -2 or rank is None or rank > exit_:
                del portfolio[tk]
        used = {p['slot_idx'] for p in portfolio.values()}
        free = sorted(i for i in range(slots) if i not in used)
        cands = []
        for tk, r in sorted(nr.items(), key=lambda x: x[1]):
            if r > entry:
                break
            if tk in portfolio or consec.get(tk, 0) < 3:
                continue
            info = td.get(tk, {})
            if info.get('min_seg', 0) < 0:
                continue
            px = info.get('price')
            if px and px > 0:
                cands.append((tk, px))
        for si in free:
            if not cands:
                break
            tk, px = cands.pop(0)
            portfolio[tk] = {'entry_price': px, 'slot_idx': si, 'weight': weights[si]}
    cum = 1.0
    for r in drs:
        cum *= (1 + r / 100)
    return (cum - 1) * 100


def main():
    print('=' * 100)
    dates, raw, price_full = load_raw()
    eligible = dates[:-MIN_HOLD]
    seeds = []
    for s in range(N_SEEDS):
        random.seed(s)
        seeds.append(random.sample(eligible, SAMPLES))

    def avg(data, **spec):
        out = []
        for ch in seeds:
            out.append(sum(simulate(dates, data, price_full, start_date=sd, **spec) for sd in ch) / SAMPLES)
        return out

    universes = [('전체', frozenset()), ('MU 제외', frozenset({'MU'})), ('SNDK 제외', frozenset({'SNDK'}))]

    print('PART A — C1 boost vs no_boost (80/20)')
    print('-' * 100)
    for uname, ex in universes:
        data = build_data(raw, ex)
        nb = avg(data, weights=[80, 20], boost_field=None, boost=0)
        for bval in (3, 5):
            c1 = avg(data, weights=[80, 20], boost_field='is_c1', boost=bval)
            edge = [a - b for a, b in zip(c1, nb)]
            print(f'  [{uname:<8}] C1 b={bval}: 평균 edge {sum(edge)/N_SEEDS:+.2f}%p / C1승 {sum(1 for e in edge if e>0)}/500 / 최악 {min(edge):+.2f}%p / 최고 {max(edge):+.2f}%p')

    print('\nPART B — 비중 80/20 vs 50/50 vs 70/30 (boost 없음)')
    print('-' * 100)
    for uname, ex in universes:
        data = build_data(raw, ex)
        w80 = avg(data, weights=[80, 20])
        w50 = avg(data, weights=[50, 50])
        w70 = avg(data, weights=[70, 30])
        e_80_50 = [a - b for a, b in zip(w80, w50)]
        e_80_70 = [a - b for a, b in zip(w80, w70)]
        print(f'  [{uname:<8}] 평균수익: 80/20 {sum(w80)/N_SEEDS:+.1f}% | 50/50 {sum(w50)/N_SEEDS:+.1f}% | 70/30 {sum(w70)/N_SEEDS:+.1f}%')
        print(f'             80/20 vs 50/50 edge: 평균 {sum(e_80_50)/N_SEEDS:+.2f}%p / 80승 {sum(1 for e in e_80_50 if e>0)}/500 / 최악 {min(e_80_50):+.2f}%p')
        print(f'             80/20 vs 70/30 edge: 평균 {sum(e_80_70)/N_SEEDS:+.2f}%p / 80승 {sum(1 for e in e_80_70 if e>0)}/500')


if __name__ == '__main__':
    main()
