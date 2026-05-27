"""gate vs no_boost robust 검증 — MU(슈퍼위너) 제거 시에도 gate 우위 유지되나?

질문: 등수 +3 밀어주고 gate 거는 게 C2 boost 아예 안 주는 것보다 나은가?
그리드상 gate > no_boost (+2%p)지만 대부분 MU 한 종목 기여 의심.

테스트: 전체 우주 vs MU 제외 우주 각각에서 (gate - no_boost) edge 직접 측정.
  - edge가 MU 제거 후에도 양수 유지 → boost 자체가 robust
  - edge가 MU 제거 후 0/음수 → "boost = MU 운빨", no_boost가 더 정직
"""
import sys
import random
from pathlib import Path
from collections import defaultdict
import sqlite3

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'research'))

from bt_c2_eps_aware_grid import rerank, simulate  # noqa
import daily_runner as dr

DB_PATH = ROOT / 'eps_momentum_data.db'
N_SEEDS = 500
SAMPLES = 3
MIN_HOLD_DAYS = 10
LOOKBACK = 30


def load_raw():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px

    def p30(tk, today):
        if today not in dates:
            return None
        di = dates.index(today)
        if di < LOOKBACK:
            return None
        pd = dates[di - LOOKBACK]
        pp = price_full.get(pd, {}).get(tk)
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
                'is_c2': (ew > 0 and pp is not None and pp < 0), 'p30': pp,
            }
    conn.close()
    return dates, raw, price_full


def build_data(raw, exclude=frozenset()):
    """exclude 종목 제거 후 wgap 내림차순으로 base_rank 재부여."""
    data = {}
    for d, day in raw.items():
        tks = [t for t in day if t not in exclude]
        order = sorted(tks, key=lambda t: -day[t]['wgap'])
        br = {t: i + 1 for i, t in enumerate(order)}
        data[d] = {t: {**day[t], 'base_rank': br[t]} for t in tks}
    return data


def avg_ret(spec, dates, data, price_full, starts):
    return sum(simulate(dates, data, price_full, start_date=sd, **spec)['total_return'] for sd in starts) / len(starts)


def main():
    print('=' * 100)
    print('gate vs no_boost robust — MU 제거 시 edge 유지 여부')
    print('=' * 100)
    dates, raw, price_full = load_raw()
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for s in range(N_SEEDS):
        random.seed(s)
        seed_starts.append(random.sample(eligible, SAMPLES))
    step24 = max(1, len(eligible) // 25)
    starts24 = [eligible[step24 * i] for i in range(1, 25)]
    start_42 = next((d for d in eligible if d >= '2026-04-02'), eligible[-1])

    W = [80, 20]
    SPEC = {
        'no_boost': {'weights': W, 'entry': 30, 'exit_': 10, 'mode': 'baseline', 'params': {}},
        'gate10':   {'weights': W, 'entry': 30, 'exit_': 10, 'mode': 'gate', 'params': {'boost': 3, 'T': 10}},
        'binary':   {'weights': W, 'entry': 30, 'exit_': 10, 'mode': 'binary', 'params': {'boost': 3}},
    }

    for label, exclude in [('전체 우주', frozenset()), ('MU 제외', frozenset({'MU'}))]:
        data = build_data(raw, exclude)
        print(f'\n### {label} ###')
        # seed별 (gate - no_boost) edge
        nb_seed = [avg_ret(SPEC['no_boost'], dates, data, price_full, ch) for ch in seed_starts]
        g_seed = [avg_ret(SPEC['gate10'], dates, data, price_full, ch) for ch in seed_starts]
        b_seed = [avg_ret(SPEC['binary'], dates, data, price_full, ch) for ch in seed_starts]
        g_edge = [g - n for g, n in zip(g_seed, nb_seed)]
        b_edge = [b - n for b, n in zip(b_seed, nb_seed)]
        # 24 multistart
        nb24 = [simulate(dates, data, price_full, start_date=sd, **SPEC['no_boost'])['total_return'] for sd in starts24]
        g24 = [simulate(dates, data, price_full, start_date=sd, **SPEC['gate10'])['total_return'] for sd in starts24]
        g24e = [g - n for g, n in zip(g24, nb24)]
        # 4/2
        nb42 = simulate(dates, data, price_full, start_date=start_42, **SPEC['no_boost'])['total_return']
        g42 = simulate(dates, data, price_full, start_date=start_42, **SPEC['gate10'])['total_return']

        print(f'  R 평균수익 (random 500): no_boost {sum(nb_seed)/N_SEEDS:+.2f}%  gate {sum(g_seed)/N_SEEDS:+.2f}%')
        print(f'  gate edge vs no_boost  : 평균 {sum(g_edge)/N_SEEDS:+.2f}%p / gate승 {sum(1 for e in g_edge if e>0)}/500 / 최악 {min(g_edge):+.2f}%p / 최고 {max(g_edge):+.2f}%p')
        print(f'  binary edge vs no_boost: 평균 {sum(b_edge)/N_SEEDS:+.2f}%p / binary승 {sum(1 for e in b_edge if e>0)}/500 (참고)')
        print(f'  M24 gate edge          : 평균 {sum(g24e)/24:+.2f}%p / gate승 {sum(1 for e in g24e if e>0)}/24 / 최악 {min(g24e):+.2f}%p')
        print(f'  4/2 gate edge          : {g42-nb42:+.2f}%p')


if __name__ == '__main__':
    main()
