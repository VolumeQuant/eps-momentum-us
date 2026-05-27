"""비중 결정 — 80/20 vs 75/25 vs 70/30 vs 50/50 (boost 없음)

집중도 ↑ = 평균수익 ↑ + tail risk ↑ 의 trade-off를 정량화.
지표: R 평균/최악, MDD(최악), M24 min. 우주: 전체 / MU 제외 / SNDK 제외.
"""
import sys
import random
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'research'))

from bt_c1_and_weights_robust import load_raw, build_data, rerank  # noqa

N_SEEDS = 500
SAMPLES = 3
MIN_HOLD = 10


def simulate_mdd(dates_all, data, price_full, weights, entry=30, exit_=10, start_date=None):
    slots = len(weights)
    dates = [d for d in dates_all if (not start_date or d >= start_date)]
    portfolio = {}
    consec = defaultdict(int)
    drs = []
    if start_date:
        for d in dates_all:
            if d >= start_date:
                break
            nr = rerank(data.get(d, {}), None, 0)
            nc = defaultdict(int)
            for tk, r in nr.items():
                if r <= 30:
                    nc[tk] = consec.get(tk, 0) + 1
            consec = nc
    for di, today in enumerate(dates):
        if today not in data:
            continue
        td = data[today]
        nr = rerank(td, None, 0)
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
    peak = 1.0
    mdd = 0
    for r in drs:
        cum *= (1 + r / 100)
        peak = max(peak, cum)
        mdd = min(mdd, (cum - peak) / peak * 100)
    return (cum - 1) * 100, mdd


def main():
    dates, raw, price_full = load_raw()
    eligible = dates[:-MIN_HOLD]
    seeds = []
    for s in range(N_SEEDS):
        random.seed(s)
        seeds.append(random.sample(eligible, SAMPLES))
    step24 = max(1, len(eligible) // 25)
    starts24 = [eligible[step24 * i] for i in range(1, 25)]

    WEIGHTS = [('80/20', [80, 20]), ('75/25', [75, 25]), ('70/30', [70, 30]), ('50/50', [50, 50])]
    universes = [('전체', frozenset()), ('MU 제외', frozenset({'MU'})), ('SNDK 제외', frozenset({'SNDK'}))]

    for uname, ex in universes:
        data = build_data(raw, ex)
        print(f'\n### {uname} ###')
        print(f'  {"비중":<7}{"R평균":>9}{"R최악seed":>11}{"MDD최악":>10}{"M24평균":>9}{"M24최악":>9}')
        for wn, w in WEIGHTS:
            seed_avgs = []
            worst_mdd = 0
            for ch in seeds:
                rs = []
                for sd in ch:
                    ret, mdd = simulate_mdd(dates, data, price_full, w, start_date=sd)
                    rs.append(ret)
                    worst_mdd = min(worst_mdd, mdd)
                seed_avgs.append(sum(rs) / SAMPLES)
            m24 = [simulate_mdd(dates, data, price_full, w, start_date=sd)[0] for sd in starts24]
            print(f'  {wn:<7}{sum(seed_avgs)/N_SEEDS:>+8.1f}%{min(seed_avgs):>+10.1f}%{worst_mdd:>+9.1f}%{sum(m24)/24:>+8.1f}%{min(m24):>+8.1f}%')


if __name__ == '__main__':
    main()
