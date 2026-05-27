"""sticky 슬롯 weight vs 매일 재배분(현재 순위 기반) — 수익 비교

현행(production) = sticky: 진입 슬롯에 80/20 고정, 이탈 전까지 유지.
대안 = rebalance: 매일 보유 2종목 중 현재 w_gap 높은 쪽 80%, 낮은 쪽 20% 재배정.

질문: 재배분이 수익을 더 주나? (비용 미반영 gross. 재배분은 매일 거래 발생 → 실제론 비용 불리)
robustness: 전체 / MU 제외 / SNDK 제외. 지표: R평균 / MDD최악 / M24평균·최악.
boost 없음 (v83.2).
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


def simulate(dates_all, data, price_full, weights, rebalance=False,
             entry=30, exit_=10, start_date=None, cost_bps=0):
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
        # day_ret — 어제 확정 weight 사용
        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di - 1]
            for tk, pi in portfolio.items():
                cp = td.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                pp = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if cp and pp and pp > 0:
                    day_ret += (pi['weight'] / 100.0) * (cp - pp) / pp * 100
        drs.append(day_ret)
        # exit
        for tk in list(portfolio.keys()):
            rank = nr.get(tk)
            if td.get(tk, {}).get('min_seg', 0) < -2 or rank is None or rank > exit_:
                del portfolio[tk]
        # entry
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
        # rebalance: 보유 종목을 현재 순위로 재정렬해 weight 재배정
        if rebalance and portfolio:
            held = sorted(portfolio.keys(), key=lambda t: nr.get(t, 999))
            turnover = 0
            for idx, tk in enumerate(held):
                new_w = weights[idx] if idx < len(weights) else 0
                turnover += abs(new_w - portfolio[tk]['weight'])
                portfolio[tk]['weight'] = new_w
            # 재배분 거래비용 (양방향 turnover의 절반 * cost)
            if cost_bps and turnover:
                drs[-1] -= (turnover / 100.0) / 2 * (cost_bps / 100.0)
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
    universes = [('전체', frozenset()), ('MU 제외', frozenset({'MU'})), ('SNDK 제외', frozenset({'SNDK'}))]

    CONFIGS = [
        ('sticky (현행)',        dict(rebalance=False)),
        ('rebalance (gross)',    dict(rebalance=True)),
        ('rebalance (cost 10bp)', dict(rebalance=True, cost_bps=10)),
    ]

    def measure(data, **kw):
        seed_avgs, worst_mdd = [], 0
        for ch in seeds:
            rs = []
            for sd in ch:
                ret, mdd = simulate(dates, data, price_full, [80, 20], start_date=sd, **kw)
                rs.append(ret)
                worst_mdd = min(worst_mdd, mdd)
            seed_avgs.append(sum(rs) / SAMPLES)
        m24 = [simulate(dates, data, price_full, [80, 20], start_date=sd, **kw)[0] for sd in starts24]
        return sum(seed_avgs) / N_SEEDS, worst_mdd, sum(m24) / 24, min(m24)

    for uname, ex in universes:
        data = build_data(raw, ex)
        print(f'\n### {uname} ###  {"방식":<22}{"R평균":>9}{"MDD최악":>10}{"M24평균":>9}{"M24최악":>9}')
        base = None
        for label, kw in CONFIGS:
            r, md, m24a, m24m = measure(data, **kw)
            if base is None:
                base = r
            diff = '' if label.startswith('sticky') else f'  (vs sticky {r-base:+.1f}%p)'
            print(f'{"":>13}{label:<22}{r:>+8.1f}%{md:>+9.1f}%{m24a:>+8.1f}%{m24m:>+8.1f}%{diff}')


if __name__ == '__main__':
    main()
