"""진입/이탈/슬롯 robustness — (2,10,2) 최적 검증, single-stock 잣대

exit (회전 knob): 6/8/10/12/14 @ slots2 80/20
slots (집중 knob): 1[100] / 2[80,20] / 3[70,20,10] @ exit10
지표: R평균 / MDD최악 / M24최악. 우주: 전체 / MU 제외 / SNDK 제외.
boost 없음 (제거 확정).
"""
import sys
import random
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'research'))

from bt_c1_and_weights_robust import load_raw, build_data  # noqa
from bt_weights_decision import simulate_mdd  # noqa

N_SEEDS = 500
SAMPLES = 3
MIN_HOLD = 10


def run(label, dates, raw, ex, weights, exit_):
    data = build_data(raw, ex)
    random.seed(0)
    seeds = [random.sample(dates[:-MIN_HOLD], SAMPLES) for _ in range(N_SEEDS)]
    # 재현: 동일 시드셋
    seeds = []
    for s in range(N_SEEDS):
        random.seed(s)
        seeds.append(random.sample(dates[:-MIN_HOLD], SAMPLES))
    return data, seeds


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

    def measure(data, weights, exit_):
        seed_avgs, worst_mdd = [], 0
        for ch in seeds:
            rs = []
            for sd in ch:
                ret, mdd = simulate_mdd(dates, data, price_full, weights, exit_=exit_, start_date=sd)
                rs.append(ret)
                worst_mdd = min(worst_mdd, mdd)
            seed_avgs.append(sum(rs) / SAMPLES)
        m24 = [simulate_mdd(dates, data, price_full, weights, exit_=exit_, start_date=sd)[0] for sd in starts24]
        return sum(seed_avgs) / N_SEEDS, worst_mdd, sum(m24) / 24, min(m24)

    print('=== EXIT 그리드 (slots2 80/20) ===')
    for uname, ex in universes:
        data = build_data(raw, ex)
        print(f'\n[{uname}]  {"exit":<6}{"R평균":>9}{"MDD최악":>10}{"M24평균":>9}{"M24최악":>9}')
        for xv in (6, 8, 10, 12, 14):
            r, md, m24a, m24m = measure(data, [80, 20], xv)
            mark = ' ←현재' if xv == 10 else ''
            print(f'         {xv:<6}{r:>+8.1f}%{md:>+9.1f}%{m24a:>+8.1f}%{m24m:>+8.1f}%{mark}')

    print('\n=== SLOTS 그리드 (exit10) ===')
    SLOTS = [('1 [100]', [100]), ('2 [80,20]', [80, 20]), ('3 [70,20,10]', [70, 20, 10])]
    for uname, ex in universes:
        data = build_data(raw, ex)
        print(f'\n[{uname}]  {"slots":<14}{"R평균":>9}{"MDD최악":>10}{"M24평균":>9}{"M24최악":>9}')
        for sn, w in SLOTS:
            r, md, m24a, m24m = measure(data, w, 10)
            mark = ' ←현재' if sn.startswith('2') else ''
            print(f'         {sn:<14}{r:>+8.1f}%{md:>+9.1f}%{m24a:>+8.1f}%{m24m:>+8.1f}%{mark}')


if __name__ == '__main__':
    main()
