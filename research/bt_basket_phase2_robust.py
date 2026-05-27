"""Phase 2 — 집중(2종목) vs 바스켓 robustness (MU/SNDK 제외 + 랜덤시작)

Phase 1: 2종목 +224% 압도. 하지만 MU/SNDK 슈퍼위너 의존 의심.
핵심 질문: 슈퍼위너 빼면 집중 우위가 무너지나(운빨)? 바스켓은 robust한가?
 A) leave-winner-out: none/MU/SNDK/both × {2종목, Top5/10/20}
 B) 랜덤시작 500 (3샘플): 2종목 vs Top-10 평균수익 + 바스켓이 이기는 비율(일관성)
"""
import sys
import random
from pathlib import Path
import numpy as np
import pandas as pd

pd.set_option('future.no_silent_downcasting', True)
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'research'))
from bt_c1_and_weights_robust import load_raw, build_data  # noqa
from bt_weights_decision import simulate_mdd  # noqa

N_SEEDS = 500
SAMPLES = 3
MIN_HOLD = 10


def basket_ret(dates, data, price_full, topn, start_date=None):
    seg = [d for d in dates if (not start_date or d >= start_date)]
    rets = []
    for i in range(1, len(seg)):
        d, pd_ = seg[i], seg[i - 1]
        held = [tk for tk, info in data.get(pd_, {}).items() if info['base_rank'] <= topn]
        day = []
        for tk in held:
            cp = data.get(d, {}).get(tk, {}).get('price') or price_full.get(d, {}).get(tk)
            pp = data.get(pd_, {}).get(tk, {}).get('price') or price_full.get(pd_, {}).get(tk)
            if cp and pp and pp > 0:
                day.append((cp - pp) / pp)
        rets.append(np.mean(day) if day else 0.0)
    nav = (1 + pd.Series(rets)).cumprod()
    return (nav.iloc[-1] - 1) * 100 if len(nav) else 0.0


def main():
    dates, raw, price_full = load_raw()
    eligible = dates[:-MIN_HOLD]

    print('=' * 78)
    print('A) leave-winner-out — full window 총수익 (%)')
    print(f'{"우주":<14}{"2종목80/20":>12}{"Top-5":>9}{"Top-10":>9}{"Top-20":>9}')
    print('-' * 78)
    for label, ex in [('전체', frozenset()), ('MU 제외', frozenset({'MU'})),
                      ('SNDK 제외', frozenset({'SNDK'})), ('둘다 제외', frozenset({'MU', 'SNDK'}))]:
        data = build_data(raw, ex)
        s2, _ = simulate_mdd(dates, data, price_full, [80, 20], start_date=dates[0])
        b5 = basket_ret(dates, data, price_full, 5)
        b10 = basket_ret(dates, data, price_full, 10)
        b20 = basket_ret(dates, data, price_full, 20)
        print(f'{label:<14}{s2:>+11.1f}%{b5:>+8.1f}%{b10:>+8.1f}%{b20:>+8.1f}%')

    print('\n' + '=' * 78)
    print('B) 랜덤시작 500 — 2종목 vs Top-10 바스켓 (전체 우주)')
    data = build_data(raw, frozenset())
    seeds = []
    for s in range(N_SEEDS):
        random.seed(s)
        seeds.append(random.sample(eligible, SAMPLES))
    s2_avgs, b10_avgs, basket_wins = [], [], 0
    for ch in seeds:
        s2v = np.mean([simulate_mdd(dates, data, price_full, [80, 20], start_date=sd)[0] for sd in ch])
        b10v = np.mean([basket_ret(dates, data, price_full, 10, start_date=sd) for sd in ch])
        s2_avgs.append(s2v); b10_avgs.append(b10v)
        if b10v > s2v:
            basket_wins += 1
    print(f'  2종목 평균 {np.mean(s2_avgs):+.1f}% (표준편차 {np.std(s2_avgs):.1f})')
    print(f'  Top-10 평균 {np.mean(b10_avgs):+.1f}% (표준편차 {np.std(b10_avgs):.1f})')
    print(f'  Top-10이 2종목 이긴 비율: {basket_wins}/{N_SEEDS} ({basket_wins/N_SEEDS*100:.0f}%)')
    print(f'  최악 시작 — 2종목 {min(s2_avgs):+.1f}% / Top-10 {min(b10_avgs):+.1f}%')

    print('\n해석: MU/SNDK 빼도 2종목이 바스켓 크게 앞서면 집중이 robust. '
          '무너지면(바스켓 수준 수렴) 집중 우위=슈퍼위너 운빨. '
          '랜덤시작 변동성/최악도 비교 — 바스켓이 안정적이면 분산 가치.')


if __name__ == '__main__':
    main()
