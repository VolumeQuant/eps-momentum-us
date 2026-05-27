"""블렌드 비율 최적화 — 헤드라인 수익 무시, 위험조정·robust 기준

주식 비중 100%→0% sweep. 각 비율의:
  - 전체 full-window: Sharpe / Calmar / MDD
  - 랜덤시작 300 평균 Sharpe (일관성)
  - 둘다제외(MU/SNDK) full-window Sharpe / Calmar  ← 로테이션 대비 핵심 시나리오
최적 = Sharpe/Calmar 최고 + 슈퍼위너 제외에도 견고.
"""
import sys
import random
from pathlib import Path
import numpy as np
import pandas as pd

import warnings
warnings.filterwarnings('ignore')
pd.set_option('future.no_silent_downcasting', True)
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'research'))
from bt_c1_and_weights_robust import load_raw, build_data  # noqa
from bt_blend_validate import stock2_daily, etf_daily  # noqa

N_SEEDS = 300
MIN_DAYS = 20


def m(daily):
    nav = (1 + daily).cumprod()
    tot = (nav.iloc[-1] - 1) * 100
    mdd = ((nav - nav.cummax()) / nav.cummax()).min() * 100
    sh = daily.mean() / daily.std() * np.sqrt(252) if daily.std() > 0 else 0
    return tot, mdd, sh, (tot / abs(mdd) if mdd < 0 else float('nan'))


def main():
    dates, raw, price_full = load_raw()
    etf_s = etf_daily(dates)
    stock_full = stock2_daily(dates, build_data(raw, frozenset()), price_full)
    stock_nowin = stock2_daily(dates, build_data(raw, frozenset({'MU', 'SNDK'})), price_full)

    eligible = dates[:-MIN_DAYS]
    starts = [random.Random(s).choice(eligible) for s in range(N_SEEDS)]

    print('주식/ETF 비율 sweep (헤드라인 수익 무시, 위험조정 기준)')
    print(f'{"주식%":>5}{"전체수익":>9}{"MDD":>8}{"Sharpe":>8}{"Calmar":>8}{"랜덤Sh":>8}{"제외Sh":>8}{"제외Cal":>8}')
    print('-' * 70)
    best = []
    for sw in [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0]:
        bd = sw * stock_full + (1 - sw) * etf_s
        tot, mdd, sh, cal = m(bd)
        # 랜덤시작 평균 Sharpe
        rsh = []
        for sd in starts:
            i = dates.index(sd)
            rsh.append(m(sw * stock_full.iloc[i:] + (1 - sw) * etf_s.iloc[i:])[2])
        rsh = np.mean(rsh)
        # 둘다제외
        bd_nw = sw * stock_nowin + (1 - sw) * etf_s
        _, _, nsh, ncal = m(bd_nw)
        mark = ''
        print(f'{int(sw*100):>5}{tot:>+8.0f}%{mdd:>+7.1f}%{sh:>8.2f}{cal:>8.2f}{rsh:>8.2f}{nsh:>8.2f}{ncal:>8.2f}{mark}')
        best.append((sw, sh, cal, rsh, nsh, ncal))

    # 최적 추천
    by_sharpe = max(best, key=lambda x: x[1])
    by_rand = max(best, key=lambda x: x[3])
    by_nowin = max(best, key=lambda x: x[4])
    print(f'\n전체 Sharpe 최고: 주식 {int(by_sharpe[0]*100)}% (Sh {by_sharpe[1]:.2f})')
    print(f'랜덤시작 Sharpe 최고: 주식 {int(by_rand[0]*100)}% (Sh {by_rand[3]:.2f})')
    print(f'슈퍼위너제외 Sharpe 최고: 주식 {int(by_nowin[0]*100)}% (Sh {by_nowin[4]:.2f}) ← 로테이션 대비')
    print('\n해석: 세 기준이 수렴하는 비율이 robust 최적. 로테이션 믿으면 제외Sh 가중.')


if __name__ == '__main__':
    main()
