"""개별종목(집중) vs 상위리스트 바스켓 vs 관련 ETF — 성과 비교

질문: 개별 2종목(80/20) 대신 Top-N 리스트 바스켓이나 관련 ETF를 사면?
같은 기간(DB window 2026-02~05) 총수익 + MDD 비교.
 - 현 전략: 2종목 80/20 (part2_rank, exit>10)
 - 바스켓: Top-5/10/20 동일비중 매일 리밸런스
 - 관련 ETF: QQQ(나스닥100) / SMH(반도체) / XLK(기술) / SPY (buy&hold)
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

pd.set_option('future.no_silent_downcasting', True)
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'research'))
from bt_c1_and_weights_robust import load_raw, build_data  # noqa
from bt_weights_decision import simulate_mdd  # noqa
from regime_eda_market import fetch_close  # noqa


def nav_stats(nav):
    tot = (nav.iloc[-1] - 1) * 100
    mdd = ((nav - nav.cummax()) / nav.cummax()).min() * 100
    return tot, mdd


def basket_nav(dates, data, price_full, topn):
    """매일 base_rank ≤ topn 종목 동일비중 보유, 일별 리밸런스."""
    rets = []
    for i in range(1, len(dates)):
        d, pd_ = dates[i], dates[i - 1]
        held = [tk for tk, info in data.get(pd_, {}).items() if info['base_rank'] <= topn]
        day = []
        for tk in held:
            cp = data.get(d, {}).get(tk, {}).get('price') or price_full.get(d, {}).get(tk)
            pp = data.get(pd_, {}).get(tk, {}).get('price') or price_full.get(pd_, {}).get(tk)
            if cp and pp and pp > 0:
                day.append((cp - pp) / pp)
        rets.append(np.mean(day) if day else 0.0)
    return (1 + pd.Series(rets)).cumprod()


def main():
    dates, raw, price_full = load_raw()
    data = build_data(raw)
    w0, w1 = dates[0], dates[-1]
    print(f'window: {w0} ~ {w1} ({len(dates)} 거래일)\n')

    rows = []
    # 현 전략 2종목 80/20 (full window)
    ret, mdd = simulate_mdd(dates, data, price_full, [80, 20], start_date=dates[0])
    rows.append(('전략 2종목 80/20 (현행)', ret, mdd))

    # 바스켓
    for n in (5, 10, 20):
        nav = basket_nav(dates, data, price_full, n)
        t, m = nav_stats(nav)
        rows.append((f'Top-{n} 동일비중 바스켓', t, m))

    # 관련 ETF (buy&hold, 같은 window)
    for tk, label in [('QQQ', 'QQQ 나스닥100'), ('SMH', 'SMH 반도체'),
                      ('XLK', 'XLK 기술'), ('SPY', 'SPY S&P500')]:
        px = fetch_close(tk)
        seg = px.loc[w0:w1]
        if len(seg) > 1:
            nav = seg / seg.iloc[0]
            t, m = nav_stats(nav)
            rows.append((f'{label} (보유)', t, m))

    print(f'{"방식":<26} {"총수익":>9} {"MDD":>9}')
    print('-' * 48)
    for name, t, m in rows:
        print(f'{name:<26} {t:>+8.1f}% {m:>+8.1f}%')

    print('\n해석: 강세장 단일 window라 집중(2종목)이 압도 예상. '
          '바스켓/ETF는 수익 낮지만 MDD(변동성)도 작을 것 — 분산 트레이드오프.')
    print('단 약세장 보호는 regime 오버레이가 별도 담당 → 평시 집중의 알파가 핵심.')


if __name__ == '__main__':
    main()
