"""레버리지 ETF (SOXL 3x반도체 / TQQQ 3x나스닥) — 위험 측정

질문: 동적매칭이 SOXX/XLK였는데, 3배 레버리지로 갔다면? 너무 위험한가?
 A) 우리 71일 강세장 window: SOXL/TQQQ/동적3x buy&hold
 B) standalone 역사적 최악낙폭 (2010~, 2022·COVID) — 레버리지 진짜 위험
 C) regime 오버레이(SPX<MA200 15d OR VIX>36 2d)로 감싸면 살 만한지 (boost=3x, defense=현금)
주의: 레버리지 ETF는 일간리밸 변동성 감쇠 → 실제 ETF 가격(감쇠 포함)으로 측정.
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

pd.set_option('future.no_silent_downcasting', True)
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'research'))
from regime_eda_market import fetch_close, confirm  # noqa
import sqlite3


def stats(nav):
    yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1 if yrs > 0.3 else (nav.iloc[-1] - 1)
    mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    return (nav.iloc[-1] - 1) * 100, cagr * 100, mdd * 100, (cagr / abs(mdd) if mdd < 0 else float('nan'))


def main():
    spx = fetch_close('^GSPC')
    vix = fetch_close('^VIX').reindex(spx.index).ffill()
    px = {s: fetch_close(s) for s in ('SOXL', 'TQQQ', 'SOXX', 'QQQ')}
    for s, p in px.items():
        print(f'  {s}: {p.index[0].date()}~{p.index[-1].date()} ({len(p)})')

    con = sqlite3.connect(ROOT / 'eps_momentum_data.db')
    dates = [r[0] for r in con.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
    w0, w1 = dates[0], dates[-1]

    # A) 우리 window buy&hold
    print(f'\n=== A) 우리 강세장 window {w0}~{w1} (buy&hold) ===')
    print(f'{"":<14}{"총수익":>9}{"MDD":>8}')
    for s in ('SOXL', 'TQQQ', 'SOXX', 'QQQ'):
        seg = px[s].loc[w0:w1]
        nav = seg / seg.iloc[0]
        t, _, m, _ = stats(nav)
        print(f'{s:<14}{t:>+8.1f}%{m:>+7.1f}%')
    print('  (참고: 2종목 +223.6% / 동적매칭 1x ETF +70.1% / Top-20 +53.9%)')

    # B) standalone 역사적 (각 ETF 전체 기간)
    print('\n=== B) standalone 역사 전체 (레버리지 진짜 위험) ===')
    print(f'{"":<14}{"기간":>12}{"누적":>11}{"CAGR":>8}{"최악MDD":>9}{"Cal":>6}')
    for s in ('SOXL', 'TQQQ', 'SOXX', 'QQQ'):
        nav = px[s] / px[s].iloc[0]
        tot, cagr, mdd, cal = stats(nav)
        print(f'{s:<14}{str(px[s].index[0].date()):>12}{tot:>+10.0f}%{cagr:>+7.1f}%{mdd:>+8.1f}%{cal:>6.2f}')

    # 특정 약세장 낙폭
    print('\n  특정 약세장 최대낙폭 (peak→trough):')
    for s in ('SOXL', 'TQQQ'):
        for name, (a, b) in [('2022', ('2022-01-01', '2022-12-31')), ('COVID', ('2020-02-01', '2020-04-30'))]:
            seg = px[s].loc[a:b]
            if len(seg) > 1:
                dd = ((seg - seg.cummax()) / seg.cummax()).min() * 100
                print(f'    {s} {name}: {dd:+.0f}%')

    # C) regime 오버레이 (boost=3x, defense=현금), 3x 전체 기간
    print('\n=== C) regime 오버레이 (boost=ETF, defense=현금) 2010~ ===')
    ma = spx.rolling(200).mean()
    reg = (confirm((spx < ma).fillna(False), 15) | confirm((vix > 36).fillna(False), 2))
    print(f'{"":<22}{"누적":>11}{"CAGR":>8}{"MDD":>9}{"Cal":>6}')
    for s in ('TQQQ', 'SOXL', 'QQQ'):
        p = px[s]
        r = reg.reindex(p.index).ffill().fillna(False).astype(bool)
        pos = (~r).shift(1, fill_value=False)
        strat = np.where(pos.values, p.pct_change().fillna(0).values, 0.0)
        nav = (1 + pd.Series(strat, index=p.index)).cumprod()
        tot, cagr, mdd, cal = stats(nav)
        # buy&hold 대비
        bh = p / p.iloc[0]
        _, _, bh_mdd, _ = stats(bh)
        print(f'{(s+" 오버레이"):<22}{tot:>+10.0f}%{cagr:>+7.1f}%{mdd:>+8.1f}%{cal:>6.2f}  (buy&hold MDD {bh_mdd:+.0f}%)')

    print('\n해석: A) 강세장선 3x 폭발적. B) standalone은 2022/COVID에 -80~95% = 파멸적.')
    print('C) 오버레이가 MDD를 얼마나 줄이나 — 그래도 3x는 lag/감쇠로 위험 잔존 여부.')


if __name__ == '__main__':
    main()
