"""Regime EDA Phase 2 — SPX<MA200 정밀화 + defense 자산(현금 vs 채권ETF)

Phase 1 결론: SPX<MA200이 최강 신호. 여기서:
  A) MA 기간(150/200/250) × 확인일(3/5/8/10/15) 그리드 → robust 선택
  B) 신호 고정 후 defense 자산 비교: 현금 / IEF / AGG / TLT / BIL
     - 채권ETF는 inception 제약 → 공통 구간에서 현금과 apples-to-apples
     - 2008/2020/2022 각 약세장 구간 자산별 수익도 따로
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'research'))
from regime_eda_market import fetch_close, confirm, KNOWN_BEARS  # noqa


def overlay_nav(proxy, regime_def, defense_px=None):
    """boost=proxy, defense=현금(0) or defense_px 수익. 신호 1일 지연. 공통구간만."""
    idx = proxy.index
    if defense_px is not None:
        idx = idx.intersection(defense_px.index)
    px = proxy.reindex(idx).ffill().dropna()
    reg = regime_def.reindex(px.index).ffill().fillna(False).astype(bool)
    pos = (~reg).shift(1).fillna(False).astype(bool)
    pret = px.pct_change().fillna(0)
    if defense_px is not None:
        dret = defense_px.reindex(px.index).ffill().pct_change().fillna(0)
        strat = np.where(pos.values, pret.values, dret.values)
    else:
        strat = np.where(pos.values, pret.values, 0.0)
    nav = (1 + pd.Series(strat, index=px.index)).cumprod()
    return nav


def stats(nav):
    yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1
    peak = nav.cummax()
    mdd = ((nav - peak) / peak).min()
    return cagr * 100, mdd * 100, (cagr / abs(mdd) if mdd < 0 else float('nan')), nav.iloc[-1]


def main():
    spx = fetch_close('^GSPC')
    qqq = fetch_close('QQQ')

    # === A) MA × confirm 그리드 (QQQ 현금 오버레이) ===
    print('=' * 100)
    print('A) SPX<MA 그리드 — MA기간 × 확인일 (QQQ 현금 defense)')
    print(f'{"MA":>4} {"conf":>5} {"%def":>6} {"flips":>5} {"CAGR":>7} {"MDD":>7} {"Cal":>5} {"NAV":>6}')
    print('-' * 60)
    best = None
    for ma_p in (150, 200, 250):
        ma = spx.rolling(ma_p).mean()
        raw = (spx < ma)
        for cf in (3, 5, 8, 10, 15):
            reg = confirm(raw.fillna(False), cf)
            nav = overlay_nav(qqq, reg)
            cagr, mdd, cal, navend = stats(nav)
            fl = int((reg != reg.shift(1)).sum())
            mark = ''
            if best is None or cal > best[0]:
                best = (cal, ma_p, cf)
            if ma_p == 200 and cf == 10:
                mark = ' ←phase1'
            print(f'{ma_p:>4} {cf:>5} {reg.mean()*100:>5.1f}% {fl:>5} {cagr:>+6.1f}% {mdd:>+6.1f}% {cal:>5.2f} x{navend:>4.1f}{mark}')
    print(f'\n최고 Calmar: MA{best[1]} × {best[2]}일확인 (Cal {best[0]:.2f})')

    # === B) defense 자산 비교 (신호 = SPX<MA200 10일) ===
    ma200 = spx.rolling(200).mean()
    reg = confirm((spx < ma200).fillna(False), 10)
    print('\n' + '=' * 100)
    print('B) defense 자산 비교 — 신호 SPX<MA200(10일), proxy=QQQ. 각 자산 공통구간')
    print(f'{"defense자산":<14} {"구간시작":>10} {"CAGR":>7} {"MDD":>7} {"Cal":>5} {"NAV":>6}')
    print('-' * 64)
    assets = {'현금': None}
    for tk in ('BIL', 'IEF', 'AGG', 'TLT'):
        px = fetch_close(tk)
        if px is not None:
            assets[f'{tk}'] = px
    for name, dpx in assets.items():
        nav = overlay_nav(qqq, reg, dpx)
        cagr, mdd, cal, navend = stats(nav)
        start = nav.index[0].date()
        print(f'{name:<14} {str(start):>10} {cagr:>+6.1f}% {mdd:>+6.1f}% {cal:>5.2f} x{navend:>4.1f}')

    # === C) 약세장별 자산 수익 (defense 기간 동안) ===
    print('\n' + '=' * 100)
    print('C) 각 약세장 구간 자산 총수익 (% — defense 자산이 현금 0% 대비 나은지)')
    print(f'{"자산":<8} ' + ' '.join(f'{k.split()[0][:8]:>9}' for k in KNOWN_BEARS))
    print('-' * 56)
    for name, dpx in assets.items():
        if dpx is None:
            row = ' '.join(f'{"0.0":>8}%' for _ in KNOWN_BEARS)
            print(f'{name:<8} {row}')
            continue
        cells = []
        for k, (s, e) in KNOWN_BEARS.items():
            seg = dpx.loc[s:e]
            cells.append(f'{((seg.iloc[-1]/seg.iloc[0]-1)*100) if len(seg)>1 else float("nan"):>7.1f}%')
        print(f'{name:<8} ' + ' '.join(f'{c:>9}' for c in cells))

    print('\n해석: A) MDD↓+Cal↑+flips 적은 plateau 중앙 선택. '
          'B) 채권이 현금보다 Cal 높으면 채권 우위(단 2022 주의). '
          'C) 2022 금리상승장에선 장기채(TLT)가 마이너스 — 현금/단기채(BIL) 안전.')


if __name__ == '__main__':
    main()
