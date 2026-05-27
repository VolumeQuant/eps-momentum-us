"""Regime defense 자산 — 인버스 ETF 추가 측정

사용자 질문: defense에 현금/채권 대신 지수 인버스 ETF 사면?
신호 = SPX<MA200(10d) OR VIX>36(2d) (Phase 3 확정). proxy=QQQ.
인버스는 inception 2006+ → 공통구간 2006~ (dotcom 제외, GFC·COVID·2022 포함).

비교: 현금 / IEF / BIL / SH(-1x SPX) / PSQ(-1x QQQ) / SDS(-2x SPX)
인버스 caveat: 일간 리밸 감쇠 + 오발 시 실손 → 실제 ETF 가격(감쇠 포함)으로 측정.
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

pd.set_option('future.no_silent_downcasting', True)
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'research'))
from regime_eda_market import fetch_close, confirm, KNOWN_BEARS  # noqa


def main():
    spx = fetch_close('^GSPC')
    qqq = fetch_close('QQQ').reindex(spx.index).ffill()
    vix = fetch_close('^VIX').reindex(spx.index).ffill()
    ma200 = spx.rolling(200).mean()
    reg = (confirm((spx < ma200).fillna(False), 10) | confirm((vix > 36).fillna(False), 2))

    assets = {'현금': None}
    for tk in ('IEF', 'BIL', 'SH', 'PSQ', 'SDS'):
        px = fetch_close(tk)
        if px is not None:
            assets[tk] = px

    # 공통 시작 (인버스 inception에 맞춤)
    common_start = max(p.index[0] for p in assets.values() if p is not None)
    print(f'공통 분석 시작: {common_start.date()} (인버스 ETF inception 제약)')
    idx = spx.loc[common_start:].index
    qq = qqq.reindex(idx).ffill()
    rg = reg.reindex(idx).ffill().fillna(False).astype(bool)

    def stats(nav):
        yrs = (nav.index[-1] - nav.index[0]).days / 365.25
        cagr = nav.iloc[-1] ** (1 / yrs) - 1
        mdd = ((nav - nav.cummax()) / nav.cummax()).min()
        return cagr * 100, mdd * 100, (cagr / abs(mdd) if mdd < 0 else float('nan')), nav.iloc[-1]

    def overlay(dpx):
        pos = (~rg).shift(1, fill_value=False)
        pret = qq.pct_change().fillna(0)
        if dpx is None:
            strat = np.where(pos.values, pret.values, 0.0)
        else:
            dret = dpx.reindex(idx).ffill().pct_change().fillna(0)
            strat = np.where(pos.values, pret.values, dret.values)
        return (1 + pd.Series(strat, index=idx)).cumprod()

    print('\n' + '=' * 92)
    print(f'defense 자산별 오버레이 ({common_start.date()}~, proxy=QQQ, 신호 MA200|VIX>36)')
    print('=' * 92)
    print(f'{"defense자산":<10} {"CAGR":>7} {"MDD":>8} {"Cal":>6} {"NAV":>7}')
    print('-' * 44)
    bh = (1 + qq.pct_change().fillna(0)).cumprod()
    c, m, cl, nv = stats(bh)
    print(f'{"QQQ보유":<10} {c:>+6.1f}% {m:>+7.1f}% {cl:>5.2f} x{nv:>5.1f}  (오버레이 없음)')
    for name, dpx in assets.items():
        c, m, cl, nv = stats(overlay(dpx))
        print(f'{name:<10} {c:>+6.1f}% {m:>+7.1f}% {cl:>5.2f} x{nv:>5.1f}')

    # 약세장 vs 비-약세-defense 구간서 인버스 수익
    print('\n' + '=' * 92)
    print('약세장 구간 자산 총수익 (인버스가 진짜 폭락서 버는지)')
    print(f'{"자산":<6} ' + ' '.join(f'{k.split()[0][:7]:>8}' for k in KNOWN_BEARS if k != 'dotcom 2000-02'))
    print('-' * 40)
    for name, dpx in assets.items():
        if dpx is None:
            continue
        cells = []
        for k, (s, e) in KNOWN_BEARS.items():
            if k == 'dotcom 2000-02':
                continue
            seg = dpx.loc[s:e]
            cells.append(f'{((seg.iloc[-1]/seg.iloc[0]-1)*100) if len(seg)>1 else float("nan"):>6.1f}%')
        print(f'{name:<6} ' + ' '.join(f'{c:>8}' for c in cells))

    # defense 구간 중 "가짜"(QQQ가 오른 defense 날) 비율 → 인버스 손실 위험
    pos = (~rg).shift(1, fill_value=False)
    defense_days = ~pos
    qret = qq.pct_change()
    false_def = (defense_days & (qret > 0)).sum()
    true_def = (defense_days & (qret < 0)).sum()
    print(f'\ndefense 일수: {int(defense_days.sum())} | QQQ 오른 날(인버스 손실) {int(false_def)} '
          f'/ 내린 날(인버스 이익) {int(true_def)} = {false_def/(false_def+true_def)*100:.0f}% 손실일')
    print('→ defense 기간의 절반 가까이 QQQ가 오르는 날 = 인버스는 그만큼 실손 + 감쇠.')


if __name__ == '__main__':
    main()
