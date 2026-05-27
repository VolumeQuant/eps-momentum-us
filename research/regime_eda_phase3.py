"""Regime EDA Phase 3 — MA200에 빠른 보조 트리거 결합 (V자 급락 보완)

Phase 1/2: SPX<MA200(10~15d)이 느린 약세장(dotcom/GFC/2022) 잡지만 COVID(V자)는 놓침.
→ 빠른 트리거(VIX 스파이크 / HY 급등)를 OR 결합해서:
  - COVID 포착 올리되
  - 강세장 휘프소(가짜 defense)·Cal 손해 최소
관건: VIX 임계값이 너무 낮으면 강세장 조정(2018·2024 등)에 오발 → 임계 탐색.
defense=현금 고정 (신호 비교 목적, 26년 dotcom 포함 동일 조건).
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


def overlay(proxy, regime_def):
    px = proxy.copy()
    reg = regime_def.reindex(px.index).ffill().fillna(False).astype(bool)
    pos = (~reg).shift(1, fill_value=False)
    pret = px.pct_change().fillna(0)
    strat = np.where(pos.values, pret.values, 0.0)
    nav = (1 + pd.Series(strat, index=px.index)).cumprod()
    yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1
    peak = nav.cummax()
    mdd = ((nav - peak) / peak).min()
    return cagr * 100, mdd * 100, (cagr / abs(mdd) if mdd < 0 else float('nan')), nav.iloc[-1]


def cover(reg):
    return {k: reg.loc[s:e].mean() * 100 if len(reg.loc[s:e]) else float('nan')
            for k, (s, e) in KNOWN_BEARS.items()}


def main():
    spx = fetch_close('^GSPC')
    qqq = fetch_close('QQQ').reindex(spx.index).ffill()
    vix = fetch_close('^VIX').reindex(spx.index).ffill()
    ma200 = spx.rolling(200).mean()

    ma_def = confirm((spx < ma200).fillna(False), 10)  # 기본 신호

    # HY 급등 트리거
    HY = ROOT / 'data_cache' / 'hy_spread.parquet'
    hy = None
    if HY.exists():
        h = pd.read_parquet(HY)
        h.index = pd.to_datetime(h.index).tz_localize(None)
        hy = h['hy_spread'].reindex(spx.index).ffill()

    variants = [('MA200(10d) 단독', ma_def)]
    # VIX 스파이크 OR 결합 (raw VIX, 2일 확인)
    for thr in (28, 32, 36, 40):
        vix_def = confirm((vix > thr).fillna(False), 2)
        variants.append((f'MA200 | VIX>{thr}(2d)', (ma_def | vix_def)))
    # HY 1개월 급등 OR 결합
    if hy is not None:
        hy_jump = (hy - hy.shift(21)) > 1.0  # 1개월 +1%p 이상 급등
        hyj_def = confirm(hy_jump.fillna(False), 2)
        variants.append(('MA200 | HY+1%/1m(2d)', (ma_def | hyj_def)))
    # 비대칭: VIX 급등은 즉시(1d) defense, 해제는 MA200 회복까지 (fast out)
    vix_fast = confirm((vix > 32).fillna(False), 1)
    asym = (ma_def | vix_fast)
    variants.append(('MA200 | VIX>32(1d) fastout', asym))

    print('=' * 116)
    print('신호 변형 — defense% / 전환 / 약세장포착 / QQQ 현금오버레이 (defense=현금, 2000~)')
    print('=' * 116)
    base_cagr, base_mdd, base_cal, base_nav = overlay(qqq, ma_def)
    print(f'{"변형":<26} {"%def":>6} {"전환":>4}  | {"dotcom":>6} {"GFC":>6} {"COVID":>6} {"2022":>6}  | {"CAGR":>7} {"MDD":>7} {"Cal":>5} {"NAV":>6}')
    print('-' * 116)
    for name, reg in variants:
        c = cover(reg)
        cagr, mdd, cal, nav = overlay(qqq, reg)
        print(f'{name:<26} {reg.mean()*100:>5.1f}% {int((reg!=reg.shift(1)).sum()):>4}  | '
              f'{c["dotcom 2000-02"]:>5.0f}% {c["GFC 2008"]:>5.0f}% {c["COVID 2020"]:>5.0f}% {c["rate 2022"]:>5.0f}%  | '
              f'{cagr:>+6.1f}% {mdd:>+6.1f}% {cal:>5.2f} x{nav:>4.1f}')

    print('\n해석: COVID 포착 ↑ + MDD ↓ + Cal 유지/개선 + 전환수 증가 작음 = 좋은 결합.')
    print('VIX 임계 너무 낮으면(28) 강세장 오발로 전환↑·Cal↓. 높으면(40) COVID만 살짝.')


if __name__ == '__main__':
    main()
