# -*- coding: utf-8 -*-
"""조기경보 후보 raw 신호들 (True = 위험 ON). harness.D 컬럼에서 파생.

설계: 각 신호는 IDX 정렬 bool Series. validate.combined()로 base 게이트에 OR(조기진입).
검증 배터리(validate.compare)가 휩쏘/과적합/lateness를 정직하게 드러낸다.
"""
import numpy as np
import pandas as pd
import harness as H

IDX = H.IDX
D = H.D
spx = D['spx']


def _s(series):
    return series.reindex(IDX).ffill()


def ma(series, n):
    return series.rolling(n).mean()


# --- 1) 수익률곡선(장단기 금리역전) ---
def term_spread():
    return (_s(D['tnx']) - _s(D['irx']))  # 10y - 3m, % 단위


def ycurve_inverted():
    """10y-3m < 0 (역전). 선행: 침체 6~18개월 선행이나 lead 가변."""
    return term_spread() < 0


def ycurve_inv_trendbreak():
    """역전 AND 가격<MA50 — 역전상태에서 추세 균열 시 빠른 디리스크."""
    return (term_spread() < 0) & (spx < ma(spx, 50))


def ycurve_resteepen():
    """역전 후 재가팔라짐(bull steepener) — 침체 임박 고전 신호. 역전했었고 지금 spread 상승중."""
    ts = term_spread()
    was_inv = (ts < 0).rolling(252).max() > 0  # 최근 1년 내 역전 있었음
    return was_inv & (ts > ts.shift(20)) & (ts < 0.5)


# --- 2) 브레드스 ---
def breadth_rsp_weak():
    """등가가중/시총가중(RSP/SPY) < 60일MA — 시장 협소화(소수 대형주 의존). 2003+."""
    r = _s(D['rsp']) / _s(D['spy'])
    return r < ma(r, 60)


def breadth_divergence():
    """지수는 60일 신고가인데 RSP/SPY는 아님 = 브레드스 다이버전스(고전 천장신호). 2003+."""
    hi = spx >= spx.rolling(60).max() * 0.999
    r = _s(D['rsp']) / _s(D['spy'])
    r_not_hi = r < r.rolling(60).max() * 0.985
    return hi & r_not_hi


# --- 3) 방어 섹터 로테이션 (XL* 1998+, 전체 커버) ---
def defensive_rotation():
    """필수소비/임의소비(XLP/XLY) 비율이 20일 전보다 상승 + 60일MA 위 = 방어로테이션(리스크오프 선행)."""
    r = _s(D['xlp']) / _s(D['xly'])
    return (r > ma(r, 60)) & (r > r.shift(20))


def utility_rotation():
    """유틸/임의소비(XLU/XLY) 상승추세 = 방어선호."""
    r = _s(D['xlu']) / _s(D['xly'])
    return (r > ma(r, 60)) & (r > r.shift(20))


# --- 4) 크로스에셋 ---
def coppergold_weak():
    """구리/금 < 60일MA = 글로벌 성장공포(리스크오프 선행). 2000+."""
    r = _s(D['copper']) / _s(D['gold'])
    return r < ma(r, 60)


def dxy_surge():
    """달러 20일 급등(+3%+) = 글로벌 유동성 타이트닝/리스크오프."""
    dx = _s(D['dxy'])
    return dx / dx.shift(20) - 1 > 0.03


def hyg_lqd_weak():
    """하이일드/투자등급(HYG/LQD) < 40일MA = 신용 리스크오프. 2007+."""
    r = _s(D['hyg']) / _s(D['lqd'])
    return r < ma(r, 40)


# --- 5) 변동성 구조 ---
def skew_high():
    """CBOE SKEW > 145 = 옵션시장 꼬리위험 가격화. 1990+ (선행성 논란)."""
    return _s(D['skew']) > 145


def vvix_high():
    """VVIX(vol-of-vol) > 110 = 변동성 자체의 불안정. 2006+."""
    vv = D.get('vvix')
    if vv is None:
        return pd.Series(False, index=IDX)
    return _s(vv) > 110


# --- 6) 절대(시계열) 모멘텀 — dual momentum 방어스위치 ---
def abs_momentum_neg():
    """SPX 12개월(252일) 수익률 < 0 = 절대모멘텀 음전환(Antonacci 방어스위치)."""
    return spx / spx.shift(252) - 1 < 0


def abs_momentum_6m():
    """SPX 6개월(126일) 수익률 < 0."""
    return spx / spx.shift(126) - 1 < 0


# --- 7) 초기레그-갭 캐처 (진단상 MDD의 원천: 느린게이트 발화 전 고점→급락) ---
def dd_from_high(thr=0.10, win=60):
    """SPX가 최근 win일 고점 대비 -thr 이상 하락 = 드로다운 서킷. KR선 휩쏘로 기각됐으나
    US 초기레그-갭 진단상 직접 타깃. 확인일로 휩쏘 통제 시도."""
    hi = spx.rolling(win).max()
    return spx / hi - 1 < -thr


def ma50_break():
    """가격 < MA50 — MA200(15일)보다 빠른 추세균열. 휩쏘 多 예상."""
    return spx < ma(spx, 50)


def vix_roc(thr=0.50, win=5):
    """VIX가 win일 만에 +thr(50%) 급등 — 36 미만이어도 공포 가속 포착(초기레그)."""
    vx = D['vix']
    return vx / vx.shift(win) - 1 > thr


def vix_pctile(thr=0.90, win=252):
    """VIX가 1년 분포 상위 thr분위 초과 — 절대36 대신 상대적 고변동."""
    vx = D['vix']
    rank = vx.rolling(win).apply(lambda x: (x[-1] >= x).mean(), raw=True)
    return rank > thr


def vvix_spike(thr=0.30, win=5):
    """VVIX(vol-of-vol)가 win일 +thr 급등 = 변동성 자체 불안정 가속(VIX 선행 주장). 2006+."""
    vv = D.get('vvix')
    if vv is None:
        return pd.Series(False, index=IDX)
    vv = _s(vv)
    return vv / vv.shift(win) - 1 > thr


REGISTRY = {
    'dd_from_high_10': lambda: dd_from_high(0.10, 60),
    'dd_from_high_12': lambda: dd_from_high(0.12, 60),
    'ma50_break': ma50_break,
    'vix_roc_50_5': lambda: vix_roc(0.50, 5),
    'vix_pctile_90': lambda: vix_pctile(0.90, 252),
    'vvix_spike': lambda: vvix_spike(0.30, 5),
    'ycurve_inverted': ycurve_inverted,
    'ycurve_inv_trendbreak': ycurve_inv_trendbreak,
    'ycurve_resteepen': ycurve_resteepen,
    'breadth_rsp_weak': breadth_rsp_weak,
    'breadth_divergence': breadth_divergence,
    'defensive_rotation': defensive_rotation,
    'utility_rotation': utility_rotation,
    'coppergold_weak': coppergold_weak,
    'dxy_surge': dxy_surge,
    'hyg_lqd_weak': hyg_lqd_weak,
    'skew_high': skew_high,
    'vvix_high': vvix_high,
    'abs_momentum_neg': abs_momentum_neg,
    'abs_momentum_6m': abs_momentum_6m,
}
