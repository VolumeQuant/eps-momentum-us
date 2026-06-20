# -*- coding: utf-8 -*-
"""Tier-1/2 정제 후보 (멀티에이전트 리서치 종합 기반). True = 위험 ON.

리서치 출처별 핵심:
  - 수익률곡선 dis-inversion(재가팔라짐): 침체는 역전 해소 시점에 시작 → 슬로우탑 선행. yfinance ^TNX/^IRX.
  - 방어/경기 섹터 로테이션: 방어주 상대강도 상승이 천장 1~3개월 선행. XL* 1998+ 전체커버.
  - 섹터 브레드스: 11개 SPDR 중 자기 200DMA 위 비율 하락 = 협소화(슬로우탑 선행).
  - 추세 앙상블 투표(Hurst-Ooi-Pedersen/Newfound): 단일임계 과적합 회피, 휩쏘↓.
  - MA200 기울기: 가격이 15일 하회 확정 전에 평균이 먼저 꺾임.
  - SKEW-VIX 안주 다이버전스: 꼬리헤지 매수(SKEW)인데 VIX 낮음 = 천장 2~4개월 선행(1990+ 전체).
"""
import numpy as np
import pandas as pd
from pathlib import Path
import harness as H

IDX = H.IDX
D = H.D
spx = D['spx']
_P = Path(__file__).resolve().parent


def _s(x):
    return x.reindex(IDX).ffill()


def ma(x, n):
    return x.rolling(n).mean()


_SEC = pd.read_parquet(_P / 'sectors.parquet')
_SEC.index = pd.to_datetime(_SEC.index)
SEC = {c: _s(_SEC[c]) for c in _SEC.columns}


# ===== 1) 수익률곡선 dis-inversion =====
def term_spread():
    return _s(D['tnx']) - _s(D['irx'])


def ycurve_disinversion(margin=0.25, arm_win=378, trough_win=250):
    """역전(<0) 이력이 arm_win일 내 있고(ARMED), 250일 저점 대비 +margin%p 재가팔라짐 = 위험."""
    ts = term_spread()
    armed = (ts < 0).rolling(arm_win).max().fillna(0) > 0
    trough = ts.rolling(trough_win).min()
    resteepen = (ts - trough) >= margin
    return (armed & resteepen & (ts < 0.5)).reindex(IDX).fillna(False)


# ===== 2) 방어/경기 섹터 로테이션 =====
def defensive_cyclical(thr=0.05, win=63):
    """방어바스켓(XLU·XLP·XLV) / 경기바스켓(XLY·XLK·XLI) 비율의 win일 ROC > thr = 방어선호 가속."""
    defb = (SEC['XLU'] * SEC['XLP'] * SEC['XLV']) ** (1 / 3)
    cyc = (SEC['XLY'] * SEC['XLK'] * SEC['XLI']) ** (1 / 3)
    r = defb / cyc
    return (r / r.shift(win) - 1 > thr).reindex(IDX).fillna(False)


# ===== 3) 섹터 브레드스 카운트 =====
def sector_breadth_frac():
    """가용 섹터 중 자기 200DMA 위 비율 (0~1)."""
    avail = None
    above = None
    for c, s in SEC.items():
        a = (s > ma(s, 200))
        ok = s.notna() & ma(s, 200).notna()
        above = a.astype(float).where(ok, 0) if above is None else above + a.astype(float).where(ok, 0)
        avail = ok.astype(float) if avail is None else avail + ok.astype(float)
    return (above / avail.replace(0, np.nan)).reindex(IDX).ffill()


def sector_breadth_weak(thr=0.45):
    """섹터 브레드스 비율 < thr = 참여 협소(위험)."""
    return (sector_breadth_frac() < thr).reindex(IDX).fillna(False)


# ===== 4) 추세 앙상블 투표 =====
def _trend_votes():
    votes = []
    for n in (100, 150, 200, 250):
        votes.append((spx < ma(spx, n)).astype(int))
    for n in (126, 189, 252):
        votes.append((spx / spx.shift(n) - 1 < 0).astype(int))
    return sum(votes)  # 0~7


def trend_ensemble(K=5):
    """7개 추세서브신호(가격<MA100/150/200/250 + 6/9/12개월 절대모멘텀<0) 중 K개 이상 위험."""
    return (_trend_votes() >= K).reindex(IDX).fillna(False)


# ===== 5) MA200 기울기 =====
def ma200_slope_down():
    m = ma(spx, 200)
    return (m < m.shift(21)).reindex(IDX).fillna(False)


# ===== 6) SKEW-VIX 안주 다이버전스 =====
def skew_vix_diverge(skew_thr=145, vix_thr=16):
    sk = _s(D['skew']); vx = D['vix']
    return ((sk >= skew_thr) & (vx <= vix_thr) & (sk > sk.shift(20))).reindex(IDX).fillna(False)


# ===== 7) Growth-Trend-Timing: 추세 AND 매크로(곡선) =====
def gtt_macro_armed(arm_win=504):
    """수익률곡선이 arm_win일 내 역전 이력 = 매크로 경고. 추세exit를 이때만 신뢰(휩쏘 억제)."""
    ts = term_spread()
    return ((ts < 0).rolling(arm_win).max().fillna(0) > 0).reindex(IDX).fillna(False)


OVERLAY = {  # base 게이트에 OR(조기진입)
    'ycurve_disinv': lambda: ycurve_disinversion(0.25, 378),
    'defensive_cyclical': lambda: defensive_cyclical(0.05, 63),
    'sector_breadth_weak': lambda: sector_breadth_weak(0.45),
    'ma200_slope_down': ma200_slope_down,
    'skew_vix_diverge': lambda: skew_vix_diverge(145, 16),
}
