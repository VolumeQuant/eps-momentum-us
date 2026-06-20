# -*- coding: utf-8 -*-
"""Early-warning 방어신호 연구용 재사용 백테스트 하네스 (오프라인, 캐시 parquet).

production 국면 게이트와 동일 관례:
  - 프록시 = QQQ (성장 틸트, EPS 시스템 대용)
  - 방어자산 = 발행어음 3% 연 (NOTE), 신호 1일 지연
  - confirm 상태기계(히스테리시스): 진입 ne일·퇴출 nx일 연속
  - 약세장 포착 + WF 3블록 + 인접CV + lateness(고점대비 진입시점)

regime_eda_phase10_transition.py 관례를 그대로 계승(검증된 baseline 재현 보장).
"""
import numpy as np
import pandas as pd
from pathlib import Path

_R = Path(__file__).resolve().parent.parent  # research/
NOTE = (1.03) ** (1 / 252) - 1  # 발행어음 3% 일수익


def load():
    """캐시 parquet 로드 → QQQ 인덱스에 정렬된 시리즈 dict 반환."""
    A = pd.read_parquet(_R / 'regime_assets.parquet'); A.index = pd.to_datetime(A.index)
    qqq = A['QQQ'].dropna()
    idx = qqq.index
    s = A['^GSPC'].reindex(idx).ffill()
    v = A['^VIX'].reindex(idx).ffill()
    qr = qqq.pct_change().fillna(0)
    out = {'idx': idx, 'qqq': qqq, 'spx': s, 'vix': v, 'qr': qr}
    # 선택: VIX3M/VVIX (2006~)
    try:
        B = pd.read_parquet(_R / 'regime_vix3m.parquet'); B.index = pd.to_datetime(B.index)
        out['vix3m'] = B['VIX3M'].reindex(idx).ffill()
        out['vvix'] = B['VVIX'].reindex(idx).ffill()
    except Exception:
        pass
    # 선택: 추가 매크로 캐시(있으면) — early_warn/extra.parquet
    try:
        E = pd.read_parquet(Path(__file__).resolve().parent / 'extra.parquet')
        E.index = pd.to_datetime(E.index)
        for c in E.columns:
            out[c] = E[c].reindex(idx).ffill()
    except Exception:
        pass
    return out


D = load()
IDX = D['idx']


def conf(raw, ne, nx):
    """raw(bool Series, 오래된→최신) → defense 상태 Series. 진입 ne일·퇴출 nx일 연속."""
    st = False; sd = sb = 0; o = []
    for d in np.asarray(raw.values, dtype=bool):
        sd = sd + 1 if d else 0
        sb = 0 if d else sb + 1
        if not st and sd >= ne:
            st = True
        elif st and sb >= nx:
            st = False
        o.append(st)
    return pd.Series(o, index=raw.index)


def base_regime(ma_p=200, ec=15, rc=15, vix_thr=36, death=False):
    """production 국면 게이트: SPX<MA200(15/15) OR VIX>36(2/2)."""
    s, v = D['spx'], D['vix']
    if death:
        raw = s.rolling(50).mean() < s.rolling(ma_p).mean()
    else:
        raw = s < s.rolling(ma_p).mean()
    d = conf(raw, ec, rc)
    if vix_thr:
        d = d | conf(v > vix_thr, 2, 2)
    return d.reindex(IDX).fillna(False)


def ev(dfn, a=None, b=None):
    """방어 게이트 dfn(bool Series) → (CAGR%, MDD%, Calmar). 1일 지연, 방어=NOTE."""
    dfn = dfn.reindex(IDX).fillna(False)
    pos = (~dfn).shift(1, fill_value=False)
    r = pd.Series(np.where(pos.values, D['qr'].values, NOTE), index=IDX)
    if a is not None:
        r = r[(r.index >= a) & (r.index < b)]
    if len(r) < 30:
        return 0.0, 0.0, 0.0
    nav = (1 + r).cumprod()
    yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1
    mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    return cagr * 100, mdd * 100, (cagr / abs(mdd) if mdd < 0 else 0.0)


def metrics(dfn, a=None, b=None):
    """풍부한 지표 dict: cagr/mdd/calmar/sharpe/transitions/defense_days/defense_frac."""
    dfn = dfn.reindex(IDX).fillna(False)
    cagr, mdd, cal = ev(dfn, a, b)
    sub = dfn
    if a is not None:
        sub = dfn[(dfn.index >= a) & (dfn.index < b)]
    trans = int((sub.astype(int).diff().abs() == 1).sum())
    ddays = int(sub.sum())
    n = len(sub)
    pos = (~dfn).shift(1, fill_value=False)
    r = pd.Series(np.where(pos.values, D['qr'].values, NOTE), index=IDX)
    if a is not None:
        r = r[(r.index >= a) & (r.index < b)]
    sharpe = (r.mean() / r.std() * np.sqrt(252)) if r.std() > 0 else 0.0
    return dict(cagr=cagr, mdd=mdd, calmar=cal, sharpe=sharpe,
                transitions=trans, defense_days=ddays, defense_frac=(ddays / n if n else 0))


BLK = [('99-08', '1999-01-01', '2008-01-01'),
       ('08-16', '2008-01-01', '2016-01-01'),
       ('16-26', '2016-01-01', '2026-07-01')]


def wf_min(dfn):
    return min(ev(dfn, pd.Timestamp(a), pd.Timestamp(b))[2] for _, a, b in BLK)


def wf_each(dfn):
    return {nm: ev(dfn, pd.Timestamp(a), pd.Timestamp(b))[2] for nm, a, b in BLK}


BEARS = {'2000닷컴': ('2000-03-24', '2002-10-10'),
         '2008GFC': ('2007-10-09', '2009-03-09'),
         '2020COVID': ('2020-02-19', '2020-03-23'),
         '2022': ('2022-01-03', '2022-10-12'),
         '2018Q4': ('2018-10-01', '2018-12-24'),
         '2011유럽': ('2011-04-29', '2011-10-03'),
         '2025-4월(휩쏘)': ('2025-02-19', '2025-04-08')}

# LOWO용 V-crash 마스크(빠른 회복형, 13단계 관례)
VCRASH = [('2020-02-01', '2020-12-31'), ('2025-02-15', '2025-07-31')]


def vmask():
    m = pd.Series(True, index=IDX)
    for a, b in VCRASH:
        m &= ~((IDX >= a) & (IDX <= b))
    return m


def capture(dfn):
    """약세장별 포착 여부(O/X) + 포착 커버리지%."""
    dfn = dfn.reindex(IDX).fillna(False)
    out = {}
    for nm, (a, b) in BEARS.items():
        seg = IDX[(IDX >= a) & (IDX < b)]
        if len(seg) == 0:
            out[nm] = ('·', 0.0); continue
        cov = dfn.reindex(seg).fillna(False).mean() * 100
        out[nm] = ('O' if cov > 0 else 'X', cov)
    return out


def lateness(dfn, lookback=252):
    """각 방어 진입 시점이 '직전 고점 대비 몇 % 빠진 뒤'인지 + 진입 후 추가하락 회피분.

    Returns: DataFrame[entry_date, peak_date, dd_at_entry%, trough_after%, avoided%]
    peak = 진입 직전 lookback일 내 종가 최고. dd_at_entry = price/peak-1.
    trough_after = 방어구간(연속 defense) 동안 프록시 최저 대비 진입가.
    avoided = 진입가→방어구간최저 추가하락(방어가 피한 분).
    """
    dfn = dfn.reindex(IDX).fillna(False)
    px = D['qqq']
    diff = dfn.astype(int).diff()
    entries = IDX[diff == 1]
    rows = []
    arr = dfn.values
    for e in entries:
        i = IDX.get_loc(e)
        win = px.iloc[max(0, i - lookback):i + 1]
        peak = win.max(); peak_dt = win.idxmax()
        p_entry = px.iloc[i]
        dd_at = (p_entry / peak - 1) * 100
        j = i
        while j < len(arr) and arr[j]:
            j += 1
        seg = px.iloc[i:j]
        trough = seg.min()
        avoided = (trough / p_entry - 1) * 100
        rows.append(dict(entry=e.date(), peak=peak_dt.date(), dd_at_entry=dd_at,
                         dur=(j - i), avoided=avoided))
    return pd.DataFrame(rows)


if __name__ == '__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    d = base_regime()
    print('baseline metrics:', metrics(d))
    print('wf:', wf_each(d))
