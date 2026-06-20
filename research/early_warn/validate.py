# -*- coding: utf-8 -*-
"""후보 검증 배터리 — baseline 대비 정직한 합격판정.

합격 기준(사용자): MDD↓(덜 음수) AND Calmar 비악화, 점추정 우위 OUT.
  → 고정config + FULL + WF 3블록최소 + LOWO(V-crash 제외) + 인접CV<0.3 + 약세장포착 + lateness + 휩쏘(전환수).
best-vs-best 금지: 후보의 주임계값을 인접 스윕해 CV로 robust 확인.
"""
import numpy as np
import pandas as pd
import harness as H

IDX = H.IDX


def combined(cand_raw, ne=2, nx=15):
    """base 게이트에 후보 조기진입을 OR. 후보 raw(bool Series) ne일 확인 진입, nx일 확인 퇴출.
    퇴출은 MA200/VIX가 OR로 잡고 있어 후보 단독 false alarm만 잠깐 방어(=휩쏘 비용 측정 대상)."""
    base = H.base_regime()
    cd = H.conf(cand_raw.reindex(IDX).fillna(False), ne, nx)
    return (base | cd).reindex(IDX).fillna(False)


def replace_ma(trend_raw, ne=15, nx=15):
    """MA200 leg를 후보 추세신호로 교체, VIX36 leg는 유지. (앙상블/절대모멘텀용)."""
    v = H.D['vix']
    d = H.conf(trend_raw.reindex(IDX).fillna(False), ne, nx) | H.conf(v > 36, 2, 2)
    return d.reindex(IDX).fillna(False)


def _lowo_cal(dfn):
    m = H.vmask().values
    pos = (~dfn.reindex(IDX).fillna(False)).shift(1, fill_value=False)
    r = pd.Series(np.where(pos.values, H.D['qr'].values, H.NOTE), index=IDX)[m]
    nav = (1 + r).cumprod(); yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1; mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    return cagr / abs(mdd) if mdd < 0 else 0.0, cagr * 100, mdd * 100


_BASE = H.base_regime()
_B = H.metrics(_BASE)
_B_wf = H.wf_min(_BASE)
_B_lowo = _lowo_cal(_BASE)[0]
_B_late = H.lateness(_BASE)['dd_at_entry'].mean()


def row(name, dfn):
    """후보 dfn 한 줄 비교 + 델타. dict 반환."""
    m = H.metrics(dfn)
    wf = H.wf_min(dfn)
    lo = _lowo_cal(dfn)[0]
    late = H.lateness(dfn)['dd_at_entry'].mean()
    return dict(name=name, cagr=m['cagr'], mdd=m['mdd'], calmar=m['calmar'], wf_min=wf,
                lowo=lo, trans=m['transitions'], dfrac=m['defense_frac'] * 100, late=late)


def print_header():
    print(f'{"전략":<30}{"CAGR%":>7}{"MDD%":>8}{"Cal":>6}{"WFmin":>6}{"LOWO":>6}{"전환":>5}{"방어%":>6}{"늦음%":>7}')
    b = row('BASELINE (현행)', _BASE)
    _pr(b)
    return b


def _pr(r):
    print(f'{r["name"]:<30}{r["cagr"]:>+7.1f}{r["mdd"]:>+8.1f}{r["calmar"]:>6.2f}'
          f'{r["wf_min"]:>6.2f}{r["lowo"]:>6.2f}{r["trans"]:>5}{r["dfrac"]:>6.1f}{r["late"]:>+7.1f}')


def compare(name, dfn, verbose=True):
    r = row(name, dfn)
    if verbose:
        _pr(r)
    # 판정
    mdd_better = r['mdd'] > _B['mdd'] + 0.3          # MDD 덜 음수(개선) — 0.3%p 여유
    cal_ok = r['calmar'] >= _B['calmar'] - 0.01      # Calmar 비악화
    wf_ok = r['wf_min'] >= _B_wf - 0.01
    lowo_ok = r['lowo'] >= _B_lowo - 0.01
    late_better = r['late'] > _B_late + 0.5          # 더 일찍(고점대비 덜 빠짐)
    r['verdict'] = dict(mdd_better=mdd_better, cal_ok=cal_ok, wf_ok=wf_ok,
                        lowo_ok=lowo_ok, late_better=late_better,
                        PASS=(mdd_better and cal_ok and wf_ok and lowo_ok))
    return r


def adjacency(make_dfn, grid, label='param'):
    """make_dfn(p)->dfn 를 grid 전체로 돌려 Calmar CV(robust) + 각 셀 출력."""
    cals = []
    print(f'  -- 인접 스윕 ({label}) --')
    for p in grid:
        d = make_dfn(p)
        m = H.metrics(d)
        cals.append(m['calmar'])
        print(f'     {label}={p!s:>8}  Cal {m["calmar"]:.2f}  MDD {m["mdd"]:+.1f}  WFmin {H.wf_min(d):.2f}  전환 {m["transitions"]}')
    cv = np.std(cals) / np.mean(cals) if np.mean(cals) else 9.9
    print(f'     → Calmar CV={cv:.3f} ({"robust plateau" if cv < 0.30 else "NON-robust ⚠"})')
    return cv


if __name__ == '__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    print_header()
    print(f'\nbaseline 참조: WFmin {_B_wf:.2f} / LOWO {_B_lowo:.2f} / 평균늦음 {_B_late:+.1f}%')
