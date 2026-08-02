# -*- coding: utf-8 -*-
"""스탑 방식 × 매출성장 필터 결합 측정 (2026-08-03)

사용자 지시: ①"매출 성장 필터 당연히 걸어야 하는 것 아니냐 — 성과 측정해봐"
            ②"dd_30_25는 트레일링스탑 15%로 대신하기로 했잖아"

스탑 4방식 (전부 ru1·na3 현행 게이트 고정, 진입/보유 처리만 다름):
  A 현행     : dd25 진입유예 + HOLD_BAND 7 (8/2 배포분)
  B 대칭15   : 30일 고점 −15% 아래면 목표 제외(진입·보유 공통, 무기억) — 고객 TS15%와 동일 숫자
  C 대칭25   : 융합안(4문장) 스탑 — 30일 고점 −25% 대칭
  D 스탑없음 : 참고 기준선
× 매출성장 필터 {없음, rev_growth ≥ 10%(구시스템 하드필터 정의)} = 8셀
판정: exec_lag=1 · 위상평균 · LOWO최악 · 스트레스(6/15~) · 회전(연간 주문수)
"""
import sys, os, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
import daily_runner as dr
import unified_vm_track as U
from _joint_gate_audit_2026_08_01 import DAY, AD, PX

# 가격 이력(고점 계산용): px_full + PX 병합
import pandas as pd
PXH = {}
try:
    dfp = pd.read_parquet(os.path.join(BASE, 'research', 'px_full_2026_07_04.parquet'))
    for dt in dfp.index:
        row = dfp.loc[dt].dropna()
        PXH[str(dt)[:10]] = dict(zip(row.index, row.values))
except Exception as e:
    print('[px_full 스킵: %s]' % e)
for d in AD:
    PXH.setdefault(d, {}).update(PX.get(d, {}))
HD = sorted(PXH); HI = {d: i for i, d in enumerate(HD)}

def high30(tk, d):
    i = HI[d]
    vals = [PXH[HD[j]].get(tk) for j in range(max(0, i - 29), i + 1)]
    vals = [v for v in vals if v]
    return max(vals) if vals else None

# rev_growth (당일 없으면 최근값 carry)
RG = {}
_c = sqlite3.connect(dr.DB_PATH)
rows = list(_c.execute('SELECT date,ticker,rev_growth FROM ntm_screening WHERE rev_growth IS NOT NULL ORDER BY date'))
_c.close()
cur = {}
from collections import defaultdict
byd = defaultdict(dict)
for d, tk, v in rows:
    byd[d][tk] = v
for d in AD:
    for tk, v in byd.get(d, {}).items():
        cur[tk] = v
    RG[d] = dict(cur)

RU1, NA3 = 1, 3


def ranked(d, revg=None):
    """게이트 통과 후보를 괴리율 순으로 (현행: dv300·PE30·ms-2·ru1)"""
    out = []
    seen = set()
    for tk, a, m, g, dvv, fpe, u in DAY[d]:
        if dvv is None or dvv < 300.0: continue
        if fpe > 30.0 or m < -2.0 or u < RU1: continue
        if revg is not None:
            rg = RG.get(d, {}).get(tk)
            if rg is not None and rg < revg: continue
        k = U.DUAL_CLASS.get(tk, tk)
        if k in seen: continue
        seen.add(k)
        out.append(tk)
    return out


def dd_frac(tk, d):
    h = high30(tk, d); p = PXH.get(d, {}).get(tk)
    return (p / h - 1) if (h and p) else 0.0


def target(mode, d, held, RK):
    """mode별 목표 5종목"""
    rank = {t: i + 1 for i, t in enumerate(RK)}
    if mode == 'A':          # dd25 진입유예 + band7
        keep = [t for t in RK if t in held and rank[t] <= 7][:5]
        for t in RK:
            if len(keep) >= 5: break
            if t in keep: continue
            if dd_frac(t, d) <= -0.25: continue    # 신규 진입만 차단
            keep.append(t)
        return keep
    if mode in ('B', 'C'):   # 대칭 무기억: 고점 -X% 아래면 목표 제외(진입·보유 공통)
        thr = -0.15 if mode == 'B' else -0.25
        return [t for t in RK if dd_frac(t, d) > thr][:5]
    return RK[:5]            # D 스탑없음


def run(mode, revg, phase, start=2, end=None, exclude=frozenset()):
    lo, hi = start, (end if end else len(AD))
    hold = []; pend = None; rets = []; orders = 0
    for i in range(lo, hi):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        rets.append(sum((px[t] / ppx[t] - 1) / 5 for t in hold
                        if t in px and t in ppx and ppx[t] > 0))
        if pend is not None and pend[0] == i:
            orders += len(set(pend[1]) ^ set(hold))
            hold = pend[1]; pend = None
        if i % 5 == phase:
            RK = [t for t in ranked(d, revg) if t not in exclude]
            pend = (i + 1, target(mode, d, hold, RK))
    r = np.array(rets)
    if not len(r): return None
    nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100, orders


YRS = (len(AD) - 2) / 252.0
def cal(t, m): return (((1 + t / 100) ** (1 / YRS) - 1) * 100) / abs(m) if m else 0


def battery(mode, revg):
    o = [x for x in (run(mode, revg, p) for p in range(5)) if x]
    t = np.mean([x[0] for x in o]); m = np.mean([x[1] for x in o])
    od = np.mean([x[2] for x in o]) * (252 / (len(AD) - 2))
    worst = 1e9
    for ex in [('SNDK',), ('MU',), ('SNDK', 'MU'), ('SNDK', 'MU', 'STX')]:
        o2 = [x for x in (run(mode, revg, p, exclude=frozenset(ex)) for p in range(5)) if x]
        worst = min(worst, cal(np.mean([x[0] for x in o2]), np.mean([x[1] for x in o2])))
    si = next(i for i, d in enumerate(AD) if d >= '2026-06-15')
    o3 = [x for x in (run(mode, revg, p, start=si) for p in range(5)) if x]
    st, sm = np.mean([x[0] for x in o3]), np.mean([x[1] for x in o3])
    return t, m, cal(t, m), worst, st, sm, od


if __name__ == '__main__':
    NAMES = {'A': 'A 현행(dd25유예+밴드7)', 'B': 'B 대칭15(=TS15 통일)',
             'C': 'C 대칭25(융합안)', 'D': 'D 스탑없음'}
    print('%-24s %7s | %8s %7s %7s %8s | %9s %7s | %7s'
          % ('스탑방식', '매출필터', '수익%', 'MDD%', 'Calmar', 'LOWO최악', '스트레스', 'MDD%', '연주문'))
    print('-' * 100)
    for mode in 'ABCD':
        for revg in (None, 0.10):
            t, m, c, lw, st, sm, od = battery(mode, revg)
            print('%-24s %7s | %+8.1f %+7.1f %7.2f %8.2f | %+9.1f %+7.1f | %7.0f'
                  % (NAMES[mode], '≥10%' if revg else '없음', t, m, c, lw, st, sm, od))
