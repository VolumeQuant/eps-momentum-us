# -*- coding: utf-8 -*-
"""괴리율의 창 가중치가 의미 있나 (2026-08-04)

사용자: "현재 괴리율은 왜 rev90+rev60+rev30+rev7을 일정 비율로 곱해서 더해?"
→ 정확한 지적. 괴리율(fwd_pe_chg)도 **중첩된 4개 창의 가중평균**이라 상향폭 합산과 같은 구조다.
  현행 가중치 0.30/0.10/0.10/0.50은 v80.10(2026-05)에서 ~60일 표본 그리드서치로 고른 값 =
  오늘 우리가 "선택 자체가 노이즈"라고 결론 낸 그 방식으로 정해진 숫자다.
  그렇다면 가중치를 바꾸면 성과가 뒤집히는지, 아니면 아무 상관없는지 확인해야 한다.

측정: fwd_pe_chg를 원시 데이터(가격·NTM 5종)에서 가중치만 바꿔 재계산 → 동일 게이트·N5·R5.
  (보조 승수 dir_factor·eps_quality는 전 변형 공통이므로 제외 — 창 가중치 효과만 분리)
변형: 현행 v80.10 / 2월 v44 / 등가중 / 90일단독 / 7일단독 / 30-60중심
"""
import sys, os, random, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import unified_vm_track as U
from _joint_gate_audit_2026_08_01 import DAY, AD, PX
from _stop_revg_matrix_2026_08_03 import RG

DV, MS, RU, REVG, PEMAX = 300.0, -2.0, 1, 0.10, 30.0
N, R = 5, 5
DI = {d: i for i, d in enumerate(AD)}

PXT, NTM = {}, {}
_c = sqlite3.connect(os.path.join(BASE, 'eps_momentum_data.db'))
for d, tk, p, nc, n7, n30, n60, n90 in _c.execute(
        'SELECT date,ticker,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d '
        'FROM ntm_screening WHERE price IS NOT NULL'):
    PXT.setdefault(tk, {})[d] = p
    NTM.setdefault(tk, {})[d] = (nc, n7, n30, n60, n90)
_c.close()

LAG = {'7d': 5, '30d': 21, '60d': 42, '90d': 63}
WSETS = [
    ('현행 v80.10 (.3/.1/.1/.5)', {'7d': .30, '30d': .10, '60d': .10, '90d': .50}),
    ('2월 v44   (.4/.3/.2/.1)', {'7d': .40, '30d': .30, '60d': .20, '90d': .10}),
    ('등가중     (.25 ×4)', {'7d': .25, '30d': .25, '60d': .25, '90d': .25}),
    ('90일 단독', {'90d': 1.0}),
    ('7일 단독', {'7d': 1.0}),
    ('30-60 중심', {'30d': .5, '60d': .5}),
]


def fpc(tk, d, W):
    i = DI.get(d)
    if i is None:
        return None
    px, nt = PXT.get(tk, {}), NTM.get(tk, {})
    p_now, v = px.get(d), nt.get(d)
    if not p_now or not v or not v[0] or v[0] <= 0:
        return None
    pe_now = p_now / v[0]
    s = w = 0.0
    for j, k in enumerate(('7d', '30d', '60d', '90d'), 1):
        if k not in W:
            continue
        i2 = i - LAG[k]
        if i2 < 0:
            continue
        p_then, n_then = px.get(AD[i2]), v[j]
        if not p_then or not n_then or n_then <= 0:
            continue
        pe_then = p_then / n_then
        if pe_then <= 0:
            continue
        s += W[k] * (pe_now - pe_then) / pe_then * 100
        w += W[k]
    return s / w if w else None


_CK = {}
def picks(key, W):
    if key in _CK:
        return _CK[key]
    out = {}
    for d in AD:
        rows, seen, lst = [], set(), []
        for tk, a, m, g, dvv, fpe, u in DAY[d]:
            if dvv is None or dvv < DV or m < MS or u < RU or fpe > PEMAX:
                continue
            rg = RG.get(d, {}).get(tk)
            if rg is not None and rg < REVG:
                continue
            v = fpc(tk, d, W)
            if v is not None:
                rows.append((v, tk))
        rows.sort()
        for _v, tk in rows:
            k = U.DUAL_CLASS.get(tk, tk)
            if k in seen:
                continue
            seen.add(k); lst.append(tk)
        out[d] = lst
    _CK[key] = out
    return out


def run(key, W, phase, start=2, end=None, exclude=frozenset()):
    P = picks(key, W)
    lo, hi = start, (end if end else len(AD))
    hold, pend, rets = [], None, []
    for i in range(lo, hi):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        rets.append(sum((px[t] / ppx[t] - 1) / N for t in hold
                        if t in px and t in ppx and ppx[t] > 0))
        if pend is not None and pend[0] == i:
            hold = pend[1]; pend = None
        if i % R == phase % R:
            pend = (i + 1, [t for t in P[d] if t not in exclude][:N])
    r = np.array(rets)
    if not len(r):
        return None
    nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100


YRS = (len(AD) - 2) / 252.0
def cal(t, m): return (((1 + t / 100) ** (1 / YRS) - 1) * 100) / abs(m) if m else 0
def avg(key, W, **kw):
    o = [x for x in (run(key, W, p, **kw) for p in range(5)) if x]
    return np.mean([x[0] for x in o]), np.mean([x[1] for x in o])


if __name__ == '__main__':
    si = next(i for i, d in enumerate(AD) if d >= '2026-06-01')
    print('기간 %s~%s · 창 가중치만 변경 (게이트·N5·R5·보조승수 동일)\n' % (AD[2], AD[-1]))
    print('%-26s %9s %9s %8s %9s %11s' %
          ('가중치', '수익%', 'MDD%', 'Calmar', 'LOWO둘다', '6~7월'))
    print('-' * 80)
    for lbl, W in WSETS:
        t, m = avg(lbl, W)
        t2, m2 = avg(lbl, W, exclude=frozenset(('SNDK', 'MU')))
        t3, _ = avg(lbl, W, start=si + 1)
        print('%-26s %+9.1f %+9.1f %8.2f %9.1f %+11.1f'
              % (lbl, t, m, cal(t, m), cal(t2, m2), t3))

    print('\n■ 오늘(%s) top5' % AD[-1])
    for lbl, W in WSETS:
        print('  %-26s %s' % (lbl, ', '.join(picks(lbl, W)[AD[-1]][:5])))

    print('\n■ 변형 간 top5 겹침 (현행 대비, 전 교체일 평균)')
    grid = [i for i in range(2, len(AD)) if i % R == 0]
    base = picks(WSETS[0][0], WSETS[0][1])
    for lbl, W in WSETS[1:]:
        P = picks(lbl, W)
        ov = [len(set(base[AD[i]][:N]) & set(P[AD[i]][:N])) for i in grid]
        print('  %-26s %.1f / 5종목' % (lbl, np.mean(ov)))
