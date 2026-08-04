# -*- coding: utf-8 -*-
"""'가격은 최신만' 괴리율 변형 검증 (2026-08-04)

사용자 지적: "왜 과거 주가를 봐? 주가는 항상 전일 종가로 봐야지. 예상 EPS를 기준일별로
가중치 줘서 더한 다음 전일 종가랑 비교해서 괴리를 봐야지. 과거 주가는 죽은 데이터고,
90일 전 예상 EPS는 fwd 12m 추정치라 살아있는 데이터다."

현행 식의 구조:
  pe_chg_k = PE_now / PE_k - 1 = (P_now/P_k) x (E_k/E_now) - 1
  → 과거 주가 P_k가 분모에 들어가 **가격 모멘텀이 섞인다**. 지적 정확함.

검증 변형 (전부 P_now = 전일 종가 하나만 사용):
  V1 블렌드PER 수준 : P_now / E_blend   (E_blend = Σ w_k E_k, 낮을수록 상위)
                      = "여러 시점 추정치를 섞은 EPS 대비 지금 주가가 싼가"
  V2 상향폭/PER     : (E_now/E_blend - 1) / (P_now/E_now)
                      = "전망은 올랐는데(분자) 아직 싼가(분모)" — 가격은 최신만
  V3 상향폭 블렌드   : E_now/E_blend - 1  (가격 전혀 없음, 순수 상향폭 — 대조군)
  A  현행 괴리율     (과거 주가 포함) — 기준선
가중치는 현행과 동일 (7d .30 / 30d .10 / 60d .10 / 90d .50).
게이트 체인·N5·R5·exec_lag=1 동일. 보조 승수(가속도·일관성)는 전 변형 미적용(창 효과만 분리).
"""
import sys, os, random, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import unified_vm_track as U
from _joint_gate_audit_2026_08_01 import DAY, AD, PX
from _stop_revg_matrix_2026_08_03 import RG
from _pe_window_weights_2026_08_04 import PXT, NTM, DI, LAG, fpc

DV, MS, RU, REVG, PEMAX = 300.0, -2.0, 1, 0.10, 30.0
N, R = 5, 5
W = {'7d': .30, '30d': .10, '60d': .10, '90d': .50}


def e_blend(tk, d):
    """과거 기준일들의 선행 12M EPS 추정치를 가중 블렌드 (가격 미사용)."""
    v = NTM.get(tk, {}).get(d)
    if not v or not v[0] or v[0] <= 0:
        return None, None
    s = w = 0.0
    for j, k in enumerate(('7d', '30d', '60d', '90d'), 1):
        e = v[j]
        if e and e > 0 and k in W:
            s += W[k] * e; w += W[k]
    return (s / w if w else None), v[0]


def metric(tk, d, mode):
    """반환값은 '작을수록 상위' 규약으로 통일."""
    p = PXT.get(tk, {}).get(d)
    if not p:
        return None
    eb, e_now = e_blend(tk, d)
    if eb is None or not e_now:
        return None
    if mode == 'V1':                       # 블렌드 PER 수준 (낮을수록 쌈)
        return p / eb
    if mode == 'V2':                       # 상향폭 / 현재 PER (클수록 좋음 → 부호 반전)
        rev = e_now / eb - 1
        per = p / e_now
        if per <= 0:
            return None
        return -(rev / per)
    if mode == 'V3':                       # 순수 상향폭 블렌드 (클수록 좋음)
        return -(e_now / eb - 1)
    if mode == 'A':                        # 현행: 과거 주가 포함 가중평균
        return fpc(tk, d, W)
    return None


_CK = {}
def picks(mode):
    if mode in _CK:
        return _CK[mode]
    out = {}
    for d in AD:
        rows, seen, lst = [], set(), []
        for tk, a, m, g, dvv, fpe, u in DAY[d]:
            if dvv is None or dvv < DV or m < MS or u < RU or fpe > PEMAX:
                continue
            rg = RG.get(d, {}).get(tk)
            if rg is not None and rg < REVG:
                continue
            v = metric(tk, d, mode)
            if v is not None:
                rows.append((v, tk))
        rows.sort()
        for _v, tk in rows:
            k = U.DUAL_CLASS.get(tk, tk)
            if k in seen:
                continue
            seen.add(k); lst.append(tk)
        out[d] = lst
    _CK[mode] = out
    return out


def run(mode, phase, start=2, end=None, exclude=frozenset()):
    P = picks(mode)
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
def avg(mode, **kw):
    o = [x for x in (run(mode, p, **kw) for p in range(5)) if x]
    return np.mean([x[0] for x in o]), np.mean([x[1] for x in o])


CFG = [('A 현행(과거주가 포함)', 'A'), ('V1 블렌드PER 수준', 'V1'),
       ('V2 상향폭/PER', 'V2'), ('V3 순수 상향폭', 'V3')]

if __name__ == '__main__':
    si = next(i for i, d in enumerate(AD) if d >= '2026-06-01')
    print('기간 %s~%s · N5 R5 · exec_lag=1 · 게이트 동일 · 보조승수 미적용\n' % (AD[2], AD[-1]))
    print('%-22s %9s %9s %8s %9s %11s' %
          ('', '수익%', 'MDD%', 'Calmar', 'LOWO둘다', '6~7월'))
    print('-' * 76)
    for lbl, md in CFG:
        t, m = avg(md)
        t2, m2 = avg(md, exclude=frozenset(('SNDK', 'MU')))
        t3, _ = avg(md, start=si + 1)
        print('%-22s %+9.1f %+9.1f %8.2f %9.1f %+11.1f'
              % (lbl, t, m, cal(t, m), cal(t2, m2), t3))

    print('\n■ 랜덤 진입창 150회 — 현행 대비 승률')
    random.seed(13)
    Wn = {md: [0, 0] for _l, md in CFG[1:]}
    for _ in range(150):
        s = random.randint(2, len(AD) - 30); e = min(s + random.randint(25, 80), len(AD))
        b = run('A', s % 5, start=s, end=e)
        for md in Wn:
            x = run(md, s % 5, start=s, end=e)
            if x and b:
                if x[0] > b[0]: Wn[md][0] += 1
                if x[1] > b[1]: Wn[md][1] += 1
    for lbl, md in CFG[1:]:
        print('  %-22s 수익 승률 %3.0f%% · 낙폭 승률 %3.0f%%'
              % (lbl, Wn[md][0] / 1.5, Wn[md][1] / 1.5))

    print('\n■ 회전율')
    grid = [i for i in range(2, len(AD)) if i % R == 0]
    for lbl, md in CFG:
        P = picks(md); prev = None; ch = []
        for i in grid:
            cur = set(P[AD[i]][:N])
            if prev is not None:
                ch.append(len(cur - prev))
            prev = cur
        print('  %-22s 교체당 %.1f종목' % (lbl, np.mean(ch)))

    print('\n■ 오늘(%s) top5' % AD[-1])
    for lbl, md in CFG:
        print('  %-22s %s' % (lbl, ', '.join(picks(md)[AD[-1]][:5])))
