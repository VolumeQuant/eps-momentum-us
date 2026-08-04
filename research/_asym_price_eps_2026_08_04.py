# -*- coding: utf-8 -*-
"""가격 vs EPS 비대칭 가중 — '떨어지는 칼날' 억제 실험 (2026-08-04)

사용자: "EPS 추정치에 좀 더 가중치를 주는 방식은 어때? EPS가 오르면서 가격도 오르는 게 맞지.
EPS가 미미하면서 가격만 떨어지는 건 떨어지는 칼날을 잡는 거잖아."
(같은 취지의 사전 지적: "가장 경계해야 할 건 EPS 추정치는 min_seg에 겨우 안 걸릴 정도로
부진한데 가격이 급락한 경우")

── 현행 산식의 구조적 성질 ──────────────────────────────────────────
괴리율(창 k) = PE_now/PE_k − 1 이고, 로그로 풀면

    ln(PE_now/PE_k) = ln(P_now/P_k) − ln(E_now/E_k) = Δ가격 − Δ전망

즉 **가격 변화와 전망 변화가 정확히 1:1로 상쇄**된다. 그래서
  · 전망 +2%, 가격 −40%  → 점수 −42  (떨어지는 칼날인데 최상위)
  · 전망 +40%, 가격 +10% → 점수 −30  (건강한 상향인데 아래)
사용자 지적대로 **부진한 전망 + 급락**이 구조적으로 상위에 온다.

── 실험 ────────────────────────────────────────────────────────────
가격 쪽 계수 α를 1보다 작게 두어 전망에 상대적으로 더 큰 무게를 준다:

    score_k = α · Δ가격 − Δ전망      (α=1.0이 현행, α=0이면 순수 전망상향폭)

α<1이면 위 두 예가 뒤집힌다(α=0.5: 칼날 −22 vs 건강 −35 → 건강한 쪽이 상위).
α를 0~1로 훑어 성과 곡선의 **모양**을 본다. 봉우리 하나면 노이즈, 고원이면 신호.
창 결합은 배포된 r=0.85 감쇠 가중치 고정. 게이트·N5·R5·exec_lag=1 동일.
판정: 위상평균 · LOWO · 분할 워크포워드 · 랜덤창(★무승부 별도 집계).
"""
import sys, os, random
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import unified_vm_track as U
from _joint_gate_audit_2026_08_01 import DAY, AD, PX
from _stop_revg_matrix_2026_08_03 import RG
from _pe_window_weights_2026_08_04 import PXT, NTM, DI, LAG

KEYS = ('7d', '30d', '60d', '90d')
R = 0.85
WD = {'90d': R ** 3, '60d': R ** 2 - R ** 3, '30d': R - R ** 2, '7d': 1 - R}


def score(tk, d, alpha):
    """Σ_k w_k · [α·ln(P_now/P_k) − ln(E_now/E_k)] ×100. 낮을수록 상위."""
    i = DI.get(d)
    px, v = PXT.get(tk, {}), NTM.get(tk, {}).get(d)
    if i is None or not v or not v[0] or v[0] <= 0:
        return None
    p_now = px.get(d)
    if not p_now:
        return None
    s = w = 0.0
    for idx, k in enumerate(KEYS):
        j = i - LAG[k]
        if j < 0:
            continue
        p_then, e_then = px.get(AD[j]), v[idx + 1]
        if not p_then or not e_then or e_then <= 0:
            continue
        s += WD[k] * (alpha * np.log(p_now / p_then) - np.log(v[0] / e_then)) * 100
        w += WD[k]
    return s / w if w else None


_CK = {}
def picks(a):
    if a in _CK:
        return _CK[a]
    out = {}
    for d in AD:
        rows, seen, lst = [], set(), []
        for tk, _ag, m, g, dvv, fpe, u in DAY[d]:
            if dvv is None or dvv < 300 or m < -2 or u < 1 or fpe > 30:
                continue
            rg = RG.get(d, {}).get(tk)
            if rg is not None and rg < 0.10:
                continue
            v = score(tk, d, a)
            if v is not None:
                rows.append((v, tk))
        rows.sort()
        for _v, tk in rows:
            k = U.DUAL_CLASS.get(tk, tk)
            if k in seen:
                continue
            seen.add(k); lst.append(tk)
        out[d] = lst
    _CK[a] = out
    return out


def run(P, phase, start, end, exclude=frozenset()):
    hold, pend, rets = [], None, []
    for i in range(start, end):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        rets.append(sum((px[t] / ppx[t] - 1) / 5 for t in hold
                        if t in px and t in ppx and ppx[t] > 0))
        if pend and pend[0] == i:
            hold = pend[1]; pend = None
        if i % 5 == phase % 5:
            pend = (i + 1, [t for t in P[d] if t not in exclude][:5])
    r = np.array(rets)
    if not len(r):
        return None
    nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100


def sc(P, start, end, **kw):
    o = [x for x in (run(P, p, start, end, **kw) for p in range(5)) if x]
    if not o:
        return None
    t, m = np.mean([x[0] for x in o]), np.mean([x[1] for x in o])
    y = (end - start) / 252.0
    return t, m, ((((1 + t / 100) ** (1 / y) - 1) * 100) / abs(m) if m else 0)


if __name__ == '__main__':
    AS = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.0]
    P = {a: picks(a) for a in AS}

    print('■ 가격 계수 α 스윕 (α=1.0 현행 / α=0 순수 전망상향폭) · 창 가중 r=0.85')
    print('%-8s %9s %9s %8s %9s %10s' % ('α', '수익%', 'MDD%', 'Calmar', 'LOWO둘다', '6~7월'))
    print('-' * 62)
    si = next(i for i, d in enumerate(AD) if d >= '2026-06-01')
    for a in AS:
        t, m, c = sc(P[a], 2, len(AD))
        c2 = sc(P[a], 2, len(AD), exclude=frozenset(('SNDK', 'MU')))[2]
        t3 = sc(P[a], si + 1, len(AD))[0]
        print('%-8.2f %+9.1f %+9.1f %8.2f %9.2f %+10.1f' % (a, t, m, c, c2, t3))

    print('\n■ 분할 워크포워드 (train 승자 α / 그 α의 test 등수)')
    print('%-8s %8s %10s %10s %8s' % ('분할', 'train α', 'train Cal', 'test Cal', 'test등수'))
    print('-' * 50)
    for f in (0.5, 0.6, 0.7, 0.8):
        s = int(len(AD) * f)
        tr = {a: sc(P[a], 2, s) for a in AS}
        te = {a: sc(P[a], s + 1, len(AD)) for a in AS}
        tr = {k: v for k, v in tr.items() if v}; te = {k: v for k, v in te.items() if v}
        order = sorted(te, key=lambda a: -te[a][2])
        win = max(tr, key=lambda a: tr[a][2])
        print('%-8s %8.2f %10.2f %10.2f %6d/%d' %
              (AD[s][5:], win, tr[win][2], te[win][2], order.index(win) + 1, len(te)))

    print('\n■ 랜덤 진입창 150회 — α=1.0(현행) 대비 (무승부 별도)')
    random.seed(7)
    W = {a: [0, 0, 0, 0, 0, 0] for a in AS if a != 1.0}   # 수익 승/패/무, 낙폭 승/패/무
    for _ in range(150):
        s = random.randint(2, len(AD) - 30); e = min(s + random.randint(25, 80), len(AD))
        b = run(P[1.0], s % 5, s, e)
        for a in W:
            x = run(P[a], s % 5, s, e)
            if not (x and b):
                continue
            for oi, ix in ((0, 0), (3, 1)):
                if abs(x[ix] - b[ix]) < 1e-9:
                    W[a][oi + 2] += 1
                elif x[ix] > b[ix]:
                    W[a][oi] += 1
                else:
                    W[a][oi + 1] += 1
    for a in sorted(W, reverse=True):
        w = W[a]
        rw = w[0] / max(1, w[0] + w[1]) * 100
        mw = w[3] / max(1, w[3] + w[4]) * 100
        print('  α=%.2f  수익 %3.0f%% (승%d 패%d 무%d) · 낙폭 %3.0f%% (승%d 패%d 무%d)'
              % (a, rw, w[0], w[1], w[2], mw, w[3], w[4], w[5]))

    print('\n■ 떨어지는 칼날 노출도 — 편입 시점 20일 주가가 −15% 이하인 픽 비율')
    grid = [i for i in range(2, len(AD)) if i % 5 == 0]
    for a in AS:
        cnt = tot = 0
        for i in grid:
            d = AD[i]
            for t in P[a][d][:5]:
                pxs = PXT.get(t, {})
                p0, p20 = pxs.get(d), pxs.get(AD[max(0, i - 20)])
                if not p0 or not p20:
                    continue
                tot += 1
                if p0 / p20 - 1 <= -0.15:
                    cnt += 1
        print('  α=%.2f  %4.1f%% (%d/%d)' % (a, cnt / max(1, tot) * 100, cnt, tot))

    print('\n■ 오늘(%s) top5' % AD[-1])
    for a in (1.0, 0.7, 0.5, 0.3):
        print('  α=%.2f  %s' % (a, ', '.join(P[a][AD[-1]][:5])))
