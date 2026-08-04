# -*- coding: utf-8 -*-
"""가치함정 게이트 정조준 — 'EPS는 겨우 통과, 가격만 급락' 코호트 (2026-08-04)

사용자 지적: "가장 경계해야 할 건 EPS 추정치는 min_seg에 겨우 안 걸릴 정도로 부진한데
가격이 급락한 경우."

현행 게이트(2026-08-01 배포): 30일 상향 <= +2% AND 20일 주가 하락 → 컷.
사용자가 말한 건 '30일 상향'이 아니라 **min_seg 근처**(전 구간 중 최악 구간이 겨우 -2% 위)다.
즉 축이 다르다. 여기서 확인할 것:
  1) 그 코호트가 실제로 몇 건이나 top5에 들어왔나 (현행 게이트가 못 잡는가)
  2) 그 코호트의 실제 성과가 나쁜가 (걸러야 할 대상이 맞는가)
  3) 걸러내면 전체 성과가 어떻게 되나 (비용은 얼마인가)

축 정의
  · ms_margin  = min_seg (4구간 중 최악). 게이트는 >= -2%. '겨우 통과' = -2 <= min_seg <= X
  · px20       = 20일 주가 변화율. '급락' = <= -Y
격자 X ∈ {0, 2, 5}, Y ∈ {-10, -15, -20, -25}로 정조준.
"""
import sys, os, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import unified_vm_track as U
from _joint_gate_audit_2026_08_01 import DAY, AD, PX
from _stop_revg_matrix_2026_08_03 import RG
from _pe_window_weights_2026_08_04 import PXT, NTM, DI, LAG

R = 0.85
WD = {'90d': R ** 3, '60d': R ** 2 - R ** 3, '30d': R - R ** 2, '7d': 1 - R}
KEYS = ('7d', '30d', '60d', '90d')

# min_seg, rev30, px20 로드
MS, R30, PX20 = {}, {}, {}
_c = sqlite3.connect(os.path.join(BASE, 'eps_momentum_data.db'))
for d, tk, nc, n7, n30, n60, n90 in _c.execute(
        'SELECT date,ticker,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening'):
    try:
        segs = [(a - b) / abs(b) * 100 for a, b in ((nc, n7), (n7, n30), (n30, n60), (n60, n90))
                if a is not None and b not in (None, 0)]
        if len(segs) == 4:
            MS.setdefault(d, {})[tk] = min(segs)
        if n30:
            R30.setdefault(d, {})[tk] = (nc - n30) / abs(n30) * 100
    except Exception:
        pass
_c.close()
for d in AD:
    i = DI[d]
    if i < 20:
        continue
    prev = AD[i - 20]
    for tk, px in PXT.items():
        a, b = px.get(d), px.get(prev)
        if a and b:
            PX20.setdefault(d, {})[tk] = (a / b - 1) * 100


def gap(tk, d):
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
        pe_now, pe_then = p_now / v[0], p_then / e_then
        if pe_then <= 0:
            continue
        s += WD[k] * (pe_now - pe_then) / pe_then * 100
        w += WD[k]
    return s / w if w else None


def base_rows(d):
    out = []
    for tk, _a, m, g, dvv, fpe, u in DAY[d]:
        if dvv is None or dvv < 300 or m < -2 or u < 1 or fpe > 30:
            continue
        rg = RG.get(d, {}).get(tk)
        if rg is not None and rg < 0.10:
            continue
        v = gap(tk, d)
        if v is not None:
            out.append((v, tk))
    out.sort()
    return out


def picks(xthr=None, ythr=None, trap_only=False):
    """xthr/ythr가 주어지면 (min_seg <= xthr AND px20 <= ythr) 종목을 컷."""
    out = {}
    for d in AD:
        seen, lst = set(), []
        for _v, tk in base_rows(d):
            if xthr is not None:
                ms = MS.get(d, {}).get(tk)
                p20 = PX20.get(d, {}).get(tk)
                bad = (ms is not None and ms <= xthr and p20 is not None and p20 <= ythr)
                if trap_only and not bad:
                    continue
                if (not trap_only) and bad:
                    continue
            k = U.DUAL_CLASS.get(tk, tk)
            if k in seen:
                continue
            seen.add(k); lst.append(tk)
        out[d] = lst
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


def sc(P, **kw):
    o = [x for x in (run(P, p, 2, len(AD), **kw) for p in range(5)) if x]
    t, m = np.mean([x[0] for x in o]), np.mean([x[1] for x in o])
    y = (len(AD) - 2) / 252.0
    return t, m, ((((1 + t / 100) ** (1 / y) - 1) * 100) / abs(m) if m else 0)


if __name__ == '__main__':
    grid = [i for i in range(2, len(AD)) if i % 5 == 0]
    BASE_P = picks()

    print('■ 1. 코호트가 실제 top5에 얼마나 들어오나 + 그 5일 성과')
    print('%-22s %8s %10s %10s' % ('조건 (min_seg / 20일주가)', '픽 수', '비중', '5일 평균수익'))
    print('-' * 58)
    for X in (0, 2, 5):
        for Y in (-10, -15, -20):
            hit, tot, rets = 0, 0, []
            for i in grid:
                d = AD[i]
                nd = AD[min(i + 5, len(AD) - 1)]
                for tk in BASE_P[d][:5]:
                    tot += 1
                    ms = MS.get(d, {}).get(tk)
                    p20 = PX20.get(d, {}).get(tk)
                    if ms is not None and ms <= X and p20 is not None and p20 <= Y:
                        hit += 1
                        a, b = PXT.get(tk, {}).get(nd), PXT.get(tk, {}).get(d)
                        if a and b:
                            rets.append((a / b - 1) * 100)
            print('%-22s %8d %9.1f%% %+10.1f%%'
                  % ('ms<=%+d / px20<=%d%%' % (X, Y), hit, hit / tot * 100,
                     np.mean(rets) if rets else 0))
    # 전체 픽의 5일 평균 (비교 기준)
    allr = []
    for i in grid:
        d = AD[i]; nd = AD[min(i + 5, len(AD) - 1)]
        for tk in BASE_P[d][:5]:
            a, b = PXT.get(tk, {}).get(nd), PXT.get(tk, {}).get(d)
            if a and b:
                allr.append((a / b - 1) * 100)
    print('%-22s %8d %9s %+10.1f%%' % ('전체 픽 (비교기준)', len(allr), '100%', np.mean(allr)))

    print('\n■ 2. 그 코호트만 사면? (걸러야 할 대상이 맞는지)')
    for X, Y in ((2, -15), (5, -15), (2, -20)):
        P = picks(X, Y, trap_only=True)
        n = np.mean([len(P[AD[i]]) for i in grid])
        if n < 1:
            print('  ms<=%+d/px20<=%d%%  후보 부족(평균 %.1f종목) — 측정 불가' % (X, Y, n))
            continue
        t, m, c = sc(P)
        print('  ms<=%+d/px20<=%d%%  수익 %+.1f%% · MDD %+.1f%% · Calmar %.2f (평균 후보 %.1f)'
              % (X, Y, t, m, c, n))

    print('\n■ 3. 걸러내면 전체 성과는? (비용)')
    t0, m0, c0 = sc(BASE_P)
    l0 = sc(BASE_P, exclude=frozenset(('SNDK', 'MU')))[2]
    print('%-24s %+9.1f %+9.1f %8.2f %9.2f' % ('현행(게이트 없음)', t0, m0, c0, l0))
    print('%-24s %9s %9s %8s %9s' % ('', '수익%', 'MDD%', 'Calmar', 'LOWO둘다'))
    for X in (0, 2, 5):
        for Y in (-10, -15, -20, -25):
            P = picks(X, Y)
            t, m, c = sc(P)
            l = sc(P, exclude=frozenset(('SNDK', 'MU')))[2]
            print('%-24s %+9.1f %+9.1f %8.2f %9.2f'
                  % ('ms<=%+d / px20<=%d%%' % (X, Y), t, m, c, l))
