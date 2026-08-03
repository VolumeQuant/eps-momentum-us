# -*- coding: utf-8 -*-
"""선행PER 캡 × gap 게이트 재측정 (2026-08-03)

사용자 지시: "지금 fwd per 임계치나 gap은 효과없는지 봐봐"

배경: 두 게이트는 rev90 시대(~7/30)에 정해진 값이다. 그 뒤 순위 지표가 괴리율로 바뀌고
      dv $1B→$300M, ru3→1, 매출성장 게이트 복원, dd/밴드 제거까지 체인이 통째로 달라졌다.
      QUALITY_CAMPAIGN I9("랭킹이 바뀌면 게이트의 유·불리가 뒤집힌다") 원칙상 재측정 대상.

기준: exec_lag=1 · 괴리율 순위 · N5 · R5 · 위상평균(0..4)
     + LOWO(ex-SNDK/MU/STX) + 스트레스(6/15~) + 랜덤 진입창 120회 승률
현행 체인 고정: dv≥300 · min_seg≥-2 · ru≥1 · 매출성장≥10% · 이중상장 dedup
"""
import sys, os, random
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import unified_vm_track as U
from _joint_gate_audit_2026_08_01 import DAY, AD, PX
from _stop_revg_matrix_2026_08_03 import RG          # 일자별 rev_growth (carry-forward)

DV, MS, RU, REVG = 300.0, -2.0, 1, 0.10


def ranked(d, pe_max, gap_min):
    out, seen = [], set()
    for tk, a, m, g, dvv, fpe, u in DAY[d]:
        if dvv is None or dvv < DV: continue
        if m < MS or u < RU: continue
        if pe_max is not None and fpe > pe_max: continue
        if gap_min is not None and g is not None and g < gap_min: continue   # missing=pass
        rg = RG.get(d, {}).get(tk)
        if rg is not None and rg < REVG: continue
        k = U.DUAL_CLASS.get(tk, tk)
        if k in seen: continue
        seen.add(k); out.append(tk)
    return out


_CK = {}
def picks(pe_max, gap_min):
    key = (pe_max, gap_min)
    if key not in _CK:
        _CK[key] = {d: ranked(d, pe_max, gap_min) for d in AD}
    return _CK[key]


def run(pe_max, gap_min, phase, start=2, end=None, exclude=frozenset()):
    P = picks(pe_max, gap_min)
    lo, hi = start, (end if end else len(AD))
    hold, pend, rets = [], None, []
    for i in range(lo, hi):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        rets.append(sum((px[t] / ppx[t] - 1) / 5 for t in hold
                        if t in px and t in ppx and ppx[t] > 0))
        if pend is not None and pend[0] == i:
            hold = pend[1]; pend = None
        if i % 5 == phase:
            pend = (i + 1, [t for t in P[d] if t not in exclude][:5])
    r = np.array(rets)
    if not len(r): return None
    nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100


YRS = (len(AD) - 2) / 252.0
def cal(t, m): return (((1 + t / 100) ** (1 / YRS) - 1) * 100) / abs(m) if m else 0
def avg(pe, gp, **kw):
    o = [x for x in (run(pe, gp, phase=p, **kw) for p in range(5)) if x]
    return np.mean([x[0] for x in o]), np.mean([x[1] for x in o])


if __name__ == '__main__':
    si = next(i for i, d in enumerate(AD) if d >= '2026-06-15')
    PES = [None, 40, 30, 25, 20]
    GAPS = [None, 1.5, 2.5]

    print('■ 선행PER 캡 단독 (gap 게이트 없음 = 현행)')
    print('%-10s %9s %9s %8s %9s | %9s %9s | %7s' %
          ('PER 캡', '수익%', 'MDD%', 'Calmar', 'LOWO최악', '스트레스', 'MDD%', '후보수'))
    print('-' * 88)
    for pe in PES:
        r, m = avg(pe, None)
        w = min(cal(*avg(pe, None, exclude=frozenset(e)))
                for e in [('SNDK',), ('MU',), ('SNDK', 'MU'), ('SNDK', 'MU', 'STX')])
        sr, sm = avg(pe, None, start=si)
        n = np.mean([len(picks(pe, None)[d]) for d in AD[-20:]])
        print('%-10s %+9.1f %+9.1f %8.2f %9.2f | %+9.1f %+9.1f | %7.1f%s'
              % ('없음' if pe is None else '%d배' % pe, r, m, cal(r, m), w, sr, sm, n,
                 '  ←현행' if pe == 30 else ''))

    print('\n■ gap 게이트 (선행EPS/후행EPS ≥ X, 결측 통과) — PER 캡 30 고정')
    print('%-10s %9s %9s %8s %9s | %7s' % ('gap 컷', '수익%', 'MDD%', 'Calmar', 'LOWO최악', '후보수'))
    print('-' * 68)
    for gp in GAPS:
        r, m = avg(30, gp)
        w = min(cal(*avg(30, gp, exclude=frozenset(e)))
                for e in [('SNDK',), ('MU',), ('SNDK', 'MU'), ('SNDK', 'MU', 'STX')])
        n = np.mean([len(picks(30, gp)[d]) for d in AD[-20:]])
        print('%-10s %+9.1f %+9.1f %8.2f %9.2f | %7.1f%s'
              % ('없음' if gp is None else '%.1f배' % gp, r, m, cal(r, m), w, n,
                 '  ←현행' if gp is None else ''))

    print('\n■ 결합 그리드 (Calmar / LOWO최악)')
    print('%-10s %s' % ('', ''.join('%16s' % ('gap ' + ('없음' if g is None else '%.1f' % g))
                                    for g in GAPS)))
    for pe in PES:
        row = ''
        for gp in GAPS:
            c = cal(*avg(pe, gp))
            w = min(cal(*avg(pe, gp, exclude=frozenset(e)))
                    for e in [('SNDK',), ('MU',), ('SNDK', 'MU')])
            row += '%16s' % ('%.1f / %.1f' % (c, w))
        print('%-10s %s' % ('PER ' + ('없음' if pe is None else str(pe)), row))

    print('\n■ 랜덤 진입창 120회 — 현행(PER30·gap없음) 대비 승률')
    random.seed(7)
    CAND = [(None, None), (40, None), (25, None), (30, 1.5), (30, 2.5)]
    W = {c: [0, 0] for c in CAND}
    for _ in range(120):
        s = random.randint(2, len(AD) - 30); e = min(s + random.randint(25, 80), len(AD))
        b = run(30, None, phase=s % 5, start=s, end=e)
        for c in CAND:
            x = run(c[0], c[1], phase=s % 5, start=s, end=e)
            if x and b:
                if x[0] > b[0]: W[c][0] += 1
                if x[1] > b[1]: W[c][1] += 1
    for c in CAND:
        lb = 'PER %s · gap %s' % ('없음' if c[0] is None else c[0],
                                  '없음' if c[1] is None else c[1])
        print('  %-18s 수익승률 %3.0f%% | MDD승률 %3.0f%%' % (lb, W[c][0] / 1.2, W[c][1] / 1.2))

    print('\n■ 오늘(%s) 후보 top6 변화' % AD[-1])
    for pe, gp in [(30, None), (None, None), (25, None), (30, 1.5)]:
        print('  PER %-4s gap %-4s : %s'
              % ('없음' if pe is None else pe, '없음' if gp is None else gp,
                 ', '.join(picks(pe, gp)[AD[-1]][:6])))
