# -*- coding: utf-8 -*-
"""슬롯 수(N) 스윕 + 이중상장 가드 반영 (2026-07-31)

사용자 질문: "5종목이 최선 맞아? 4종목이나 3종목은?"
★이번 스윕부터 production과 동일하게 이중상장(GOOG/GOOGL 등) 중복 제거 적용.
★exec_lag=1 정직 기준. 헤드라인만 보지 않고 LOWO·랜덤진입일·스트레스 구간 병기.
"""
import sys, os, random
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
from _prod_faithful_bt_2026_07_31 import load
import unified_vm_track as U

FULL, AG, FUND = load(); TE = C._load_te('full'); AD, _, _, TC, _ = C._load()
PX = {d: {t: v['px'] for t, v in FULL.get(d, {}).items() if v['px']} for d in AD}
STRESS = '2026-06-15'
MS, PE, DV = -2.0, 30.0, 1000.0


def ranked(d):
    """게이트 통과 후보를 괴리율 순으로 (이중상장 중복 제거 적용)."""
    cand = []
    for tk, v in FULL.get(d, {}).items():
        if not C._industry_ok(tk, TC): continue
        p, nc = v['px'], v['nc']
        if v['dv'] is None or v['dv'] < DV: continue
        if nc <= 0 or (v['n90'] or 0) <= 0.1: continue
        if p / nc > PE or p < 10 or (v['na'] or 0) < 3: continue
        if (nc - (v['n90'] or 0)) / abs(v['n90'] or 1) <= 0: continue
        om, fcf, roe = FUND.get(tk, (None, None, None))
        if om is not None and om < 0.05: continue
        if fcf is not None and roe is not None and fcf < 0 and roe < 0: continue
        if min(C._ms(dict(nc=nc, n7=v['n7'], n30=v['n30'], n60=v['n60'], n90=v['n90'])), 0) < MS:
            continue
        a = AG.get(d, {}).get(tk)
        if a is None: continue
        cand.append((a, tk))
    cand.sort()
    seen, out = set(), []
    for _, tk in cand:                      # ★이중상장 가드 (production 동일)
        k = U.DUAL_CLASS.get(tk, tk)
        if k in seen: continue
        seen.add(k); out.append(tk)
    return out


_RK = {d: ranked(d) for d in AD}


def run(N, R=5, phase=0, start=2, end=None, exclude=frozenset()):
    lo, hi = start, (end if end else len(AD))
    hold = []; pend = None; rets = []
    for i in range(lo, hi):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        rets.append(sum((px[t] / ppx[t] - 1) / N for t in hold
                        if t in px and t in ppx and ppx[t] > 0))
        if pend is not None and pend[0] == i:
            hold = pend[1]; pend = None
        if i % R == phase:
            pend = (i + 1, [t for t in _RK[d] if t not in exclude][:N])
    r = np.array(rets)
    if not len(r): return None
    nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100


def avg(N, **kw):
    o = [run(N, phase=p, **kw) for p in range(5)]
    o = [x for x in o if x]
    return np.mean([x[0] for x in o]), np.mean([x[1] for x in o])


YRS = (len(AD) - 2) / 252.0
def cal(r, m): return (((1 + r/100) ** (1/YRS) - 1) * 100) / abs(m) if m else 0

if __name__ == '__main__':
    si = next(i for i, d in enumerate(AD) if d >= STRESS)
    NS = [2, 3, 4, 5, 6, 8, 10]
    print('슬롯 수(N) 스윕 · 이중상장 가드 적용 · exec_lag=1 · 위상평균')
    print('%-6s %9s %9s %8s | %9s %9s' % ('N', '수익%', 'MDD%', 'Calmar', '스트레스', 'MDD%'))
    print('-' * 62)
    base = {}
    for N in NS:
        r, m = avg(N); sr, sm = avg(N, start=si)
        base[N] = (r, m)
        mk = ' ←현행' if N == 5 else ''
        print('%-6d %+9.1f %+9.1f %8.2f | %+9.1f %+9.1f%s' % (N, r, m, cal(r, m), sr, sm, mk))

    print('\nLOWO (Calmar) — 슈퍼위너 제외해도 유지되나')
    print('%-18s %s' % ('제외', ''.join('%9d' % n for n in NS)))
    for ex in [(), ('SNDK',), ('MU',), ('SNDK', 'MU'), ('SNDK', 'MU', 'STX')]:
        e = frozenset(ex)
        row = ''.join('%9.2f' % cal(*avg(n, exclude=e)) for n in NS)
        print('%-18s %s' % ('ex-' + ('/'.join(ex) if ex else '없음'), row))

    print('\n랜덤 진입일 150회 — N=5 대비 승률')
    random.seed(3)
    W = {n: [0, 0] for n in NS if n != 5}
    for _ in range(150):
        s = random.randint(2, len(AD) - 30); e = min(s + random.randint(25, 80), len(AD))
        b = run(5, phase=s % 5, start=s, end=e)
        for n in W:
            x = run(n, phase=s % 5, start=s, end=e)
            if x and b:
                if x[0] > b[0]: W[n][0] += 1
                if x[1] > b[1]: W[n][1] += 1
    for n in sorted(W):
        print('  N=%-3d 수익승률 %3.0f%% | MDD승률 %3.0f%%' % (n, W[n][0]/1.5, W[n][1]/1.5))
