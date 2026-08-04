# -*- coding: utf-8 -*-
"""워크포워드 정직성 검사 (2026-08-03)

사용자: "백테스트 자료라는 힌트지가 있는 상태로, 그 자료에서 제일 성과 잘 나오는 버전으로
        계속 바꾸고 있잖아. 그럼 당연히 6개월치에서 최적 조합은 현재 조합이겠지."
→ 맞다. 그래서 '같은 표본에서 1등'은 증거가 못 된다. 유일하게 남은 정직한 검사는
  **고르는 데 안 쓴 구간에서 시험**하는 것이다.

설계
  train = 2026-02-12 ~ 05-31 (약 76일)  ← 여기서만 보고 최적 조합 선택
  test  = 2026-06-01 ~ 07-31 (약 41일)  ← 선택에 전혀 쓰지 않음. 여기 성적이 정직한 성적.
  탐색 격자 = 지표(괴리율/rev90) × N(2/3/5) × R(1/5) × PER캡(30/없음) × 매출게이트(on/off)
  선택 기준 = train Calmar 최대
  비교군 = ①현행 조합 ②train 승자 ③격자 전체 평균(무작위로 골랐을 때의 기대) ④QQQ

읽는 법
  · train 승자가 test에서도 상위면 → 신호가 실재할 가능성
  · train 승자가 test에서 평균 이하로 무너지면 → 우리가 6개월간 한 게 과적합
  · 현행이 test에서 격자 평균보다 나으면, 최소한 '지금 조합은 나쁘지 않다'는 약한 증거
⚠️ test 구간 41일 = 표본 자체가 얇다. 이 검사는 결론이 아니라 방향 표시다.
"""
import sys, os, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import unified_vm_track as U
from _joint_gate_audit_2026_08_01 import DAY, AD, PX
from _stop_revg_matrix_2026_08_03 import RG
import _honest_benchmark_2026_08_03 as H

SPLIT = '2026-06-01'
SI = next(i for i, d in enumerate(AD) if d >= SPLIT)
DV, MS, RU = 300.0, -2.0, 1

_R90 = {}
def rev90_map(d):
    if d not in _R90:
        c = sqlite3.connect(os.path.join(BASE, 'eps_momentum_data.db'))
        m = {}
        for tk, nc, n90 in c.execute(
                'SELECT ticker,ntm_current,ntm_90d FROM ntm_screening WHERE date=?', (d,)):
            if nc and n90 and n90 > 0.1:
                m[tk] = (nc - n90) / abs(n90) * 100
        c.close(); _R90[d] = m
    return _R90[d]


_CK = {}
def picks(metric, pe_cap, revg):
    key = (metric, pe_cap, revg)
    if key in _CK:
        return _CK[key]
    out = {}
    for d in AD:
        rows = []
        for tk, a, m, g, dvv, fpe, u in DAY[d]:
            if dvv is None or dvv < DV or m < MS or u < RU:
                continue
            if pe_cap and fpe > pe_cap:
                continue
            if revg:
                rg = RG.get(d, {}).get(tk)
                if rg is not None and rg < revg:
                    continue
            rows.append((tk, a))
        if metric == 'gap':
            rows.sort(key=lambda x: x[1])
        else:
            R = rev90_map(d)
            rows = [(t, a) for t, a in rows if R.get(t) is not None]
            rows.sort(key=lambda x: -R[x[0]])
        seen, lst = set(), []
        for tk, _ in rows:
            k = U.DUAL_CLASS.get(tk, tk)
            if k in seen:
                continue
            seen.add(k); lst.append(tk)
        out[d] = lst
    _CK[key] = out
    return out


def run(cfg, phase, start, end):
    metric, N, R, pe_cap, revg = cfg
    P = picks(metric, pe_cap, revg)
    hold, pend, rets = [], None, []
    for i in range(start, end):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        rets.append(sum((px[t] / ppx[t] - 1) / N for t in hold
                        if t in px and t in ppx and ppx[t] > 0))
        if pend is not None and pend[0] == i:
            hold = pend[1]; pend = None
        if i % R == phase % R:
            pend = (i + 1, P[d][:N])
    return rets


def score(cfg, start, end):
    yrs = (end - start) / 252.0
    o = []
    for p in range(min(5, max(1, cfg[2]))):
        r = run(cfg, p, start, end)
        if r:
            o.append(H.stats(r, yrs))
    if not o:
        return None
    return (float(np.mean([x[0] for x in o])), float(np.mean([x[1] for x in o])),
            float(np.mean([x[2] for x in o])))


if __name__ == '__main__':
    GRID = [(mt, n, r, pe, rg)
            for mt in ('gap', 'rev90')
            for n in (2, 3, 5)
            for r in (1, 5)
            for pe in (30.0, None)
            for rg in (0.10, None)]
    CUR = ('gap', 5, 5, 30.0, 0.10)

    tr = {c: score(c, 2, SI) for c in GRID}
    te = {c: score(c, SI + 1, len(AD)) for c in GRID}
    tr = {k: v for k, v in tr.items() if v}
    te = {k: v for k, v in te.items() if v}

    def nm(c):
        return '%-5s N%d R%d PER%-4s 매출%s' % (
            c[0], c[1], c[2], ('30' if c[3] else '없음'), ('10%' if c[4] else '없음'))

    win = max(tr, key=lambda c: tr[c][2])
    te_ranked = sorted(te, key=lambda c: -te[c][2])
    te_rank = {c: i + 1 for i, c in enumerate(te_ranked)}

    print('train %s~%s (%d일) / test %s~%s (%d일) · 격자 %d조합'
          % (AD[2], AD[SI - 1], SI - 2, AD[SI], AD[-1], len(AD) - SI, len(GRID)))
    print()
    print('%-34s %10s %10s | %10s %10s %6s'
          % ('', 'train수익', 'trainCal', 'test수익', 'testCal', 'test등수'))
    print('-' * 92)
    rows = [('train 승자 → ' + nm(win), win), ('현행       → ' + nm(CUR), CUR)]
    for lbl, c in rows:
        if c not in te:
            continue
        print('%-34s %+10.1f %10.2f | %+10.1f %10.2f %5d위'
              % (lbl, tr[c][0], tr[c][2], te[c][0], te[c][2], te_rank[c]))

    avg_te_ret = np.mean([v[0] for v in te.values()])
    avg_te_cal = np.mean([v[2] for v in te.values()])
    print('%-34s %10s %10s | %+10.1f %10.2f  (격자 평균)'
          % ('무작위로 골랐다면(격자 평균)', '-', '-', avg_te_ret, avg_te_cal))

    q = H.bench('QQQ', [d for d in AD if d >= AD[SI]])
    if q:
        t, m, cal = H.stats(q, (len(AD) - SI) / 252.0)
        print('%-34s %10s %10s | %+10.1f %10.2f' % ('QQQ 매수보유', '-', '-', t, cal))

    print('\n■ train 상위 5조합이 test에서 몇 등이 되나 (과적합이면 순위가 무너진다)')
    for c in sorted(tr, key=lambda c: -tr[c][2])[:5]:
        if c in te:
            print('  %-32s train %5.2f (%d위) → test %5.2f (%d위)'
                  % (nm(c), tr[c][2],
                     sorted(tr, key=lambda x: -tr[x][2]).index(c) + 1, te[c][2], te_rank[c]))

    print('\n■ train 순위 vs test 순위 상관 (1에 가까우면 재현, 0이면 무작위)')
    ks = [c for c in tr if c in te]
    a = np.array([tr[c][2] for c in ks]); b = np.array([te[c][2] for c in ks])
    ra = a.argsort().argsort(); rb = b.argsort().argsort()
    print('  스피어만 상관 = %+.3f  (n=%d)' % (np.corrcoef(ra, rb)[0, 1], len(ks)))
