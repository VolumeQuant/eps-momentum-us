# -*- coding: utf-8 -*-
"""순위지표(괴리율 vs rev90) × 트레일링 스탑 결합 측정 (2026-08-03)

사용자 질문: "이럴 거면 rev90으로 트레일링스탑 15% 하는 게 낫지 않나?"
맥락: 괴리율은 '주가가 빠져도' 커지는 성질이 있어 오늘 목표 5개 중 3개가
      30일 고점 대비 -32~47% 무너진 종목이다. rev90은 전망 상승 '속도'만 보므로
      급락주를 위로 올리지 않는다. 대신 7/31 지표 대결에서 rev90은 하락장 MDD가
      두 배였다(-27.9 vs -15.8). 스탑이 그 약점을 메우면 순위가 뒤집힐 수 있다.

측정: 지표 2종 × 스탑 4종 = 8셀. 나머지 체인은 현행 고정
      (dv>=300 · PER<=30 · min_seg>=-2 · ru>=1 · 매출성장>=10% · dedup · N5 · R5 · lag=1)
스탑 정의(전부 무기억 = 보유 이력 불필요):
  없음      : 스탑 없음(현행)
  TS15/20   : 보유 중 최고가 대비 -X% 이탈 시 즉시 현금(다음 리밸까지 그 슬롯 비움)
              ★진입 이후 최고가를 슬롯 단위로 추적 — 고객 트레일링 스탑 안내와 같은 규칙
  고점25    : 30일 고점 대비 -25% 종목은 목표에서 제외(대칭 dd, 비교군)
판정: 위상평균(0..4) · LOWO(ex-SNDK/MU/STX) · 스트레스(6/15~) · 랜덤창 120회 승률
"""
import sys, os, random
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import unified_vm_track as U
from _joint_gate_audit_2026_08_01 import DAY, AD, PX
from _stop_revg_matrix_2026_08_03 import RG, PXH, high30

DV, MS, RU, REVG, PE = 300.0, -2.0, 1, 0.10, 30.0
N, R = 5, 5


def ranked(d, metric):
    """metric: 'gap'(괴리율, 작을수록 상위) | 'rev90'(전망 상향폭, 클수록 상위)"""
    rows, seen, out = [], set(), []
    for tk, a, m, g, dvv, fpe, u in DAY[d]:
        if dvv is None or dvv < DV: continue
        if m < MS or u < RU or fpe > PE: continue
        rg = RG.get(d, {}).get(tk)
        if rg is not None and rg < REVG: continue
        rows.append((tk, a))
    if metric == 'gap':
        rows.sort(key=lambda x: x[1])                     # adj_gap 오름차순
    else:
        R90 = _rev90(d)
        rows = [(tk, a) for tk, a in rows if R90.get(tk) is not None]
        rows.sort(key=lambda x: -R90[x[0]])               # rev90 내림차순
    for tk, _a in rows:
        k = U.DUAL_CLASS.get(tk, tk)
        if k in seen: continue
        seen.add(k); out.append(tk)
    return out


_R90 = {}
def _rev90(d):
    """rev90 = ntm_current/ntm_90d - 1 (DB 직접). 게이트 rev90>0은 DAY 구성 시 이미 반영."""
    if d not in _R90:
        import sqlite3
        c = sqlite3.connect(os.path.join(BASE, 'eps_momentum_data.db'))
        m = {}
        for tk, nc, n90 in c.execute(
                'SELECT ticker,ntm_current,ntm_90d FROM ntm_screening WHERE date=?', (d,)):
            if nc and n90 and n90 > 0.1:
                m[tk] = (nc - n90) / abs(n90) * 100
        c.close()
        _R90[d] = m
    return _R90[d]


_CK = {}
def picks(metric):
    if metric not in _CK:
        _CK[metric] = {d: ranked(d, metric) for d in AD}
    return _CK[metric]


def run(metric, stop, phase, start=2, end=None, exclude=frozenset()):
    """stop: None | ('ts', 0.15) | ('ts', 0.20) | ('dd', 0.25)"""
    P = picks(metric)
    lo, hi = start, (end if end else len(AD))
    hold, pend, rets = [], None, []
    peak = {}                     # 슬롯별 진입 후 최고가 (TS용)
    stopped = set()               # 이번 리밸 주기에 스탑 맞은 종목
    for i in range(lo, hi):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        live = [t for t in hold if t not in stopped]
        rets.append(sum((px[t] / ppx[t] - 1) / N for t in live
                        if t in px and t in ppx and ppx[t] > 0))
        if stop and stop[0] == 'ts':
            for t in live:
                p = px.get(t)
                if not p: continue
                peak[t] = max(peak.get(t) or p, p)
                if p / peak[t] - 1 <= -stop[1]:
                    stopped.add(t)                 # 다음 리밸까지 현금
        if pend is not None and pend[0] == i:
            hold = pend[1]; pend = None
            stopped = set()
            peak = {t: v for t, v in ((t, PX.get(d, {}).get(t)) for t in hold) if v}
        if i % R == phase:
            cand = [t for t in P[d] if t not in exclude]
            if stop and stop[0] == 'dd':
                cand = [t for t in cand
                        if not (high30(t, d) and PXH.get(d, {}).get(t)
                                and PXH[d][t] / high30(t, d) - 1 <= -stop[1])]
            pend = (i + 1, cand[:N])
    r = np.array(rets)
    if not len(r): return None
    nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100


YRS = (len(AD) - 2) / 252.0
def cal(t, m): return (((1 + t / 100) ** (1 / YRS) - 1) * 100) / abs(m) if m else 0
def avg(mt, st, **kw):
    o = [x for x in (run(mt, st, phase=p, **kw) for p in range(5)) if x]
    return np.mean([x[0] for x in o]), np.mean([x[1] for x in o])


if __name__ == '__main__':
    si = next(i for i, d in enumerate(AD) if d >= '2026-06-15')
    STOPS = [('없음', None), ('TS 15%', ('ts', 0.15)), ('TS 20%', ('ts', 0.20)),
             ('고점 -25% 제외', ('dd', 0.25))]

    print('%-8s %-14s %9s %9s %8s %9s | %9s %9s' %
          ('지표', '스탑', '수익%', 'MDD%', 'Calmar', 'LOWO최악', '스트레스', 'MDD%'))
    print('-' * 92)
    for mt, lbl in (('gap', '괴리율'), ('rev90', 'rev90')):
        for sl, st in STOPS:
            r, m = avg(mt, st)
            w = min(cal(*avg(mt, st, exclude=frozenset(e)))
                    for e in [('SNDK',), ('MU',), ('SNDK', 'MU'), ('SNDK', 'MU', 'STX')])
            sr, sm = avg(mt, st, start=si)
            mark = '  ←현행' if (mt == 'gap' and st is None) else ''
            print('%-8s %-14s %+9.1f %+9.1f %8.2f %9.2f | %+9.1f %+9.1f%s'
                  % (lbl, sl, r, m, cal(r, m), w, sr, sm, mark))
        print('-' * 92)

    print('\n랜덤 진입창 120회 — 현행(괴리율·스탑없음) 대비 승률')
    random.seed(11)
    CAND = [('gap', ('ts', 0.15)), ('rev90', None), ('rev90', ('ts', 0.15)),
            ('rev90', ('ts', 0.20))]
    W = {c: [0, 0] for c in CAND}
    for _ in range(120):
        s = random.randint(2, len(AD) - 30); e = min(s + random.randint(25, 80), len(AD))
        b = run('gap', None, phase=s % 5, start=s, end=e)
        for c in CAND:
            x = run(c[0], c[1], phase=s % 5, start=s, end=e)
            if x and b:
                if x[0] > b[0]: W[c][0] += 1
                if x[1] > b[1]: W[c][1] += 1
    for c in CAND:
        print('  %-22s 수익승률 %3.0f%% | MDD승률 %3.0f%%'
              % ('%s · %s' % (c[0], '없음' if c[1] is None else c[1][0] + str(c[1][1])),
                 W[c][0] / 1.2, W[c][1] / 1.2))

    print('\n오늘(%s) top5' % AD[-1])
    for mt, lbl in (('gap', '괴리율'), ('rev90', 'rev90')):
        print('  %-7s %s' % (lbl, ', '.join(picks(mt)[AD[-1]][:5])))
