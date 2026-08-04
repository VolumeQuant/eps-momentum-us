# -*- coding: utf-8 -*-
"""다중창 합산 지표 검증 (2026-08-04)

사용자 제안: "괴리율 말고 rev90+rev60+rev30+rev7 이렇게 더해서 보는 건?"

동기(타당함): 단일 창은 그 창의 기준일 하나에 성과가 걸린다(rev90은 91일 전 실적 계단에
좌우 — 7/23 하이닉스 사고가 그 사례). 여러 창을 합치면 특정 기준일 의존이 희석된다.
⚠️단, 네 창은 중첩(rev90 ⊃ rev60 ⊃ rev30 ⊃ rev7)이라 단순 합은 '최근을 4중 계상'하는 효과 =
  암묵적 최근성 가중. 그래서 순수 합 외에 등가중/최근성역가중도 같이 잰다.

비교군: A 괴리율(현행) · B rev90 단독 · D 합산(제안) · E 정규화합 · F 장기가중합
게이트 체인·N5·R5·exec_lag=1 전부 동일. 판정 = 위상평균 + LOWO + 구간분해 + 랜덤창.
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

# 창별 상향폭 로드
REV = {}
_c = sqlite3.connect(os.path.join(BASE, 'eps_momentum_data.db'))
for d, tk, nc, n7, n30, n60, n90 in _c.execute(
        'SELECT date,ticker,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening'):
    if not nc or nc <= 0:
        continue
    row = {}
    for k, v in (('7', n7), ('30', n30), ('60', n60), ('90', n90)):
        if v and v > 0.1:
            row[k] = (nc - v) / abs(v) * 100
    if len(row) == 4:
        REV.setdefault(d, {})[tk] = row
_c.close()


def score(tk, d, mode):
    r = REV.get(d, {}).get(tk)
    if not r:
        return None
    if mode == 'sum':                 # 사용자 제안: 단순 합 (최근이 4중 계상됨)
        return r['7'] + r['30'] + r['60'] + r['90']
    if mode == 'norm':                # 각 창을 '구간 순증'으로 분해해 등가중
        s7, s30, s60, s90 = r['7'], r['30'], r['60'], r['90']
        return (s7 + (s30 - s7) + (s60 - s30) + (s90 - s60)) / 4     # = s90/4 (참고용)
    if mode == 'longw':               # 장기 가중 (90일에 무게)
        return 0.1 * r['7'] + 0.2 * r['30'] + 0.3 * r['60'] + 0.4 * r['90']
    if mode == 'rev90':
        return r['90']
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
            if mode == 'gap':
                rows.append((a, tk))            # 괴리율: 작을수록 상위
            else:
                s = score(tk, d, mode)
                if s is None:
                    continue
                rows.append((-s, tk))           # 상향폭: 클수록 상위
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
def cal(t, m, yrs=YRS):
    return (((1 + t / 100) ** (1 / yrs) - 1) * 100) / abs(m) if m else 0
def avg(mode, **kw):
    o = [x for x in (run(mode, p, **kw) for p in range(5)) if x]
    return np.mean([x[0] for x in o]), np.mean([x[1] for x in o])


CFG = [('A 괴리율(현행)', 'gap'), ('B rev90 단독', 'rev90'),
       ('D 합산(제안)', 'sum'), ('F 장기가중합', 'longw')]

if __name__ == '__main__':
    print('기간 %s~%s · N5 R5 · exec_lag=1 · 게이트 동일\n' % (AD[2], AD[-1]))
    print('■ 1. 전체 (위상 0~4 평균)')
    print('%-16s %10s %10s %9s' % ('', '수익%', 'MDD%', 'Calmar'))
    print('-' * 50)
    for lbl, md in CFG:
        t, m = avg(md)
        print('%-16s %+10.1f %+10.1f %9.2f' % (lbl, t, m, cal(t, m)))

    print('\n■ 2. LOWO (Calmar)')
    EXS = [((), '전체'), (('SNDK',), 'ex-SNDK'), (('MU',), 'ex-MU'),
           (('SNDK', 'MU'), 'ex-둘다'), (('SNDK', 'MU', 'STX'), 'ex-셋')]
    print('%-16s %s' % ('', ''.join('%11s' % l for _e, l in EXS)))
    print('-' * 72)
    for lbl, md in CFG:
        print('%-16s %s' % (lbl, ''.join('%11.1f' % cal(*avg(md, exclude=frozenset(e)))
                                         for e, _l in EXS)))

    print('\n■ 3. 구간 분해')
    si = next(i for i, d in enumerate(AD) if d >= '2026-06-01')
    print('%-16s %22s %22s' % ('', '2~5월(상승)', '6~7월(조정)'))
    print('-' * 64)
    for lbl, md in CFG:
        t1, m1 = avg(md, end=si)
        t2, m2 = avg(md, start=si + 1)
        print('%-16s %10.1f%% (MDD %5.1f) %10.1f%% (MDD %5.1f)' % (lbl, t1, m1, t2, m2))

    print('\n■ 4. 랜덤 진입창 150회 — 괴리율 대비 승률')
    random.seed(9)
    W = {md: [0, 0] for _l, md in CFG[1:]}
    for _ in range(150):
        s = random.randint(2, len(AD) - 30); e = min(s + random.randint(25, 80), len(AD))
        b = run('gap', s % 5, start=s, end=e)
        for md in W:
            x = run(md, s % 5, start=s, end=e)
            if x and b:
                if x[0] > b[0]: W[md][0] += 1
                if x[1] > b[1]: W[md][1] += 1
    for lbl, md in CFG[1:]:
        print('  %-16s 수익 승률 %3.0f%% · 낙폭 승률 %3.0f%%'
              % (lbl, W[md][0] / 1.5, W[md][1] / 1.5))

    print('\n■ 5. 회전율 (교체일당 바뀌는 종목 수 / 평균 보유)')
    grid = [i for i in range(2, len(AD)) if i % R == 0]
    for lbl, md in CFG:
        P = picks(md); prev = None; ch = []
        seen = {}; runs = []
        pset = set()
        for i in grid:
            cur = set(P[AD[i]][:N])
            if prev is not None:
                ch.append(len(cur - prev))
            for t in cur:
                seen[t] = seen.get(t, 0) + 1
            for t in (pset - cur):
                runs.append(seen.get(t, 0)); seen[t] = 0
            prev = cur; pset = cur
        runs += [v for v in seen.values() if v]
        print('  %-16s 교체당 %.1f종목 · 평균 %.1f회 연속(%.0f거래일)'
              % (lbl, np.mean(ch), np.mean(runs), np.mean(runs) * R))

    print('\n■ 6. 오늘(%s) top5' % AD[-1])
    for lbl, md in CFG:
        print('  %-16s %s' % (lbl, ', '.join(picks(md)[AD[-1]][:5])))
