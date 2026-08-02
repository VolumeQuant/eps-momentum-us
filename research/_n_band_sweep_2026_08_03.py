# -*- coding: utf-8 -*-
"""N(슬롯)×보유밴드(이탈선) 결합 스윕 (2026-08-03, 사용자 "4·3종목 성과 + 진입/이탈/슬롯 결합 최적화 측정").

기준: 현행 production 패리티 체인(_campaign_lib: dv300·PER30·ms-2·ru1·na6·가치함정·dedup)
     + 8/2 배포 안전장치 2종(dd_30_25 진입유예 · 보유밴드) 재현.
     exec_lag=1 · R5 · 위상 0~4 평균 · 스트레스(6/15~) · LOWO 3종.
⚠️측정 전용 — N5/밴드7은 구조 파라미터로 판정일 안건. 이 스윕으로 즉시 변경하지 않는다.
"""
import sys, os
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import _campaign_lib as L

AD, PX, IDX, YRS = L.AD, L.PX, L.IDX, L.YRS
STRESS_I = next(i for i, d in enumerate(AD) if d >= L.STRESS)
DD_THR = -25.0

# 30세션 고점 (v84 high30: 오늘 포함 30개 날짜 최고가) — production HI30 정의 동일
HI30 = {}
for i, d in enumerate(AD):
    lo = max(0, i - 29)
    h = {}
    for j in range(lo, i + 1):
        for t, p in PX.get(AD[j], {}).items():
            if p and p > h.get(t, 0):
                h[t] = p
    HI30[d] = h

P = L.base_picks()   # 날짜별 전체 랭킹(괴리율 오름차순, 게이트 통과자 전원)


def _dd_blocked(d, t):
    p = PX.get(d, {}).get(t)
    h = HI30.get(d, {}).get(t)
    return bool(p and h and (p / h - 1) * 100 <= DD_THR)


def run(N, band, phase, dd=True, ex=frozenset(), start=2, end=None, R=5):
    """진입 topN / 보유 순위<=band 유지 / 빈슬롯 충원 시 dd_30_25 스킵 / lag=1."""
    hold = []; pend = None; rets = []
    hi = end if end else len(AD)
    for i in range(start, hi):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        rets.append(sum((px[t] / ppx[t] - 1) / N for t in hold
                        if t in px and t in ppx and ppx[t] > 0))
        if pend is not None and pend[0] <= i:
            hold = pend[1]; pend = None
        if i % R == phase:
            lst = [t for t in P[d] if t not in ex]
            rank = {t: k + 1 for k, t in enumerate(lst)}
            sel = [t for t in hold if rank.get(t, 10**9) <= band][:N]
            for t in lst:
                if len(sel) >= N:
                    break
                if t in sel or (dd and _dd_blocked(d, t)):
                    continue
                sel.append(t)
            pend = (i + 1, sel)
    r = np.array(rets)
    if not len(r):
        return None
    nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100


def avg(N, band, **kw):
    o = [x for x in (run(N, band, p, **kw) for p in range(5)) if x]
    return np.mean([x[0] for x in o]), np.mean([x[1] for x in o])


def cal(r, m):
    return (((1 + r / 100) ** (1 / YRS) - 1) * 100) / abs(m) if m else 0


def turnover(N, band, **kw):
    """리밸당 평균 교체 종목 수 (위상평균)."""
    tos = []
    for phase in range(5):
        hold = []; pend = None; cnt = [];
        for i in range(2, len(AD)):
            d = AD[i]
            if pend is not None and pend[0] <= i:
                hold = pend[1]; pend = None
            if i % 5 == phase:
                lst = P[d]
                rank = {t: k + 1 for k, t in enumerate(lst)}
                sel = [t for t in hold if rank.get(t, 10**9) <= band][:N]
                for t in lst:
                    if len(sel) >= N:
                        break
                    if t in sel or _dd_blocked(d, t):
                        continue
                    sel.append(t)
                cnt.append(len(set(sel) - set(hold)) if hold else 0)
                pend = (i + 1, sel)
        if cnt:
            tos.append(np.mean(cnt[1:]) if len(cnt) > 1 else 0)
    return float(np.mean(tos))


if __name__ == '__main__':
    grid = [(N, b) for N in (5, 4, 3) for b in range(N, N + 5)]
    print('N(슬롯) x 보유밴드 결합 스윕 — 현행 체인+dd유예, lag=1, 위상평균, %d일' % (len(AD) - 2))
    print('%-9s %8s %7s %7s | %8s %7s | %5s | %s' %
          ('N/밴드', '수익%', 'MDD%', 'Calmar', '스트레스', 'MDD%', '교체', 'LOWO Cal(exS / exM / ex3)'))
    print('-' * 100)
    RES = {}
    for N, b in grid:
        r, m = avg(N, b); sr, sm = avg(N, b, start=STRESS_I)
        lo = []
        for ex in [('SNDK',), ('MU',), ('SNDK', 'MU', 'STX')]:
            lr, lm = avg(N, b, ex=frozenset(ex)); lo.append(cal(lr, lm))
        to = turnover(N, b)
        RES[(N, b)] = (r, m, cal(r, m), sr, sm, lo)
        tag = '  <- 현행' if (N, b) == (5, 7) else ''
        print('%d / %-4d %+8.1f %+7.1f %7.2f | %+8.1f %+7.1f | %5.2f | %6.2f %6.2f %6.2f%s'
              % (N, b, r, m, cal(r, m), sr, sm, to, lo[0], lo[1], lo[2], tag))

    # 밴드 없는(진입=이탈) 순수 topN 참고선
    print('\n[참고] 밴드 없음(진입=이탈 topN, dd유예만) — 8/2 이전 구조')
    for N in (5, 4, 3):
        r, m = avg(N, N); sr, sm = avg(N, N, start=STRESS_I)
        print('  top%d: %+8.1f / MDD %+6.1f / Cal %6.2f | 스트레스 %+7.1f / %+6.1f' % (N, r, m, cal(r, m), sr, sm))

    # dd유예 없는 변형(순수 N×밴드 효과 분리)
    print('\n[분해] dd유예 OFF (N x 밴드만)')
    for N, b in [(5, 7), (4, 6), (4, 5), (3, 5), (3, 4)]:
        r, m = avg(N, b, dd=False)
        print('  N%d/밴드%d: %+8.1f / MDD %+6.1f / Cal %6.2f' % (N, b, r, m, cal(r, m)))
