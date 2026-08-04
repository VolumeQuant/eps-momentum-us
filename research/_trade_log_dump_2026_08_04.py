# -*- coding: utf-8 -*-
"""세 변형의 전체 매매내역 덤프 (2026-08-04)

사용자: "2월 시스템 시작일부터 매매내역 전부 각각 보여줘봐."
대상 = 괴리율(현행) / rev90 / rev90+TS15%  — 게이트 체인은 동일 고정.
기간 = 2026-02-12 ~ 07-31, N5 · R5 · exec_lag=1 · 균등 20%.
위상은 0으로 고정(단일 경로여야 매매내역이 하나로 나온다. 성과표의 위상평균과 소수점 차이 있음).
"""
import sys, os
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
from _metric_ts_matrix_2026_08_03 import picks, AD, PX, N, R
import _honest_benchmark_2026_08_03 as H

PHASE = 0


def trades(metric, ts=None):
    """반환: [(진입일, 청산일, 티커, 진입가, 청산가, 수익률%, 보유일, 청산사유)]"""
    P = picks(metric)
    hold, pend, out = {}, None, []
    peak, stopped = {}, set()
    for i in range(2, len(AD)):
        d = AD[i]
        px = PX.get(d, {})
        if ts:
            for t in list(hold):
                if t in stopped:
                    continue
                p = px.get(t)
                if not p:
                    continue
                peak[t] = max(peak.get(t) or p, p)
                if p / peak[t] - 1 <= -ts:
                    # ★스탑 = 포지션 청산. hold에서 제거해야 다음 리밸에 새 진입가로 재진입한다.
                    #   (제거하지 않으면 같은 진입가로 매일 재차 스탑 기록되는 중복 발생)
                    stopped.add(t)
                    e = hold.pop(t)
                    out.append((e[0], d, t, e[1], p, (p / e[1] - 1) * 100,
                                i - e[2], '스탑'))
        if pend is not None and pend[0] == i:
            nxt = pend[1]
            for t in list(hold):
                if t not in nxt:
                    if t in stopped:
                        del hold[t]; continue
                    p = px.get(t)
                    e = hold[t]
                    if p:
                        out.append((e[0], d, t, e[1], p, (p / e[1] - 1) * 100,
                                    i - e[2], '교체'))
                    del hold[t]
            for t in nxt:
                if t not in hold:
                    p = px.get(t)
                    if p:
                        hold[t] = (d, p, i); peak[t] = p
            stopped = set(); pend = None
        if i % R == PHASE:
            pend = (i + 1, P[d][:N])
    last = AD[-1]
    for t, e in hold.items():
        p = PX.get(last, {}).get(t)
        if p:
            out.append((e[0], last, t, e[1], p, (p / e[1] - 1) * 100,
                        len(AD) - 1 - e[2], '보유중'))
    return out


def show(name, tl):
    tl = sorted(tl, key=lambda x: (x[0], x[2]))
    print('\n' + '=' * 78)
    print('■ %s — 총 %d거래' % (name, len(tl)))
    print('%-11s %-11s %-6s %9s %9s %8s %5s %s'
          % ('진입일', '청산일', '종목', '진입가', '청산가', '수익률', '보유', '사유'))
    print('-' * 78)
    for a, b, t, p1, p2, r, hd, why in tl:
        print('%-11s %-11s %-6s %9.2f %9.2f %+7.1f%% %4d일 %s'
              % (a, b, t, p1, p2, r, hd, why))
    rs = [x[5] for x in tl]
    win = [r for r in rs if r > 0]
    print('-' * 78)
    print('승률 %.0f%% (%d승 %d패) · 평균 %+.1f%% · 평균보유 %.0f일 · 최대 %+.1f%% / 최소 %+.1f%%'
          % (len(win) / len(rs) * 100, len(win), len(rs) - len(win),
             np.mean(rs), np.mean([x[6] for x in tl]), max(rs), min(rs)))
    tot = sum(sorted(rs, reverse=True)[:3])
    print('상위 3거래 수익률 합 %+.1f%% (전체 합 %+.1f%% 중 %.0f%%)'
          % (tot, sum(rs), tot / sum(rs) * 100 if sum(rs) else 0))


if __name__ == '__main__':
    print('기간 %s ~ %s · N5 R5 위상0 · exec_lag=1 · 균등 20%%' % (AD[2], AD[-1]))
    show('괴리율 (현행)', trades('gap'))
    show('rev90', trades('rev90'))
    show('rev90 + 트레일링스탑 15%', trades('rev90', ts=0.15))
