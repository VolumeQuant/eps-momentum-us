# -*- coding: utf-8 -*-
"""2026-02 시스템 시작일부터의 전체 거래내역·잔고·수익률·MDD (2026-07-31)

사양: 최종 확정 설정 그대로
  괴리율(adj_gap) 순위 · 미국단독 · 5종목 각 20% · 5거래일 리밸
  게이트: 거래대금 $1B · 선행PER<=30 · min_seg>=-2% · 안전필터 5종 · gap 해제
  ★exec_lag=1 — 신호는 D일 종가로 계산되고 체결은 D+1 종가 (정직 기준)
  ★위상은 실제 라이브 리밸 격자(앵커 2026-07-02, R5)에 정렬
"""
import sys, os
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
from _slots_sweep_2026_07_31 import _RK, AD, PX   # ★이중상장 가드 적용된 랭킹

N, R = 5, 5
CFG = dict(ms_thr=-2.0, gap_thr=None, pe=30.0, dv=1000.0, N=N)
START_CAP = 100.0


def live_phase():
    """실제 라이브 리밸 격자(2026-07-02 앵커)와 같은 위상 찾기."""
    try:
        import unified_vm_track as u
        g = u._us_grid()
        if g and g[0] in AD:
            return AD.index(g[0]) % R
    except Exception:
        pass
    return 2


def main():
    ph = live_phase()
    print('=' * 74)
    print(' 미국 시스템 — 2026년 2월 시작 가정 전체 거래내역')
    print(' 괴리율 순위 · 5종목 각 20% · 5거래일 교체 · 실행지연 1일 반영')
    print(' 위상 %d · 이중상장 중복제거 적용 · 시작자금 100' % ph)
    print('=' * 74)

    nav = START_CAP
    navs = []
    hold = {}          # ticker -> (진입일, 진입가)
    pend = None
    trades = []
    for i in range(2, len(AD)):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        if hold:
            rr = [(px[t] / ppx[t] - 1) for t in hold if t in px and t in ppx and ppx[t] > 0]
            if rr:
                nav *= 1 + sum(rr) / N
        navs.append((d, nav))
        if pend is not None and pend[0] == i:
            new = pend[1]; pend = None
            for t in list(hold):
                if t not in new:
                    d0, p0 = hold.pop(t)
                    p1 = px.get(t)
                    if p1:
                        trades.append(dict(tk=t, d0=d0, d1=d, p0=p0, p1=p1,
                                           ret=(p1 / p0 - 1) * 100,
                                           days=AD.index(d) - AD.index(d0)))
            for t in new:
                if t not in hold and px.get(t):
                    hold[t] = (d, px[t])
        if i % R == ph:
            pend = (i + 1, _RK[d][:N])

    last = AD[-1]
    for t, (d0, p0) in hold.items():
        p1 = PX.get(last, {}).get(t)
        if p1:
            trades.append(dict(tk=t, d0=d0, d1='보유중', p0=p0, p1=p1,
                               ret=(p1 / p0 - 1) * 100,
                               days=len(AD) - 1 - AD.index(d0)))

    print('\n【 거래 내역 】  총 %d건' % len(trades))
    print('%-6s %-11s %-11s %5s %10s %10s %9s' %
          ('종목', '매수일', '매도일', '보유일', '매수가', '매도가', '수익률'))
    print('-' * 74)
    for t in sorted(trades, key=lambda x: x['d0']):
        print('%-6s %-11s %-11s %5d %10.2f %10.2f %+8.1f%%' %
              (t['tk'], t['d0'], t['d1'], t['days'], t['p0'], t['p1'], t['ret']))

    rets = np.array([t['ret'] for t in trades])
    win = (rets > 0).sum()
    print('-' * 74)
    print('승률 %d/%d (%.0f%%) · 평균 %+.1f%% · 최고 %+.1f%%(%s) · 최악 %+.1f%%(%s)'
          % (win, len(rets), win / len(rets) * 100, rets.mean(),
             rets.max(), trades[int(rets.argmax())]['tk'],
             rets.min(), trades[int(rets.argmin())]['tk']))
    print('평균 보유 %.0f거래일 · 이익합 %+.0f%%p · 손실합 %+.0f%%p'
          % (np.mean([t['days'] for t in trades]),
             rets[rets > 0].sum(), rets[rets <= 0].sum()))

    arr = np.array([v for _, v in navs])
    peak = np.maximum.accumulate(arr)
    dd = arr / peak - 1
    mi = int(dd.argmin())
    pi = int(arr[:mi + 1].argmax())
    yrs = len(arr) / 252
    print('\n【 잔고 · 수익률 】')
    print('  시작(%s)  %8.1f' % (navs[0][0], START_CAP))
    print('  현재(%s)  %8.1f   ← 1억이면 %.2f억' % (navs[-1][0], arr[-1], arr[-1] / 100))
    print('  누적 수익률           %+8.1f%%' % ((arr[-1] / START_CAP - 1) * 100))
    print('  연환산(CAGR)          %+8.1f%%  ※%.2f년 강세장이라 과대' % (((arr[-1] / START_CAP) ** (1 / yrs) - 1) * 100, yrs))
    print('  최고점(%s)           %8.1f' % (navs[int(arr.argmax())][0], arr.max()))
    print('\n【 최대낙폭(MDD) 】')
    print('  %+.1f%%   (%s %.1f → %s %.1f)' % (dd[mi] * 100, navs[pi][0], arr[pi], navs[mi][0], arr[mi]))
    print('  회복 여부: %s' % ('회복 완료' if arr[-1] >= arr[pi] else '미회복 (고점 대비 %+.1f%%)' % ((arr[-1] / arr[pi] - 1) * 100)))

    print('\n【 월별 성과 】')
    bym = {}
    for d, v in navs:
        bym.setdefault(d[:7], []).append(v)
    prev = START_CAP
    print('  %-9s %10s %10s' % ('월', '월수익', '월말잔고'))
    for m in sorted(bym):
        end = bym[m][-1]
        print('  %-9s %+9.1f%% %10.1f' % (m, (end / prev - 1) * 100, end))
        prev = end


if __name__ == '__main__':
    main()
