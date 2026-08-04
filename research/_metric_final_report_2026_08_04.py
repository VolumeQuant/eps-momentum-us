# -*- coding: utf-8 -*-
"""지표 3변형 최종 비교 보고 (2026-08-04)

사용자: "다시 제대로 보고해. LOWO 테스트도 포함해서."
경위: 앞선 보고에서 ①위상평균 값과 단일경로(매매내역) 값을 섞어 제시 ②재계산 스크립트 오류로
      말이 안 되는 수치 출력 — 두 실수 모두 결론은 같았으나 신뢰를 깎았다. 여기서 한 번에 정리한다.

비교 대상 (게이트 체인 동일 고정: dv>=300 · PER<=30 · min_seg>=-2 · ru>=1 · 매출>=10% · dedup)
  A 괴리율(현행)  B rev90  C rev90 + 트레일링스탑 15%
공통: 2026-02-12~08-03 · N5 · R5 · exec_lag=1 · 균등 20% · 수수료 미반영

보고 항목
  1) 위상 0~4 전부 (시작일을 언제 잡느냐에 따른 편차 = 단일 경로의 운)
  2) LOWO — SNDK/MU/STX를 유니버스에서 제거했을 때 (슈퍼위너 의존 검사)
  3) 구간 분해 (2~5월 상승 / 6~7월 조정)
  4) 랜덤 진입창 150회 승률
  5) 거래 통계 (회전·승률·집중도)
"""
import sys, os, random, collections
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import _metric_ts_matrix_2026_08_03 as M
from _trade_log_dump_2026_08_04 import trades

AD = M.AD
YRS = (len(AD) - 2) / 252.0
CFG = [('A 괴리율(현행)', 'gap', None),
       ('B rev90', 'rev90', None),
       ('C rev90+TS15%', 'rev90', ('ts', 0.15))]


def cal(t, m, yrs=YRS):
    return (((1 + t / 100) ** (1 / yrs) - 1) * 100) / abs(m) if m else 0


def avg(mt, ts, **kw):
    o = [x for x in (M.run(mt, ts, p, **kw) for p in range(5)) if x]
    return np.mean([x[0] for x in o]), np.mean([x[1] for x in o])


if __name__ == '__main__':
    print('기간 %s ~ %s (%d거래일) · N5 R5 · exec_lag=1 · 수수료 미반영\n'
          % (AD[2], AD[-1], len(AD) - 2))

    print('■ 1. 위상별 전체 (시작일을 5가지로 바꿔본 것)')
    print('%-16s %s %11s %11s' % ('', ''.join('%9s' % ('위상%d' % p) for p in range(5)),
                                  '평균수익', '평균MDD'))
    print('-' * 78)
    for lbl, mt, ts in CFG:
        o = [M.run(mt, ts, p) for p in range(5)]
        print('%-16s %s %+11.1f %+11.1f'
              % (lbl, ''.join('%+9.1f' % x[0] for x in o),
                 np.mean([x[0] for x in o]), np.mean([x[1] for x in o])))

    print('\n■ 2. LOWO — 큰 수익 종목을 유니버스에서 빼면 (위상평균)')
    EXS = [((), '전체'), (('SNDK',), 'ex-SNDK'), (('MU',), 'ex-MU'),
           (('SNDK', 'MU'), 'ex-둘다'), (('SNDK', 'MU', 'STX'), 'ex-셋')]
    print('%-16s %s' % ('', ''.join('%14s' % l for _e, l in EXS)))
    print('-' * 88)
    for lbl, mt, ts in CFG:
        row = ''
        for ex, _l in EXS:
            t, m = avg(mt, ts, exclude=frozenset(ex))
            row += '%14s' % ('%+.0f%% / %.1f' % (t, cal(t, m)))
        print('%-16s %s' % (lbl, row))
    print('  (표기 = 수익% / Calmar)')

    print('\n■ 3. 구간 분해 (위상평균)')
    si = next(i for i, d in enumerate(AD) if d >= '2026-06-01')
    print('%-16s %22s %22s' % ('', '2~5월 (상승)', '6~7월 (조정)'))
    print('-' * 64)
    for lbl, mt, ts in CFG:
        t1, m1 = avg(mt, ts, end=si)
        t2, m2 = avg(mt, ts, start=si + 1)
        print('%-16s %10.1f%% (MDD %5.1f) %10.1f%% (MDD %5.1f)' % (lbl, t1, m1, t2, m2))

    print('\n■ 4. 랜덤 진입창 150회 — 괴리율(현행) 대비 승률')
    random.seed(3)
    W = {(mt, ts): [0, 0] for _l, mt, ts in CFG[1:]}
    for _ in range(150):
        s = random.randint(2, len(AD) - 30); e = min(s + random.randint(25, 80), len(AD))
        b = M.run('gap', None, s % 5, start=s, end=e)
        for k in W:
            x = M.run(k[0], k[1], s % 5, start=s, end=e)
            if x and b:
                if x[0] > b[0]: W[k][0] += 1
                if x[1] > b[1]: W[k][1] += 1
    for lbl, mt, ts in CFG[1:]:
        w = W[(mt, ts)]
        print('  %-16s 수익 승률 %3.0f%% · 낙폭(MDD) 승률 %3.0f%%' % (lbl, w[0] / 1.5, w[1] / 1.5))

    print('\n■ 5. 거래 통계 (위상0 단일 경로 = 매매내역과 동일)')
    print('%-16s %6s %7s %9s %8s %12s' % ('', '거래수', '승률', '평균수익', '평균보유', '상위3거래비중'))
    print('-' * 66)
    for lbl, mt, ts in CFG:
        tl = trades(mt, ts=(ts[1] if ts else None))
        rs = [x[5] for x in tl]
        win = sum(1 for r in rs if r > 0)
        top3 = sum(sorted(rs, reverse=True)[:3])
        print('%-16s %6d %6.0f%% %+8.1f%% %7.0f일 %11.0f%%'
              % (lbl, len(tl), win / len(rs) * 100, np.mean(rs),
                 np.mean([x[6] for x in tl]), top3 / sum(rs) * 100 if sum(rs) else 0))
