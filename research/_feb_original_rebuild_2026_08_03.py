# -*- coding: utf-8 -*-
"""2월 최초 버전 복원 시도 (2026-08-03)

사용자: "내가 말하는 건 5,6월 규칙이 아니라 2월 버전이다."

앞선 측정이 쓴 DB의 part2_rank는 5월(v80.9/v80.10)에 당시 최신 산식으로 과거를 덮어쓴 값이라
2월 신호가 아니다. 여기서는 **원시 컬럼(price, ntm_current/7d/30d/60d/90d)에서 2월 산식을
직접 재계산**한다.

2월(v41~v44) 산식과 5월 이후의 차이:
  · fwd_pe_chg 가중치 7d/30d/60d/90d = **0.4/0.3/0.2/0.1** (v80.10에서 0.3/0.1/0.1/0.5로 뒤집힘)
  · 포트폴리오 = 슬롯 3 · 진입 top3 · 이탈 top8 밖 · 균등 (v82에서 2슬롯, v78에서 E3/X8, v72 E5/X12)
  · z-score 상한 100 clamp 존재(v79에서 제거) — 순위에는 거의 영향 없어 생략

★복원의 한계(정직 고지)
  · dir_factor(EPS 가속도)와 eps_quality(min_seg)는 2월 당시 정의가 문서에 남아 있지 않다.
    (eps_quality는 v54/v55=3월 도입, 그 전엔 B correction이라는 다른 보정이었다)
    → 여기서는 **fwd_pe_chg 단독**으로 순위를 매기고, 보조 승수는 넣지 않는다.
    즉 '2월 산식의 핵심 축'을 복원한 것이지 2월 그 자체는 아니다.
  · N일 전 가격은 거래일 기준(5/21/42/63)으로 근사한다(원본은 달력일 조회).
"""
import sys, os, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import _honest_benchmark_2026_08_03 as H

DB = os.path.join(BASE, 'eps_momentum_data.db')
c = sqlite3.connect(DB)
DATES = H.DATES
DI = {d: i for i, d in enumerate(DATES)}

# 종목별 (날짜→가격), (날짜→ntm 5종)
PXT, NTM = {}, {}
for d, tk, p, nc, n7, n30, n60, n90 in c.execute(
        'SELECT date,ticker,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d '
        'FROM ntm_screening WHERE price IS NOT NULL'):
    PXT.setdefault(tk, {})[d] = p
    NTM.setdefault(tk, {})[d] = (nc, n7, n30, n60, n90)
# 유동성/유니버스 근사: 그날 랭킹 대상이었던 종목(part2_rank 산출 대상)만
ELIG = {}
for d, tk in c.execute('SELECT date,ticker FROM ntm_screening WHERE part2_rank IS NOT NULL'):
    ELIG.setdefault(d, set()).add(tk)
c.close()

LAG = {'7d': 5, '30d': 21, '60d': 42, '90d': 63}
W_FEB = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}
W_MAY = {'7d': 0.30, '30d': 0.10, '60d': 0.10, '90d': 0.50}


def fwd_pe_chg(tk, d, W):
    i = DI[d]
    px, nt = PXT.get(tk, {}), NTM.get(tk, {})
    p_now = px.get(d)
    v = nt.get(d)
    if not p_now or not v or not v[0] or v[0] <= 0:
        return None
    pe_now = p_now / v[0]
    s = w = 0.0
    for j, k in enumerate(('7d', '30d', '60d', '90d'), 1):
        i2 = i - LAG[k]
        if i2 < 0:
            continue
        p_then = px.get(DATES[i2])
        n_then = v[j]
        if not p_then or not n_then or n_then <= 0:
            continue
        pe_then = p_then / n_then
        if pe_then <= 0:
            continue
        s += W[k] * (pe_now - pe_then) / pe_then * 100
        w += W[k]
    return s / w if w else None


_C = {}
def ranking(d, W, key):
    if (d, key) not in _C:
        rows = []
        for tk in ELIG.get(d, ()):
            g = fwd_pe_chg(tk, d, W)
            if g is not None:
                rows.append((g, tk))
        rows.sort()                       # PE 압축이 큰(음수) 순
        _C[(d, key)] = [tk for _g, tk in rows]
    return _C[(d, key)]


def sim(W, key, slots, entry, exit_rank, exclude=frozenset(), ts=None):
    """2월 규칙: 슬롯 N · 진입 top{entry} · 이탈 {exit_rank} 밖 · 균등 · exec_lag=1"""
    hold, pend, rets = [], None, []
    peak, stopped = {}, set()
    for i in range(1, len(DATES)):
        d, pv = DATES[i], DATES[i - 1]
        px = {t: PXT.get(t, {}).get(d) for t in hold}
        ppx = {t: PXT.get(t, {}).get(pv) for t in hold}
        live = [t for t in hold if t not in stopped]
        rets.append(sum((px[t] / ppx[t] - 1) / slots for t in live
                        if px.get(t) and ppx.get(t)))
        if ts:
            for t in live:
                p = px.get(t)
                if not p:
                    continue
                peak[t] = max(peak.get(t) or p, p)
                if p / peak[t] - 1 <= -ts:
                    stopped.add(t)
        if pend is not None and pend[0] == i:
            hold = pend[1]; pend = None
            peak = {t: PXT.get(t, {}).get(d) for t in hold}
            stopped = set()
        R = ranking(d, W, key)
        pos = {tk: n + 1 for n, tk in enumerate(R)}
        keep = [t for t in hold if t not in stopped and pos.get(t, 9999) <= exit_rank]
        cand = [t for t in R[:entry] if t not in keep and t not in exclude]
        nxt = (keep + cand)[:slots]
        if set(nxt) != set(hold):
            pend = (i + 1, nxt)
    return rets


if __name__ == '__main__':
    yrs = len(DATES) / 252.0
    EX = frozenset(('SNDK', 'MU'))
    print('기간 %s ~ %s (%d일) · exec_lag=1 · fwd_pe_chg 단독 순위(보조 승수 미복원)'
          % (DATES[0], DATES[-1], len(DATES)))
    print()
    print('%-34s %9s %9s %8s %11s' % ('', '수익%', 'MDD%', 'Calmar', 'ex-MU/SNDK'))
    print('-' * 78)
    cfgs = [
        ('2월 가중치 · 3슬롯 E3/X8', W_FEB, 'feb', 3, 3, 8, None),
        ('2월 가중치 · 2슬롯 E3/X10', W_FEB, 'feb', 2, 3, 10, None),
        ('2월 가중치 · 3슬롯 + TS15%', W_FEB, 'feb', 3, 3, 8, 0.15),
        ('2월 가중치 · 5슬롯 E5/X10', W_FEB, 'feb', 5, 5, 10, None),
        ('5월 가중치 · 3슬롯 E3/X8', W_MAY, 'may', 3, 3, 8, None),
        ('5월 가중치 · 2슬롯 E3/X10', W_MAY, 'may', 2, 3, 10, None),
    ]
    for nm, W, key, s, e, x, ts in cfgs:
        t, m, cal = H.stats(sim(W, key, s, e, x, ts=ts), yrs)
        t2, m2, cal2 = H.stats(sim(W, key, s, e, x, exclude=EX, ts=ts), yrs)
        print('%-34s %+9.1f %+9.1f %8.2f %11.2f' % (nm, t, m, cal, cal2))

    print('\n■ 구간 분해 (2월 가중치 3슬롯 E3/X8)')
    for a, b, lbl in (('2026-02-12', '2026-05-31', '2~5월'), ('2026-06-01', '2026-07-31', '6~7월')):
        idx = [i for i, d in enumerate(DATES) if a <= d <= b]
        if len(idx) < 10:
            continue
        r = sim(W_FEB, 'feb', 3, 3, 8)[idx[0]:idx[-1]]
        t, m, _ = H.stats(r, len(idx) / 252.0)
        print('  %-6s %+9.1f%% · MDD %+6.1f%%' % (lbl, t, m))

    print('\n■ 오늘 기준 2월 산식 top5 vs 현행')
    print('  2월 산식:', ', '.join(ranking(DATES[-1], W_FEB, 'feb')[:5]))
    print('  5월 산식:', ', '.join(ranking(DATES[-1], W_MAY, 'may')[:5]))
