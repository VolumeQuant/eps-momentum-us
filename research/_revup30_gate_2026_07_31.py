# -*- coding: utf-8 -*-
"""rev_up30 게이트 재검증 (2026-07-31)

계기: 사용자 "NXPI는 정보부족이면서 뭘 5등을 추천해?"
      실측 — TOP5 중 NXPI만 30일 상향 애널 0명 (VRT 2·STX 5·SNDK 6·GOOGL 4).
구시스템엔 rev_up30 >= 3 필터가 있었다(v80.8 '단일 분석가 의존 종목 차단', WELL 사례).
현 VM 게이트 체인엔 없다 — 애널 '커버 수'(num_analysts>=3)만 보고 '최근 상향 활동'은 안 본다.

기준: exec_lag=1(정직) · 괴리율 순위 · N5 · R5 · 위상평균
     전체 성적 + 스트레스 구간(6/15~) + LOWO + 랜덤 진입일 — 헤드라인만 보지 않는다.
"""
import sys, os, random, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
import daily_runner as dr
from _prod_faithful_bt_2026_07_31 import load
import unified_vm_track as U

FULL, AG, FUND = load(); TE = C._load_te('full'); AD, _, _, TC, _ = C._load()
PX = {d: {t: v['px'] for t, v in FULL.get(d, {}).items() if v['px']} for d in AD}
STRESS = '2026-06-15'
# ★2026-08-01 정정: DV 1000 하드코드는 이 스크립트 작성 시점(dv $1B) 값 — 같은 날 dv가
#   $300M로 재정의된 뒤에도 후속 게이트 비교들이 이 상수를 물려받아 구 유니버스에서 측정되는
#   사고 발생(na<=5 종목이 $1B 유동주엔 없어 "na 게이트 영향 0"이라는 착시. 사용자
#   "SNDK, MU 빼보면 되잖아" 도전으로 발견). production 패리티로 정정, env로 재현 가능.
MS, PE, DV = -2.0, 30.0, float(__import__('os').environ.get('VM_DV_MIN', '300'))

# rev_up30 / rev_down30 로드
RU = {}
_c = sqlite3.connect(dr.DB_PATH)
for tk, d, u30, d30 in _c.execute(
        'SELECT ticker,date,rev_up30,rev_down30 FROM ntm_screening WHERE rev_up30 IS NOT NULL'):
    RU.setdefault(d, {})[tk] = (u30 or 0, d30 or 0)
_c.close()


def ranked(d, up_min=0):
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
        if up_min:                       # ★검증 대상 게이트
            u30, _d30 = RU.get(d, {}).get(tk, (0, 0))
            if u30 < up_min: continue
        a = AG.get(d, {}).get(tk)
        if a is None: continue
        cand.append((a, tk))
    cand.sort()
    seen, out = set(), []
    for _, tk in cand:                   # 이중상장 가드 (production 동일)
        k = U.DUAL_CLASS.get(tk, tk)
        if k in seen: continue
        seen.add(k); out.append(tk)
    return out


_CK = {}
def picks(up_min):
    if up_min not in _CK:
        _CK[up_min] = {d: ranked(d, up_min) for d in AD}
    return _CK[up_min]


def run(up_min, N=5, R=5, phase=0, start=2, end=None, exclude=frozenset()):
    P = picks(up_min)
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
            pend = (i + 1, [t for t in P[d] if t not in exclude][:N])
    r = np.array(rets)
    if not len(r): return None
    nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100


def avg(up_min, **kw):
    o = [x for x in (run(up_min, phase=p, **kw) for p in range(5)) if x]
    return np.mean([x[0] for x in o]), np.mean([x[1] for x in o])


YRS = (len(AD) - 2) / 252.0
def cal(r, m): return (((1 + r/100) ** (1/YRS) - 1) * 100) / abs(m) if m else 0

if __name__ == '__main__':
    si = next(i for i, d in enumerate(AD) if d >= STRESS)
    LV = [0, 1, 2, 3, 5]

    print('rev_up30 게이트 스윕 (0 = 미적용, 현행)')
    print('%-10s %9s %9s %8s | %9s %9s | %8s' %
          ('임계', '수익%', 'MDD%', 'Calmar', '스트레스', 'MDD%', '후보수'))
    print('-' * 76)
    for v in LV:
        r, m = avg(v); sr, sm = avg(v, start=si)
        npk = np.mean([len(picks(v)[d]) for d in AD[-20:]])
        print('%-10s %+9.1f %+9.1f %8.2f | %+9.1f %+9.1f | %8.1f%s'
              % ('>= %d' % v if v else '미적용', r, m, cal(r, m), sr, sm, npk,
                 '  ←현행' if v == 0 else ''))

    print('\nLOWO (Calmar)')
    print('%-18s %s' % ('제외', ''.join('%10s' % (('>=%d' % v) if v else '미적용') for v in LV)))
    for ex in [(), ('SNDK',), ('MU',), ('SNDK', 'MU'), ('SNDK', 'MU', 'STX')]:
        e = frozenset(ex)
        row = ''.join('%10.2f' % cal(*avg(v, exclude=e)) for v in LV)
        print('%-18s %s' % ('ex-' + ('/'.join(ex) if ex else '없음'), row))

    print('\n랜덤 진입일 150회 — 미적용(현행) 대비 승률')
    random.seed(5)
    W = {v: [0, 0] for v in LV if v}
    for _ in range(150):
        s = random.randint(2, len(AD) - 30); e = min(s + random.randint(25, 80), len(AD))
        b = run(0, phase=s % 5, start=s, end=e)
        for v in W:
            x = run(v, phase=s % 5, start=s, end=e)
            if x and b:
                if x[0] > b[0]: W[v][0] += 1
                if x[1] > b[1]: W[v][1] += 1
    for v in sorted(W):
        print('  >=%d  수익승률 %3.0f%% | MDD승률 %3.0f%%' % (v, W[v][0]/1.5, W[v][1]/1.5))

    print('\n오늘(%s) 기준 TOP5 변화' % AD[-1])
    for v in LV:
        print('  %-8s %s' % (('>=%d' % v) if v else '미적용', ', '.join(picks(v)[AD[-1]][:5])))
