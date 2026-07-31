# -*- coding: utf-8 -*-
"""게이트 전면 재스윕 — look-ahead 수정 후 (2026-07-31)

원칙:
  ① 모든 측정은 exec_lag=1 (신호 판정 다음날 체결). lag=0은 낙관 편향.
  ② 강세장 평균만 보면 보험료만 보이고 보험금이 안 보인다 →
     전체 성적 + 스트레스 구간(6/15~, 두 전략이 갈라진 뒤 유일한 하락 표본) 병기.
  ③ 종목 단위 꼬리(최악 1종목·큰손실 비율)도 본다 — 사고는 평균이 아니라 꼬리에서 난다.
"""
import sys, os
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
from _prod_faithful_bt_2026_07_31 import load

FULL, AG, FUND = load(); TE = C._load_te('full'); AD, _, _, TC, _ = C._load()
PX = {d: {t: v['px'] for t, v in FULL.get(d, {}).items() if v['px']} for d in AD}
STRESS = '2026-06-15'


def rank_at(d, ms_thr=0.0, gap_thr=None, pe=30.0, dv=1000.0, N=5):
    """ms_thr=None이면 min_seg 미적용, gap_thr=None이면 gap 미적용."""
    cand = []
    for tk, v in FULL.get(d, {}).items():
        if not C._industry_ok(tk, TC): continue
        p, nc = v['px'], v['nc']
        if dv is not None and (v['dv'] is None or v['dv'] < dv): continue
        if nc <= 0 or (v['n90'] or 0) <= 0.1: continue
        if pe is not None and p / nc > pe: continue
        if p < 10 or (v['na'] or 0) < 3: continue
        if (nc - (v['n90'] or 0)) / abs(v['n90'] or 1) <= 0: continue
        om, fcf, roe = FUND.get(tk, (None, None, None))
        if om is not None and om < 0.05: continue
        if fcf is not None and roe is not None and fcf < 0 and roe < 0: continue
        if ms_thr is not None:
            ms = min(C._ms(dict(nc=nc, n7=v['n7'], n30=v['n30'], n60=v['n60'], n90=v['n90'])), 0)
            if ms < ms_thr: continue
        if gap_thr is not None:
            te = C._pit_te(TE, tk, d); g = (nc / te) if (te and te > 0) else None
            if g is not None and g < gap_thr: continue
        a = AG.get(d, {}).get(tk)
        if a is None: continue
        cand.append((a, tk))
    cand.sort()
    return [t for _, t in cand[:N]]


_C = {}
def picks(**kw):
    k = tuple(sorted(kw.items()))
    if k not in _C:
        _C[k] = {d: rank_at(d, **kw) for d in AD}
    return _C[k]


def run(P, R=5, phase=0, N=5, start=2, end=None):
    lo, hi = start, (end if end else len(AD))
    hold = []; pend = None; rets = []; poss = []
    for i in range(lo, hi):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        rets.append(sum((px[t] / ppx[t] - 1) / N for t in hold
                        if t in px and t in ppx and ppx[t] > 0))
        if pend is not None and pend[0] == i:
            hold = pend[1]; pend = None
        if i % R == phase:
            pend = (i + 1, P[d])                       # ★exec_lag=1
            for t in P[d]:                             # 종목 단위 5일 수익
                j = min(i + 6, len(AD) - 1)
                a = PX.get(AD[i + 1], {}).get(t) if i + 1 < len(AD) else None
                b = PX.get(AD[j], {}).get(t)
                if a and b: poss.append((b / a - 1) * 100)
    r = np.array(rets)
    if not len(r): return None
    nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return dict(ret=float(nav[-1] - 1) * 100, mdd=float((nav / pk - 1).min()) * 100,
                poss=np.array(poss) if poss else np.array([0.0]))


def evaluate(label, **kw):
    P = picks(**kw)
    si = next(i for i, d in enumerate(AD) if d >= STRESS)
    full = [run(P, phase=p) for p in range(5)]
    strs = [run(P, phase=p, start=si) for p in range(5)]
    YRS = (len(AD) - 2) / 252.0
    ret = np.mean([x['ret'] for x in full]); mdd = np.mean([x['mdd'] for x in full])
    cal = (((1 + ret/100) ** (1/YRS) - 1) * 100) / abs(mdd) if mdd else 0
    sret = np.mean([x['ret'] for x in strs]); smdd = np.mean([x['mdd'] for x in strs])
    allp = np.concatenate([x['poss'] for x in full])
    return dict(label=label, ret=ret, mdd=mdd, cal=cal, sret=sret, smdd=smdd,
                worst=allp.min(), bad=(allp <= -10).mean() * 100)


def show(title, rows, base=None):
    print('\n=== %s ===' % title)
    print('%-20s %8s %8s %7s | %9s %8s | %8s %7s'
          % ('', '수익%', 'MDD%', 'Calmar', '스트레스', 'MDD%', '최악종목', '큰손실%'))
    print('-' * 92)
    for r in rows:
        mk = ' ←현행' if base and r['label'] == base else ''
        print('%-20s %+8.1f %+8.1f %7.2f | %+9.1f %+8.1f | %+8.1f %6.0f%%%s'
              % (r['label'], r['ret'], r['mdd'], r['cal'], r['sret'], r['smdd'],
                 r['worst'], r['bad'], mk))


if __name__ == '__main__':
    BASEKW = dict(ms_thr=0.0, gap_thr=None, pe=30.0, dv=1000.0)
    print('전 게이트 재스윕 · exec_lag=1 · 괴리율 순위 · N5 R5 · 위상평균')
    print('스트레스 구간 = %s~%s (두 전략이 갈라진 뒤 유일한 하락 표본)' % (STRESS, AD[-1]))

    rows = []
    for lbl, v in [('없음(해제)', None), ('>= -5%', -5.0), ('>= -2%', -2.0),
                   ('>= 0% (현행)', 0.0)]:
        kw = dict(BASEKW); kw['ms_thr'] = v
        rows.append(evaluate(lbl, **kw))
    show('① min_seg 임계', rows, '>= 0% (현행)')

    rows = []
    for lbl, v in [('없음 (현행)', None), ('>= 1.0', 1.0), ('>= 1.5', 1.5),
                   ('>= 2.0', 2.0), ('>= 2.5', 2.5)]:
        kw = dict(BASEKW); kw['gap_thr'] = v
        rows.append(evaluate(lbl, **kw))
    show('② gap 임계', rows, '없음 (현행)')

    rows = []
    for lbl, v in [('없음', None), ('<= 20', 20.0), ('<= 25', 25.0),
                   ('<= 30 (현행)', 30.0), ('<= 40', 40.0)]:
        kw = dict(BASEKW); kw['pe'] = v
        rows.append(evaluate(lbl, **kw))
    show('③ 선행PER 임계', rows, '<= 30 (현행)')

    rows = []
    for lbl, v in [('없음', None), ('$0.5B', 500.0), ('$0.75B', 750.0),
                   ('$1B (현행)', 1000.0), ('$1.5B', 1500.0), ('$2B', 2000.0)]:
        kw = dict(BASEKW); kw['dv'] = v
        rows.append(evaluate(lbl, **kw))
    show('④ 거래대금 임계', rows, '$1B (현행)')
