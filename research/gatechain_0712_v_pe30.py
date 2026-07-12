# -*- coding: utf-8 -*-
"""적대적 검증 (2026-07-12): keep-pe-30 권고 재검.

검증 대상 주장 (gatechain_0712_agenda2.py C-1):
  1) pe30이 LOWO 고원(30~40: exSM 74.5/82.4/90.5) 위에 있고
  2) MDD 최선(-17.7 vs pe35 -19.2, pe40 -20.9, inf -25.1)
  3) pe25는 raw +121.6이나 exSM +40.6 붕괴

적대 포인트:
  A) 원 그리드 재현(동일 격자점 pe 15~inf @ gap1.5, LOWO 포함) — 수치 일치 여부
  B) 세분화 격자(pe 22/25/27/28/30/32/35) — pe30이 고원 중앙인가 25→30 사이 절벽의
     '운 좋은 첫 점'인가, exSM 붕괴가 25 단일점인가 연속 구조인가
  C) LOWO MDD — MDD 우위(-17.7)가 winner 포함 착시인지 exSM에서도 유지되는지
읽기 전용, DB 쓰기 없음. agenda2 C-1과 동일 규약(full TE, N5, dv1000, agroup 없음, 위상 0~4 평균).
"""
import sys, os, json
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.dirname(HERE)
sys.path.insert(0, BASE)
sys.path.insert(0, HERE)
import vm_canonical_bt as vc

AD, FULL, DVDB, TC, TE_SPARSE = vc._load()
TE_FULL = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm_full.json'), encoding='utf-8'))
TE_FULL.pop('_meta', None)


def bt(pe_max=30, gap_thr=1.5, N=5, R=5, start=2, dv_min=1000.0, phase=0, exclude=frozenset()):
    hold = []; rets = []
    for i in range(start, len(AD)):
        d, pv = AD[i], AD[i - 1]
        px = FULL.get(d, {}); ppx = FULL.get(pv, {})
        drr = 0.0
        for t in hold:
            cu = px.get(t, {}).get('px'); pp = ppx.get(t, {}).get('px')
            if cu and pp and pp > 0:
                drr += (1.0 / N) * (cu - pp) / pp
        rets.append(drr)
        if i % R == phase:
            cand = []
            for tk, v in FULL.get(d, {}).items():
                if tk in exclude or not vc._industry_ok(tk, TC):
                    continue
                dv = DVDB.get(d, {}).get(tk)
                if dv is None or dv < dv_min:
                    continue
                if vc._ms(v) < 0:
                    continue
                if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1:
                    continue
                if v['px'] / v['nc'] > pe_max:
                    continue
                if gap_thr:
                    te_v = vc._pit_te(TE_FULL, tk, d)
                    g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
                    if g is not None and g < gap_thr:
                        continue
                cand.append((tk, vc._rev90(v)))
            cand.sort(key=lambda x: -x[1])
            hold = [t for t, _ in cand[:N]]
    r = np.array(rets)
    nav = np.cumprod(1 + r); peak = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / peak - 1).min()) * 100


def pavg(**kw):
    ph = [bt(phase=p, **kw) for p in range(5)]
    return (float(np.mean([x[0] for x in ph])), float(np.mean([x[1] for x in ph])))


PES = [15, 20, 22, 25, 27, 28, 30, 32, 35, 40, 60, 10 ** 9]
print('═══ pe 스윕 @gap1.5/N5/dv1000 — 위상평균 raw + LOWO(수익/MDD 모두) ═══')
print(f"{'pe':>6} | {'raw ret/mdd':>16} | {'exSNDK':>16} | {'exMU':>16} | {'exBOTH':>16}")
for pe in PES:
    lbl = 'inf' if pe > 1e6 else str(pe)
    raw = pavg(pe_max=pe)
    exS = pavg(pe_max=pe, exclude=frozenset({'SNDK'}))
    exM = pavg(pe_max=pe, exclude=frozenset({'MU'}))
    exSM = pavg(pe_max=pe, exclude=frozenset({'SNDK', 'MU'}))
    def f(t):
        return f'{t[0]:+7.1f}/{t[1]:+6.1f}'
    print(f'{lbl:>6} | {f(raw):>16} | {f(exS):>16} | {f(exM):>16} | {f(exSM):>16}')
print('\n완료.')
