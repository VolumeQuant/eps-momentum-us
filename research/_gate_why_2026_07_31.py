# -*- coding: utf-8 -*-
"""gap·min_seg 게이트가 괴리율 순위에서 왜 해로운가 — 인과 분해 (2026-07-31).
사용자 질문: "중복이라 나쁘다"는 추론 말고 실제로 뭐가 잘리고 그게 얼마나 손해인지 측정.
"""
import sys, os
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
from _prod_faithful_bt_2026_07_31 import load

FULL, AG, FUND = load(); TE = C._load_te('full'); AD, _, _, TC, _ = C._load()


def base_pool(d):
    """dv/PER/안전필터까지 통과한 후보 + 각 게이트 판정값"""
    out = []
    for tk, v in FULL.get(d, {}).items():
        if not C._industry_ok(tk, TC): continue
        p, nc = v['px'], v['nc']
        if v['dv'] is None or v['dv'] < 1000.0: continue
        if nc <= 0 or (v['n90'] or 0) <= 0.1: continue
        if p / nc > 30.0 or p < 10 or (v['na'] or 0) < 3: continue
        if (nc - (v['n90'] or 0)) / abs(v['n90'] or 1) <= 0: continue
        om, fcf, roe = FUND.get(tk, (None, None, None))
        if om is not None and om < 0.05: continue
        if fcf is not None and roe is not None and fcf < 0 and roe < 0: continue
        a = AG.get(d, {}).get(tk)
        if a is None: continue
        ms = min(C._ms(dict(nc=nc, n7=v['n7'], n30=v['n30'], n60=v['n60'], n90=v['n90'])), 0)
        te = C._pit_te(TE, tk, d); g = (nc / te) if (te and te > 0) else None
        out.append(dict(tk=tk, ag=a, ms=ms, gap=g, px=p,
                        rev90=(nc - v['n90']) / abs(v['n90']) * 100))
    return out


def fwd(tk, i, h=5):
    j = i + h
    if j >= len(AD): return None
    a = FULL.get(AD[i], {}).get(tk, {}).get('px'); b = FULL.get(AD[j], {}).get(tk, {}).get('px')
    return (b / a - 1) * 100 if a and b else None


if __name__ == '__main__':
    print('=== ① 괴리율(adj_gap)이 정말 min_seg를 이미 반영하나 ===')
    xs, ys, gs = [], [], []
    for i in range(2, len(AD)):
        for r in base_pool(AD[i]):
            xs.append(r['ag']); ys.append(r['ms'])
            if r['gap'] is not None: gs.append((r['ag'], r['gap']))
    xs, ys = np.array(xs), np.array(ys)
    print('  adj_gap ↔ min_seg 상관: %+.3f  (n=%d)' % (np.corrcoef(xs, ys)[0, 1], len(xs)))
    ga = np.array([x[0] for x in gs]); gb = np.array([x[1] for x in gs])
    print('  adj_gap ↔ gap      상관: %+.3f  (n=%d)' % (np.corrcoef(ga, gb)[0, 1], len(gb)))
    print('  → 상관이 크면 "이미 반영"(중복), 작으면 "무관한 축을 잘라내는 것"')

    print('\n=== ② 각 게이트가 괴리율 top5를 실제로 몇 번 자르나 ===')
    for gate in ('min_seg', 'gap'):
        blocked = kept = 0; bl_names = {}
        for i in range(2, len(AD)):
            if i % 5 != 0: continue
            pool = sorted(base_pool(AD[i]), key=lambda r: r['ag'])[:5]
            for r in pool:
                bad = (r['ms'] < 0) if gate == 'min_seg' else (r['gap'] is not None and r['gap'] < 1.5)
                if bad:
                    blocked += 1; bl_names[r['tk']] = bl_names.get(r['tk'], 0) + 1
                else: kept += 1
        top = sorted(bl_names.items(), key=lambda x: -x[1])[:6]
        print('  %-8s top5 슬롯 %d개 중 %d개 차단(%.0f%%) — 주로 %s'
              % (gate, blocked + kept, blocked, blocked / (blocked + kept) * 100,
                 ', '.join('%s(%d회)' % t for t in top)))

    print('\n=== ③ 잘린 종목 vs 대체 종목의 실제 5일 수익 ===')
    for gate in ('min_seg', 'gap'):
        bl, rp = [], []
        for i in range(2, len(AD)):
            if i % 5 != 0: continue
            pool = sorted(base_pool(AD[i]), key=lambda r: r['ag'])
            def ok(r):
                return (r['ms'] >= 0) if gate == 'min_seg' else not (r['gap'] is not None and r['gap'] < 1.5)
            top5 = pool[:5]; filt = [r for r in pool if ok(r)][:5]
            for r in top5:
                if not ok(r):
                    f = fwd(r['tk'], i)
                    if f is not None: bl.append(f)
            for r in filt:
                if r['tk'] not in {x['tk'] for x in top5}:
                    f = fwd(r['tk'], i)
                    if f is not None: rp.append(f)
        print('  %-8s 잘린 종목 평균 %+6.2f%% (n=%d)  |  대체 투입 종목 평균 %+6.2f%% (n=%d)  → 차이 %+.2f%%p'
              % (gate, np.mean(bl) if bl else 0, len(bl),
                 np.mean(rp) if rp else 0, len(rp),
                 (np.mean(bl) if bl else 0) - (np.mean(rp) if rp else 0)))
