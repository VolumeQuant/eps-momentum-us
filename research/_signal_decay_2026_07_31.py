# -*- coding: utf-8 -*-
"""신호 수명 측정 (2026-07-31) — 괴리율 vs rev90의 예측력이 며칠 가는가.
사용자 질문: "괴리율은 매일 가격에 반응하는데 5일에 한 번 교체하면 괴리를 놓치는 것 아니냐"
방법: 각 날짜 t에 지표로 순위 → h일 후 수익과의 순위상관(Spearman IC). h별 감쇠 곡선.
"""
import sys, os
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
from _prod_faithful_bt_2026_07_31 import load

FULL, AG, FUND = load()
TE = C._load_te('full')
ad, _, _, TC, _ = C._load()


def eligible(d):
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
        out.append((tk, -(a) if a is not None else None,
                    (nc - v['n90']) / abs(v['n90']) * 100, p))
    return out


def rank(x):
    o = np.argsort(np.argsort(x))
    return (o - o.mean()) / (o.std() or 1)


def ic_curve(hs=(1, 2, 3, 5, 10, 20)):
    res = {h: {'gap': [], 'rev': []} for h in hs}
    for i in range(2, len(ad)):
        d = ad[i]; el = eligible(d)
        el = [e for e in el if e[1] is not None]
        if len(el) < 30: continue
        for h in hs:
            j = i + h
            if j >= len(ad): continue
            fw = FULL.get(ad[j], {})
            rows = [(g, r, (fw[t]['px'] / p - 1)) for t, g, r, p in el
                    if t in fw and fw[t]['px'] and p]
            if len(rows) < 30: continue
            g = rank(np.array([x[0] for x in rows]))
            r = rank(np.array([x[1] for x in rows]))
            f = rank(np.array([x[2] for x in rows]))
            res[h]['gap'].append(float((g * f).mean()))
            res[h]['rev'].append(float((r * f).mean()))
    return res


if __name__ == '__main__':
    hs = (1, 2, 3, 5, 10, 20)
    res = ic_curve(hs)
    print('신호 예측력(IC) — 값이 클수록 h일 후 수익을 잘 맞힘\n')
    print('%-8s %12s %12s %10s' % ('보유일수', '괴리율 IC', 'rev90 IC', '표본'))
    print('-' * 46)
    g0 = np.mean(res[1]['gap']); r0 = np.mean(res[1]['rev'])
    for h in hs:
        g, r = np.mean(res[h]['gap']), np.mean(res[h]['rev'])
        print('%-8d %12.4f %12.4f %10d' % (h, g, r, len(res[h]['gap'])))
    print()
    print('1일 대비 잔존율 (신호가 얼마나 살아있나)')
    print('%-8s %12s %12s' % ('보유일수', '괴리율', 'rev90'))
    for h in hs:
        g, r = np.mean(res[h]['gap']), np.mean(res[h]['rev'])
        print('%-8d %11.0f%% %11.0f%%' % (h, g / g0 * 100 if g0 else 0,
                                          r / r0 * 100 if r0 else 0))
