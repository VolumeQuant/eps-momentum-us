# -*- coding: utf-8 -*-
"""게이트 16셀 1등(괴리율+dv+PER, gap/mseg 제거)의 max-selection 편향 검증.
현행 VM 및 인접 셀과 LOWO·기간분할·위상분산 비교."""
import sys, os
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
from _pure_eps_followup_2026_07_31 import agg, run

CELLS = [
    ('현행VM(rev90 전게이트)', dict(metric='rev90',   use_dv=1, use_pe=1, use_gap=1, use_seg=1)),
    ('★괴리율+dv+PER',        dict(metric='adj_gap', use_dv=1, use_pe=1, use_gap=0, use_seg=0)),
    ('  인접: +mseg',          dict(metric='adj_gap', use_dv=1, use_pe=1, use_gap=0, use_seg=1)),
    ('  인접: +gap',           dict(metric='adj_gap', use_dv=1, use_pe=1, use_gap=1, use_seg=0)),
    ('  인접: -PER',           dict(metric='adj_gap', use_dv=1, use_pe=0, use_gap=0, use_seg=0)),
    ('  인접: -dv',            dict(metric='adj_gap', use_dv=0, use_pe=1, use_gap=0, use_seg=0)),
    ('괴리율 무게이트',           dict(metric='adj_gap', use_dv=0, use_pe=0, use_gap=0, use_seg=1)),
]

def line(lbl, t, m):
    return '%-24s %+8.1f %+8.1f  %5.2f' % (lbl, t, m, t / abs(m) if m else 0)

if __name__ == '__main__':
    end = sys.argv[1] if len(sys.argv) > 1 else None
    print('=== 인접 안정성 (1등 주변이 절벽인가 고원인가) ===')
    print('%-24s %8s %8s  %5s' % ('', '수익%', 'MDD%', '비율'))
    ratios = []
    for lbl, kw in CELLS:
        t, m, _ = agg(end_date=end, **kw); print(line(lbl, t, m))
        if lbl.startswith(('★', '  인접')): ratios.append(t / abs(m))
    print('  → 1등+인접 4셀 비율 CV = %.3f' % (np.std(ratios) / np.mean(ratios)))

    print('\n=== LOWO (슈퍼위너 제외) ===')
    print('%-20s %22s %22s %22s' % ('제외', '현행VM', '★괴리율+dv+PER', '괴리율무게이트'))
    for ex in [(), ('SNDK',), ('MU',), ('SNDK', 'MU'), ('STX',), ('SNDK', 'MU', 'STX'), ('DELL',)]:
        e = frozenset(ex); cells = []
        for _, kw in [CELLS[0], CELLS[1], CELLS[6]]:
            t, m, _ = agg(end_date=end, exclude=e, **kw)
            cells.append('%+8.1f/%+7.1f' % (t, m))
        print('%-20s %22s %22s %22s' % ('ex-' + ('/'.join(ex) if ex else '없음'), *cells))

    print('\n=== 기간 분할 ===')
    print('%-20s %22s %22s %22s' % ('구간', '현행VM', '★괴리율+dv+PER', '괴리율무게이트'))
    for lbl, ed in [('~5/15', '2026-05-15'), ('~6/15', '2026-06-15'),
                    ('~7/15', '2026-07-15'), ('~7/30 전체', None)]:
        cells = []
        for _, kw in [CELLS[0], CELLS[1], CELLS[6]]:
            t, m, _ = agg(end_date=ed, **kw); cells.append('%+8.1f/%+7.1f' % (t, m))
        print('%-20s %22s %22s %22s' % (lbl, *cells))

    print('\n=== 위상별 분산 + 회전/비용 ===')
    for lbl, kw in [CELLS[0], CELLS[1], CELLS[6]]:
        t, m, outs = agg(end_date=end, **kw)
        tv = np.mean([np.mean(o['turnover'][1:]) for o in outs])
        t20, m20, _ = agg(end_date=end, cost_bp=20, **kw)
        print('%-24s 위상수익 %s' % (lbl, [round(o['tot'], 1) for o in outs]))
        print('%-24s 위상MDD  %s  | 교체 %.2f/5 | 20bp후 %+.1f/%+.1f'
              % ('', [round(o['mdd'], 1) for o in outs], tv, t20, m20))
