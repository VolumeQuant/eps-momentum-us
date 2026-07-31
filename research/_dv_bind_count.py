# -*- coding: utf-8 -*-
"""dv 게이트가 '보유 종목'을 실제로 몇 번이나 쳐냈나 (구속 빈도)."""
import sys, os
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C

ad, FULL, DVDB, TC, _ = C._load()
TE = C._load_te('full')
PE, GAP, N, R = 30, 1.5, 5, 5

tot_reb = 0; binds = []
for phase in range(R):
    hold = []
    for i in range(2, len(ad)):
        if i % R != phase: continue
        d = ad[i]; prev = set(hold); cand = []
        for tk, v in FULL.get(d, {}).items():
            if not C._industry_ok(tk, TC): continue
            dv = DVDB.get(d, {}).get(tk)
            other_ok = (C._ms(v) >= 0 and v['nc'] > 0 and (v['n90'] or 0) > 0.1
                        and v['px'] / v['nc'] <= PE)
            if other_ok and GAP:
                te_v = C._pit_te(TE, tk, d); g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
                if g is not None and g < GAP: other_ok = False
            dv_ok = (dv is not None and dv >= 1000.0)
            # 보유 중인데 dv만으로 탈락한 케이스
            if tk in prev and other_ok and not dv_ok:
                binds.append((phase, d, tk, dv, round(C._rev90(v), 1)))
            if not dv_ok or not other_ok: continue
            cand.append((tk, C._rev90(v)))
        cand.sort(key=lambda x: -x[1]); hold = [t for t, _ in cand[:N]]
        tot_reb += 1

print('총 리밸 횟수(위상 5개 합): %d' % tot_reb)
print('보유종목이 dv만으로 탈락한 사례: %d건\n' % len(binds))
for b in binds:
    print('  위상%d  %s  %-6s  dv $%sM  rev90 %+.1f%%'
          % (b[0], b[1], b[2], ('%.0f' % b[3]) if b[3] else 'None', b[4]))
