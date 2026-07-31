# -*- coding: utf-8 -*-
"""라이브 원장 기준: 통합(US+KR) vs 미국만 — 한국 편입이 도움이 되나 (2026-07-31)"""
import sys, os, csv
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import unified_vm_track as u

rows = list(csv.DictReader(open(os.path.join(BASE, 'data_cache', 'unified_vm_log.csv'),
                                encoding='utf-8')))
days, blocks = u._ledger_blocks(rows)
usd = u._us_grid()

def replay(mode='all', N=5):
    """mode: all=통합 top5 / us=미국만 top5 / kr=한국만 top5"""
    hold = {}; nav = 1.0; series = []; hist = []
    for i, d in enumerate(days):
        blk = blocks[d]
        px = {r['ticker']: float(r['price']) for r in blk if r.get('price')}
        if hold:
            rs = [(px[t] / p - 1) for t, p in hold.items() if t in px]
            if rs: nav *= (1 + float(np.mean(rs)))
            for t in list(hold):
                if t in px: hold[t] = px[t]
        series.append(nav)
        ud = blk[0]['us_date']
        gi = usd.index(ud) if ud in usd else None
        is_rb = (i == 0) or (gi is not None and gi % u.REBAL == 0)
        if is_rb:
            cand = [r for r in blk if (mode == 'all' or r['market'] == mode.upper())]
            cand = sorted(cand, key=lambda r: -float(r['pct'] or 0))[:N]
            hold = {r['ticker']: float(r['price']) for r in cand if r.get('price')}
            hist.append((d, [r['ticker'] for r in cand]))
    a = np.array(series); pk = np.maximum.accumulate(a)
    return (a[-1] - 1) * 100, float((a / pk - 1).min()) * 100, hist

if __name__ == '__main__':
    print('라이브 원장 기간: %s ~ %s (%d 거래일)' % (days[0], days[-1], len(days)))
    print()
    print('%-16s %10s %10s' % ('구성', '수익%', 'MDD%'))
    print('-' * 40)
    for mode, lbl in [('all', '통합(US+KR)'), ('us', '미국만'), ('kr', '한국만')]:
        t, m, h = replay(mode)
        print('%-16s %+10.2f %+10.2f' % (lbl, t, m))
    print()
    print('리밸별 보유 (통합 / 미국만):')
    _, _, ha = replay('all'); _, _, hu = replay('us')
    for (d, a), (_, b) in zip(ha, hu):
        same = set(a) == set(b)
        print('  %s  통합 %-38s %s' % (d, ','.join(a), '(미국만 동일)' if same else '/ 미국만 ' + ','.join(b)))
