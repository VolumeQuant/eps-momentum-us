# -*- coding: utf-8 -*-
"""괴리율 vs rev90 팩트체크 — 결과를 깨뜨릴 수 있는 것부터 검증.
 A. 유니버스 편향: adj_gap 결측 6% 제외가 유리하게 작용했나
    → rev90을 '같은 유니버스(adj_gap 있는 종목만)'로 제한해 재측정
 B. 기간 격리: MDD 우위가 7월 폭락에서만 나오나 (같은 게이트, 지표만 교체)
 C. LOWO: 1등 셀이 아니라 '현행 게이트 그대로' 조건에서
 D. PIT 위생: adj_gap이 미래 정보를 쓰나
"""
import sys, os, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
import daily_runner as dr
from _pure_eps_vs_vm_2026_07_31 import load_ag


def bt(metric='rev90', gates=None, same_universe=False, N=5, R=5, phase=0,
       end_date=None, exclude=frozenset(), start=2):
    """same_universe=True → adj_gap 있는 종목만 (유니버스 동일화)"""
    gates = gates or {}
    ad, FULL, DVDB, TC, _ = C._load(); TE = C._load_te('full'); AG = load_ag()
    if end_date: ad = tuple(d for d in ad if d <= end_date)
    hold = []; rets = []
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]
        px = FULL.get(d, {}); ppx = FULL.get(pv, {})
        drr = 0.0
        for t in hold:
            cu = px.get(t, {}).get('px'); pp = ppx.get(t, {}).get('px')
            if cu and pp and pp > 0: drr += (1.0 / N) * (cu - pp) / pp
        rets.append(drr)
        if i % R == phase:
            cand = []
            for tk, v in FULL.get(d, {}).items():
                if tk in exclude or not C._industry_ok(tk, TC): continue
                if gates.get('dv'):
                    dv = DVDB.get(d, {}).get(tk)
                    if dv is None or dv < 1000.0: continue
                if gates.get('seg') and C._ms(v) < 0: continue
                if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
                if gates.get('pe') and v['px'] / v['nc'] > 30.0: continue
                if gates.get('gap'):
                    te_v = C._pit_te(TE, tk, d)
                    g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
                    if g is not None and g < 1.5: continue
                a = AG.get(d, {}).get(tk)
                if (metric == 'adj_gap' or same_universe) and a is None: continue
                cand.append((a if metric == 'adj_gap' else -C._rev90(v), tk))
            cand.sort(); hold = [t for _, t in cand[:N]]
    r = np.array(rets); nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100


def rp(**kw):
    o = [bt(phase=p, **kw) for p in range(5)]
    return float(np.mean([x[0] for x in o])), float(np.mean([x[1] for x in o]))


G_NOW = dict(dv=1, pe=1, gap=1, seg=1)          # 현행 게이트
G_BEST = dict(dv=1, pe=1)                        # gap·mseg 제거

if __name__ == '__main__':
    print('=== A. 유니버스 편향 검증 (adj_gap 결측 제외가 유리했나) ===')
    conn = sqlite3.connect(dr.DB_PATH)
    tot, miss = conn.execute(
        'SELECT COUNT(*), SUM(adj_gap IS NULL) FROM ntm_screening').fetchone()
    print('adj_gap 결측: %d/%d (%.1f%%)' % (miss, tot, miss / tot * 100))
    print('결측 종목의 특성:')
    for lbl, q in [('결측', 'adj_gap IS NULL'), ('보유', 'adj_gap IS NOT NULL')]:
        r = conn.execute(
            'SELECT AVG(num_analysts), AVG(dollar_volume_30d), COUNT(DISTINCT ticker) '
            'FROM ntm_screening WHERE %s' % q).fetchone()
        print('  %-4s 애널 %.1f명  거래대금 $%.0fM  종목수 %d' % (lbl, r[0] or 0, r[1] or 0, r[2]))
    conn.close()
    print('\n%-40s %9s %9s' % ('', '수익%', 'MDD%'))
    for lbl, kw in [
        ('현행(rev90, 전체 유니버스)', dict(metric='rev90', gates=G_NOW)),
        ('현행(rev90, adj_gap 있는 종목만)', dict(metric='rev90', gates=G_NOW, same_universe=True)),
        ('괴리율(같은 유니버스)', dict(metric='adj_gap', gates=G_NOW)),
    ]:
        t, m = rp(**kw); print('%-40s %+9.1f %+9.1f' % (lbl, t, m))

    print('\n=== B. 기간 격리 (현행 게이트 고정, 지표만 교체) ===')
    print('%-16s %19s %19s' % ('구간', 'rev90', '괴리율'))
    for lbl, ed in [('~4/15', '2026-04-15'), ('~5/15', '2026-05-15'), ('~6/15', '2026-06-15'),
                    ('~7/15', '2026-07-15'), ('~7/30', None)]:
        a = rp(metric='rev90', gates=G_NOW, end_date=ed)
        b = rp(metric='adj_gap', gates=G_NOW, end_date=ed)
        print('%-16s %+9.1f/%+8.1f %+9.1f/%+8.1f' % (lbl, a[0], a[1], b[0], b[1]))

    print('\n=== C. LOWO (현행 게이트 그대로, 지표만 교체) ===')
    print('%-18s %19s %19s' % ('제외', 'rev90', '괴리율'))
    for ex in [(), ('SNDK',), ('MU',), ('SNDK', 'MU'), ('SNDK', 'MU', 'STX')]:
        e = frozenset(ex)
        a = rp(metric='rev90', gates=G_NOW, exclude=e)
        b = rp(metric='adj_gap', gates=G_NOW, exclude=e)
        print('%-18s %+9.1f/%+8.1f %+9.1f/%+8.1f'
              % ('ex-' + ('/'.join(ex) if ex else '없음'), a[0], a[1], b[0], b[1]))
