# -*- coding: utf-8 -*-
"""순수 EPS 모멘텀(게이트 없음) vs 현행 VM(게이트 체인) — 지표×게이트 2축 비교.
사용자 질문(2026-07-31): "거래대금·fwd_per 조건 다 빼고 순수하게 EPS 상향 대비
가격 괴리율로만 뽑으면 성과가 더 낫지 않냐?"

지표 2종 (둘은 다른 것 — 구분해서 잰다):
  rev90   = (nc-n90)/|n90|  내림차순 — 가격 무시, 순수 EPS 상향폭 (현행 VM)
  adj_gap = fwd_pe_chg×(1+dir)×eps_q  오름차순 — EPS 대비 가격 괴리율 (구시스템)
게이트 4종: dv$1B / fwd_PER<=30 / gap>=1.5 / min_seg>=0  (업종제외는 항상 유지)
보고: 위상 0~R-1 평균 (정본 규약).
"""
import sys, os, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
import daily_runner as dr

_CACHE = {}
def load_ag():
    if 'ag' in _CACHE: return _CACHE['ag']
    conn = sqlite3.connect(dr.DB_PATH)
    ag = {}
    for tk, d, v in conn.execute(
            'SELECT ticker,date,adj_gap FROM ntm_screening WHERE adj_gap IS NOT NULL'):
        ag.setdefault(d, {})[tk] = float(v)
    conn.close(); _CACHE['ag'] = ag; return ag


def bt(metric='rev90', use_dv=True, use_pe=True, use_gap=True, use_seg=True,
       N=5, R=5, phase=0, start=2, end_date=None, exclude=frozenset(), trace=False):
    ad, FULL, DVDB, TC, _ = C._load()
    TE = C._load_te('full'); AG = load_ag()
    if end_date: ad = tuple(d for d in ad if d <= end_date)
    hold = []; rets = []; log = []
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
                if use_dv:
                    dv = DVDB.get(d, {}).get(tk)
                    if dv is None or dv < 1000.0: continue
                if use_seg and C._ms(v) < 0: continue
                if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
                if use_pe and v['px'] / v['nc'] > 30.0: continue
                if use_gap:
                    te_v = C._pit_te(TE, tk, d)
                    g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
                    if g is not None and g < 1.5: continue
                if metric == 'rev90':
                    key = -C._rev90(v)                      # 내림차순
                else:
                    a = AG.get(d, {}).get(tk)
                    if a is None: continue
                    key = a                                  # 오름차순(압축 클수록 좋음)
                cand.append((key, tk))
            cand.sort(); hold = [t for _, t in cand[:N]]
            if trace: log.append((d, len(cand), list(hold)))
    r = np.array(rets); nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100, log


def rep(**kw):
    R = kw.pop('R', 5)
    ph = [bt(R=R, phase=p, **kw)[:2] for p in range(R)]
    return float(np.mean([x[0] for x in ph])), float(np.mean([x[1] for x in ph]))


if __name__ == '__main__':
    ad, *_ = C._load(); end = sys.argv[1] if len(sys.argv) > 1 else None
    print('데이터 마지막:', ad[-1], '| N=5, R=5, 위상평균\n')
    ALL = dict(use_dv=True, use_pe=True, use_gap=True, use_seg=True)
    NONE = dict(use_dv=False, use_pe=False, use_gap=False, use_seg=True)
    print('%-46s %9s %9s' % ('구성', '수익%', 'MDD%'))
    print('-' * 66)
    rows = [
        ('현행 VM (rev90 + 게이트 전부)', dict(metric='rev90', **ALL)),
        ('rev90, 게이트 전부 제거', dict(metric='rev90', **NONE)),
        ('★괴리율(adj_gap), 게이트 전부 제거', dict(metric='adj_gap', **NONE)),
        ('괴리율(adj_gap) + 게이트 전부', dict(metric='adj_gap', **ALL)),
    ]
    res = {}
    for name, kw in rows:
        t, m = rep(end_date=end, **kw); res[name] = (t, m)
        print('%-46s %+9.1f %+9.1f' % (name, t, m))

    print('\n--- 게이트 하나씩만 제거 (rev90 기준, 기여도 분해) ---')
    for lbl, ov in [('dv $1B 제거', dict(use_dv=False)), ('fwd_PER<=30 제거', dict(use_pe=False)),
                    ('gap>=1.5 제거', dict(use_gap=False)), ('min_seg>=0 제거', dict(use_seg=False))]:
        kw = dict(metric='rev90', **ALL); kw.update(ov)
        t, m = rep(end_date=end, **kw)
        print('%-46s %+9.1f %+9.1f' % (lbl, t, m))

    print('\n--- 슬롯 수 N 민감도 (괴리율·게이트 없음 vs 현행) ---')
    for N in (2, 3, 5, 8):
        a = rep(metric='adj_gap', N=N, end_date=end, **NONE)
        b = rep(metric='rev90', N=N, end_date=end, **ALL)
        print('N=%d  괴리율無게이트 %+8.1f/%+7.1f   현행VM %+8.1f/%+7.1f'
              % (N, a[0], a[1], b[0], b[1]))


def battery(end=None):
    ALL = dict(use_dv=True, use_pe=True, use_gap=True, use_seg=True)
    NONE = dict(use_dv=False, use_pe=False, use_gap=False, use_seg=True)
    A = dict(metric='rev90', **ALL)      # 현행
    B = dict(metric='adj_gap', **NONE)   # 괴리율·게이트없음

    print('\n\n=== 검증 1: 기간 분할 (7월 폭락 한 번의 착시인가) ===')
    print('%-22s %19s %19s' % ('구간', '현행VM', '괴리율無게이트'))
    for lbl, ed in [('~5/15 (전반)', '2026-05-15'), ('~6/15', '2026-06-15'),
                    ('~7/15 (폭락 전)', '2026-07-15'), ('~7/30 (전체)', None)]:
        a = rep(end_date=ed, **A); b = rep(end_date=ed, **B)
        print('%-22s %+9.1f/%+7.1f %+9.1f/%+7.1f' % (lbl, a[0], a[1], b[0], b[1]))

    print('\n=== 검증 2: LOWO (슈퍼위너 제외해도 유지되나) ===')
    print('%-22s %19s %19s' % ('제외', '현행VM', '괴리율無게이트'))
    for ex in [(), ('SNDK',), ('MU',), ('SNDK', 'MU'), ('STX',), ('SNDK', 'MU', 'STX')]:
        e = frozenset(ex)
        a = rep(end_date=end, exclude=e, **A); b = rep(end_date=end, exclude=e, **B)
        print('%-22s %+9.1f/%+7.1f %+9.1f/%+7.1f'
              % ('ex-' + ('/'.join(ex) if ex else '없음'), a[0], a[1], b[0], b[1]))

    print('\n=== 검증 3: 위상별 분산 (평균이 한 위상에 끌려가나) ===')
    for lbl, kw in [('현행VM', A), ('괴리율無게이트', B)]:
        ph = [bt(R=5, phase=p, end_date=end, **kw)[:2] for p in range(5)]
        print('%-14s 수익 %s' % (lbl, [round(x[0], 1) for x in ph]))
        print('%-14s MDD  %s' % ('', [round(x[1], 1) for x in ph]))
