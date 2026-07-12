# -*- coding: utf-8 -*-
"""교차 상호작용 검증 (2026-07-12) — 세 딥다이브(안건1 다중창 / 안건2 3필터 / 안건3 MA120)의
interaction_hypotheses를 단일 하네스에서 실측.

원칙: 위상 0~4 평균 필수 + LOWO(-SNDK/-MU/-both) + paired 차분. 읽기 전용(DB write 0).

핵심 설계: 안건2가 확인한 대로 production 게이트는 full TE(trailing_eps_ttm_full.json,
1,536종목)를 쓰므로 **모든 조합 실험은 full TE 기계 위에서** 수행한다. 안건1·3은 sparse TE로
측정됐으므로, 그 결론(다중창 전패·MA필터 전패)이 full TE에서도 유지되는지가 그 자체로
상호작용 검증 항목(S1b, S3).

섹션:
  S0  무결성: sparse 베이스라인 == vm_canonical_bt (프로그램 검증) + full TE 베이스라인
      (안건2 파트A 수치와 대조용 출력)
  S1  H(안건1×안건2): recency 게이트(rev30>=2) × gap 임계 {1.25,1.5,1.75,2.0} 2D —
      최근성 조건이 gap plateau를 이동시키는가
  S1b (a)조합: 안건1 랭킹 변형(blend/단일창)이 full TE + 권고임계 위에서도 전패인가
  S2  H(안건1×안건3): recency × ma120 — 중복 보험 검정 (superadditivity + 컷셋 겹침)
  S3  H(안건3×안건2): ma120 × gap {1.5, 2.0} — gap을 올리면 눌린종목 농축이 픽으로 전달되는가
  S4  H(안건1 N가설): N {3,4,5} × recency / × ma120 — 손실이 topN 경계 의존인가
  S5  H(안건2 dv가설): dv {500,1000} × {recency, ma120} — 다른 필터가 dv 완화 붕괴를 구조하는가
  S6  패키지: 채택 후보 조합(전부 '현행 유지') vs 베이스라인 + 기각 변형 총합 패키지
"""
import sys, os, json, sqlite3, functools, itertools
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.dirname(HERE)
sys.path.insert(0, BASE); sys.path.insert(0, HERE)
import vm_canonical_bt as vb
import daily_runner as dr

R, START = 5, 2
LOWO = [frozenset(), frozenset({'SNDK'}), frozenset({'MU'}), frozenset({'SNDK', 'MU'})]
LBL = {frozenset(): 'full', frozenset({'SNDK'}): '-SNDK', frozenset({'MU'}): '-MU',
       frozenset({'SNDK', 'MU'}): '-both'}


# ── 데이터 로드 (ma60/ma120/high30 포함 확장 로더) ──────────────────────────
@functools.lru_cache(maxsize=1)
def load():
    conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
    ad = tuple(r[0] for r in c.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'))
    full = {}
    for tk, d, px, nc, n7, n30, n60, n90, m60, m120, h30 in c.execute(
            'SELECT ticker,date,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,'
            'ma60,ma120,high30 FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0'):
        full.setdefault(d, {})[tk] = dict(px=px, nc=nc, n7=n7, n30=n30, n60=n60, n90=n90,
                                          ma60=m60, ma120=m120, h30=h30)
    dvdb = {}
    for tk, d, dv in c.execute(
            'SELECT ticker,date,dollar_volume_30d FROM ntm_screening WHERE dollar_volume_30d IS NOT NULL'):
        dvdb.setdefault(d, {})[tk] = float(dv)
    conn.close()
    tc = json.load(open(os.path.join(BASE, 'ticker_info_cache.json'), encoding='utf-8'))
    te_s = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm.json'), encoding='utf-8'))
    te_f = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm_full.json'), encoding='utf-8'))
    te_f.pop('_meta', None)
    return ad, full, dvdb, tc, te_s, te_f


AD, FULL, DVDB, TC, TE_S, TE_F = load()


def rev(v, k):
    b = v.get(k)
    if b is None or abs(b) <= 0.01: return None
    return (v['nc'] - b) / abs(b) * 100


def rev0(v, k):
    r = rev(v, k)
    return 0.0 if r is None else r


RANKS = {
    'rev90': lambda v: rev0(v, 'n90'),
    'rev60': lambda v: rev0(v, 'n60'),
    'blend_v8010': lambda v: 0.30 * rev0(v, 'n7') + 0.10 * rev0(v, 'n30')
                             + 0.10 * rev0(v, 'n60') + 0.50 * rev0(v, 'n90'),
    'blend_eq': lambda v: (rev0(v, 'n7') + rev0(v, 'n30') + rev0(v, 'n60') + rev0(v, 'n90')) / 4,
}


# ── FEAT 사전계산: 자격(업종+min_seg+sanity) 통과자별 피처 테이블 ────────────
@functools.lru_cache(maxsize=2)
def feats(te_key):
    """te_key in ('sparse','full'). {date: [(tk, per, gap|None, r30|None, below120, below60,
    dd25bad, dv|None, vdict), ...]} — dv/게이트 임계는 여기서 안 자름(가변 파라미터)."""
    te = TE_S if te_key == 'sparse' else TE_F
    out = {}
    for d, mp in FULL.items():
        rows = []
        for tk, v in mp.items():
            if not vb._industry_ok(tk, TC): continue
            if vb._ms(v) < 0: continue
            if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
            te_v = vb._pit_te(te, tk, d)
            g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
            b120 = (v['ma120'] is not None and v['px'] < v['ma120'])
            m120_missing = v['ma120'] is None
            b60 = (v['ma60'] is not None and v['px'] < v['ma60'])
            dd25bad = (v['h30'] is not None and v['h30'] > 0 and v['px'] / v['h30'] - 1 <= -0.25)
            rows.append((tk, v['px'] / v['nc'], g, rev(v, 'n30'), b120, m120_missing, b60,
                         dd25bad, DVDB.get(d, {}).get(tk), v))
        out[d] = rows
    return out


def bt(te_key='full', pe_max=30, gap_thr=1.5, dv_min=1000.0, N=5, rank='rev90',
       rec_min=None, trend=None, phase=0, exclude=frozenset(), trace=False):
    FT = feats(te_key); rk = RANKS[rank]
    hold = []; rets = []; log = []
    for i in range(START, len(AD)):
        d, pv = AD[i], AD[i - 1]
        px = FULL.get(d, {}); ppx = FULL.get(pv, {})
        drr = 0.0
        for t in hold:
            cu = px.get(t, {}).get('px'); pp = ppx.get(t, {}).get('px')
            if cu and pp and pp > 0: drr += (1.0 / N) * (cu - pp) / pp
        rets.append(drr)
        if i % R == phase:
            cand = []
            for tk, per, g, r30, b120, m120miss, b60, dd25bad, dv, v in FT.get(d, []):
                if tk in exclude: continue
                if dv is None or dv < dv_min: continue
                if per > pe_max: continue
                if gap_thr and g is not None and g < gap_thr: continue
                if rec_min is not None and r30 is not None and r30 < rec_min: continue
                if trend == 'ma120' and b120: continue          # missing=pass
                if trend == 'ma60' and b60: continue
                if trend == 'dd25' and dd25bad: continue
                cand.append((tk, rk(v)))
            cand.sort(key=lambda x: -x[1]); hold = [t for t, _ in cand[:N]]
            if trace: log.append((d, i, list(hold)))
    r = np.array(rets); nav = np.cumprod(1 + r); peak = np.maximum.accumulate(nav)
    tot = float(nav[-1] - 1) * 100; mdd = float((nav / peak - 1).min()) * 100
    return (tot, mdd, log) if trace else (tot, mdd)


def rep(lowo_sets=LOWO, **kw):
    """{exclude: (avg_ret, avg_mdd, per_phase dict)}"""
    out = {}
    for ex in lowo_sets:
        per = {p: bt(phase=p, exclude=ex, **kw) for p in range(R)}
        out[ex] = (float(np.mean([v[0] for v in per.values()])),
                   float(np.mean([v[1] for v in per.values()])), per)
    return out


def show(name, r, base=None, lowo_sets=LOWO):
    cells = []
    for ex in lowo_sets:
        t, m, per = r[ex]
        if base is not None:
            diffs = [per[p][0] - base[ex][2][p][0] for p in range(R)]
            wins = sum(1 for x in diffs if x > 0)
            cells.append(f'{LBL[ex]} {t:+6.1f}/{m:+5.1f} (Δ{np.mean(diffs):+6.1f}p {wins}/5승)')
        else:
            cells.append(f'{LBL[ex]} {t:+6.1f}/{m:+5.1f}')
    print(f'  {name:34s}: ' + '  '.join(cells))


def main():
    # ═══ S0 무결성 ═══
    print('=' * 100)
    print('S0) 무결성: sparse 베이스라인 == vm_canonical_bt(gap1.5,N5,dv1000) 프로그램 대조')
    ok = True
    for p in range(R):
        a = bt(te_key='sparse', phase=p)
        b = vb.canonical_bt(30, 1.5, 5, R, phase=p, dv_min=1000.0)
        m = abs(a[0] - b[0]) < 1e-9 and abs(a[1] - b[1]) < 1e-9
        ok &= m
        print(f'  위상{p}: local {a[0]:+8.2f}/{a[1]:+6.2f}  canonical {b[0]:+8.2f}/{b[1]:+6.2f}  {"OK" if m else "MISMATCH"}')
    if not ok: raise SystemExit('무결성 실패 — 이하 무효')
    print('\n  베이스라인 (pe30/gap1.5/N5/dv1000, 위상평균+LOWO):')
    BASE_S = rep(te_key='sparse')
    BASE_F = rep(te_key='full')
    show('sparse TE(142) [안건1·3의 기계]', BASE_S)
    show('full TE(1536)  [production 기계]', BASE_F)

    # ═══ S1 recency × gap 2D ═══
    print('\n' + '=' * 100)
    print('S1) 안건1×안건2: recency(rev30>=2, missing=pass) × gap 임계 — full TE, paired Δ는 같은 gap의 rec=None 대비')
    for gthr in (1.25, 1.5, 1.75, 2.0):
        r_no = rep(te_key='full', gap_thr=gthr)
        r_rc = rep(te_key='full', gap_thr=gthr, rec_min=2)
        show(f'gap{gthr:>4} rec=None', r_no, BASE_F if gthr != 1.5 else None)
        show(f'gap{gthr:>4} +rec2   ', r_rc, r_no)
    # gap plateau 이동 검사: rec 유/무에서 gap1.5 대비 인접 임계 차
    print('  → plateau 형상 비교는 위 rec=None 행(BASE 대비 Δ)과 +rec2 행(같은 gap 대비 Δ)으로 판독')

    # ═══ S1b 랭킹 변형 × full TE ═══
    print('\n' + '=' * 100)
    print('S1b) (a)조합: 안건1 랭킹 변형이 full TE(권고 기계) 위에서도 전패인가 — paired Δ vs full TE rev90')
    for rk in ('rev60', 'blend_v8010', 'blend_eq'):
        show(f'rank={rk}', rep(te_key='full', rank=rk), BASE_F)

    # ═══ S2 recency × ma120 중복보험 ═══
    print('\n' + '=' * 100)
    print('S2) 안건1×안건3: recency × ma120 — superadditivity + 컷셋 겹침 (full TE)')
    r_rec = rep(te_key='full', rec_min=2)
    r_ma = rep(te_key='full', trend='ma120')
    r_both = rep(te_key='full', rec_min=2, trend='ma120')
    show('base', BASE_F)
    show('+rec2', r_rec, BASE_F)
    show('+ma120', r_ma, BASE_F)
    show('+rec2+ma120', r_both, BASE_F)
    print('  --- superadditivity: Δ(both) vs Δ(rec)+Δ(ma) [위상평균 paired, %p] ---')
    for ex in LOWO:
        d_rec = r_rec[ex][0] - BASE_F[ex][0]
        d_ma = r_ma[ex][0] - BASE_F[ex][0]
        d_bo = r_both[ex][0] - BASE_F[ex][0]
        print(f'    {LBL[ex]:5s}: Δrec {d_rec:+6.1f}  Δma {d_ma:+6.1f}  합 {d_rec+d_ma:+6.1f}  Δboth {d_bo:+6.1f}'
              f'  → 상호작용항 {d_bo-(d_rec+d_ma):+6.1f}'
              f'  ({"중복(겹침, sub-additive 해악)" if d_bo > d_rec+d_ma else "증폭(super-additive 해악)"})')
    # 컷셋 겹침 (게이트 통과자 관측 레벨)
    FT = feats('full')
    n = n_rec = n_ma = n_both_c = 0
    for i in range(START, len(AD)):
        d = AD[i]
        for tk, per, g, r30, b120, m120miss, b60, dd25bad, dv, v in FT.get(d, []):
            if dv is None or dv < 1000: continue
            if per > 30: continue
            if g is not None and g < 1.5: continue
            rc = (r30 is not None and r30 < 2); mc = b120
            n += 1; n_rec += rc; n_ma += mc; n_both_c += (rc and mc)
        # (m120 결측=컷 아님 규약 유지)
    p_r, p_m, p_b = n_rec / n, n_ma / n, n_both_c / n
    lift = p_b / (p_r * p_m) if p_r * p_m > 0 else float('nan')
    print(f'  --- 컷셋 겹침 (게이트통과 관측 {n}건): P(rec컷)={p_r:.1%} P(ma120컷)={p_m:.1%} '
          f'P(둘다)={p_b:.1%} 독립기대 {p_r*p_m:.1%} lift={lift:.2f} ---')
    print(f'      rec컷 중 ma120컷 비율 = {p_b/p_r if p_r else float("nan"):.1%} / ma120컷 중 rec컷 비율 = {p_b/p_m if p_m else float("nan"):.1%}')

    # ═══ S3 ma120 × gap ═══
    print('\n' + '=' * 100)
    print('S3) 안건3×안건2: gap 임계 ↑ 시 눌린종목 농축 → ma120 필터 손익 변화 + below-ma120 픽 비율')
    for gthr in (1.5, 2.0):
        r_no = rep(te_key='full', gap_thr=gthr, lowo_sets=[frozenset(), frozenset({'SNDK', 'MU'})])
        r_ma = rep(te_key='full', gap_thr=gthr, trend='ma120', lowo_sets=[frozenset(), frozenset({'SNDK', 'MU'})])
        show(f'gap{gthr} base ', r_no, lowo_sets=[frozenset(), frozenset({'SNDK', 'MU'})])
        show(f'gap{gthr} +ma120', r_ma, r_no, lowo_sets=[frozenset(), frozenset({'SNDK', 'MU'})])
        # below-ma120 픽 비율 (전 위상 유니온)
        tot_p = bel_p = 0
        for p in range(R):
            _, _, log = bt(te_key='full', gap_thr=gthr, phase=p, trace=True)
            for d, i, hold in log:
                for tk in hold:
                    v = FULL[d].get(tk)
                    if not v or v['ma120'] is None: continue
                    tot_p += 1; bel_p += (v['px'] < v['ma120'])
        print(f'    gap{gthr}: below-ma120 픽 비율 {bel_p}/{tot_p} = {bel_p/tot_p:.1%}')

    # ═══ S4 N × recency / ma120 ═══
    print('\n' + '=' * 100)
    print('S4) N {3,4,5} × recency / ma120 — 손실이 topN 경계 의존인가 (full TE, Δ vs 같은 N base)')
    for Nv in (3, 4, 5):
        b = rep(te_key='full', N=Nv, lowo_sets=[frozenset(), frozenset({'SNDK', 'MU'})])
        rr = rep(te_key='full', N=Nv, rec_min=2, lowo_sets=[frozenset(), frozenset({'SNDK', 'MU'})])
        rm = rep(te_key='full', N=Nv, trend='ma120', lowo_sets=[frozenset(), frozenset({'SNDK', 'MU'})])
        show(f'N{Nv} base  ', b, lowo_sets=[frozenset(), frozenset({'SNDK', 'MU'})])
        show(f'N{Nv} +rec2 ', rr, b, lowo_sets=[frozenset(), frozenset({'SNDK', 'MU'})])
        show(f'N{Nv} +ma120', rm, b, lowo_sets=[frozenset(), frozenset({'SNDK', 'MU'})])

    # ═══ S5 dv 완화 구조 실험 ═══
    print('\n' + '=' * 100)
    print('S5) 안건2 dv가설: dv$500M 붕괴를 recency/ma120/A군류 필터가 구조하는가 (full TE, Δ vs dv1000 base)')
    b1000 = BASE_F
    for dvm in (500.0, 1000.0):
        for extra, kw in [('없음', {}), ('+rec2', dict(rec_min=2)), ('+ma120', dict(trend='ma120')),
                          ('+rec2+ma120', dict(rec_min=2, trend='ma120'))]:
            if dvm == 1000.0 and extra != '없음': continue
            r = rep(te_key='full', dv_min=dvm, lowo_sets=[frozenset(), frozenset({'SNDK', 'MU'})], **kw)
            show(f'dv{int(dvm):>4} {extra:12s}', r, b1000, lowo_sets=[frozenset(), frozenset({'SNDK', 'MU'})])

    # ═══ S6 패키지 ═══
    print('\n' + '=' * 100)
    print('S6) 패키지 검증')
    print('  채택 후보 패키지 = 세 딥다이브 권고 전부 "현행 유지" → 조합 = 베이스라인 그 자체 (Δ=0, 신규 파라미터 0개)')
    print('  기각 변형 총합 패키지(rec2+ma120, 참고): S2의 +rec2+ma120 행 참조')
    print('  하네스 패치(안건2 권고: full TE) 채택 시 정본 수치 이동: '
          f'sparse {BASE_S[frozenset()][0]:+.1f}/{BASE_S[frozenset()][1]:+.1f} → '
          f'full {BASE_F[frozenset()][0]:+.1f}/{BASE_F[frozenset()][1]:+.1f} '
          f'(-both: {BASE_S[frozenset({"SNDK","MU"})][0]:+.1f} → {BASE_F[frozenset({"SNDK","MU"})][0]:+.1f})')


if __name__ == '__main__':
    main()
