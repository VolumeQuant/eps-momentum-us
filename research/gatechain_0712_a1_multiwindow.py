# -*- coding: utf-8 -*-
"""안건1 (2026-07-12): rev90 단일창 vs 다중창(7/30/60/90d) 리비전 시그널.

사용자 가설: "rev90 높음 + 최근창(rev30/rev7) ~0 = stale revision인데 rev90이 높게 쳐줌"

섹션:
  [1] stale EDA — 게이트 통과자·일별 top5 중 stale 비중, 실제 픽 stale 사례+이후 수익
  [2] IC/버킷 — rev7/30/60/90/블렌드의 fwd 5d/20d IC, rev90高 그룹의 rev30 tercile 분해(직접 검정)
  [3] BT 변형 — rev60/rev30/블렌드(v80.10 아날로그+균등)/rev90+최근성게이트 스윕/기울기평균
      전부 위상 0~4 평균 + LOWO 3종 + paired 차분 (게이트는 현행 고정 PER30/gap1.5/N5/dv$1B)
  [4] 상호작용 — 최근성 게이트의 binding(누굴 자르나), MA120 중복성, KR base-effect 완충 여부

정본 하네스 vm_canonical_bt._load() 재사용 — 후보 필터는 canonical_bt와 바이트 동일 로직.
BT 무결성 검증: rank=rev90·recency=None 이 canonical_bt(30,1.5,5,dv_min=1000)와 전 위상 일치해야 함.
"""
import sys, os, sqlite3, functools, itertools
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.dirname(HERE)
sys.path.insert(0, BASE); sys.path.insert(0, HERE)
import vm_canonical_bt as vb

AD, FULL, DVDB, TC, TE = vb._load()
PE_MAX, GAP_THR, N, R, START, DV_MIN = 30, 1.5, 5, 5, 2, 1000.0
LOWO_SETS = [frozenset(), frozenset({'SNDK'}), frozenset({'MU'}), frozenset({'SNDK', 'MU'})]
IDX = {d: i for i, d in enumerate(AD)}


def rev(v, k):
    b = v.get(k)
    if b is None or abs(b) <= 0.01: return None
    return (v['nc'] - b) / abs(b) * 100


def rev0(v, k):
    r = rev(v, k)
    return 0.0 if r is None else r


# ---- 랭킹 키 ----
RANKS = {
    'rev90': lambda v: rev0(v, 'n90'),
    'rev60': lambda v: rev0(v, 'n60'),
    'rev30': lambda v: rev0(v, 'n30'),
    'rev7':  lambda v: rev0(v, 'n7'),
    'blend_v8010': lambda v: 0.30 * rev0(v, 'n7') + 0.10 * rev0(v, 'n30')
                             + 0.10 * rev0(v, 'n60') + 0.50 * rev0(v, 'n90'),
    'blend_eq': lambda v: (rev0(v, 'n7') + rev0(v, 'n30') + rev0(v, 'n60') + rev0(v, 'n90')) / 4,
    'slope_avg': lambda v: (rev0(v, 'n7') / 7 + rev0(v, 'n30') / 30
                            + rev0(v, 'n60') / 60 + rev0(v, 'n90') / 90) / 4,
}


def passers(d, exclude=frozenset(), recency_min=None):
    """canonical_bt 후보 필터와 동일 로직 + 옵션 최근성 게이트(rev30, missing=pass)."""
    out = []
    for tk, v in FULL.get(d, {}).items():
        if tk in exclude or not vb._industry_ok(tk, TC): continue
        dv = DVDB.get(d, {}).get(tk)
        if dv is None or dv < DV_MIN: continue
        if vb._ms(v) < 0: continue
        if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
        if v['px'] / v['nc'] > PE_MAX: continue
        te_v = vb._pit_te(TE, tk, d)
        g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
        if g is not None and g < GAP_THR: continue
        if recency_min is not None:
            r30 = rev(v, 'n30')
            if r30 is not None and r30 < recency_min: continue
        out.append(tk)
    return out


def bt(rank_name='rev90', recency_min=None, phase=0, exclude=frozenset(), trace=False):
    """canonical_bt 복제 — 랭킹 키/최근성 게이트만 파라미터화."""
    rk = RANKS[rank_name]
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
            cand = [(tk, rk(FULL[d][tk])) for tk in passers(d, exclude, recency_min)]
            cand.sort(key=lambda x: -x[1]); hold = [t for t, _ in cand[:N]]
            if trace: log.append((d, i, len(cand), list(hold)))
    r = np.array(rets); nav = np.cumprod(1 + r); peak = np.maximum.accumulate(nav)
    tot = float(nav[-1] - 1) * 100; mdd = float((nav / peak - 1).min()) * 100
    return (tot, mdd, log) if trace else (tot, mdd)


def report(rank_name='rev90', recency_min=None, exclude=frozenset()):
    per = {p: bt(rank_name, recency_min, p, exclude) for p in range(R)}
    return (float(np.mean([v[0] for v in per.values()])),
            float(np.mean([v[1] for v in per.values()])), per)


def fwd_ret(tk, i, hz):
    if i + hz >= len(AD): return None
    p0 = FULL.get(AD[i], {}).get(tk, {}).get('px')
    p1 = FULL.get(AD[i + hz], {}).get(tk, {}).get('px')
    if not p0 or not p1 or p0 <= 0: return None
    return (p1 / p0 - 1) * 100


def spearman(x, y):
    x = np.asarray(x, float); y = np.asarray(y, float)
    rx = np.argsort(np.argsort(x)); ry = np.argsort(np.argsort(y))
    if rx.std() == 0 or ry.std() == 0: return None
    return float(np.corrcoef(rx, ry)[0, 1])


def main():
    print('=' * 78)
    print('[0] BT 무결성 검증: 본 스크립트 baseline == vm_canonical_bt')
    ok = True
    for p in range(R):
        a = bt('rev90', None, p)
        b = vb.canonical_bt(PE_MAX, GAP_THR, N, R, phase=p, dv_min=DV_MIN)
        m = (abs(a[0] - b[0]) < 1e-9 and abs(a[1] - b[1]) < 1e-9)
        ok &= m
        print(f'  위상{p}: local {a[0]:+7.2f}/{a[1]:+6.2f}  canonical {b[0]:+7.2f}/{b[1]:+6.2f}  {"OK" if m else "MISMATCH"}')
    if not ok: raise SystemExit('무결성 실패 — 이하 결과 무효')

    # ================= [1] stale EDA =================
    print('\n' + '=' * 78)
    print('[1] stale revision EDA (stale := rev90>=20 & rev30<=2; 민감도 병기)')
    n_pass = n_stale = 0; stale_days = []
    top5_obs = []  # (d,i,tk,rev90,rev30,stale)
    for i in range(START, len(AD)):
        d = AD[i]
        ps = passers(d)
        rows = [(tk, rev0(FULL[d][tk], 'n90'), rev(FULL[d][tk], 'n30')) for tk in ps]
        st = [r for r in rows if r[1] >= 20 and r[2] is not None and r[2] <= 2]
        n_pass += len(rows); n_stale += len(st)
        stale_days.append((d, len(st), len(rows)))
        rows.sort(key=lambda x: -x[1])
        for tk, r90, r30 in rows[:5]:
            top5_obs.append((d, i, tk, r90, r30,
                             r90 >= 20 and r30 is not None and r30 <= 2))
    print(f'  게이트 통과자 관측 총 {n_pass}건(일평균 {n_pass/len(stale_days):.1f}) 중 stale {n_stale}건 = {n_stale/n_pass*100:.1f}%')
    for thr30 in (1, 2, 5):
        c = sum(1 for i in range(START, len(AD)) for tk in passers(AD[i])
                if rev0(FULL[AD[i]][tk], 'n90') >= 20
                and rev(FULL[AD[i]][tk], 'n30') is not None
                and rev(FULL[AD[i]][tk], 'n30') <= thr30)
        print(f'    민감도 rev30<={thr30}: {c}건 ({c/n_pass*100:.1f}%)')
    st5 = [o for o in top5_obs if o[5]]
    print(f'  일별 top5 관측 {len(top5_obs)}건 중 stale {len(st5)}건 = {len(st5)/len(top5_obs)*100:.1f}%')
    # stale top5 사례 목록(티커별 집계 + 이후 수익)
    from collections import defaultdict
    agg = defaultdict(list)
    for d, i, tk, r90, r30, s in st5: agg[tk].append((d, i, r90, r30))
    print('  --- 일별 top5 진입 stale 사례 (티커: 일수, 기간, 평균rev90/rev30, fwd5/fwd20 평균) ---')
    for tk, lst in sorted(agg.items(), key=lambda x: -len(x[1])):
        f5 = [fwd_ret(tk, i, 5) for _, i, _, _ in lst]; f5 = [x for x in f5 if x is not None]
        f20 = [fwd_ret(tk, i, 20) for _, i, _, _ in lst]; f20 = [x for x in f20 if x is not None]
        print(f'    {tk:6s}: {len(lst):3d}일  {lst[0][0]}~{lst[-1][0]}  rev90 {np.mean([x[2] for x in lst]):+6.1f} rev30 {np.mean([x[3] for x in lst]):+5.2f}'
              f'  fwd5 {np.mean(f5) if f5 else float("nan"):+6.2f}%  fwd20 {np.mean(f20) if f20 else float("nan"):+6.2f}%')
    ns5 = [o for o in top5_obs if not o[5]]
    for nm, grp in (('stale top5', st5), ('non-stale top5', ns5)):
        f5 = [fwd_ret(tk, i, 5) for _, i, tk, _, _, _ in grp]; f5 = [x for x in f5 if x is not None]
        f20 = [fwd_ret(tk, i, 20) for _, i, tk, _, _, _ in grp]; f20 = [x for x in f20 if x is not None]
        print(f'  {nm:15s}: fwd5 평균 {np.mean(f5):+6.2f}% (n={len(f5)})  fwd20 평균 {np.mean(f20):+6.2f}% (n={len(f20)})')

    # 실제 리밸 픽(위상 0~4 전부) 중 stale
    print('  --- 실제 리밸 픽(위상 0~4 유니온) 중 stale 픽과 보유기간(5d) 수익 ---')
    pick_rows = []
    for p in range(R):
        _, _, log = bt('rev90', None, p, trace=True)
        for d, i, nc_, hold in log:
            for tk in hold:
                v = FULL[d][tk]
                s = rev0(v, 'n90') >= 20 and rev(v, 'n30') is not None and rev(v, 'n30') <= 2
                pick_rows.append((p, d, i, tk, rev0(v, 'n90'), rev(v, 'n30'), s, fwd_ret(tk, i, 5)))
    sp = [r for r in pick_rows if r[6]]; nsp = [r for r in pick_rows if not r[6]]
    print(f'  전체 픽-리밸 관측 {len(pick_rows)}건 중 stale {len(sp)}건 = {len(sp)/len(pick_rows)*100:.1f}%')
    for p, d, i, tk, r90, r30, s, f5 in sp:
        print(f'    위상{p} {d} {tk:6s} rev90 {r90:+6.1f} rev30 {r30:+5.2f} → 5d {f5 if f5 is not None else float("nan"):+.2f}%')
    v5s = [r[7] for r in sp if r[7] is not None]; v5n = [r[7] for r in nsp if r[7] is not None]
    if v5s: print(f'  stale 픽 5d 평균 {np.mean(v5s):+.2f}% (n={len(v5s)}) vs non-stale {np.mean(v5n):+.2f}% (n={len(v5n)})')

    # ================= [2] IC / 버킷 =================
    print('\n' + '=' * 78)
    print('[2] 게이트 통과자 IC (Spearman, 일별→평균) + rev90高 그룹의 rev30 tercile 분해')
    sigs = list(RANKS.keys())
    for hz in (5, 20):
        print(f'  --- fwd {hz}d ---')
        for s in sigs:
            ics = []
            for i in range(START, len(AD) - hz):
                d = AD[i]; ps = passers(d)
                xs, ys = [], []
                for tk in ps:
                    f = fwd_ret(tk, i, hz)
                    if f is None: continue
                    xs.append(RANKS[s](FULL[d][tk])); ys.append(f)
                if len(xs) >= 10:
                    ic = spearman(xs, ys)
                    if ic is not None: ics.append(ic)
            ics = np.array(ics)
            t = ics.mean() / ics.std() * np.sqrt(len(ics)) if len(ics) > 2 and ics.std() > 0 else float('nan')
            print(f'    {s:12s}: IC {ics.mean():+.4f} (std {ics.std():.3f}, n일 {len(ics)}, t~{t:+.1f} ⚠중첩표본)')
    # 핵심 검정: rev90 상위(일별 70%ile 이상) 그룹 내 rev30 tercile → fwd
    print('  --- 핵심 검정: rev90 상위30% 그룹을 rev30 tercile로 분해 (day-level 평균) ---')
    for hz in (5, 20):
        day_terc = {0: [], 1: [], 2: []}; day_spread = []
        for i in range(START, len(AD) - hz):
            d = AD[i]; ps = passers(d)
            rows = []
            for tk in ps:
                f = fwd_ret(tk, i, hz)
                r30 = rev(FULL[d][tk], 'n30')
                if f is None or r30 is None: continue
                rows.append((rev0(FULL[d][tk], 'n90'), r30, f))
            if len(rows) < 15: continue
            r90s = np.array([r[0] for r in rows])
            cut = np.percentile(r90s, 70)
            grp = [r for r in rows if r[0] >= cut]
            if len(grp) < 6: continue
            grp.sort(key=lambda x: x[1])
            k = len(grp) // 3
            lo, mid, hi = grp[:k], grp[k:2 * k], grp[2 * k:]
            m = [np.mean([g[2] for g in b]) for b in (lo, mid, hi)]
            for j in range(3): day_terc[j].append(m[j])
            day_spread.append(m[2] - m[0])
        sp_ = np.array(day_spread)
        print(f'    fwd{hz}d: rev30 low {np.mean(day_terc[0]):+.2f}%  mid {np.mean(day_terc[1]):+.2f}%  high {np.mean(day_terc[2]):+.2f}%'
              f'  | spread(high-low) {sp_.mean():+.2f}% (std {sp_.std():.2f}, n일 {len(sp_)}, 양수일 {np.mean(sp_>0)*100:.0f}%)')

    # ================= [3] BT 변형 =================
    print('\n' + '=' * 78)
    print('[3] BT 변형 — 위상 0~4 평균, LOWO 3종, paired 차분 vs baseline(rev90)')
    base = {}
    for ex in LOWO_SETS:
        base[ex] = report('rev90', None, ex)
    lbl = {frozenset(): 'full', frozenset({'SNDK'}): '-SNDK', frozenset({'MU'}): '-MU',
           frozenset({'SNDK', 'MU'}): '-both'}
    print(f'  baseline rev90    : ' + '  '.join(
        f'{lbl[ex]} {base[ex][0]:+6.1f}%/{base[ex][1]:+5.1f}' for ex in LOWO_SETS))
    variants = [('rev60', None), ('rev30', None), ('rev7', None),
                ('blend_v8010', None), ('blend_eq', None), ('slope_avg', None)]
    for x in (-2, -1, 0, 1, 2, 3, 5):
        variants.append(('rev90', x))
    for rn, rc in variants:
        name = rn if rc is None else f'rev90+rec30>={rc}'
        cells = []
        for ex in LOWO_SETS:
            t, m, per = report(rn, rc, ex)
            # paired 차분 = 같은 위상끼리 차분 후 평균 (위상평균 차와 동일하나 분산 병기)
            diffs = [per[p][0] - base[ex][2][p][0] for p in range(R)]
            cells.append((t, m, float(np.mean(diffs)), float(np.min(diffs)), float(np.max(diffs))))
        print(f'  {name:18s}: ' + '  '.join(
            f'{lbl[ex]} {c[0]:+6.1f}%/{c[1]:+5.1f} (Δ{c[2]:+5.1f} [{c[3]:+.0f},{c[4]:+.0f}])'
            for ex, c in zip(LOWO_SETS, cells)))

    # ================= [4] 상호작용 =================
    print('\n' + '=' * 78)
    print('[4] 상호작용: 최근성 게이트 binding / MA120 중복성')
    conn = sqlite3.connect(os.path.join(BASE, 'eps_momentum_data.db'))
    ma120 = {}
    for tk, d, m in conn.execute('SELECT ticker,date,ma120 FROM ntm_screening WHERE ma120 IS NOT NULL'):
        ma120.setdefault(d, {})[tk] = m
    conn.close()
    for x in (0, 2):
        cut_n = kept_n = 0; cut_pool_short = 0
        cut_per, kept_per, cut_below120, kept_below120 = [], [], [], []
        cut_gap, kept_gap = [], []
        for i in range(START, len(AD)):
            d = AD[i]; ps = passers(d)
            ps_rec = set(passers(d, recency_min=x))
            for tk in ps:
                v = FULL[d][tk]
                per_ = v['px'] / v['nc']
                te_v = vb._pit_te(TE, tk, d)
                g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
                b120 = (v['px'] < ma120.get(d, {}).get(tk)) if ma120.get(d, {}).get(tk) else None
                if tk in ps_rec:
                    kept_n += 1; kept_per.append(per_)
                    if g: kept_gap.append(g)
                    if b120 is not None: kept_below120.append(b120)
                else:
                    cut_n += 1; cut_per.append(per_)
                    if g: cut_gap.append(g)
                    if b120 is not None: cut_below120.append(b120)
            if len(ps_rec) < N: cut_pool_short += 1
        print(f'  rec30>={x}: 컷 {cut_n}/{cut_n+kept_n}건({cut_n/(cut_n+kept_n)*100:.1f}%), 풀<N인 날 {cut_pool_short}/{len(AD)-START}')
        print(f'    컷 그룹: fwd_PER 중앙값 {np.median(cut_per):.1f}, gap 중앙값 {np.median(cut_gap) if cut_gap else float("nan"):.2f}, MA120 아래 비율 {np.mean(cut_below120)*100 if cut_below120 else float("nan"):.0f}%')
        print(f'    킵 그룹: fwd_PER 중앙값 {np.median(kept_per):.1f}, gap 중앙값 {np.median(kept_gap) if kept_gap else float("nan"):.2f}, MA120 아래 비율 {np.mean(kept_below120)*100 if kept_below120 else float("nan"):.0f}%')
    # stale 그룹 자체의 MA120 상태 (최근성 조건이 하락모멘텀을 이미 거르나의 역질문)
    st_b, ns_b = [], []
    for i in range(START, len(AD)):
        d = AD[i]
        for tk in passers(d):
            v = FULL[d][tk]
            m = ma120.get(d, {}).get(tk)
            if not m: continue
            s = rev0(v, 'n90') >= 20 and rev(v, 'n30') is not None and rev(v, 'n30') <= 2
            (st_b if s else ns_b).append(v['px'] < m)
    print(f'  stale 통과자 MA120 아래 비율 {np.mean(st_b)*100:.0f}% (n={len(st_b)}) vs non-stale {np.mean(ns_b)*100:.0f}% (n={len(ns_b)})')


if __name__ == '__main__':
    main()
