# -*- coding: utf-8 -*-
"""안건3 — MA120(가격추세) 조건을 게이트 체인에 넣어야 하는가 (2026-07-12).

vm_canonical_bt.py 정본 하네스 복사·확장(원본 무수정). 읽기 전용(DB write 0).

파트:
  A) EDA: P(price<ma120 | 게이트통과) vs P(price<ma120 | 유니버스) — gap+rev90이 이미 거르나
  B) 케이스: ma120 아래에서 top5 픽된 사례 전수 + fwd 5d/20d
  C) 게이트통과자 ma120 위/아래 fwd 수익 same-date paired 비교
  D) BT: 자격단계 trend 필터 변형(ma120/ma60/dd25=high30 -25%컷) 위상평균+LOWO+paired
     + 필터가 base top5에서 막은 픽의 보유기간 수익 분해(휩쏘 vs 회피)

주의: ma120은 2026-02-06~02-18 전결측 → 필터는 missing=pass(gap과 동일 규약).
"""
import sys, os, json, sqlite3, functools
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr

_BAD_IND = dr.COMMODITY_INDUSTRIES | dr.OFF_STRATEGY_INDUSTRIES
_BAD_TK = set(dr.COMMODITY_TICKERS)

PE_MAX, GAP_THR, N_BASE, DV_MIN, R = 30, 1.5, 5, 1000.0, 5


@functools.lru_cache(maxsize=1)
def _load():
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
    te = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm.json'), encoding='utf-8'))
    return ad, full, dvdb, tc, te


def _industry_ok(tk, tc):
    if tk in _BAD_TK: return False
    v = tc.get(tk)
    ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v)
    return not (isinstance(ind, str) and ind in _BAD_IND)


def _pit_te(te, tk, d):
    r = te.get(tk); v = None
    if not r: return None
    for rd, e in r:
        if rd <= d: v = e
        else: break
    return v


def _ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)


def _rev90(v):
    return (v['nc'] - v['n90']) / abs(v['n90']) * 100 if (v['n90'] and abs(v['n90']) > 0.01) else 0


def _trend_ok(v, mode):
    """가격추세 필터. missing=pass (gap 규약과 동일)."""
    if mode is None: return True
    if mode == 'ma120':
        return v['ma120'] is None or v['px'] >= v['ma120']
    if mode == 'ma60':
        return v['ma60'] is None or v['px'] >= v['ma60']
    if mode == 'dd25':  # 구시스템 dd_30_25 아날로그: 30일 high 대비 -25%+ 컷
        return v['h30'] is None or v['h30'] <= 0 or v['px'] / v['h30'] - 1 > -0.25
    raise ValueError(mode)


def eligible_pass(tk, v, d, dvmap, TC, TE, exclude=frozenset(), trend=None):
    """자격+게이트 전체 통과 여부 (trend 필터는 자격단계 삽입)."""
    if tk in exclude or not _industry_ok(tk, TC): return False
    dv = dvmap.get(d, {}).get(tk)
    if dv is None or dv < DV_MIN: return False
    if _ms(v) < 0: return False
    if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: return False
    if not _trend_ok(v, trend): return False
    if v['px'] / v['nc'] > PE_MAX: return False
    te_v = _pit_te(TE, tk, d); g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
    if g is not None and g < GAP_THR: return False
    return True


def canonical_bt(N=N_BASE, phase=0, end_date=None, exclude=frozenset(), trend=None,
                 start=2, trace=False):
    ad, FULL, DVDB, TC, TE = _load()
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
            cand = [(tk, _rev90(v)) for tk, v in FULL.get(d, {}).items()
                    if eligible_pass(tk, v, d, DVDB, TC, TE, exclude, trend)]
            cand.sort(key=lambda x: -x[1]); hold = [t for t, _ in cand[:N]]
            if trace: log.append((d, list(hold)))
    r = np.array(rets)
    nav = np.cumprod(1 + r); peak = np.maximum.accumulate(nav)
    tot = float(nav[-1] - 1) * 100; mdd = float((nav / peak - 1).min()) * 100
    return (tot, mdd, log) if trace else (tot, mdd)


def report(trend=None, exclude=frozenset()):
    pp = {p: canonical_bt(phase=p, exclude=exclude, trend=trend) for p in range(R)}
    return dict(per_phase=pp,
                avg_ret=float(np.mean([v[0] for v in pp.values()])),
                avg_mdd=float(np.mean([v[1] for v in pp.values()])))


def _fwd(FULL, ad, i, tk, k):
    """i번째 날짜 기준 k거래일 forward 수익(%). 데이터 없으면 None."""
    if i + k >= len(ad): return None
    p0 = FULL.get(ad[i], {}).get(tk, {}).get('px')
    p1 = FULL.get(ad[i + k], {}).get(tk, {}).get('px')
    if not p0 or not p1: return None
    return (p1 / p0 - 1) * 100


def part_a_eda():
    print('=' * 70)
    print('A) EDA: 게이트 통과자 vs 유니버스 — price<ma120 / <ma60 비중')
    ad, FULL, DVDB, TC, TE = _load()
    tot_u = tot_u_b120 = tot_u_b60 = 0
    tot_g = tot_g_b120 = tot_g_b60 = 0
    tot_e = tot_e_b120 = 0  # 자격(eligible)만 통과(게이트 전)
    daily = []
    for i, d in enumerate(ad):
        if d < '2026-02-19': continue  # ma120 전결측 구간 제외
        u = g = ub120 = ub60 = gb120 = gb60 = e = eb120 = 0
        for tk, v in FULL.get(d, {}).items():
            if v['ma120'] is None: continue
            b120 = v['px'] < v['ma120']
            b60 = v['ma60'] is not None and v['px'] < v['ma60']
            u += 1; ub120 += b120; ub60 += b60
            # 자격단계만(게이트 전): 업종+dv+min_seg
            if (_industry_ok(tk, TC) and (DVDB.get(d, {}).get(tk) or 0) >= DV_MIN
                    and _ms(v) >= 0 and v['nc'] > 0 and (v['n90'] or 0) > 0.1):
                e += 1; eb120 += b120
                if eligible_pass(tk, v, d, DVDB, TC, TE):
                    g += 1; gb120 += b120; gb60 += b60
        tot_u += u; tot_u_b120 += ub120; tot_u_b60 += ub60
        tot_g += g; tot_g_b120 += gb120; tot_g_b60 += gb60
        tot_e += e; tot_e_b120 += eb120
        daily.append((d, u, ub120 / u if u else 0, g, gb120 / g if g else 0))
    print(f'  유니버스(price>0,nc>0):        {tot_u:>7} 종목일, P(<ma120)={tot_u_b120/tot_u:6.1%}  P(<ma60)={tot_u_b60/tot_u:6.1%}')
    print(f'  자격통과(dv+min_seg+업종):     {tot_e:>7} 종목일, P(<ma120)={tot_e_b120/tot_e:6.1%}')
    print(f'  게이트통과(+PER30+gap1.5):     {tot_g:>7} 종목일, P(<ma120)={tot_g_b120/tot_g:6.1%}  P(<ma60)={tot_g_b60/tot_g:6.1%}')
    # 일별 분포 요약
    ur = [x[2] for x in daily]; gr = [x[4] for x in daily if x[3] > 0]
    print(f'  일별 P(<ma120): 유니버스 med {np.median(ur):.1%} / 게이트통과 med {np.median(gr):.1%}'
          f' (max {max(gr):.1%})')
    return daily


def part_b_cases():
    print('=' * 70)
    print('B) 케이스: ma120 아래에서 top5 픽 전수 (전 위상 리밸 유니온, base 파라미터)')
    ad, FULL, DVDB, TC, TE = _load()
    seen = {}
    for phase in range(R):
        _, _, log = canonical_bt(phase=phase, trace=True)
        for d, hold in log:
            i = ad.index(d)
            for rk, tk in enumerate(hold, 1):
                v = FULL[d][tk]
                key = (d, tk)
                if key in seen: continue
                m120 = v['ma120']; below = (m120 is not None and v['px'] < m120)
                seen[key] = dict(rk=rk, below=below,
                                 dist=(v['px'] / m120 - 1) * 100 if m120 else None,
                                 f5=_fwd(FULL, ad, i, tk, 5), f20=_fwd(FULL, ad, i, tk, 20),
                                 phase=phase)
    picks = list(seen.items())
    below = [(k, x) for k, x in picks if x['below']]
    above = [(k, x) for k, x in picks if not x['below'] and x['dist'] is not None]
    print(f'  픽(일자,종목) 유니온 총 {len(picks)}건 중 ma120 아래 픽 = {len(below)}건 ({len(below)/len(picks):.1%})')
    for (d, tk), x in sorted(below):
        print(f'   {d} {tk:<6} rk{x["rk"]} dist={x["dist"]:+.1f}% fwd5={x["f5"] if x["f5"] is None else round(x["f5"],1)} fwd20={x["f20"] if x["f20"] is None else round(x["f20"],1)}')
    for lbl, grp in [('아래', below), ('위', above)]:
        f5 = [x['f5'] for _, x in grp if x['f5'] is not None]
        f20 = [x['f20'] for _, x in grp if x['f20'] is not None]
        if f5:
            print(f'  ma120 {lbl} 픽: n={len(grp)} fwd5 mean {np.mean(f5):+.2f}% med {np.median(f5):+.2f}% | '
                  f'fwd20 mean {np.mean(f20):+.2f}% med {np.median(f20):+.2f}% (n20={len(f20)})')
    return below


def part_c_paired():
    print('=' * 70)
    print('C) 게이트통과자 ma120 위/아래 forward 수익 — same-date paired')
    ad, FULL, DVDB, TC, TE = _load()
    diffs5, diffs20 = [], []
    pooled = dict(a5=[], b5=[], a20=[], b20=[])
    for i, d in enumerate(ad):
        if d < '2026-02-19': continue
        ab5, bb5, ab20, bb20 = [], [], [], []
        for tk, v in FULL.get(d, {}).items():
            if v['ma120'] is None: continue
            if not eligible_pass(tk, v, d, DVDB, TC, TE): continue
            f5 = _fwd(FULL, ad, i, tk, 5); f20 = _fwd(FULL, ad, i, tk, 20)
            if v['px'] < v['ma120']:
                if f5 is not None: bb5.append(f5)
                if f20 is not None: bb20.append(f20)
            else:
                if f5 is not None: ab5.append(f5)
                if f20 is not None: ab20.append(f20)
        if ab5 and bb5: diffs5.append(np.mean(ab5) - np.mean(bb5))
        if ab20 and bb20: diffs20.append(np.mean(ab20) - np.mean(bb20))
        pooled['a5'] += ab5; pooled['b5'] += bb5; pooled['a20'] += ab20; pooled['b20'] += bb20
    for k, dif in [('fwd5', diffs5), ('fwd20', diffs20)]:
        if dif:
            arr = np.array(dif)
            print(f'  {k}: 위-아래 paired(같은 날 둘 다 존재 {len(arr)}일) mean {arr.mean():+.2f}%p '
                  f'med {np.median(arr):+.2f}%p, 위 우세일 {int((arr>0).sum())}/{len(arr)}')
    print(f'  pooled: 위 fwd5 {np.mean(pooled["a5"]):+.2f}%(n={len(pooled["a5"])}) vs 아래 {np.mean(pooled["b5"]):+.2f}%(n={len(pooled["b5"])})')
    print(f'          위 fwd20 {np.mean(pooled["a20"]):+.2f}%(n={len(pooled["a20"])}) vs 아래 {np.mean(pooled["b20"]):+.2f}%(n={len(pooled["b20"])})')


def part_d_bt():
    print('=' * 70)
    print('D) BT: trend 필터 변형 — 위상 0~4 + 평균 + LOWO + paired 차분')
    variants = [None, 'ma120', 'ma60', 'dd25']
    lowo = [frozenset(), frozenset({'SNDK'}), frozenset({'MU'}), frozenset({'SNDK', 'MU'})]
    base_rep = {}
    for exc in lowo:
        lbl = '-'.join(sorted(exc)) or 'full'
        base_rep[lbl] = {}
        for tr in variants:
            rep = report(trend=tr, exclude=exc)
            base_rep[lbl][tr or 'base'] = rep
        b = base_rep[lbl]['base']
        print(f'\n  [{lbl}] base: {b["avg_ret"]:+.1f}% / MDD {b["avg_mdd"]:+.1f}%  '
              f'(위상별 {[round(v[0],1) for v in b["per_phase"].values()]})')
        for tr in variants[1:]:
            r = base_rep[lbl][tr]
            d_ret = r['avg_ret'] - b['avg_ret']; d_mdd = r['avg_mdd'] - b['avg_mdd']
            per = [round(r['per_phase'][p][0] - b['per_phase'][p][0], 1) for p in range(R)]
            wins = sum(1 for p in range(R) if r['per_phase'][p][0] > b['per_phase'][p][0])
            print(f'    +{tr:<6}: {r["avg_ret"]:+.1f}% / MDD {r["avg_mdd"]:+.1f}%  '
                  f'Δret {d_ret:+.1f}p Δmdd {d_mdd:+.1f}p  위상별Δ {per} (승 {wins}/5)')
    return base_rep


def part_d2_blocked():
    print('=' * 70)
    print('D2) ma120 필터가 base top5에서 막은 픽 분해 (전 위상): 휩쏘 vs 회피')
    ad, FULL, DVDB, TC, TE = _load()
    rows = []
    for phase in range(R):
        _, _, log_b = canonical_bt(phase=phase, trace=True)
        _, _, log_f = canonical_bt(phase=phase, trend='ma120', trace=True)
        lf = dict(log_f)
        for d, hold in log_b:
            blocked = [t for t in hold if t not in lf.get(d, [])]
            i = ad.index(d)
            for tk in blocked:
                v = FULL[d][tk]
                if v['ma120'] is None or v['px'] >= v['ma120']:
                    continue  # 순위 재편성으로 빠진 것 말고 필터 직접 차단만
                f5 = _fwd(FULL, ad, i, tk, 5)
                rows.append((d, phase, tk, (v['px'] / v['ma120'] - 1) * 100, f5))
    if not rows:
        print('  차단 사례 0건'); return
    uniq = {}
    for d, ph, tk, dist, f5 in rows:
        uniq.setdefault((d, tk), (dist, f5, []))[2].append(ph)
    ups = [x for x in rows if x[4] is not None and x[4] > 0]
    dns = [x for x in rows if x[4] is not None and x[4] <= 0]
    print(f'  차단 (일자,종목,위상) {len(rows)}건 / 유니크 (일자,종목) {len(uniq)}건')
    for (d, tk), (dist, f5, phs) in sorted(uniq.items()):
        print(f'   {d} {tk:<6} dist={dist:+.1f}% fwd5(보유대체기간)={f5 if f5 is None else round(f5,1)} 위상{sorted(phs)}')
    f5s = [x[4] for x in rows if x[4] is not None]
    if f5s:
        print(f'  차단픽 fwd5: mean {np.mean(f5s):+.2f}% | 상승(휩쏘비용) {len(ups)}건 합 {sum(x[4] for x in ups):+.1f}%'
              f' | 하락(회피이득) {len(dns)}건 합 {sum(x[4] for x in dns):+.1f}%')


if __name__ == '__main__':
    part_a_eda()
    part_b_cases()
    part_c_paired()
    part_d_bt()
    part_d2_blocked()
