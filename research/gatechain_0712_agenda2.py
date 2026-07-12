# -*- coding: utf-8 -*-
"""안건2 (2026-07-12): 3필터(fwd_PER<=30 · gap>=1.5 · dv $1B)의 역할·한계성·임계 적정성.

파트:
  A. BT 하네스 충실도: vm_canonical_bt는 sparse TE(142) 로드 — production은 full TE(1,536).
     양쪽으로 베이스라인(pe30/gap1.5/N5/dv1000) 재현해 어느 쪽이 정본 +103/-17.7인지 확정.
  B. 한계성 EDA: 일별 자격통과 후보 대상 3필터 pass/fail 행렬 + top5 실영향(필터 하나 빼면 픽이 바뀌나).
  C. 임계 스윕: pe_max x gap_thr 2D 그리드 + dv_min 스윕 (위상 0~4 평균 + LOWO).
  D. dv 임계별 통과 유니버스 시총 분포 (중형주 배제율).
읽기 전용 — DB 쓰기 없음.
"""
import sys, os, json, sqlite3
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.dirname(HERE)
sys.path.insert(0, BASE)
sys.path.insert(0, HERE)
import vm_canonical_bt as vc
import daily_runner as dr

AD, FULL, DVDB, TC, TE_SPARSE = vc._load()
TE_FULL = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm_full.json'), encoding='utf-8'))
TE_FULL.pop('_meta', None)

# num_analysts + fund(carry-forward) 로드 (A군 필터 EDA용)
conn = sqlite3.connect(dr.DB_PATH)
NA = {}
for tk, d, na in conn.execute('SELECT ticker,date,num_analysts FROM ntm_screening WHERE num_analysts IS NOT NULL'):
    NA.setdefault(d, {})[tk] = na
# carry-forward mc/om/fcf/roe (전 이력 forward-fill; 날짜별 스냅샷은 아래서 순차 갱신)
ROWS_FUND = list(conn.execute(
    'SELECT date,ticker,market_cap,operating_margin,free_cashflow,roe FROM ntm_screening ORDER BY date'))
conn.close()


def fund_snap_at(date_str):
    s = {}
    for d, tk, mc, om, fcf, roe in ROWS_FUND:
        if d > date_str:
            break
        e = s.setdefault(tk, [None, None, None, None])
        if mc is not None: e[0] = mc
        if om is not None: e[1] = om
        if fcf is not None: e[2] = fcf
        if roe is not None: e[3] = roe
    return s


def bt(te, pe_max=30, gap_thr=1.5, N=5, R=5, start=2, dv_min=1000.0, phase=0,
       exclude=frozenset(), agroup=False, fund_snaps=None):
    """canonical_bt 규약 복제 + TE 주입 + (옵션) A군 안전필터."""
    hold = []; rets = []
    for i in range(start, len(AD)):
        d, pv = AD[i], AD[i - 1]
        px = FULL.get(d, {}); ppx = FULL.get(pv, {})
        drr = 0.0
        for t in hold:
            cu = px.get(t, {}).get('px'); pp = ppx.get(t, {}).get('px')
            if cu and pp and pp > 0:
                drr += (1.0 / N) * (cu - pp) / pp
        rets.append(drr)
        if i % R == phase:
            fs = fund_snaps.get(d) if fund_snaps else None
            cand = []
            for tk, v in FULL.get(d, {}).items():
                if tk in exclude or not vc._industry_ok(tk, TC):
                    continue
                dv = DVDB.get(d, {}).get(tk)
                if dv is None or dv < dv_min:
                    continue
                if vc._ms(v) < 0:
                    continue
                if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1:
                    continue
                if v['px'] / v['nc'] > pe_max:
                    continue
                if gap_thr:
                    te_v = vc._pit_te(te, tk, d)
                    g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
                    if g is not None and g < gap_thr:
                        continue
                r90 = vc._rev90(v)
                if agroup:
                    if v['px'] < 10 or r90 <= 0 or (NA.get(d, {}).get(tk) or 0) < 3:
                        continue
                    if fs:
                        f = fs.get(tk)
                        om, fcf, roe = (f[1], f[2], f[3]) if f else (None, None, None)
                        if om is not None and om < 0.05:
                            continue
                        if fcf is not None and roe is not None and fcf < 0 and roe < 0:
                            continue
                cand.append((tk, r90))
            cand.sort(key=lambda x: -x[1])
            hold = [t for t, _ in cand[:N]]
    r = np.array(rets)
    nav = np.cumprod(1 + r); peak = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / peak - 1).min()) * 100


def phase_avg(te, lowo=False, **kw):
    ph = [bt(te, phase=p, **kw) for p in range(5)]
    out = dict(ret=float(np.mean([x[0] for x in ph])), mdd=float(np.mean([x[1] for x in ph])),
               per_phase=[(round(a, 1), round(b, 1)) for a, b in ph])
    if lowo:
        for lbl, ex in [('exS', {'SNDK'}), ('exM', {'MU'}), ('exSM', {'SNDK', 'MU'})]:
            ph2 = [bt(te, phase=p, exclude=frozenset(ex), **kw) for p in range(5)]
            out[lbl] = (round(float(np.mean([x[0] for x in ph2])), 1),
                        round(float(np.mean([x[1] for x in ph2])), 1))
    return out


# ═══ A. 하네스 충실도 ═══
print('═══ A. 베이스라인 재현: TE 소스별 (pe30/gap1.5/N5/dv1000, 위상평균) ═══')
for lbl, te in [('sparse TE(142) = vm_canonical_bt 현재 코드', TE_SPARSE),
                ('full TE(1536) = production _vm_trailing_eps', TE_FULL)]:
    r = phase_avg(te, pe_max=30, gap_thr=1.5, N=5, dv_min=1000, lowo=True)
    print(f'  {lbl}: {r["ret"]:+.1f}% / MDD {r["mdd"]:+.1f} | 위상별 {r["per_phase"]}')
    print(f'      LOWO exSNDK {r["exS"]} exMU {r["exM"]} exBOTH {r["exSM"]}')
# A군 포함(=production 완전 패리티) 베이스라인
fund_snaps = {}
for i in range(2, len(AD)):
    if i % 5 in (0, 1, 2, 3, 4):
        pass
# A군용 fund 스냅샷: 리밸 가능일 전부(위상별 상이) → 전 날짜 준비(느리면 캐시)
print('  … A군 fund 스냅샷 구축(전 날짜) …')
snap = {}
idx = 0
for d in AD:
    while idx < len(ROWS_FUND) and ROWS_FUND[idx][0] <= d:
        _, tk, mc, om, fcf, roe = ROWS_FUND[idx]
        e = snap.setdefault(tk, [None, None, None, None])
        if mc is not None: e[0] = mc
        if om is not None: e[1] = om
        if fcf is not None: e[2] = fcf
        if roe is not None: e[3] = roe
        idx += 1
    fund_snaps[d] = {k: list(v) for k, v in snap.items()}
r = phase_avg(TE_FULL, pe_max=30, gap_thr=1.5, N=5, dv_min=1000, lowo=True,
              agroup=True, fund_snaps=fund_snaps)
print(f'  full TE + A군 안전필터(=production 완전체): {r["ret"]:+.1f}% / MDD {r["mdd"]:+.1f}')
print(f'      LOWO exSNDK {r["exS"]} exMU {r["exM"]} exBOTH {r["exSM"]}')

# ═══ B. 한계성 EDA ═══
print('\n═══ B. 3필터 pass/fail 행렬 (전 106일, 자격=업종+min_seg+sanity 통과 후) ═══')
tot = dict(base=0, dv_f=0, per_f=0, gap_f=0, gap_known=0, gap_missing=0,
           u_dv=0, u_per=0, u_gap=0, all3=0, agroup_cut=0)
daily_all3 = []
for d in AD:
    fs = fund_snaps[d]
    for tk, v in FULL.get(d, {}).items():
        if not vc._industry_ok(tk, TC):
            continue
        if vc._ms(v) < 0 or v['nc'] <= 0 or (v['n90'] or 0) <= 0.1:
            continue
        tot['base'] += 1
        dv = DVDB.get(d, {}).get(tk)
        f_dv = (dv is None or dv < 1000)
        f_per = (v['px'] / v['nc'] > 30)
        te_v = vc._pit_te(TE_FULL, tk, d)
        g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
        if g is None:
            tot['gap_missing'] += 1
        else:
            tot['gap_known'] += 1
        f_gap = (g is not None and g < 1.5)
        tot['dv_f'] += f_dv; tot['per_f'] += f_per; tot['gap_f'] += f_gap
        if f_dv and not f_per and not f_gap: tot['u_dv'] += 1
        if f_per and not f_dv and not f_gap: tot['u_per'] += 1
        if f_gap and not f_dv and not f_per: tot['u_gap'] += 1
        if not (f_dv or f_per or f_gap):
            tot['all3'] += 1
            # A군이 추가로 컷하는 수
            na = NA.get(d, {}).get(tk) or 0
            r90 = vc._rev90(v)
            f = fs.get(tk)
            om, fcf, roe = (f[1], f[2], f[3]) if f else (None, None, None)
            if (v['px'] < 10 or r90 <= 0 or na < 3 or (om is not None and om < 0.05)
                    or (fcf is not None and roe is not None and fcf < 0 and roe < 0)):
                tot['agroup_cut'] += 1
b = tot['base']
print(f"  자격통과(종목-일): {b:,}")
print(f"  dv<$1B 컷: {tot['dv_f']:,} ({tot['dv_f']/b*100:.1f}%)  [고유컷 {tot['u_dv']:,}]")
print(f"  fwd_PER>30 컷: {tot['per_f']:,} ({tot['per_f']/b*100:.1f}%)  [고유컷 {tot['u_per']:,}]")
print(f"  gap<1.5 컷: {tot['gap_f']:,} ({tot['gap_f']/b*100:.1f}%)  [고유컷 {tot['u_gap']:,}]")
print(f"  gap 계산가능: {tot['gap_known']:,}/{b:,} ({tot['gap_known']/b*100:.1f}%) — missing=pass {tot['gap_missing']:,}")
print(f"  3필터 전부통과: {tot['all3']:,} — 그중 A군 추가컷 {tot['agroup_cut']:,}")

# top5 실영향: 필터 하나 빼면 top5가 바뀌는 날 수 (full 게이트 + A군, production 패리티)
def pick5(d, dv_min=1000, pe_max=30, gap_thr=1.5, use_agroup=True):
    fs = fund_snaps[d]
    cand = []
    for tk, v in FULL.get(d, {}).items():
        if not vc._industry_ok(tk, TC):
            continue
        dv = DVDB.get(d, {}).get(tk)
        if dv_min and (dv is None or dv < dv_min):
            continue
        if vc._ms(v) < 0 or v['nc'] <= 0 or (v['n90'] or 0) <= 0.1:
            continue
        if pe_max and v['px'] / v['nc'] > pe_max:
            continue
        if gap_thr:
            te_v = vc._pit_te(TE_FULL, tk, d)
            g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
            if g is not None and g < gap_thr:
                continue
        r90 = vc._rev90(v)
        if use_agroup:
            na = NA.get(d, {}).get(tk) or 0
            f = fs.get(tk)
            om, fcf, roe = (f[1], f[2], f[3]) if f else (None, None, None)
            if v['px'] < 10 or r90 <= 0 or na < 3:
                continue
            if om is not None and om < 0.05:
                continue
            if fcf is not None and roe is not None and fcf < 0 and roe < 0:
                continue
        cand.append((tk, r90))
    cand.sort(key=lambda x: -x[1])
    return [t for t, _ in cand[:5]]

diff_days = dict(dv=0, per=0, gap=0)
diff_examples = dict(dv=set(), per=set(), gap=set())
for d in AD:
    base5 = set(pick5(d))
    for k, kw in [('dv', dict(dv_min=None)), ('per', dict(pe_max=None)), ('gap', dict(gap_thr=None))]:
        alt = set(pick5(d, **kw))
        if alt != base5:
            diff_days[k] += 1
            diff_examples[k] |= (alt - base5)
print(f"\n  [top5 실영향 — 필터 하나 뺐을 때 top5가 달라지는 날 / {len(AD)}일]")
for k, lbl in [('dv', 'dv $1B'), ('per', 'fwd_PER<=30'), ('gap', 'gap>=1.5')]:
    ex = sorted(diff_examples[k])[:12]
    print(f"    {lbl} 제거 시: {diff_days[k]}일 변경 | 새로 들어올 종목(누적): {ex}")

# ═══ C. 임계 스윕 (full TE, N5, 위상평균) ═══
print('\n═══ C-1. pe_max x gap_thr 2D 그리드 (full TE, N5, dv1000, 위상평균 수익/MDD) ═══')
PES = [15, 20, 25, 30, 35, 40, 60, 10 ** 9]
GAPS = [0, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0]
grid = {}
for pe in PES:
    for gp in GAPS:
        r = phase_avg(TE_FULL, pe_max=pe, gap_thr=gp, N=5, dv_min=1000)
        grid[(pe, gp)] = r
hdr = 'pe\\gap ' + ''.join(f'{g:>14}' for g in GAPS)
print(hdr)
for pe in PES:
    lbl = 'inf' if pe > 1e6 else str(pe)
    row = ''.join(f"{grid[(pe,g)]['ret']:+7.1f}/{grid[(pe,g)]['mdd']:+6.1f}" for g in GAPS)
    print(f'{lbl:>6} ' + row)

print('\n  [LOWO(exSNDK+MU) — gap행 @pe30 / pe행 @gap1.5]')
for gp in GAPS:
    r = phase_avg(TE_FULL, pe_max=30, gap_thr=gp, N=5, dv_min=1000, lowo=True)
    print(f"    pe30/gap{gp}: {r['ret']:+.1f}/{r['mdd']:+.1f} | exS {r['exS']} exM {r['exM']} exSM {r['exSM']}")
for pe in PES:
    lbl = 'inf' if pe > 1e6 else str(pe)
    r = phase_avg(TE_FULL, pe_max=pe, gap_thr=1.5, N=5, dv_min=1000, lowo=True)
    print(f"    pe{lbl}/gap1.5: {r['ret']:+.1f}/{r['mdd']:+.1f} | exS {r['exS']} exM {r['exM']} exSM {r['exSM']}")

print('\n═══ C-2. dv_min 스윕 (pe30/gap1.5/N5, 위상평균 + LOWO) ═══')
for dvm in [100, 250, 500, 1000, 1500, 2000, 4000]:
    r = phase_avg(TE_FULL, pe_max=30, gap_thr=1.5, N=5, dv_min=dvm, lowo=True)
    print(f"    dv>=${dvm}M: {r['ret']:+.1f}/{r['mdd']:+.1f} | exS {r['exS']} exM {r['exM']} exSM {r['exSM']}"
          f" | 위상별 {r['per_phase']}")

# ═══ D. dv 임계별 유니버스 시총 분포 ═══
print('\n═══ D. dv 임계별 통과 유니버스 시총 분포 (최신일, dv 이외 전 필터 통과 후보 기준) ═══')
d_last = AD[-1]
fs = fund_snaps[d_last]
rows = []
for tk, v in FULL.get(d_last, {}).items():
    if not vc._industry_ok(tk, TC):
        continue
    if vc._ms(v) < 0 or v['nc'] <= 0 or (v['n90'] or 0) <= 0.1:
        continue
    if v['px'] / v['nc'] > 30:
        continue
    te_v = vc._pit_te(TE_FULL, tk, d_last)
    g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
    if g is not None and g < 1.5:
        continue
    dv = DVDB.get(d_last, {}).get(tk)
    mc = (fs.get(tk) or [None])[0]
    rows.append((tk, dv, mc))
BUCKETS = [(0, 2e9, '<$2B(소형)'), (2e9, 10e9, '$2-10B(중형)'), (10e9, 50e9, '$10-50B'),
           (50e9, 200e9, '$50-200B'), (200e9, 1e15, '>$200B(초대형)')]
print(f'  후보(3필터 중 dv만 미적용, {d_last}): {len(rows)}종목, mc결측 {sum(1 for _,_,m in rows if not m)}')
for lo, hi, lbl in BUCKETS:
    grp = [(tk, dv) for tk, dv, mc in rows if mc and lo <= mc < hi]
    if not grp:
        print(f'  {lbl:>14}: 0종목'); continue
    line = f'  {lbl:>14}: {len(grp):3d}종목 | dv통과: '
    for thr in [250, 500, 1000, 2000]:
        n = sum(1 for _, dv in grp if dv and dv >= thr)
        line += f'${thr}M {n:3d}({n/len(grp)*100:3.0f}%)  '
    print(line)
# 역대 top5 픽의 시총
print('\n  [역대 리밸픽(위상0)의 시총 분포 — full 게이트+A군]')
picks_mc = []
for i in range(2, len(AD)):
    if i % 5 == 0:
        d = AD[i]
        for tk in pick5(d):
            mc = (fund_snaps[d].get(tk) or [None])[0]
            picks_mc.append((d, tk, mc))
import collections
cnt = collections.Counter()
for _, tk, mc in picks_mc:
    for lo, hi, lbl in BUCKETS:
        if mc and lo <= mc < hi:
            cnt[lbl] += 1
    if not mc:
        cnt['mc결측'] += 1
print('   ', dict(cnt))
print('\n완료.')
