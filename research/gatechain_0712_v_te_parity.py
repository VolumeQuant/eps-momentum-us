# -*- coding: utf-8 -*-
"""적대적 검증 (2026-07-12): patch-canonical-harness-te 권고 재현.

검증 항목:
  1. 하네스 정합: TE_SPARSE 주입 bt() == vm_canonical_bt.canonical_bt() 위상별 바이트동일
     (paired 비교가 진짜 paired인지 — 다른 코드 차이가 끼면 비교 무효)
  2. sparse vs full TE @ pe30/gap1.5/N5/dv1000: 위상평균 수익/MDD + LOWO exS/exM/exSM
     (권고 주장: sparse가 full 대비 −1.7p, exSM −4.3p)
  3. 픽 상이: 위상0 리밸 픽 비교 (sparse vs full 게이트 통과 차이)
  4. 임계 이월성: gap 1.25/1.75/2.0/2.5 에서도 sparse≠full 괴리가 존재하는가
     (단일점 1.5에서만 나는 아티팩트인지 확인)
읽기 전용.
"""
import sys, os, json
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.dirname(HERE)
sys.path.insert(0, BASE)
sys.path.insert(0, HERE)
import vm_canonical_bt as vc

AD, FULL, DVDB, TC, TE_SPARSE = vc._load()
TE_FULL = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm_full.json'), encoding='utf-8'))
TE_FULL.pop('_meta', None)
print(f'TE sparse {len(TE_SPARSE)}종목 / full {len(TE_FULL)}종목 / 거래일 {len(AD)} ({AD[0]}~{AD[-1]})')


def bt(te, pe_max=30, gap_thr=1.5, N=5, R=5, start=2, dv_min=1000.0, phase=0,
       exclude=frozenset(), trace=False):
    """canonical_bt 복제 + TE 주입 (agenda2 파트A와 동일 규약)."""
    hold = []; rets = []; log = []
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
                cand.append((tk, vc._rev90(v)))
            cand.sort(key=lambda x: -x[1])
            hold = [t for t, _ in cand[:N]]
            if trace:
                log.append((d, list(hold)))
    r = np.array(rets)
    nav = np.cumprod(1 + r); peak = np.maximum.accumulate(nav)
    tot = float(nav[-1] - 1) * 100; mdd = float((nav / peak - 1).min()) * 100
    if trace:
        return tot, mdd, log
    return tot, mdd


def pavg(te, lowo=False, **kw):
    ph = [bt(te, phase=p, **kw) for p in range(5)]
    out = dict(ret=float(np.mean([x[0] for x in ph])), mdd=float(np.mean([x[1] for x in ph])),
               per_phase=[(round(a, 1), round(b, 1)) for a, b in ph])
    if lowo:
        for lbl, ex in [('exS', {'SNDK'}), ('exM', {'MU'}), ('exSM', {'SNDK', 'MU'})]:
            p2 = [bt(te, phase=p, exclude=frozenset(ex), **kw) for p in range(5)]
            out[lbl] = (round(float(np.mean([x[0] for x in p2])), 1),
                        round(float(np.mean([x[1] for x in p2])), 1))
    return out


# ── 1. 하네스 정합 (TE_SPARSE 주입 == canonical_bt 원본) ──
print('\n[1] 하네스 정합 (pe30/gap1.5/N5/dv1000, 위상별):')
ok = True
for p in range(5):
    a = bt(TE_SPARSE, phase=p)
    b = vc.canonical_bt(pe_max=30, gap_thr=1.5, N=5, dv_min=1000.0, phase=p)
    match = abs(a[0] - b[0]) < 1e-9 and abs(a[1] - b[1]) < 1e-9
    ok &= match
    print(f'  위상{p}: 주입 {a[0]:+8.3f}/{a[1]:+7.3f} vs canonical {b[0]:+8.3f}/{b[1]:+7.3f} {"OK" if match else "MISMATCH"}')
print(f'  => 정합 {"통과 (paired 유효)" if ok else "실패 (비교 무효!)"}')

# ── 2. sparse vs full 본비교 ──
print('\n[2] sparse vs full TE @ pe30/gap1.5/N5/dv1000 (위상평균 + LOWO):')
rs = pavg(TE_SPARSE, lowo=True)
rf = pavg(TE_FULL, lowo=True)
for lbl, r in [('sparse', rs), ('full  ', rf)]:
    print(f'  {lbl}: {r["ret"]:+.1f}% / MDD {r["mdd"]:+.1f} | 위상별 {r["per_phase"]}')
    print(f'          LOWO exS {r["exS"]} exM {r["exM"]} exSM {r["exSM"]}')
print(f'  paired 차분(full−sparse): 전체 {rf["ret"]-rs["ret"]:+.2f}p / MDD {rf["mdd"]-rs["mdd"]:+.2f}')
print(f'    exS {rf["exS"][0]-rs["exS"][0]:+.1f}p  exM {rf["exM"][0]-rs["exM"][0]:+.1f}p  exSM {rf["exSM"][0]-rs["exSM"][0]:+.1f}p')

# ── 3. 픽 상이 (위상0 트레이스) ──
print('\n[3] 리밸 픽 비교 (위상0):')
_, _, log_s = bt(TE_SPARSE, phase=0, trace=True)
_, _, log_f = bt(TE_FULL, phase=0, trace=True)
ndiff = 0
for (d, hs), (_, hf) in zip(log_s, log_f):
    if set(hs) != set(hf):
        ndiff += 1
        only_s = set(hs) - set(hf); only_f = set(hf) - set(hs)
        print(f'  {d}: sparse만 {sorted(only_s)} / full만 {sorted(only_f)}')
print(f'  상이 리밸: {ndiff}/{len(log_s)}회')

# ── 4. 임계 이월성 (gap별 full−sparse 괴리) ──
print('\n[4] gap 임계별 full−sparse 괴리 (위상평균, exSM 병기):')
for g in [1.0, 1.25, 1.5, 1.75, 2.0, 2.5]:
    a = pavg(TE_SPARSE, gap_thr=g, lowo=True)
    b = pavg(TE_FULL, gap_thr=g, lowo=True)
    print(f'  gap{g}: sparse {a["ret"]:+.1f}/exSM {a["exSM"][0]:+.1f} | full {b["ret"]:+.1f}/exSM {b["exSM"][0]:+.1f}'
          f' | 차 {b["ret"]-a["ret"]:+.1f}p / exSM 차 {b["exSM"][0]-a["exSM"][0]:+.1f}p')

print('\n완료.')
