# -*- coding: utf-8 -*-
"""dv 게이트를 '진입 전용'으로 내리는 안 — 정본 하네스 변형 BT (2026-07-30, HPE 계기).

배경: 현행 canonical_bt는 리밸마다 보유를 버리고 후보 topN 재구성 → dv($1B) 컷이
      진입게이트이자 보유·이탈게이트로 동작. HPE(rev90 +50%, 전 게이트 통과)가
      dv $996M(<$1B)만으로 순위에서 소멸 → 다음 리밸에 강제매도 예정.
      구시스템(2026-06-17)엔 "미달주도 순위에 둬야 winner를 오래 잡는다(-72%p)"는
      반대 BT가 있으나 전략이 달라 이식 불가 → 통합트랙 데이터로 재측정.

변형:
  A(현행)  dv 컷을 후보 전체에 적용 (진입=보유)
  B(진입전용) 직전 보유 종목은 dv 컷 면제 (나머지 게이트는 동일 적용)
  C(히스테리시스) 진입 $1B / 보유 $500M (유동성 진짜 붕괴 시에만 이탈)
보고: 위상 0~R-1 평균 (정본 규약), + LOWO.
"""
import sys, os, json, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C


def bt(mode='A', pe_max=30, gap_thr=1.5, N=5, R=5, start=2, end_date=None,
       dv_min=1000.0, dv_hold_min=500.0, phase=0, exclude=frozenset(), trace=False):
    ad, FULL, DVDB, TC, _ = C._load()
    TE = C._load_te('full')
    if end_date: ad = tuple(d for d in ad if d <= end_date)
    hold = []; rets = []; dates = []; log = []
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]
        px = FULL.get(d, {}); ppx = FULL.get(pv, {})
        drr = 0.0
        for t in hold:
            cu = px.get(t, {}).get('px'); pp = ppx.get(t, {}).get('px')
            if cu and pp and pp > 0: drr += (1.0 / N) * (cu - pp) / pp
        rets.append(drr); dates.append(d)
        if i % R == phase:
            prev = set(hold); cand = []
            for tk, v in FULL.get(d, {}).items():
                if tk in exclude or not C._industry_ok(tk, TC): continue
                dv = DVDB.get(d, {}).get(tk)
                # ---- dv 게이트: 모드별 ----
                if mode == 'A':
                    thr = dv_min
                elif mode == 'B':
                    thr = (0.0 if tk in prev else dv_min)
                elif mode == 'C':
                    thr = (dv_hold_min if tk in prev else dv_min)
                else:
                    raise ValueError(mode)
                if dv is None or dv < thr: continue
                # ---- 이하 전 게이트 동일 ----
                if C._ms(v) < 0: continue
                if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
                if v['px'] / v['nc'] > pe_max: continue
                if gap_thr:
                    te_v = C._pit_te(TE, tk, d); g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
                    if g is not None and g < gap_thr: continue
                cand.append((tk, C._rev90(v)))
            cand.sort(key=lambda x: -x[1]); hold = [t for t, _ in cand[:N]]
            if trace: log.append((d, len(cand), list(hold)))
    r = np.array(rets)
    nav = np.cumprod(1 + r); peak = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / peak - 1).min()) * 100, log


def report(mode, **kw):
    R = kw.pop('R', 5)
    ph = {p: bt(mode, R=R, phase=p, **kw)[:2] for p in range(R)}
    return (float(np.mean([v[0] for v in ph.values()])),
            float(np.mean([v[1] for v in ph.values()])), ph)


if __name__ == '__main__':
    end = sys.argv[1] if len(sys.argv) > 1 else None
    ad, *_ = C._load()
    print('데이터 마지막:', ad[-1])
    base = C.canonical_report(pe_max=30, gap_thr=1.5, N=5, end_date=end)
    print('\n[검증] 정본 하네스 재현: %+.1f%% / MDD %+.1f%%' % (base['avg_ret'], base['avg_mdd']))

    print('\n=== 위상평균 (수익% / MDD%) ===')
    res = {}
    for mode, name in [('A', 'A 현행(dv=진입+보유)'), ('B', 'B 진입전용(보유 면제)'),
                       ('C', 'C 히스테리시스(진입1B/보유0.5B)')]:
        ret, mdd, ph = report(mode, end_date=end)
        res[mode] = (ret, mdd)
        print('%-28s %+8.1f%%   %+7.1f%%   위상별 %s' %
              (name, ret, mdd, [round(v[0], 1) for v in ph.values()]))

    print('\n=== LOWO (슈퍼위너 제외 시에도 유지되나) ===')
    for ex in [('SNDK',), ('MU',), ('SNDK', 'MU'), ('STX',), ('HPE',)]:
        line = 'ex-%-12s' % '/'.join(ex)
        for mode in 'ABC':
            ret, mdd, _ = report(mode, end_date=end, exclude=frozenset(ex))
            line += '  %s %+7.1f%%/%+6.1f%%' % (mode, ret, mdd)
        print(line)
