# -*- coding: utf-8 -*-
"""게이트 결합 전수 감사 (2026-08-01) — 상호작용·상충·중복 점검

계기: 사용자 "필터를 하나씩 최적화해도 같이 쓰면 상충하거나 오히려 나빠질 수 있다. 점검해봐."
현 체인은 그리디 경로로 조립됐다: min_seg(-2%)는 rev_up30 도입 전에 스윕, PE30·dv$1B는
구 설정에서 상속. KR 원칙("필터 적층=손실 복리, 신규 게이트는 기존 체인 위 한계기여로 평가")
위반 여부를 결합 스윕으로 판정한다.

방법:
  5축 전수 = dv{500,750,1000,1500} × PE{20,25,30,40,None} × ms{None,-5,-2,0}
             × revup{0,2,3,5} × gap{None,1.5}  → 640셀
  전부 exec_lag=1 · N5 · R5 · 위상 0~4 평균. A군 안전필터(동전주·애널3·rev90>0·영익5%·
  FCF&ROE)는 품질 가드로 고정(축 아님).
  ★640셀 최고를 뽑는 게 목적이 아니다(그건 max-selection 편향). 목적:
    ①현 조합이 고원 위인가 절벽 옆인가 ②축별 최적이 다른 축 값에 따라 뒤집히나(상호작용)
    ③각 게이트의 고유컷(다른 게이트가 못 잡는 몫) ④현 조합을 LOWO까지 이기는 셀이 있나
"""
import sys, os, sqlite3, itertools
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
import daily_runner as dr
import unified_vm_track as U
from _prod_faithful_bt_2026_07_31 import load

FULL, AG, FUND = load(); TE = C._load_te('full'); AD, _, _, TC, _ = C._load()
PX = {d: {t: v['px'] for t, v in FULL.get(d, {}).items() if v['px']} for d in AD}
RU = {}
_c = sqlite3.connect(dr.DB_PATH)
for tk, d, uu in _c.execute('SELECT ticker,date,rev_up30 FROM ntm_screening WHERE rev_up30 IS NOT NULL'):
    RU.setdefault(d, {})[tk] = uu or 0
_c.close()

# ── 1) 일별 후보 피처 선계산 (A군 안전필터 통과분만) ──
DAY = {}
for d in AD:
    rows = []
    for tk, v in FULL.get(d, {}).items():
        if not C._industry_ok(tk, TC): continue
        p, nc = v['px'], v['nc']
        if nc <= 0 or (v['n90'] or 0) <= 0.1: continue
        if p < 10 or (v['na'] or 0) < 3: continue
        if (nc - (v['n90'] or 0)) / abs(v['n90'] or 1) <= 0: continue
        om, fcf, roe = FUND.get(tk, (None, None, None))
        if om is not None and om < 0.05: continue
        if fcf is not None and roe is not None and fcf < 0 and roe < 0: continue
        a = AG.get(d, {}).get(tk)
        if a is None: continue
        ms = min(C._ms(dict(nc=nc, n7=v['n7'], n30=v['n30'], n60=v['n60'], n90=v['n90'])), 0)
        te_r = TE.get(tk); te = te_r[-1][1] if te_r else None
        if te and te > 0 and p / te < 3.0:   # 통화 단위 가드 (2026-08-01)
            te = None
        g = (nc / te) if (te and te > 0) else None
        rows.append((tk, a, ms, g, v['dv'], p / nc, RU.get(d, {}).get(tk, 0)))
    rows.sort(key=lambda r: r[1])            # 괴리율 오름차순(좋은 순)
    DAY[d] = rows

AX = dict(dv=[500.0, 750.0, 1000.0, 1500.0],
          pe=[20.0, 25.0, 30.0, 40.0, None],
          ms=[None, -5.0, -2.0, 0.0],
          ru=[0, 2, 3, 5],
          gp=[None, 1.5])
CUR = dict(dv=1000.0, pe=30.0, ms=-2.0, ru=3, gp=None)


def picks_for(d, dv, pe, ms, ru, gp, topn=8):
    out = []; seen = set()
    for tk, a, m, g, dvv, fpe, u in DAY[d]:
        if dvv is None or dvv < dv: continue
        if pe is not None and fpe > pe: continue
        if ms is not None and m < ms: continue
        if u < ru: continue
        if gp is not None and g is not None and g < gp: continue
        k = U.DUAL_CLASS.get(tk, tk)
        if k in seen: continue
        seen.add(k); out.append(tk)
        if len(out) >= topn: break
    return out


def replay(PICKS, phase, exclude=frozenset()):
    hold = []; pend = None; rets = []
    for i in range(2, len(AD)):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        rets.append(sum((px[t] / ppx[t] - 1) / 5 for t in hold
                        if t in px and t in ppx and ppx[t] > 0))
        if pend is not None and pend[0] == i:
            hold = pend[1]; pend = None
        if i % 5 == phase:
            pend = (i + 1, [t for t in PICKS[d] if t not in exclude][:5])
    r = np.array(rets); nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100


YRS = (len(AD) - 2) / 252.0
def cal(t, m): return (((1 + t / 100) ** (1 / YRS) - 1) * 100) / abs(m) if m else 0


def evaluate(combo, lowo=False):
    PICKS = {d: picks_for(d, **combo) for d in AD}
    o = [replay(PICKS, p) for p in range(5)]
    t = np.mean([x[0] for x in o]); m = np.mean([x[1] for x in o])
    res = dict(ret=t, mdd=m, cal=cal(t, m))
    if lowo:
        worst = 1e9
        for ex in [('SNDK',), ('MU',), ('SNDK', 'MU'), ('SNDK', 'MU', 'STX')]:
            o2 = [replay(PICKS, p, frozenset(ex)) for p in range(5)]
            worst = min(worst, cal(np.mean([x[0] for x in o2]), np.mean([x[1] for x in o2])))
        res['lowo_worst'] = worst
    return res


if __name__ == '__main__':
    keys = list(AX)
    combos = [dict(zip(keys, vals)) for vals in itertools.product(*[AX[k] for k in keys])]
    print('결합 스윕: %d셀 · lag=1 · 위상평균' % len(combos))
    R = {}
    for i, cb in enumerate(combos):
        R[tuple(cb.items())] = evaluate(cb)
    curk = tuple(CUR.items())
    cur = R[curk]
    ranked_all = sorted(R.items(), key=lambda kv: -kv[1]['cal'])
    pos = [i for i, (k, _) in enumerate(ranked_all, 1) if k == curk][0]

    print('\n=== ① 현 조합의 위치 ===')
    print('현행(dv1B·PE30·ms-2·ru3·gap無): 수익 %+.1f%% MDD %+.1f%% Calmar %.2f' %
          (cur['ret'], cur['mdd'], cur['cal']))
    print('640셀 중 Calmar 순위: %d위 (상위 %.0f%%)' % (pos, pos / len(combos) * 100))
    cals = np.array([v['cal'] for v in R.values()])
    print('전 셀 분포: 중앙값 %.2f · 상위10%% %.2f · 최고 %.2f' %
          (np.median(cals), np.percentile(cals, 90), cals.max()))

    print('\n=== ② 축별 조건부 최적 (상호작용 검출) ===')
    print('다른 축을 현행에 고정했을 때 vs 구(舊)조건에 고정했을 때 — 최적값이 바뀌면 상호작용')
    OLD = dict(dv=1000.0, pe=30.0, ms=0.0, ru=0, gp=1.5)   # 그리디 시작점(rev90 시절 체인)
    for ax in keys:
        row_cur, row_old = [], []
        for v in AX[ax]:
            c1 = dict(CUR); c1[ax] = v
            c2 = dict(OLD); c2[ax] = v
            row_cur.append((R[tuple(c1.items())]['cal'], v))
            row_old.append((R[tuple(c2.items())]['cal'], v))
        b1 = max(row_cur)[1]; b2 = max(row_old)[1]
        flip = ' ★상호작용(최적이 문맥따라 다름)' if b1 != b2 else ''
        print('  %-4s 현행문맥 최적=%-6s 구문맥 최적=%-6s%s' % (ax, b1, b2, flip))
        print('        현행문맥: %s' % '  '.join('%s:%.1f' % (v, c) for c, v in row_cur))

    print('\n=== ③ 게이트 고유컷 (최근 20일 평균, A군 통과 후보 대비) ===')
    days = AD[-20:]
    tot = alone = {}
    labels = dict(dv='dv$1B', pe='PE30', ms='ms-2', ru='ru3', gp='gap1.5')
    for ax in keys:
        uniq = 0; base = 0
        for d in days:
            for tk, a, m, g, dvv, fpe, u in DAY[d]:
                base += 1
                cut = dict(
                    dv=(dvv is None or dvv < CUR['dv']),
                    pe=(CUR['pe'] is not None and fpe > CUR['pe']),
                    ms=(CUR['ms'] is not None and m < CUR['ms']),
                    ru=(u < CUR['ru']),
                    gp=(False))
                if cut[ax] and not any(cut[o] for o in keys if o != ax):
                    uniq += 1
        print('  %-6s 고유컷 %.1f%% (다른 게이트가 못 잡고 이 게이트만 잡는 몫)'
              % (labels[ax], uniq / base * 100 * len(days) / len(days)))

    print('\n=== ④ 현 조합을 이기는 셀이 견고한가 (상위 8셀 LOWO) ===')
    print('%-44s %7s %7s %8s %9s' % ('조합', '수익%', 'MDD%', 'Calmar', 'LOWO최악'))
    cur_l = evaluate(CUR, lowo=True)
    print('%-44s %+7.1f %+7.1f %8.2f %9.2f  ←현행' %
          ('dv1000·PE30·ms-2·ru3·gap無', cur_l['ret'], cur_l['mdd'], cur_l['cal'], cur_l['lowo_worst']))
    beat = 0
    for k, v in ranked_all[:8]:
        cb = dict(k)
        lab = 'dv%d·PE%s·ms%s·ru%d·gap%s' % (cb['dv'], cb['pe'] or '∞', cb['ms'], cb['ru'], cb['gp'] or '無')
        r2 = evaluate(cb, lowo=True)
        mark = ''
        if r2['cal'] > cur_l['cal'] and r2['lowo_worst'] > cur_l['lowo_worst']:
            beat += 1; mark = '  ★현행을 양면에서 이김'
        print('%-44s %+7.1f %+7.1f %8.2f %9.2f%s' %
              (lab, r2['ret'], r2['mdd'], r2['cal'], r2['lowo_worst'], mark))
    print('\n상위 8셀 중 헤드라인+LOWO 동시 우위: %d개' % beat)
