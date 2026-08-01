# -*- coding: utf-8 -*-
"""산식 내부 감사 (2026-08-01) — adj_gap의 세 부품이 엔진인가 짐인가

adj_gap = fwd_pe_chg(창가중 0.30/0.10/0.10/0.50) × (1+dir_factor) × eps_quality
전부 v80.3~v80.10(2026-04~05, 구 2슬롯 시스템)에서 검증된 값 — 현 구조(top5·R5·게이트체인)
에서는 한 번도 재검증된 적 없다. 사용자 지시: "노이즈밴드에 억지로 박은 값인지 파악해라."

방법: 가격 이력(px_full parquet + 현행 PX 병합)으로 변형 산식을 재계산해 순위 교체.
  v_db      : DB adj_gap 그대로 (승수 포함 전체 산식)
  v_longtail: 창가중 PE압축만 (0.30/0.10/0.10/0.50, 승수 없음)
  v_equal   : 균등가중 PE압축 (0.25×4)
  v_90only  : 90일 창 단독
  v_7only   : 7일 창 단독
  v_db ≈ v_longtail 이면 승수(dir·eps_q)는 승객. 창가중 지형이 평평하면 가중치도 임의값.
게이트 = 신규 스펙(dv$300M·PE30·ms-2·ru3·안전필터) 고정, exec_lag=1, 위상평균, LOWO.
창 결측 시 가용 가중치로 재정규화(production 동일 규약).
"""
import sys, os, sqlite3
import numpy as np
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import vm_canonical_bt as C
import daily_runner as dr
import unified_vm_track as U
from _joint_gate_audit_2026_08_01 import DAY, AD, PX, replay, cal

# ── 가격 이력 병합: px_full(2025-11~2026-07-02) + PX(2026-02-12~) ──
PXH = {}
try:
    dfp = pd.read_parquet(os.path.join(BASE, 'research', 'px_full_2026_07_04.parquet'))
    for dt in dfp.index:
        ds = str(dt)[:10]
        row = dfp.loc[dt].dropna()
        PXH[ds] = dict(zip(row.index, row.values))
except Exception as e:
    print('[px_full 로드 실패: %s]' % e)
for d in AD:
    PXH.setdefault(d, {}).update(PX.get(d, {}))
HDATES = sorted(PXH)
HIDX = {d: i for i, d in enumerate(HDATES)}

_c = sqlite3.connect(dr.DB_PATH)
NTM = {}
for tk, d, nc, n7, n30, n60, n90, ag in _c.execute(
        'SELECT ticker,date,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,adj_gap FROM ntm_screening '
        'WHERE ntm_current>0'):
    NTM.setdefault(d, {})[tk] = (nc, n7, n30, n60, n90, ag)
_c.close()

LOOKBACK = {'7d': 5, '30d': 21, '60d': 42, '90d': 63}   # 캘린더→거래일 근사


def px_ago(tk, d, td):
    i = HIDX.get(d)
    if i is None or i - td < 0:
        return None
    return PXH[HDATES[i - td]].get(tk)


def pe_chg(tk, d, weights):
    v = NTM.get(d, {}).get(tk)
    p_now = PXH.get(d, {}).get(tk)
    if not v or not p_now:
        return None
    nc = v[0]
    if nc <= 0:
        return None
    pe_now = p_now / nc
    num = den = 0.0
    for key, w in weights.items():
        ntm_k = {'7d': v[1], '30d': v[2], '60d': v[3], '90d': v[4]}[key]
        p_k = px_ago(tk, d, LOOKBACK[key])
        if ntm_k and ntm_k > 0 and p_k and p_k > 0:
            pe_then = p_k / ntm_k
            num += w * (pe_now - pe_then) / pe_then * 100
            den += w
    return num / den if den > 0 else None


W_LONG = {'7d': .30, '30d': .10, '60d': .10, '90d': .50}
W_EQ = {'7d': .25, '30d': .25, '60d': .25, '90d': .25}
W_90 = {'90d': 1.0}
W_7 = {'7d': 1.0}

VARIANTS = {
    'v_db': lambda tk, d: (NTM.get(d, {}).get(tk) or [None] * 6)[5],
    'v_longtail': lambda tk, d: pe_chg(tk, d, W_LONG),
    'v_equal': lambda tk, d: pe_chg(tk, d, W_EQ),
    'v_90only': lambda tk, d: pe_chg(tk, d, W_90),
    'v_7only': lambda tk, d: pe_chg(tk, d, W_7),
}


def picks_variant(d, score_fn, topn=8, dv_min=300.0):
    cand = []
    for tk, a, m, g, dvv, fpe, u in DAY[d]:
        if dvv is None or dvv < dv_min: continue
        if fpe > 30.0 or m < -2.0 or u < 3: continue
        s = score_fn(tk, d)
        if s is None: continue
        cand.append((s, tk))
    cand.sort()
    seen, out = set(), []
    for _, tk in cand:
        k = U.DUAL_CLASS.get(tk, tk)
        if k in seen: continue
        seen.add(k); out.append(tk)
        if len(out) >= topn: break
    return out


def evaluate(score_fn, lowo=True):
    P = {d: picks_variant(d, score_fn) for d in AD}
    o = [replay(P, p) for p in range(5)]
    t = np.mean([x[0] for x in o]); m = np.mean([x[1] for x in o])
    res = dict(ret=t, mdd=m, cal=cal(t, m))
    if lowo:
        worst = 1e9
        for ex in [('SNDK',), ('MU',), ('SNDK', 'MU'), ('SNDK', 'MU', 'STX')]:
            o2 = [replay(P, p, frozenset(ex)) for p in range(5)]
            worst = min(worst, cal(np.mean([x[0] for x in o2]), np.mean([x[1] for x in o2])))
        res['lowo'] = worst
    return res


if __name__ == '__main__':
    print('가격 이력: %s ~ %s (%d일)' % (HDATES[0], HDATES[-1], len(HDATES)))
    # 순위 일치도: v_db vs v_longtail (승수의 실효)
    agree = []
    for d in AD[-40:]:
        a = picks_variant(d, VARIANTS['v_db'], 5)
        b = picks_variant(d, VARIANTS['v_longtail'], 5)
        if a and b:
            agree.append(len(set(a) & set(b)) / 5)
    print('top5 일치율 v_db vs v_longtail(승수 제거): %.0f%%' % (np.mean(agree) * 100))
    print()
    print('%-12s %9s %9s %8s %9s' % ('변형', '수익%', 'MDD%', 'Calmar', 'LOWO최악'))
    print('-' * 52)
    for name, fn in VARIANTS.items():
        r = evaluate(fn)
        print('%-12s %+9.1f %+9.1f %8.2f %9.2f' % (name, r['ret'], r['mdd'], r['cal'], r['lowo']))
