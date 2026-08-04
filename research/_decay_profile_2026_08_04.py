# -*- coding: utf-8 -*-
"""괴리율 창 가중치의 금융공학적 재구성 — 감쇠 프로파일 1파라미터화 (2026-08-04)

사용자 요구: "중첩을 하되 7/30/60/90 비중 .3/.1/.1/.5에 금융공학적 논거가 있는지 고찰하고,
없으면 여러 가중치로 실험해서 성과와의 상관을 따져 어떤 식으로 비중을 가야 맞는지 연구해."

── 1. 수식 정리 (이게 핵심) ─────────────────────────────────────────────
중첩 점수 = Σ_k w_k · (PE_now / PE_k − 1)  이고, 각 항은 '기준일 k부터 지금까지의 누적 변화'다.
로그 근사에서 누적 변화 = 그 사이 구간 변화들의 합이므로, 중첩 가중치는 **구간 가중치로 정확히
번역된다**:

    구간 7→현재  의 실효 가중 = w7 + w30 + w60 + w90 = 1.00
    구간 30→7    의 실효 가중 =      w30 + w60 + w90 = 0.70
    구간 60→30   의 실효 가중 =            w60 + w90 = 0.60
    구간 90→60   의 실효 가중 =                  w90 = 0.50      (현행 .3/.1/.1/.5 기준)

즉 **현행은 '구간 나이에 따라 1.0 → 0.7 → 0.6 → 0.5로 감쇠시키는 프로파일'** 이다.
같은 번역을 다른 설정에 적용하면:
    등가중 .25×4  → 구간 가중 (0.25, 0.50, 0.75, 1.00)  = 더 가파른 선형 감쇠
    90일 단독     → 구간 가중 (1.00, 1.00, 1.00, 1.00)  = **감쇠 없음(전 구간 동일)**
    7일 단독      → 구간 가중 (0.00, 0.00, 0.00, 1.00)  = 최근만

→ '중첩 vs 비중첩'은 다른 축이 아니라 **같은 축(감쇠 프로파일)의 양 끝**이었다.
   비중첩 등가중은 감쇠 0(전 구간 동일)에 해당하고, 그래서 '오래된 괴리'와 '방금 벌어진 괴리'를
   구분하지 못했던 것이다.

── 2. 금융공학적 논거 ────────────────────────────────────────────────
감쇠가 있어야 하는 이유: 추정치 수정에 대한 가격 반응은 즉시가 아니라 수개월에 걸쳐 진행되지만
(PEAD/추정치 드리프트), 시간이 갈수록 반영률이 올라간다. 따라서
  · 방금 벌어진 괴리 = 아직 덜 반영 → 기대수익 높음  → 큰 가중
  · 오래 전에 벌어졌는데 아직 안 메워진 괴리 = 시장이 이미 알고도 안 메움 → 구조적 이유(가치함정)
    가능성 ↑ → 작은 가중
즉 **단조 감쇠**가 이론적 기본형이고, 감쇠 0(평평)이나 역감쇠(과거 중시)는 근거가 약하다.

문제는 '얼마나 가파르게'다. 이건 이론이 답을 주지 않는다 → 실측 대상.
단 4개 가중치를 따로 고르면 자유도 3개를 117일 표본에 태우게 되므로(이날 3회 실패),
**기하 감쇠 1파라미터 r로 축소**한다:
    구간 가중 = (r³, r², r¹, 1)  [90→60, 60→30, 30→7, 7→현재]
    ⇒ 창 가중 w90=r³, w60=r²−r³, w30=r−r², w7=1−r     (전부 0 이상, r∈(0,1])
    r=1.0 → 감쇠 없음(=90일 단독) / r→0 → 최근만 / 현행 ≈ r 0.8 근방

── 3. 검증 방법 ──────────────────────────────────────────────────────
r을 0.05 간격으로 훑어 **곡선의 모양**을 본다. 봉우리 하나면 노이즈, 넓은 고원이면 신호.
분할 3개(train/test)에서 각 r의 test 성적을 보고, r-성과 관계가 분할 간 일관된지 확인한다.
★1등 r을 고르지 않는다 — 고원의 존재 여부와 그 중심 위치만 본다.
"""
import sys, os
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import unified_vm_track as U
from _joint_gate_audit_2026_08_01 import DAY, AD, PX
from _stop_revg_matrix_2026_08_03 import RG
from _pe_window_weights_2026_08_04 import fpc


def weights(r):
    """기하 감쇠 r → 창 가중치. r=1이면 90일 단독, r→0이면 7일 단독."""
    w = {'90d': r ** 3, '60d': r ** 2 - r ** 3, '30d': r - r ** 2, '7d': 1 - r}
    return {k: v for k, v in w.items() if v > 1e-9}


def seg_profile(r):
    return (r ** 3, r ** 2, r, 1.0)


_CK = {}
def picks(r):
    if r in _CK:
        return _CK[r]
    W = weights(r)
    out = {}
    for d in AD:
        rows, seen, lst = [], set(), []
        for tk, a, m, g, dvv, fpe, u in DAY[d]:
            if dvv is None or dvv < 300 or m < -2 or u < 1 or fpe > 30:
                continue
            rg = RG.get(d, {}).get(tk)
            if rg is not None and rg < 0.10:
                continue
            v = fpc(tk, d, W)
            if v is not None:
                rows.append((v, tk))
        rows.sort()
        for _v, tk in rows:
            k = U.DUAL_CLASS.get(tk, tk)
            if k in seen:
                continue
            seen.add(k); lst.append(tk)
        out[d] = lst
    _CK[r] = out
    return out


def run(P, phase, start, end, exclude=frozenset()):
    hold, pend, rets = [], None, []
    for i in range(start, end):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        rets.append(sum((px[t] / ppx[t] - 1) / 5 for t in hold
                        if t in px and t in ppx and ppx[t] > 0))
        if pend and pend[0] == i:
            hold = pend[1]; pend = None
        if i % 5 == phase % 5:
            pend = (i + 1, [t for t in P[d] if t not in exclude][:5])
    r = np.array(rets)
    if not len(r):
        return None
    nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100


def sc(P, start, end, **kw):
    o = [x for x in (run(P, p, start, end, **kw) for p in range(5)) if x]
    if not o:
        return None
    t, m = np.mean([x[0] for x in o]), np.mean([x[1] for x in o])
    y = (end - start) / 252.0
    return t, m, ((((1 + t / 100) ** (1 / y) - 1) * 100) / abs(m) if m else 0)


if __name__ == '__main__':
    RS = [round(x, 2) for x in np.arange(0.30, 1.001, 0.05)]
    print('현행 .3/.1/.1/.5의 실효 구간 가중 = (0.50, 0.60, 0.70, 1.00)  [90→60, 60→30, 30→7, 7→현재]')
    print('기하 감쇠 근사: r=0.80 → (%.2f, %.2f, %.2f, %.2f)\n' % seg_profile(0.80))

    print('■ 감쇠 r 전체 스윕 (전 구간 · 위상평균)')
    print('%-6s %-26s %9s %9s %8s %9s' %
          ('r', '창 가중 (7/30/60/90)', '수익%', 'MDD%', 'Calmar', 'LOWO둘다'))
    print('-' * 76)
    full = {}
    for r in RS:
        P = picks(r)
        t, m, c = sc(P, 2, len(AD))
        _t2, _m2, c2 = sc(P, 2, len(AD), exclude=frozenset(('SNDK', 'MU')))
        full[r] = (t, m, c, c2)
        W = weights(r)
        ws = '/'.join('%.2f' % W.get(k, 0) for k in ('7d', '30d', '60d', '90d'))
        print('%-6.2f %-26s %+9.1f %+9.1f %8.2f %9.2f' % (r, ws, t, m, c, c2))

    print('\n■ 분할별 test 성적 (곡선 모양이 분할 간 일관된가)')
    SPLITS = [int(len(AD) * f) for f in (0.55, 0.65, 0.75)]
    print('%-6s %s' % ('r', ''.join('%14s' % ('test@' + AD[s][5:]) for s in SPLITS)))
    print('-' * 62)
    tests = {}
    for r in RS:
        P = picks(r)
        row = []
        for s in SPLITS:
            x = sc(P, s + 1, len(AD))
            row.append(x[2] if x else float('nan'))
        tests[r] = row
        print('%-6.2f %s' % (r, ''.join('%14.2f' % v for v in row)))

    print('\n■ 분할별 최고 r / 상위 3개 r')
    for j, s in enumerate(SPLITS):
        order = sorted(RS, key=lambda r: -tests[r][j])
        print('  test@%s  최고 r=%.2f · 상위3 %s' % (AD[s][5:], order[0],
                                                 ', '.join('%.2f' % x for x in order[:3])))
    print('  전 구간   최고 r=%.2f · 상위3 %s'
          % (max(RS, key=lambda r: full[r][2]),
             ', '.join('%.2f' % x for x in sorted(RS, key=lambda r: -full[r][2])[:3])))
