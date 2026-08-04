# -*- coding: utf-8 -*-
"""비중첩 구간 가중치 — 다중 분할 워크포워드 (2026-08-04)

사용자 질문 3개:
  Q1 비중첩으로 해도 '전망치만'보다 '괴리율'이 나은가?
  Q2 비중첩 + 시간(일수) 가중은 더 나빴던 게 맞나?
  Q3 그냥 그리드서치 말고, 구간 비중을 train/test 나눠서 제대로 실험하면 되지 않나?

Q3 설계 — 한 번의 분할은 우연에 좌우되므로 **분할을 여러 개** 둔다.
  · 분할 3개(60/70/80% 지점)에서 각각 train 최적을 고르고 test 성적/등수를 본다.
  · 개별 조합의 1등이 아니라 **구간별 '평균 test 등수'** 를 봐서 어느 구간에 무게를 주는
    방향이 일관되게 좋은지 확인한다(고원 탐색). 봉우리 하나는 노이즈, 넓은 영역이면 신호.
  · 사전 등록: 채택 조건 = ①3개 분할 전부에서 test 상위 40% ②등가중보다 우수 ③인접 조합도 동반 우수.
    하나라도 어긋나면 등가중 유지(= 고를 근거 없음).
"""
import sys, os, itertools, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import unified_vm_track as U
from _joint_gate_audit_2026_08_01 import DAY, AD, PX
from _stop_revg_matrix_2026_08_03 import RG
from _pe_window_weights_2026_08_04 import PXT, NTM, DI, LAG

KEYS = ('7d', '30d', '60d', '90d')
PAIRS = [(3, 2), (2, 1), (1, 0), (0, None)]     # 90→60, 60→30, 30→7, 7→현재
SEGLEN = [LAG['90d'] - LAG['60d'], LAG['60d'] - LAG['30d'],
          LAG['30d'] - LAG['7d'], LAG['7d']]


def _pe_rev(tk, d):
    """(구간별 PE변화율 4개, 구간별 전망상향폭 4개). 계산 불가 구간은 None."""
    i = DI.get(d)
    px, v = PXT.get(tk, {}), NTM.get(tk, {}).get(d)
    if i is None or not v or not v[0] or v[0] <= 0:
        return None, None
    p_now = px.get(d)
    if not p_now:
        return None, None

    def at(idx):
        if idx is None:
            return p_now / v[0], v[0]
        j = i - LAG[KEYS[idx]]
        if j < 0:
            return None, None
        p, e = px.get(AD[j]), v[idx + 1]
        if not p or not e or e <= 0:
            return None, None
        return p / e, e
    pe, rev = [], []
    for a, b in PAIRS:
        pa, ea = at(a); pb, eb = at(b)
        pe.append(None if (pa is None or pb is None or pa <= 0) else (pb - pa) / pa * 100)
        rev.append(None if (ea is None or eb is None or ea <= 0) else (eb - ea) / abs(ea) * 100)
    return pe, rev


CACHE = {}
def seg_vals(d):
    if d in CACHE:
        return CACHE[d]
    rows = []
    for tk, a, m, g, dvv, fpe, u in DAY[d]:
        if dvv is None or dvv < 300 or m < -2 or u < 1 or fpe > 30:
            continue
        rg = RG.get(d, {}).get(tk)
        if rg is not None and rg < 0.10:
            continue
        pe, rev = _pe_rev(tk, d)
        if pe is None:
            continue
        rows.append((tk, pe, rev))
    CACHE[d] = rows
    return rows


def picks(W, kind='pe'):
    out = {}
    for d in AD:
        scored = []
        for tk, pe, rev in seg_vals(d):
            arr = pe if kind == 'pe' else rev
            s = w = 0.0
            for v, ww in zip(arr, W):
                if v is not None and ww:
                    s += ww * v; w += ww
            if not w:
                continue
            val = s / w
            scored.append(((val if kind == 'pe' else -val), tk))
        scored.sort()
        seen, lst = set(), []
        for _v, tk in scored:
            k = U.DUAL_CLASS.get(tk, tk)
            if k in seen:
                continue
            seen.add(k); lst.append(tk)
        out[d] = lst
    return out


def run(P, phase, start, end):
    hold, pend, rets = [], None, []
    for i in range(start, end):
        d, pv = AD[i], AD[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        rets.append(sum((px[t] / ppx[t] - 1) / 5 for t in hold
                        if t in px and t in ppx and ppx[t] > 0))
        if pend and pend[0] == i:
            hold = pend[1]; pend = None
        if i % 5 == phase % 5:
            pend = (i + 1, P[d][:5])
    r = np.array(rets)
    if not len(r):
        return None
    nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    return float(nav[-1] - 1) * 100, float((nav / pk - 1).min()) * 100


def sc(P, start, end):
    o = [x for x in (run(P, p, start, end) for p in range(5)) if x]
    if not o:
        return None
    t, m = np.mean([x[0] for x in o]), np.mean([x[1] for x in o])
    y = (end - start) / 252.0
    return t, m, ((((1 + t / 100) ** (1 / y) - 1) * 100) / abs(m) if m else 0)


if __name__ == '__main__':
    EQ = (1, 1, 1, 1)
    print('■ Q1/Q2 — 비중첩 기준 지표·가중 비교 (전체 구간)')
    print('%-28s %9s %9s %8s' % ('', '수익%', 'MDD%', 'Calmar'))
    print('-' * 58)
    for lbl, W, kind in (('괴리율 · 구간 등가중', EQ, 'pe'),
                         ('괴리율 · 일수 비례(시간중립)', tuple(SEGLEN), 'pe'),
                         ('전망상향폭 · 구간 등가중', EQ, 'rev'),
                         ('전망상향폭 · 일수 비례', tuple(SEGLEN), 'rev')):
        r = sc(picks(W, kind), 2, len(AD))
        print('%-28s %+9.1f %+9.1f %8.2f' % (lbl, r[0], r[1], r[2]))

    print('\n■ Q3 — 구간 가중치 다중 분할 워크포워드')
    G = [w for w in itertools.product((0, 1, 2, 3), repeat=4) if sum(w) > 0]
    Pm = {w: picks(w, 'pe') for w in G}
    SPLITS = [int(len(AD) * f) for f in (0.6, 0.7, 0.8)]
    ranks = {w: [] for w in G}
    print('%-6s %-14s %10s %10s %10s' % ('분할', 'train승자', 'train Cal', 'test Cal', 'test등수'))
    print('-' * 60)
    for si in SPLITS:
        tr = {w: sc(Pm[w], 2, si) for w in G}
        te = {w: sc(Pm[w], si + 1, len(AD)) for w in G}
        tr = {k: v for k, v in tr.items() if v}; te = {k: v for k, v in te.items() if v}
        order = sorted(te, key=lambda w: -te[w][2])
        for i, w in enumerate(order, 1):
            ranks[w].append(i)
        win = max(tr, key=lambda w: tr[w][2])
        print('%-6s %-14s %10.2f %10.2f %8d위' %
              (AD[si][5:], str(win), tr[win][2], te[win][2], order.index(win) + 1))
        print('%-6s %-14s %10.2f %10.2f %8d위  ← 등가중' %
              ('', str(EQ), tr[EQ][2], te[EQ][2], order.index(EQ) + 1))

    print('\n■ 구간별 평균 test 등수 (낮을수록 좋음, 255개 중)')
    avg_rank = {w: np.mean(r) for w, r in ranks.items() if len(r) == len(SPLITS)}
    best = sorted(avg_rank, key=lambda w: avg_rank[w])[:8]
    for w in best:
        print('  %-14s 평균 %5.1f위  (분할별 %s)' % (str(w), avg_rank[w], ranks[w]))
    print('  %-14s 평균 %5.1f위  (분할별 %s)  ← 등가중' % (str(EQ), avg_rank[EQ], ranks[EQ]))

    print('\n■ 어느 구간에 무게를 실을 때 좋은가 (구간별 비중과 test등수 상관)')
    ws = np.array([list(w) for w in avg_rank])
    rs = np.array([avg_rank[tuple(w)] for w in ws])
    names = ['90→60', '60→30', '30→7', '7→현재']
    for j, nm in enumerate(names):
        frac = ws[:, j] / ws.sum(axis=1)
        print('  %-8s 비중↑ ↔ test등수 상관 %+.3f  (음수면 그 구간 비중이 클수록 좋음)'
              % (nm, np.corrcoef(frac, rs)[0, 1]))
