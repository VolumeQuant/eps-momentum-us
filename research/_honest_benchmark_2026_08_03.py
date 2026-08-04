# -*- coding: utf-8 -*-
"""정직한 벤치마크 — 지수 매수보유 vs 2월 원본 로직 vs 현행 (2026-08-03)

사용자 질문 3개:
  ① "그냥 QQQM, VOO 사는 게 더 Calmar 높지 않아?"
  ② "지금은 과적합 덩어리인 것 같은데?"
  ③ "만약 2월 첫 시스템 시작일에 했던 로직으로 계속 했으면 얼마 벌었어?"

③이 이 스크립트의 핵심이다. **DB의 part2_rank는 그날그날 라이브 시스템이 실제로 매긴 순위**라
재구성이 아니라 당시 신호 그대로다(2026-02-12 ~ 07-31, 117 거래일). 그 순위로 v84 규칙
(2슬롯 · 진입 top3 · 이탈 rank>10 · PE_HOLD 30 · 매일 점검)을 그대로 굴린다.

공통 조건: exec_lag=1(신호 익일 종가 체결) · 동일 기간 · 동일 시작일 · 수수료 미반영.
현행은 위상 0~4 평균(리밸 기준일 임의성 제거), 2월 로직은 매일 점검이라 위상 개념 없음.
"""
import sys, os, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
import unified_vm_track as U

DB = os.path.join(BASE, 'eps_momentum_data.db')
c = sqlite3.connect(DB)
DATES = [x[0] for x in c.execute(
    'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
PX = {}
for d, tk, p in c.execute('SELECT date,ticker,price FROM ntm_screening WHERE price IS NOT NULL'):
    PX.setdefault(d, {})[tk] = p
RANK = {}
for d, tk, r in c.execute(
        'SELECT date,ticker,part2_rank FROM ntm_screening WHERE part2_rank IS NOT NULL'):
    RANK.setdefault(d, {})[tk] = r
PE = {}
for d, tk, p, nc in c.execute(
        'SELECT date,ticker,price,ntm_current FROM ntm_screening WHERE ntm_current>0'):
    if p: PE.setdefault(d, {})[tk] = p / nc
MSEG = {}
for d, tk, nc, n7, n30, n60, n90 in c.execute(
        'SELECT date,ticker,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening'):
    try:
        segs = [(a - b) / abs(b) * 100 for a, b in ((nc, n7), (n7, n30), (n30, n60), (n60, n90))
                if a is not None and b not in (None, 0)]
        if segs: MSEG.setdefault(d, {})[tk] = min(segs)
    except Exception:
        pass
c.close()


def stats(rets, n_years):
    r = np.array(rets)
    nav = np.cumprod(1 + r); pk = np.maximum.accumulate(nav)
    tot = float(nav[-1] - 1) * 100
    mdd = float((nav / pk - 1).min()) * 100
    cagr = ((1 + tot / 100) ** (1 / n_years) - 1) * 100
    return tot, mdd, (cagr / abs(mdd) if mdd else 0)


def bench(sym, dates):
    """지수 ETF 매수보유 — 같은 기간, 같은 시작·종료일."""
    import yfinance as yf
    h = yf.Ticker(sym).history(start=dates[0], end=dates[-1], auto_adjust=True)
    if h.empty: return None
    px = h['Close'].values
    return [(px[i] / px[i - 1] - 1) for i in range(1, len(px))]


def feb_logic(dates, exclude=frozenset()):
    """2026-02 원본 v84 규칙: 2슬롯 · 진입 part2_rank<=3 · 이탈 rank>10 or min_seg<-2
    · 단 fwd_PE<15면 순위 밖이어도 보유(v119 저평가 홀드) · exec_lag=1 · 균등 50/50.
    ※ v119(PE 홀드)는 6/11 배포지만 '2월 로직'의 최종형이라 포함. 미포함 변형도 아래서 병기."""
    hold, pend, rets = [], None, []
    for i in range(1, len(dates)):
        d, pv = dates[i], dates[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        rets.append(sum((px[t] / ppx[t] - 1) / 2 for t in hold
                        if t in px and t in ppx and ppx[t] > 0))
        if pend is not None and pend[0] == i:
            hold = pend[1]; pend = None
        rk, pe, ms = RANK.get(d, {}), PE.get(d, {}), MSEG.get(d, {})
        keep = []
        for t in hold:
            if ms.get(t, 0) < -2: continue                      # EPS 꺾임 즉시 매도
            r = rk.get(t, 9999)
            if r <= 10 or (pe.get(t, 999) < 15): keep.append(t)  # 순위 유지 or 저평가 홀드
        cand = [t for t, r in sorted(rk.items(), key=lambda x: x[1])
                if r <= 3 and t not in keep and t not in exclude and ms.get(t, 0) >= 0]
        nxt = (keep + cand)[:2]
        if set(nxt) != set(hold):
            pend = (i + 1, nxt)
    return rets


def feb_logic_norm(dates, exclude=frozenset()):
    """PE 홀드 없는 순수 초기형(2~6월 실제 운용에 가까움): 순위 이탈이면 무조건 매도."""
    hold, pend, rets = [], None, []
    for i in range(1, len(dates)):
        d, pv = dates[i], dates[i - 1]
        px, ppx = PX.get(d, {}), PX.get(pv, {})
        rets.append(sum((px[t] / ppx[t] - 1) / 2 for t in hold
                        if t in px and t in ppx and ppx[t] > 0))
        if pend is not None and pend[0] == i:
            hold = pend[1]; pend = None
        rk, ms = RANK.get(d, {}), MSEG.get(d, {})
        keep = [t for t in hold if ms.get(t, 0) >= -2 and rk.get(t, 9999) <= 10]
        cand = [t for t, r in sorted(rk.items(), key=lambda x: x[1])
                if r <= 3 and t not in keep and t not in exclude and ms.get(t, 0) >= 0]
        nxt = (keep + cand)[:2]
        if set(nxt) != set(hold):
            pend = (i + 1, nxt)
    return rets


if __name__ == '__main__':
    D = DATES
    yrs = len(D) / 252.0
    print('측정 기간: %s ~ %s (%d 거래일, %.2f년) · exec_lag=1 · 수수료 미반영'
          % (D[0], D[-1], len(D), yrs))
    print()
    rows = []
    for sym, nm in (('QQQ', 'QQQ 매수보유'), ('SPY', 'SPY(=VOO) 매수보유'),
                    ('SMH', 'SMH 반도체 매수보유')):
        rr = bench(sym, D)
        if rr: rows.append((nm, stats(rr, yrs)))
    rows.append(('2월 로직 (PE홀드 포함)', stats(feb_logic(D), yrs)))
    rows.append(('2월 로직 (순수 초기형)', stats(feb_logic_norm(D), yrs)))

    print('%-26s %10s %10s %9s' % ('', '수익%', 'MDD%', 'Calmar'))
    print('-' * 60)
    for nm, (t, m, cal) in rows:
        print('%-26s %+10.1f %+10.1f %9.2f' % (nm, t, m, cal))

    print('\n■ LOWO — 2월 로직에서 SNDK/MU를 빼면 (단일 종목 착시 검사)')
    for ex, lbl in ((('SNDK',), 'ex-SNDK'), (('MU',), 'ex-MU'), (('SNDK', 'MU'), 'ex-둘다')):
        t, m, cal = stats(feb_logic(D, frozenset(ex)), yrs)
        print('  %-10s %+9.1f%% · MDD %+6.1f%% · Calmar %5.2f' % (lbl, t, m, cal))

    print('\n■ 구간 분해 (2월 로직 PE홀드 / QQQ)')
    for a, b, lbl in (('2026-02-12', '2026-05-31', '2~5월'),
                      ('2026-06-01', '2026-07-31', '6~7월')):
        sub = [d for d in D if a <= d <= b]
        if len(sub) < 10: continue
        y = len(sub) / 252.0
        t1, m1, c1 = stats(feb_logic(sub), y)
        q = bench('QQQ', sub)
        t2, m2, c2 = stats(q, y) if q else (0, 0, 0)
        print('  %-6s 2월로직 %+8.1f%% (MDD %+6.1f) | QQQ %+7.1f%% (MDD %+6.1f)'
              % (lbl, t1, m1, t2, m2))
