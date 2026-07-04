# -*- coding: utf-8 -*-
"""재설계 v2 자율주행 — 가치우선 순위 + 데이터파이프라인(dv carry-forward) + 단일 top-N.
Phase1: 전 종목(순위밖 포함) 유니버스 + dv carry-forward(마지막 알려진 거래대금).
Phase2: 가치게이트(fwd_PER<=X) + 전망건강(min_seg>=0) + 유동성($1B) → 그중 top-N 동일보유, 주기리밸.
Phase3: pe_max × N × 순위기준(모멘텀/가치/블렌드) 스윕 + coherence + LOWO + SNDK/MU 포함확인.
"""
import sys, os, json, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr
conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
adset = set(ad)
raw = {}
for tk, d, px, nc, dv, n7, n30, n60, n90 in c.execute(
        'SELECT ticker,date,price,ntm_current,dollar_volume_30d,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0 ORDER BY date'):
    raw.setdefault(tk, {})[d] = dict(px=px, nc=nc, dv=dv, n7=n7, n30=n30, n60=n60, n90=n90)
conn.close()
# ── Phase1: dv carry-forward (마지막 알려진 거래대금) ──
DVF = {}  # tk -> {date: 최근 알려진 dv}
for tk, dd in raw.items():
    last = None
    for d in sorted(dd):
        if dd[d]['dv'] is not None: last = dd[d]['dv']
        DVF.setdefault(tk, {})[d] = last
# 날짜별 데이터 재구성 (ad 날짜만)
FULL = {}
for tk, dd in raw.items():
    for d, v in dd.items():
        if d in adset:
            FULL.setdefault(d, {})[tk] = v
AP = {d: {t: v['px'] for t, v in FULL.get(d, {}).items()} for d in ad}
def ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)
def mom(v):  # 90일 전망 상향폭 = 모멘텀
    return (v['nc'] / v['n90'] - 1) * 100 if (v['n90'] and abs(v['n90']) > 0.01) else 0

def pick(d, N, pe_max, rankby):
    q = []
    for tk, v in FULL.get(d, {}).items():
        if (DVF.get(tk, {}).get(d) or 0) < 1000: continue   # 유동성(carry-fwd)
        if ms(v) < 0: continue                               # 전망 건강
        fpe = v['px'] / v['nc']
        if fpe > pe_max: continue                            # ★가치 게이트
        q.append((tk, fpe, mom(v)))
    if rankby == 'value': q.sort(key=lambda x: x[1])
    elif rankby == 'mom': q.sort(key=lambda x: -x[2])
    else:  # blend: 가치순위 + 모멘텀순위 평균
        bv = {t: i for i, (t, _, _) in enumerate(sorted(q, key=lambda x: x[1]))}
        bm = {t: i for i, (t, _, _) in enumerate(sorted(q, key=lambda x: -x[2]))}
        q.sort(key=lambda x: bv[x[0]] + bm[x[0]])
    return [t for t, _, _ in q[:N]]

def run(N, R, pe_max, rankby, start=2, ban=()):
    hold = []; nav = 1.0; peak = 1.0; mdd = 0.0; turn = 0
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(hold); drr = 0.0
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp * 100
        nav *= (1 + drr / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if (i - start) % R == 0:
            tgt = [t for t in pick(d, N, pe_max, rankby) if t not in ban]
            turn += len([t for t in tgt if t not in hold]); hold = tgt
    return (nav - 1) * 100, mdd * 100, turn, set(hold)

print('=== Phase1 확인: dv carry-forward로 SNDK/MU 이제 보이나 ===')
ld = ad[-1]
for tk in ['SNDK', 'MU', 'NVDA']:
    v = FULL.get(ld, {}).get(tk)
    if v: print(f'  {tk}: fwd_PER {v["px"]/v["nc"]:.1f} / dv(carry) {DVF.get(tk,{}).get(ld)}M / min_seg {ms(v):+.1f} / mom {mom(v):+.0f}%')

print('\n=== Phase3: 가치게이트 × N × 순위기준 (주1회 리밸) ===')
print(f'{"게이트/N/순위":26}{"수익%":>8}{"MDD":>6}{"매수":>5}  오늘보유')
for pe in [15, 20, 30]:
    for rankby in ['mom', 'value', 'blend']:
        r = run(5, 5, pe, rankby)
        print(f'{f"PER<={pe}/top5/{rankby}":26}{r[0]:>+7.0f}%{r[1]:>+6.0f}{r[2]:>4}회  {sorted(r[3])}')

print('\n=== SNDK/MU 오늘 top10 포함 (가치게이트별, blend) ===')
for pe in [15, 20, 30]:
    t10 = pick(ld, 10, pe, 'blend')
    print(f'  PER<={pe}: SNDK={"O" if "SNDK" in t10 else "X"} MU={"O" if "MU" in t10 else "X"} | {t10}')

print('\n=== COHERENCE (PER<=20 top5 blend 주1회) 시작일 무관? ===')
for s in [2, 20, 40]:
    print(f'  시작 {ad[s]}: {sorted(run(5,5,20,"blend",s)[3])}')

print('\n=== LOWO (PER<=20 top5 blend) — winner 빼도 robust? ===')
for ban, nm in [(set(), 'full'), ({'MU','SNDK'}, '-MU·SNDK'), ({'MU','SNDK','NVDA','AVGO'}, '-4대')]:
    print(f'  {nm:10} {run(5,5,20,"blend",2,ban)[0]:+.0f}%')
