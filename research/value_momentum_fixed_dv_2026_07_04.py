# -*- coding: utf-8 -*-
"""가치게이트+모멘텀 재설계 — ★전종목 dv 복구 후 재검증 (2026-07-04).

이전 value_momentum_both_2026_07_04.py의 치명 결함: dv(거래대금)가 순위종목만 DB에 있어
  `(v['dv'] or 0) >= 1000` 필터가 SNDK($21B/일)/MU($56B/일)를 '데이터없음'으로 통째 제외.
  → +154%/−15는 SNDK/MU 없는 반쪽 숫자였음.
이 스크립트: research/dv_full_2026_07_04.parquet(yfinance 재구축, DB 대조 오차 0.00%)로
  전종목 PIT dv 복구 후 동일 설계 재검증.

설계(사용자 스펙): ①가치 게이트(fwd_PER<=X) 먼저 → ②그 안에서 모멘텀(90일 전망 상향폭) top-N
  ③단일 포트폴리오, R일마다 리밸런싱(절대 캘린더 앵커=시작일 무관), carryover/에폭 없음.
위생필터: min_seg>=0(전망 안꺾임) + dv>=$1B + 업종제외(원자재/엔터/전문소매, production 동일).
검증: PER×N×R 스윕 / LOWO(기여 상위 각각 제외) / coherence(시작일) / 기여도 / 회전·휩쏘.
"""
import sys, os, json, sqlite3
import numpy as np
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr

# ── 데이터 로드 ────────────────────────────────────────────────
conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
FULL = {}
for tk, d, p2, px, nc, n7, n30, n60, n90 in c.execute(
        'SELECT ticker,date,part2_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d '
        'FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0'):
    FULL.setdefault(d, {})[tk] = dict(p2=p2, px=px, nc=nc, n7=n7, n30=n30, n60=n60, n90=n90)
conn.close()
AP = {d: {t: v['px'] for t, v in FULL.get(d, {}).items()} for d in ad}

DVF = pd.read_parquet(os.path.join(BASE, 'research', 'dv_full_2026_07_04.parquet'))
DVF.index = pd.to_datetime(DVF.index).strftime('%Y-%m-%d')
DV = {d: {t: (DVF.loc[d, t] if not pd.isna(DVF.loc[d, t]) else None) for t in DVF.columns} for d in DVF.index if d in set(ad)}

TC = json.load(open(os.path.join(BASE, 'ticker_info_cache.json'), encoding='utf-8'))
BAD_IND = dr.COMMODITY_INDUSTRIES | dr.OFF_STRATEGY_INDUSTRIES
BAD_TK = set(dr.COMMODITY_TICKERS)
def industry_ok(tk):
    if tk in BAD_TK: return False
    v = TC.get(tk)
    ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v)
    return not (isinstance(ind, str) and ind in BAD_IND)

def ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)
def rev90(v):
    return (v['nc'] - v['n90']) / abs(v['n90']) * 100 if (v['n90'] and abs(v['n90']) > 0.01) else 0

def pick(d, N, pe_max, exclude=frozenset()):
    cand = []
    for tk, v in FULL.get(d, {}).items():
        if tk in exclude or not industry_ok(tk): continue
        dv = DV.get(d, {}).get(tk)
        if dv is None or dv < 1000: continue
        if ms(v) < 0: continue
        if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
        fpe = v['px'] / v['nc']
        if fpe > pe_max: continue
        cand.append((tk, rev90(v)))
    cand.sort(key=lambda x: -x[1])
    return [t for t, _ in cand[:N]]

def run(N, R, pe_max, start=2, exclude=frozenset(), detail=False):
    hold = []; nav = 1.0; peak = 1.0; mdd = 0.0; buys = 0
    contrib = {}; hold_days = {}; rebuy = 0; seen_exit = {}
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(hold); drr = 0.0
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0:
                r = (1.0 / n) * (cu - pp) / pp
                drr += r; contrib[t] = contrib.get(t, 0) + r * nav
            hold_days[t] = hold_days.get(t, 0) + 1
        nav *= (1 + drr); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if i % R == 0:  # ★절대 캘린더 앵커 → 시작일 무관 동일 리밸런싱일
            tgt = pick(d, N, pe_max, exclude)
            for t in tgt:
                if t not in hold:
                    buys += 1
                    if t in seen_exit and i - seen_exit[t] <= 10: rebuy += 1
            for t in hold:
                if t not in tgt: seen_exit[t] = i
            hold = tgt
    out = ((nav - 1) * 100, mdd * 100, buys, set(hold))
    if detail:
        return out + (contrib, hold_days, rebuy)
    return out

# ── ①스윕: PER게이트 × N × R ──────────────────────────────────
print('=== ①가치게이트+모멘텀 스윕 (dv복구 후) — 수익% / MDD / 매수횟수 ===')
print('    (dv결함 이전값: PER20·top5·주1 = +154/−15 [SNDK/MU 제외된 반쪽])')
print(f'{"PER게이트":>8} | ' + ' | '.join(f'{c:^20}' for c in ['top3 R5', 'top5 R5', 'top5 R1', 'top5 R10', 'top8 R5']))
for pe in [12, 15, 20, 25, 30, 999]:
    cells = []
    for N, R in [(3, 5), (5, 5), (5, 1), (5, 10), (8, 5)]:
        r = run(N, R, pe)
        cells.append(f'{r[0]:+6.0f}%/{r[1]:+4.0f}/{r[2]:3}회')
    lbl = f'<={pe}' if pe < 999 else '없음(모멘텀만)'
    print(f'{lbl:>8} | ' + ' | '.join(cells))

# ── ②기여도 + LOWO (대표 config 후보들) ───────────────────────
print('\n=== ②기여도 + LOWO — 후보 config별 ===')
for pe, N, R in [(15, 5, 5), (20, 5, 5), (20, 3, 5), (25, 5, 5)]:
    ret, mdd, buys, hold, contrib, hdays, rebuy = run(N, R, pe, detail=True)
    top = sorted(contrib.items(), key=lambda x: -x[1])[:6]
    print(f'\n-- PER<={pe} top{N} R{R}: {ret:+.0f}% MDD{mdd:+.0f} 매수{buys}회 재매수(10일내){rebuy}회')
    print('   기여상위:', ', '.join(f'{t} {v*100:+.0f}p({hdays.get(t,0)}일)' for t, v in top))
    for ex_t, _ in top[:3]:
        r2 = run(N, R, pe, exclude=frozenset([ex_t]))
        print(f'   LOWO −{ex_t:5}: {r2[0]:+6.0f}% MDD{r2[1]:+4.0f}')
    r3 = run(N, R, pe, exclude=frozenset([t for t, _ in top[:2]]))
    print(f'   LOWO −상위2 : {r3[0]:+6.0f}% MDD{r3[1]:+4.0f}')

# ── ③coherence: 시작일 무관 최종보유 동일? ────────────────────
print('\n=== ③coherence (PER<=20 top5 R5): 시작일 2/15/30/50 → 최종보유 ===')
finals = []
for s in [2, 15, 30, 50]:
    r = run(5, 5, 20, start=s)
    finals.append(tuple(sorted(r[3])))
    print(f'  시작 {ad[s]}: {sorted(r[3])}  ({r[0]:+.0f}%)')
print('  →', '✅ 일관' if len(set(finals)) == 1 else '❌ 제각각')

# ── ④오늘 이 방식이면 보유 ────────────────────────────────────
print(f'\n=== ④오늘({ad[-1]}) 보유 (게이트별 top5) ===')
for pe in [15, 20, 25, 999]:
    picks = pick(ad[-1], 5, pe)
    rows = []
    for t in picks:
        v = FULL[ad[-1]][t]
        rows.append(f'{t}(PER{v["px"]/v["nc"]:.0f},상향{rev90(v):+.0f}%)')
    print(f'  PER<={pe if pe<999 else "∞"}: ' + ', '.join(rows))
