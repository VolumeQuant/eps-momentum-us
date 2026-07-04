# -*- coding: utf-8 -*-
"""공정비교: 현행 carryover vs 가치게이트+모멘텀 top-N — ★시작일별 forward 수익.

핵심 질문: '+210%(현행) vs +108%(재설계)'는 2월 진입자 기준. 실제 유저는 아무 날이나 진입.
  → 시작일 s마다 s~끝 forward 수익을 두 설계로 비교 = 경로의존(에폭·carryover) 정량화.
추가: walk-forward 반기 / gap>=2.5 토글 / rev_up30>=3 위생 / SPY 벤치마크.
"""
import sys, os, json, sqlite3
import numpy as np
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr

conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
FULL = {}
for tk, d, p2, px, nc, n7, n30, n60, n90, ru in c.execute(
        'SELECT ticker,date,part2_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,rev_up30 '
        'FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0'):
    FULL.setdefault(d, {})[tk] = dict(p2=p2, px=px, nc=nc, n7=n7, n30=n30, n60=n60, n90=n90, ru=ru)
conn.close()
AP = {d: {t: v['px'] for t, v in FULL.get(d, {}).items()} for d in ad}

DVF = pd.read_parquet(os.path.join(BASE, 'research', 'dv_full_2026_07_04.parquet'))
DVF.index = pd.to_datetime(DVF.index).strftime('%Y-%m-%d')
DV = {d: {t: (None if pd.isna(DVF.loc[d, t]) else float(DVF.loc[d, t])) for t in DVF.columns} for d in DVF.index if d in set(ad)}

TC = json.load(open(os.path.join(BASE, 'ticker_info_cache.json'), encoding='utf-8'))
BAD_IND = dr.COMMODITY_INDUSTRIES | dr.OFF_STRATEGY_INDUSTRIES
BAD_TK = set(dr.COMMODITY_TICKERS)
def industry_ok(tk):
    if tk in BAD_TK: return False
    v = TC.get(tk)
    ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v)
    return not (isinstance(ind, str) and ind in BAD_IND)

TE = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm.json'), encoding='utf-8'))
def pit_te(tk, d):
    r = TE.get(tk); v = None
    if not r: return None
    for rd, e in r:
        if rd <= d: v = e
        else: break
    return v
def gap_of(tk, v, d):
    te = pit_te(tk, d)
    return (v['nc'] / te) if (te and te > 0 and v['nc'] and v['nc'] > 0) else None

def ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)
def rev90(v):
    return (v['nc'] - v['n90']) / abs(v['n90']) * 100 if (v['n90'] and abs(v['n90']) > 0.01) else 0

def pick(d, N, pe_max, use_gap=False, use_ru=False, exclude=frozenset()):
    cand = []
    for tk, v in FULL.get(d, {}).items():
        if tk in exclude or not industry_ok(tk): continue
        dv = DV.get(d, {}).get(tk)
        if dv is None or dv < 1000: continue
        if ms(v) < 0: continue
        if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
        if use_ru and (v.get('ru') or 0) < 3: continue
        if v['px'] / v['nc'] > pe_max: continue
        if use_gap:
            g = gap_of(tk, v, d)
            if g is not None and g < 2.5: continue
        cand.append((tk, rev90(v)))
    cand.sort(key=lambda x: -x[1])
    return [t for t, _ in cand[:N]]

def run_vm(N, R, pe_max, start=2, end=None, use_gap=False, use_ru=False, exclude=frozenset()):
    end = end if end is not None else len(ad)
    hold = []; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(start, end):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(hold); drr = 0.0
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp
        nav *= (1 + drr); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if i % R == 0:
            hold = pick(ad[i], N, pe_max, use_gap, use_ru, exclude)
    return (nav - 1) * 100, mdd * 100, set(hold)

# 현행 carryover 재현 (coherent_redesign와 동일 로직, gap게이트 ON=production)
def run_carry(start=2, end=None):
    end = end if end is not None else len(ad)
    PH = dr.PE_HOLD
    pf = {}; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(start, end):
        d, pv = ad[i], ad[i - 1]
        data = {t: v for t, v in FULL.get(d, {}).items() if v.get('p2') is not None}
        px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(pf); drr = 0.0
        for t in pf:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp
        nav *= (1 + drr); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        for t in list(pf):
            it = data.get(t)
            full_row = FULL.get(d, {}).get(t)
            if it is not None and ms(it) < -2:
                del pf[t]; continue
            if it is None:
                if full_row and full_row['nc'] and full_row['nc'] > 0 and full_row['px'] and \
                        (full_row['px'] / full_row['nc']) >= PH:
                    del pf[t]
                continue
            rk = it['p2']
            if rk is not None and rk <= dr.EXIT_RANK: continue
            _pe = (it['px'] / it['nc']) if (it['px'] and it['nc'] and it['nc'] > 0) else 999
            if _pe >= PH: del pf[t]
        if len(pf) < 2:
            def eok(tk, v):
                if v['p2'] is None or v['p2'] > 5 or ms(v) < 0: return False
                dvv = DV.get(d, {}).get(tk)
                if dvv is None or dvv < 1000: return False
                g = gap_of(tk, v, d)
                return not (g is not None and g < 2.5)
            cand = sorted([(t, v['p2']) for t, v in data.items() if t not in pf and eok(t, v)], key=lambda x: x[1])
            for t, _ in cand:
                if len(pf) >= 2: break
                pf[t] = 1
    return (nav - 1) * 100, mdd * 100, set(pf)

# ── ①시작일별 forward 수익: 경로의존 정량화 ────────────────────
print('=== ①시작일별 forward 수익 (그날 진입한 유저가 끝까지 들었을 때) ===')
print(f'{"시작일":12}{"현행carry":>12}{"VM PER25top5":>14}{"VM PER20top3":>14}')
carry_rets, vm25_rets, vm20_rets = [], [], []
for s in [2, 10, 20, 30, 40, 50, 60, 70, 80]:
    rc = run_carry(s); r25 = run_vm(5, 5, 25, s); r20 = run_vm(3, 5, 20, s)
    carry_rets.append(rc[0]); vm25_rets.append(r25[0]); vm20_rets.append(r20[0])
    print(f'{ad[s]:12}{rc[0]:>+11.0f}%{r25[0]:>+13.0f}%{r20[0]:>+13.0f}%')
print(f'{"평균":12}{np.mean(carry_rets):>+11.0f}%{np.mean(vm25_rets):>+13.0f}%{np.mean(vm20_rets):>+13.0f}%')
print(f'{"최악":12}{min(carry_rets):>+11.0f}%{min(vm25_rets):>+13.0f}%{min(vm20_rets):>+13.0f}%')

# ── ②walk-forward 반기 ────────────────────────────────────────
half = len(ad) // 2
print(f'\n=== ②walk-forward: 전반({ad[2]}~{ad[half]}) / 후반({ad[half]}~{ad[-1]}) ===')
for nm, fn in [('현행carry', lambda s, e: run_carry(s, e)),
               ('VM PER25 top5', lambda s, e: run_vm(5, 5, 25, s, e)),
               ('VM PER20 top3', lambda s, e: run_vm(3, 5, 20, s, e)),
               ('VM PER20 top5', lambda s, e: run_vm(5, 5, 20, s, e))]:
    a = fn(2, half); b = fn(half, len(ad))
    print(f'  {nm:15} 전반 {a[0]:+6.0f}%/MDD{a[1]:+4.0f}   후반 {b[0]:+6.0f}%/MDD{b[1]:+4.0f}')

# ── ③토글: gap>=2.5 / rev_up30>=3 ─────────────────────────────
print('\n=== ③위생 토글 (PER<=25 top5 R5 기준) ===')
for g, u in [(False, False), (True, False), (False, True), (True, True)]:
    r = run_vm(5, 5, 25, use_gap=g, use_ru=u)
    print(f'  gap{"O" if g else "X"} ru{"O" if u else "X"}: {r[0]:+6.0f}% MDD{r[1]:+4.0f}  보유 {sorted(r[2])}')

# ── ④SPY 벤치마크 ─────────────────────────────────────────────
try:
    import yfinance as yf
    spy = yf.download('SPY QQQ SMH', start='2026-02-10', end='2026-07-04', auto_adjust=True, progress=False)['Close']
    for b in ['SPY', 'QQQ', 'SMH']:
        s = spy[b].dropna()
        print(f'\n{b} 같은기간: {(s.iloc[-1]/s.iloc[0]-1)*100:+.0f}%  MDD {((s/s.cummax()).min()-1)*100:+.0f}%', end='')
    print()
except Exception as e:
    print('SPY fetch 실패:', e)
