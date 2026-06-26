# -*- coding: utf-8 -*-
"""forward_PER 전수 테스트: ①이탈임계(PE_HOLD) 스윕 ②진입캡 스윕 ③진입×이탈 그리드.
질문: 이탈30이 최적·robust인가? 진입에 forward_PER 넣으면 다 해로운가? (2슬롯 faithful, 91일)
"""
import sys, os, json, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr

conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
AP = {d: {r[0]: r[1] for r in c.execute('SELECT ticker,price FROM ntm_screening WHERE date=?', (d,))} for d in ad}
DD = {}
for d in ad:
    DD[d] = {r[0]: {'p2': r[1], 'nc': r[2], 'n7': r[3], 'n30': r[4], 'n60': r[5], 'n90': r[6], 'dv': r[7]}
             for r in c.execute('SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,))}
conn.close()
EXIT = dr.EXIT_RANK


def _ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)


def run(pe_hold=30, entry_fpe=None, ban=()):
    """pe_hold=이탈 forward_PER 임계(보유veto). entry_fpe=진입 forward_PER 캡(None=무, missing=pass)."""
    pf = {}; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]; data = DD.get(d, {}); px = AP.get(d, {}); ppx = AP.get(pv, {})
        ms = {t: _ms(v) for t, v in data.items()}; wr = {t: v['p2'] for t, v in data.items() if v.get('p2')}
        elig = sorted([(t, v['p2']) for t, v in data.items() if ms.get(t, 0) >= -2 and v.get('p2')], key=lambda x: x[1])
        n = len(pf); drr = 0.0
        for t in pf:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp * 100
        nav *= (1 + drr / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        for t in list(pf):
            it = data.get(t)
            if it is None or px.get(t) is None: continue
            rk = wr.get(t); m = ms.get(t, 0); nc = it.get('nc'); cp = px.get(t)
            fpe = (cp / nc) if (cp and nc and nc > 0) else 999
            if m < -2 or ((rk is None or rk > EXIT) and fpe >= pe_hold):
                del pf[t]
        if len(pf) < 2:
            cand = [t for t, _ in elig if t not in pf and t not in ban and ms.get(t, -9) >= 0
                    and wr.get(t, 999) <= 5 and (data.get(t, {}).get('dv') or 0) >= 1000]
            if entry_fpe is not None:
                def fok(t):
                    nc = data[t]['nc']; pr = px.get(t); fpe = (pr / nc) if (pr and nc and nc > 0) else None
                    return fpe is None or fpe <= entry_fpe   # missing=pass
                cand = [t for t in cand if fok(t)]
            cand.sort(key=lambda t: wr.get(t, 999))
            for t in cand:
                if len(pf) >= 2: break
                pf[t] = 1
    return (nav - 1) * 100, mdd * 100


print('=== ① 이탈 forward_PER 임계(PE_HOLD) 스윕 — 진입캡 無 ===')
print(f'{"PE_HOLD":>8}{"누적%":>9}{"MDD":>7}   비고')
for pe in [15, 20, 25, 30, 40, 50, 999]:
    r = run(pe_hold=pe)
    note = ' ← 현행' if pe == 30 else (' (veto無=항상보유)' if pe == 999 else '')
    print(f'{pe:>8}{r[0]:>+8.0f}%{r[1]:>+7.0f}{note}')

print('\n=== ② 진입 forward_PER 캡 스윕 — 이탈 30 고정 ===')
print(f'{"진입캡":>8}{"누적%":>9}{"MDD":>7}   비고')
for fp in [15, 20, 25, 30, 40, None]:
    r = run(pe_hold=30, entry_fpe=fp)
    lbl = '無(현행)' if fp is None else str(fp)
    note = ' ← 현행' if fp is None else ''
    print(f'{lbl:>8}{r[0]:>+8.0f}%{r[1]:>+7.0f}{note}')

print('\n=== ③ 진입캡 × 이탈임계 그리드 (누적%) ===')
exits = [20, 30, 40]; entries = [None, 20, 30, 40]
print(f'{"진입\\이탈":>9}' + ''.join(f'{e:>9}' for e in exits))
for en in entries:
    row = ''.join(f'{run(pe_hold=e, entry_fpe=en)[0]:>+8.0f}%' for e in exits)
    print(f'{("無" if en is None else en):>9}' + row)

print('\n=== ④ 이탈 임계 robust성 (LOWO, 진입캡無) ===')
print(f'{"제거":>12}' + ''.join(f'{"PE"+str(p):>8}' for p in [20, 30, 40]))
for bs, nm in [(set(), 'full'), ({'MU', 'SNDK'}, '-MU·SNDK'), ({'MU', 'SNDK', 'STX', 'LITE'}, '-4winner')]:
    row = ''.join(f'{run(pe_hold=p, ban=bs)[0]:>+7.0f}%' for p in [20, 30, 40])
    print(f'{nm:>12}' + row)
print('\n판정: 이탈30이 평지(15~30 무비용)·40은 froth주워 노이즈/하락 → 30 robust. 진입캡은 전부 base이하 → 안 넣음.')
