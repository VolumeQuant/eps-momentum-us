# -*- coding: utf-8 -*-
"""사용자 아이디어(KR식): gap 하드컷 대신 — 다 통과시키되 gap≥3.0이면 비중 최대 3배 확대.
진입 안 막음(winner 보존) + 고gap에 사이징 틸트. vs 현행 50/50 vs 하드게이트. LOWO 필수.
gap missing=1x(boost 안 줌). 2슬롯, 일별 gap 기준 가중.
"""
import sys, os, json, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr
TE = json.load(open(os.path.join('data_cache', 'trailing_eps_ttm.json')))


def pit_te(tk, d):
    rec = TE.get(tk)
    if not rec: return None
    v = None
    for rd, e in rec:
        if rd <= d: v = e
        else: break
    return v


conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
AP = {d: {r[0]: r[1] for r in c.execute('SELECT ticker,price FROM ntm_screening WHERE date=?', (d,))} for d in ad}
DD = {}
for d in ad:
    DD[d] = {r[0]: {'p2': r[1], 'nc': r[2], 'n7': r[3], 'n30': r[4], 'n60': r[5], 'n90': r[6], 'dv': r[7]}
             for r in c.execute('SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,))}
conn.close()
EXIT, PE_HOLD = dr.EXIT_RANK, dr.PE_HOLD


def _ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)


def gap(tk, v, d):
    te = pit_te(tk, d)
    return (v['nc'] / te) if (te and te > 0 and v['nc'] and v['nc'] > 0) else None


def run(mode='equal', boost=3.0, hard=False, ban=()):
    """mode: equal(50/50) / gapw(gap≥3.0이면 boost배). hard=True면 gap<3.0 진입컷(하드게이트)."""
    pf = {}; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]; data = DD.get(d, {}); px = AP.get(d, {}); ppx = AP.get(pv, {})
        ms = {t: _ms(v) for t, v in data.items()}; wr = {t: v['p2'] for t, v in data.items() if v.get('p2')}
        elig = sorted([(t, v['p2']) for t, v in data.items() if ms.get(t, 0) >= -2 and v.get('p2')], key=lambda x: x[1])
        # ── 가중 일수익 ──
        held = list(pf)
        ws = {}
        for t in held:
            if mode == 'gapw':
                g = gap(t, data.get(t, {}), d) if t in data else None
                ws[t] = boost if (g is not None and g >= 3.0) else 1.0
            else:
                ws[t] = 1.0
        tot = sum(ws.values()) or 1.0
        drr = 0.0
        for t in held:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (ws[t] / tot) * (cu - pp) / pp * 100
        nav *= (1 + drr / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        # ── 매도 ──
        for t in list(pf):
            it = data.get(t)
            if it is None or px.get(t) is None: continue
            rk = wr.get(t); m = ms.get(t, 0); nc = it.get('nc'); cp = px.get(t)
            if m < -2 or ((rk is None or rk > EXIT) and ((cp / nc if (cp and nc and nc > 0) else 999) >= PE_HOLD)):
                del pf[t]
        # ── 진입 ──
        if len(pf) < 2:
            cand = [t for t, _ in elig if t not in pf and t not in ban and ms.get(t, -9) >= 0
                    and wr.get(t, 999) <= 5 and (data.get(t, {}).get('dv') or 0) >= 1000]
            if hard:
                cand = [t for t in cand if not (gap(t, data[t], d) is not None and gap(t, data[t], d) < 3.0)]
            cand.sort(key=lambda t: wr.get(t, 999))
            for t in cand:
                if len(pf) >= 2: break
                pf[t] = 1
    return (nav - 1) * 100, mdd * 100


print('=== gap 가중(KR식) vs 현행 50/50 vs 하드게이트 (2슬롯) ===')
rows = [('현행 50/50', dict(mode='equal')),
        ('gap≥3.0 → 2배 비중', dict(mode='gapw', boost=2.0)),
        ('gap≥3.0 → 3배 비중', dict(mode='gapw', boost=3.0)),
        ('하드게이트(gap<3.0 컷)', dict(mode='equal', hard=True))]
res = {}
for nm, kw in rows:
    r = run(**kw); res[nm] = r
    print(f'  {nm:22} {r[0]:+7.0f}%  MDD{r[1]:+5.0f}')

print('\n=== ★LOWO (winner 제외해도 가중이 robust한가 — 사후몰빵 착시 검사) ===')
print(f'{"제거":>12}' + ''.join(f'{nm[:11]:>13}' for nm, _ in rows))
for bs, lbl in [(set(), 'full'), ({'MU', 'SNDK'}, '-MU·SNDK'), ({'MU', 'SNDK', 'STX', 'LITE'}, '-4winner')]:
    line = ''.join(f'{run(ban=bs, **kw)[0]:>+12.0f}%' for _, kw in rows)
    print(f'{lbl:>12}' + line)
print('\n판정: gap가중이 현행보다 높아도 LOWO(-winner)서 무너지면(현행 이하로) 사후몰빵 착시=기각.')
print('⚠️ 91일 단일강세장. KR은 broad·직교라 가중이 먹히나 US 좁은풀은 같은종목 몰빵 위험.')
