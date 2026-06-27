# -*- coding: utf-8 -*-
"""valuation 진입게이트 그리드서치: gap_min × fwd_PER_max (2슬롯 faithful).
사용자 "직관 말고 최적 그리드 돌려라". 단 91일 단일강세장이라 peak 채택=과적합 →
채택판정은 ①full수익 ②LOWO(-MU·SNDK·-4winner) ③인접 plateau(이웃셀 평균) ④MDD.
"""
import sys, os, json, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr
TE = json.load(open(os.path.join('data_cache', 'trailing_eps_ttm.json')))


def pit(tk, d):
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
EXIT, PH = dr.EXIT_RANK, dr.PE_HOLD


def ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)


def gp(tk, v, d):
    te = pit(tk, d)
    return (v['nc'] / te) if (te and te > 0 and v['nc'] and v['nc'] > 0) else None


def run(gmin=None, fpe_max=None, ban=()):
    pf = {}; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]; data = DD.get(d, {}); px = AP.get(d, {}); ppx = AP.get(pv, {})
        m = {t: ms(v) for t, v in data.items()}; wr = {t: v['p2'] for t, v in data.items() if v.get('p2')}
        elig = sorted([(t, v['p2']) for t, v in data.items() if m.get(t, 0) >= -2 and v.get('p2')], key=lambda x: x[1])
        n = len(pf); drr = 0.0
        for t in pf:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp * 100
        nav *= (1 + drr / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        for t in list(pf):
            it = data.get(t)
            if it is None or px.get(t) is None: continue
            rk = wr.get(t); mm = m.get(t, 0); nc = it.get('nc'); cp = px.get(t)
            if mm < -2 or ((rk is None or rk > EXIT) and ((cp / nc if (cp and nc and nc > 0) else 999) >= PH)):
                del pf[t]
        if len(pf) < 2:
            cand = [t for t, _ in elig if t not in pf and t not in ban and m.get(t, -9) >= 0
                    and wr.get(t, 999) <= 5 and (data.get(t, {}).get('dv') or 0) >= 1000]
            def ok(t):
                g = gp(t, data[t], d); nc = data[t]['nc']; pr = px.get(t); fpe = (pr / nc) if (pr and nc and nc > 0) else None
                return ((gmin is None) or (g is None) or (g >= gmin)) and ((fpe_max is None) or (fpe is None) or (fpe <= fpe_max))
            cand = [t for t in cand if ok(t)]
            cand.sort(key=lambda t: wr.get(t, 999))
            for t in cand:
                if len(pf) >= 2: break
                pf[t] = 1
    return (nav - 1) * 100, mdd * 100


GAPS = [None, 2.0, 2.5, 3.0, 3.5, 4.0]
FPES = [None, 15, 20, 25, 30, 40]
gl = lambda x: '無' if x is None else str(x)
base = run()[0]
print(f'base(게이트無) = {base:+.0f}%\n')
print('=== 수익% 그리드 (행=gap_min, 열=fwd_PER_max) ===')
print('gap\\fpe ' + ''.join(f'{gl(f):>8}' for f in FPES))
grid = {}
for g in GAPS:
    row = ''
    for f in FPES:
        r = run(g, f); grid[(g, f)] = r
        row += f'{r[0]:>+7.0f}%'
    print(f'{gl(g):>6} ' + row)

# 상위 셀 (base+10p 이상만, 채택 후보)
cells = sorted(grid.items(), key=lambda kv: -kv[1][0])
print('\n=== 수익 상위 5셀 robust 판정 (LOWO + 인접 plateau + MDD) ===')
def neighbors(g, f):
    gi, fi = GAPS.index(g), FPES.index(f); out = []
    for dg in (-1, 0, 1):
        for df in (-1, 0, 1):
            ni, nj = gi + dg, fi + df
            if 0 <= ni < len(GAPS) and 0 <= nj < len(FPES) and (dg or df):
                out.append(grid[(GAPS[ni], FPES[nj])][0])
    return out
for (g, f), (ret, mdd) in cells[:5]:
    l1 = run(g, f, ban={'MU', 'SNDK'})[0] - run(ban={'MU', 'SNDK'})[0]
    l2 = run(g, f, ban={'MU', 'SNDK', 'STX', 'LITE'})[0] - run(ban={'MU', 'SNDK', 'STX', 'LITE'})[0]
    nb = neighbors(g, f); nbmean = np.mean(nb); spike = ret - nbmean
    flag = 'robust' if (l1 >= 0 and l2 >= 0 and spike < 25) else ('LOWO실패' if (l1 < 0 or l2 < 0) else '스파이크(과적합)')
    print(f'  gap{gl(g):>4}/fpe{gl(f):>4}: {ret:+.0f}%(base{ret-base:+.0f}) MDD{mdd:+.0f} | LOWO -MU·SNDK{l1:+.0f} -4w{l2:+.0f} | 이웃평균{nbmean:+.0f}(스파이크{spike:+.0f}) → {flag}')
print('\n판정: full 최댓값이라도 LOWO<0이거나 이웃과 동떨어진(스파이크) 셀=과적합 기각. robust=LOWO≥0 & 인접 평탄.')
