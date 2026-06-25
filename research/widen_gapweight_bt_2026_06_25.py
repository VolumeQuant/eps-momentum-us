# -*- coding: utf-8 -*-
"""사용자 검증: top2를 넓혀(top3/5/8) + gap(기대성장) 가중 = KR식. 효과 있나?
conviction_bt 엔진 재사용(base 2슬롯 216.95%=production). gap=ntm_current/trailingEPS. LOWO 필수.
"""
import sys, os, json, sqlite3
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr
DB = dr.DB_PATH; EXIT_RANK = dr.EXIT_RANK; PE_HOLD = dr.PE_HOLD
TE = json.load(open(os.path.join(os.path.dirname(__file__), '_trailing_eps_cache.json')))

conn = sqlite3.connect(DB); c = conn.cursor()
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
AP = {d: {r[0]: r[1] for r in c.execute('SELECT ticker,price FROM ntm_screening WHERE date=?', (d,))} for d in ad}
DD = {}
for d in ad:
    DD[d] = {r[0]: {'p2': r[1], 'nc': r[2], 'n7': r[3], 'n30': r[4], 'n60': r[5], 'n90': r[6], 'dv': r[7]}
             for r in c.execute('SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,))}
conn.close()


def ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)


def gap(tk, v):
    te = TE.get(tk)
    return (v['nc'] / te) if (te and te > 0.5 and v['nc'] and v['nc'] > 0) else None


def run(N=2, weight='equal', ban=()):
    """N슬롯, weight: equal / gap(기대성장 비례). gap없으면 equal 대체."""
    pf = {}; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]; data = DD.get(d, {}); px = AP.get(d, {}); ppx = AP.get(pv, {})
        wr = {tk: v['p2'] for tk, v in data.items() if v.get('p2')}
        msd = {tk: ms(v) for tk, v in data.items()}
        # 비중
        if pf:
            if weight == 'gap':
                gs = {tk: (gap(tk, DD.get(pv, {}).get(tk, {})) or 1.0) for tk in pf if DD.get(pv, {}).get(tk)}
                tot = sum(gs.values()) or 1
                w = {tk: gs.get(tk, 1.0) / tot for tk in pf}
            else:
                w = {tk: 1.0 / len(pf) for tk in pf}
            dr_ = 0.0
            for tk in pf:
                cu, pp = px.get(tk), ppx.get(tk)
                if cu and pp and pp > 0: dr_ += w[tk] * (cu - pp) / pp * 100
            nav *= (1 + dr_ / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        # 매도
        for tk in list(pf):
            it = data.get(tk)
            if it is None or px.get(tk) is None: continue
            rk = wr.get(tk); m = msd.get(tk, 0); nc = it.get('nc'); cp = px.get(tk)
            if m < -2 or ((rk is None or rk > EXIT_RANK) and ((cp / nc if (cp and nc and nc > 0) else 999) >= PE_HOLD)):
                del pf[tk]
        # 진입 (top-N, pool=N+3)
        if len(pf) < N:
            cand = sorted([(tk, wr[tk]) for tk in wr if tk not in ban], key=lambda x: x[1])
            cand = [tk for tk, _ in cand if tk not in pf and msd.get(tk, -9) >= 0 and wr.get(tk, 999) <= N + 3 and (data.get(tk, {}).get('dv') or 0) >= 1000]
            for tk in cand:
                if len(pf) >= N: break
                pf[tk] = 1
    cum = (nav - 1) * 100
    return cum, mdd * 100, (cum / 100) / abs(mdd) if mdd < 0 else 0


WIN = ['SNDK', 'STX', 'MU', 'NVDA', 'LITE', 'COHR', 'KEYS']
print('base 2슬롯 동일가중:', '%.1f%% MDD%.0f Cal%.2f' % run(2, 'equal'), '(production 216.95% 정합)\n')
print('=== 슬롯 넓히기 + gap 가중 (KR식) ===')
print(f'{"구성":<22}{"CAGR%":>9}{"MDD%":>8}{"Calmar":>8}')
for N in [2, 3, 5, 8]:
    for wt in ['equal', 'gap']:
        r = run(N, wt)
        print(f'  top{N} {wt:>6} 가중{"":<6}{r[0]:>+9.1f}{r[1]:>+8.1f}{r[2]:>8.2f}')
print('\n=== LOWO (winner 하나 빼서 — 분산이면 버텨야) ===')
for N, wt in [(3, 'gap'), (5, 'gap')]:
    base = run(2, 'equal')[0]; full = run(N, wt)[0] - base
    worst = 999; ww = None
    for w in WIN:
        d = run(N, wt, ban=(w,))[0] - run(2, 'equal', ban=(w,))[0]
        if d < worst: worst = d; ww = w
    print(f'  top{N} gap: full {full:+.1f}p / worst-LOWO {worst:+.1f}p → {"통과" if worst>0 else f"기각(-{ww})"}')
print('\n참고: 슬롯3은 과거 -90p였음(메모리). gap이 그걸 살리나 보는 것.')
