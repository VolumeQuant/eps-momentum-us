# -*- coding: utf-8 -*-
"""사용자 의도: gap을 '1차 유니버스 필터'(이상한 업종 거르듯)에 넣어 순위를 gap-거른 풀에서 재계산.
= 늦은 진입게이트(순위불변)와 다름. 재랭킹이 winner 타이밍 흔드는지 직접 BT.
대조군: (a)base (b)늦은 진입게이트(이미 검증) (c)유니버스필터+재랭킹 missing=pass (d)하드제외(missing도 컷=SNDK죽음).
"""
import sys, os, json, sqlite3
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


def run(mode='base', gmin=3.0, ban=()):
    """mode: base / entry_gate(순위불변, 진입만) / universe(재랭킹, missing=pass) / hard(재랭킹, missing도 제외)."""
    pf = {}; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]; data = DD.get(d, {}); px = AP.get(d, {}); ppx = AP.get(pv, {})
        ms = {t: _ms(v) for t, v in data.items()}

        def gap_ok(t):
            if t not in data: return True
            g = gap(t, data[t], d)
            if g is None:
                return mode != 'hard'      # missing=pass(단 hard는 컷)
            return g >= gmin

        # 순위(wr): universe/hard모드는 gap-거른 풀에서 재랭킹(생존자 위치), 아니면 원본 part2_rank
        if mode in ('universe', 'hard'):
            survivors = sorted([(t, v['p2']) for t, v in data.items() if v.get('p2') and gap_ok(t)], key=lambda x: x[1])
            wr = {t: i + 1 for i, (t, _) in enumerate(survivors)}   # 재랭킹된 순위
        else:
            wr = {t: v['p2'] for t, v in data.items() if v.get('p2')}
        elig = sorted([(t, wr[t]) for t, v in data.items() if ms.get(t, 0) >= -2 and t in wr], key=lambda x: x[1])

        n = len(pf); drr = 0.0
        for t in pf:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp * 100
        nav *= (1 + drr / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        for t in list(pf):
            it = data.get(t)
            if it is None or px.get(t) is None: continue
            rk = wr.get(t); m = ms.get(t, 0); nc = it.get('nc'); cp = px.get(t)
            if m < -2 or ((rk is None or rk > EXIT) and ((cp / nc if (cp and nc and nc > 0) else 999) >= PE_HOLD)):
                del pf[t]
        if len(pf) < 2:
            cand = [t for t, _ in elig if t not in pf and t not in ban and ms.get(t, -9) >= 0
                    and wr.get(t, 999) <= 5 and (data.get(t, {}).get('dv') or 0) >= 1000]
            if mode == 'entry_gate':
                cand = [t for t in cand if gap_ok(t)]   # 순위불변·진입만 게이트
            cand.sort(key=lambda t: wr.get(t, 999))
            for t in cand:
                if len(pf) >= 2: break
                pf[t] = 1
    return (nav - 1) * 100, mdd * 100, set(pf)


print('=== gap 적용 위치별 비교 (2슬롯 faithful, gap≥3.0, missing=pass기본) ===')
for mode, label in [('base', 'base(게이트無)'), ('entry_gate', '늦은 진입게이트(순위불변)'),
                    ('universe', '★1차 유니버스필터+재랭킹(missing=pass)'), ('hard', '하드제외(missing도 컷)')]:
    r = run(mode)
    print(f'  {label:36} {r[0]:+7.0f}%  MDD{r[1]:+5.0f}  보유{sorted(r[2])}')
print()
print('=== 누적 LOWO (유니버스필터 모드가 단일winner 운인가) ===')
for bs, nm in [(set(), 'full'), ({'MU', 'SNDK'}, '-MU·SNDK'), ({'MU', 'SNDK', 'STX'}, '+STX'), ({'MU', 'SNDK', 'STX', 'LITE'}, '+LITE')]:
    b = run('base', ban=bs)[0]; u = run('universe', ban=bs)[0]; e = run('entry_gate', ban=bs)[0]
    print(f'  {nm:10} base {b:+6.0f}% | 진입게이트 {e:+6.0f}%(Δ{e-b:+.0f}) | 유니버스필터 {u:+6.0f}%(Δ{u-b:+.0f})')
print()
print('판정: 유니버스필터가 진입게이트보다 나으면 채택검토, 못하면(재랭킹이 winner흔듦) 진입게이트가 정답.')
