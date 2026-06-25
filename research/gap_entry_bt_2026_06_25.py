# -*- coding: utf-8 -*-
"""사용자 아이디어: 매수 후보 뽑을 때 'fwd PER이 trailing PER보다 압도적으로 낮은지(=gap 큼)' 체크/우선.
conviction_bt 엔진 재사용(base 216.95%=production 정합). 진입규칙만 변형 + LOWO.
gap = ntm_current(date)/trailingEPS(ticker). ⚠️trailEPS=현재 스냅샷 상수근사(약한 look-ahead, 방향확인용).
"""
import sys, os, json, sqlite3
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr

DB = dr.DB_PATH; EXIT_RANK = dr.EXIT_RANK; PE_HOLD = dr.PE_HOLD
TEPS = json.load(open(os.path.join(os.path.dirname(__file__), '_trailing_eps_cache.json')))

conn = sqlite3.connect(DB); c = conn.cursor()
all_dates = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
all_prices = {d: {r[0]: r[1] for r in c.execute('SELECT ticker, price FROM ntm_screening WHERE date=?', (d,))} for d in all_dates}
daily_data = {}
for d in all_dates:
    rows = c.execute('''SELECT ticker,price,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,rev_growth,dollar_volume_30d
        FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL''', (d,)).fetchall()
    daily_data[d] = {r[0]: {'price': r[1], 'p2': r[2], 'nc': r[3], 'n7': r[4], 'n30': r[5], 'n60': r[6],
                            'n90': r[7], 'rg': r[8], 'dv': r[9]} for r in rows}
conn.close()


def gap(tk, d):
    nc = daily_data.get(d, {}).get(tk, {}).get('nc'); te = TEPS.get(tk)
    if nc and nc > 0 and te and te > 0.5:
        g = nc / te
        return g if g <= 10 else 10.0
    return None


def _ms(v):
    nc, n7, n30, n60, n90 = v['nc'], v['n7'], v['n30'], v['n60'], v['n90']
    out = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        out.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(out)


def run(mode='base', gap_thr=1.5, pool=5, ban=()):
    """mode: base(순위) / gappri(gap우선,pool넓힘) / gapfilt(gap>=thr 필터)."""
    pf = {}; nav = 1.0; peak = 1.0; mdd = 0.0; changed = 0
    for i in range(2, len(all_dates)):
        d, pv = all_dates[i], all_dates[i - 1]
        data = daily_data.get(d, {}); px = all_prices.get(d, {}); ppx = all_prices.get(pv, {})
        ms = {tk: _ms(v) for tk, v in data.items()}
        wr = {tk: v['p2'] for tk, v in data.items() if v.get('p2')}
        # 일수익 (50/50)
        dr_ = 0.0; pn = len(pf)
        for tk in pf:
            cu, pp = px.get(tk), ppx.get(tk)
            if cu and pp and pp > 0:
                dr_ += (1.0 if pn == 1 else 0.5) * (cu - pp) / pp * 100
        nav *= (1 + dr_ / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        # 매도
        for tk in list(pf):
            it = data.get(tk)
            if it is None or px.get(tk) is None:
                continue
            rk = wr.get(tk); m = ms.get(tk, 0); nc = it.get('nc'); cp = px.get(tk)
            sell = m < -2 or ((rk is None or rk > EXIT_RANK) and ((cp / nc if (cp and nc and nc > 0) else 999) >= PE_HOLD))
            if sell:
                del pf[tk]
        # 진입
        if len(pf) < 2:
            base_cand = [tk for tk, _ in sorted([(t, wr[t]) for t in wr if t not in ban], key=lambda x: x[1])
                         if tk not in pf and ms.get(tk, -9) >= 0 and wr.get(tk, 999) <= (pool if mode == 'gappri' else 5)
                         and (data.get(tk, {}).get('dv') or 0) >= 1000]
            if mode == 'gappri':       # gap 높은 순 우선
                base_cand = sorted(base_cand, key=lambda t: -(gap(t, d) or 0))
            elif mode == 'gapfilt':    # gap>=thr만 (없으면 제외)
                base_cand = [t for t in base_cand if (gap(t, d) or 0) >= gap_thr]
            for tk in base_cand:
                if len(pf) >= 2:
                    break
                if mode != 'base':
                    # 베이스가 골랐을 종목과 다르면 카운트
                    pass
                pf[tk] = {'ep': px.get(tk)}
    cum = (nav - 1) * 100
    return cum, mdd * 100, (cum / 100) / abs(mdd) if mdd < 0 else 0, sorted(pf.keys())


WINNERS = ['SNDK', 'STX', 'MU', 'NVDA', 'LITE', 'COHR', 'KEYS']


def lowo(mode, gap_thr=1.5, pool=5):
    base = run('base')[0]
    full = run(mode, gap_thr, pool)[0] - base
    worst = 999; ww = None
    for w in WINNERS:
        d = run(mode, gap_thr, pool, ban=(w,))[0] - run('base', ban=(w,))[0]
        if d < worst:
            worst = d; ww = w
    return full, worst, ww


if __name__ == '__main__':
    b = run('base')
    print(f'base(순위만): {b[0]:+.1f}% MDD{b[1]:+.1f} Cal{b[2]:.2f} 보유{b[3]}  (production 216.95% 정합)')
    print()
    print('=== 변형 (사용자 아이디어) ===')
    for label, mode, kw in [
        ('gap 우선(pool≤5)', 'gappri', dict(pool=5)),
        ('gap 우선(pool≤8)', 'gappri', dict(pool=8)),
        ('gap≥1.5 필터', 'gapfilt', dict(gap_thr=1.5)),
        ('gap≥2.0 필터', 'gapfilt', dict(gap_thr=2.0)),
        ('gap≥2.5 필터(압도적)', 'gapfilt', dict(gap_thr=2.5)),
    ]:
        r = run(mode, **kw)
        print(f'  {label:<20} {r[0]:+8.1f}% MDD{r[1]:+6.1f} Cal{r[2]:5.2f}  보유{r[3]}  (Δ{r[0]-b[0]:+.1f}p)')
    print()
    print('=== LOWO (winner 하나 빼서 음수면 비robust) ===')
    for label, mode, kw in [('gap 우선(pool≤8)', 'gappri', dict(pool=8)), ('gap≥2.0 필터', 'gapfilt', dict(gap_thr=2.0))]:
        full, worst, ww = lowo(mode, kw.get('gap_thr', 1.5), kw.get('pool', 5))
        print(f'  {label:<20} full {full:+.1f}p / worst-LOWO {worst:+.1f}p  → {"통과" if worst>0 else f"기각(-{ww})"}')
