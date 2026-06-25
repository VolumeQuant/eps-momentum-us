# -*- coding: utf-8 -*-
"""놓친종목/손실종목 EDA → 개선가설 검증 BT (2026-06-25, 자율연구)

발단: 사용자 "놓치거나 어이없이 손실난 종목 EDA해서 개선포인트 찾아라".
방법: faithful production-replay (DB part2_rank + yfinance mark-to-market).
      보유종목이 top30 밖으로 밀려도 가격추적 가능해야 회전룰 검증 가능 → yfinance 사용.
      검증기준 = 프로젝트 표준: 전체 + walk-forward 3블록 + leave-one-winner-out(LOWO).

핵심결과(요약): ROT_CAP(저평가보유 순위상한)은 전체 +34~54p·WF 전블록+·−MU 생존이나
      ★worst-LOWO(−SNDK/−STX)서 −23~−29p로 뒤집힘 = 단일winner 착시 → 기각.
      섹터분산가드는 MDD −3.7p 악화(테마집중이 맞음). slot3·entry폭·PE_HOLD 전부 null/기각.
      = US 91일 단일강세장에선 모든 레버가 few-winner 착시(거래대금완화와 동일 패턴).
"""
import sqlite3, pandas as pd
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
EXIT_RANK = 12
SEMI = {'NVDA','AVGO','MU','LITE','AMAT','ASML','LRCX','MPWR','MCHP','ADI','TER','MKSI',
        'FORM','COHR','CRDO','LSCC','NVMI','SIMO','AEIS','MTSI','KEYS','TSM','SNDK','STX','CGNX','CIEN','ANET'}

def load():
    c = sqlite3.connect(DB); cur = c.cursor()
    all_dates = [r[0] for r in cur.execute(
        "SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date")]
    data = {}
    for d in all_dates:
        data[d] = {r[0]: {'p2': r[1], 'nc': r[2], 'n7': r[3], 'n30': r[4], 'n60': r[5], 'n90': r[6], 'dv': r[7]}
                   for r in cur.execute(
            "SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d "
            "FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL", (d,))}
    tks = sorted({r[0] for r in cur.execute("SELECT DISTINCT ticker FROM ntm_screening WHERE part2_rank<=12")})
    return all_dates, data, tks

def fetch_px(tks):
    import yfinance as yf
    return yf.download(tks, start='2026-02-01', end='2026-06-25', auto_adjust=False, progress=False)['Close']

def minseg(v):
    return min((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0
               for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])])

def make_run(all_dates, data, PX):
    pxidx = {d.strftime('%Y-%m-%d'): i for i, d in enumerate(PX.index)}
    def px(tk, d):
        i = pxidx.get(d)
        if i is None: return None
        try:
            v = PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
        except Exception: return None
    def run(ROT_CAP=999, PE_HOLD=30, sector_guard=False, entry_rank=5, slots=2, ban=(), d_lo=None, d_hi=None):
        tradable = set(PX.columns)
        rng = [d for d in all_dates if (d_lo is None or d >= d_lo) and (d_hi is None or d <= d_hi)]
        pf = {}; nav = 1.0; peak = 1.0; mdd = 0.0; nrot = 0
        for k in range(2, len(rng)):
            d, pv = rng[k], rng[k - 1]; dd = data.get(d, {})
            ms = {tk: minseg(v) for tk, v in dd.items()}
            wrank = {tk: v['p2'] for tk, v in dd.items() if v.get('p2')}
            elig = sorted([(tk, v['p2']) for tk, v in dd.items() if ms.get(tk, 0) >= -2 and v.get('p2')], key=lambda x: x[1])
            dr = 0.0
            for tk, info in pf.items():
                w = info['weight'] / 100; cu, pp = px(tk, d), px(tk, pv)
                if cu and pp and pp > 0: dr += w * (cu - pp) / pp * 100
            nav *= (1 + dr / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
            for tk in list(pf.keys()):
                cp = px(tk, d)
                if cp is None: continue
                it = dd.get(tk); sell = False
                if it is None:
                    if ROT_CAP >= 999: continue
                    sell = True; nrot += 1
                else:
                    rk, nc, m = it['p2'], it['nc'], minseg(it)
                    if m < -2: sell = True
                    elif rk > EXIT_RANK:
                        pe = (cp / nc) if (nc and nc > 0) else 999
                        if pe >= PE_HOLD: sell = True
                        elif rk > ROT_CAP: sell = True; nrot += 1
                if sell: del pf[tk]
            if len(pf) < slots:
                used = {info['slot_idx'] for info in pf.values()}
                free = sorted([s for s in range(slots) if s not in used])
                held_semi = sector_guard and any(t in SEMI for t in pf)
                cands = [tk for tk, _ in elig if tk not in pf and tk in tradable and tk not in ban
                         and ms.get(tk, -9) >= 0 and wrank.get(tk, 999) <= entry_rank
                         and (dd.get(tk, {}).get('dv') or 0) >= 1000]
                cands.sort(key=lambda t: wrank.get(t, 999))
                for tk in cands:
                    if len(pf) >= slots: break
                    if sector_guard and tk in SEMI and held_semi: continue
                    ip = px(tk, d); ix = free.pop(0) if free else len(pf)
                    if ip:
                        pf[tk] = {'entry_price': ip, 'slot_idx': ix, 'weight': 0}
                        if tk in SEMI: held_semi = True
            n = len(pf)
            for info in pf.values(): info['weight'] = 100 / n if n else 0
        return {'cum': (nav - 1) * 100, 'mdd': mdd * 100, 'nrot': nrot, 'hold': sorted(pf.keys())}
    return run

if __name__ == '__main__':
    all_dates, data, tks = load()
    PX = fetch_px(tks)
    run = make_run(all_dates, data, PX)
    base = run()
    print(f"SANITY baseline {base['cum']:+.1f}% (목표 ~+216.9% DB) hold={base['hold']}")
    print("\nROT_CAP 스윕:")
    for cap in [999, 40, 30, 25, 20, 15]:
        r = run(ROT_CAP=cap)
        print(f"  CAP{cap}: {r['cum']:+.1f}% (Δ{r['cum']-base['cum']:+.1f}p) MDD{r['mdd']:.1f}%")
    print("\n최종판정 worst-LOWO (winner 빼서 음수면 기각):")
    winners = ['MU','SNDK','STX','NVDA','LITE','AVGO']
    for cap in [20, 25, 30]:
        worst = min(run(ROT_CAP=cap, ban=(w,))['cum'] - run(ban=(w,))['cum'] for w in winners)
        print(f"  CAP{cap}: worst-LOWO {worst:+.1f}p → {'통과' if worst>0 else '기각'}")
    g = run(sector_guard=True)
    print(f"\n섹터분산가드: {g['cum']:+.1f}% MDD{g['mdd']:.1f}% (base MDD{base['mdd']:.1f}%) → 테마집중이 MDD우위")
