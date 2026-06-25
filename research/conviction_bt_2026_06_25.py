# -*- coding: utf-8 -*-
"""사용자 아이디어 직접 검증: 2슬롯 top(최상위 순위) 종목 비중을 2x/3x 늘리면?
KR 확신가중(Calmar 3.92→5.52, KR서 작동)의 US 2슬롯판. 로컬 DB faithful replay.
- conc_w: 2종목 보유 시 top순위에 conc_w%(나머지 100-conc_w) — 100%내 집중.
- lev: top순위 종목 일수익에 배수(>100% = 레버리지, 2배/3배).
- LOWO: winner 하나씩 빼서 robust 여부(프로젝트 표준).
base(lev=1, conc_w=50)가 _get_system_performance 251.03% 재현해야 정합.
"""
import sys, os
import sqlite3
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr  # DB_PATH, EXIT_RANK, PE_HOLD

DB = dr.DB_PATH
EXIT_RANK = dr.EXIT_RANK
PE_HOLD = dr.PE_HOLD

conn = sqlite3.connect(DB); c = conn.cursor()
all_dates = [r[0] for r in c.execute(
    'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]
all_prices = {}
for d in all_dates:
    all_prices[d] = {r[0]: r[1] for r in c.execute('SELECT ticker, price FROM ntm_screening WHERE date=?', (d,)).fetchall()}
daily_data = {}
for d in all_dates:
    rows = c.execute('''SELECT ticker, price, part2_rank, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, rev_growth, dollar_volume_30d
        FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL''', (d,)).fetchall()
    daily_data[d] = {r[0]: {'price': r[1], 'part2_rank': r[2], 'nc': r[3], 'n7': r[4], 'n30': r[5],
                            'n60': r[6], 'n90': r[7], 'rg': r[8], 'dv': r[9]} for r in rows}
conn.close()


def _min_seg(nc, n7, n30, n60, n90):
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        segs.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(segs)


def run(lev=1.0, conc_w=50, ban=()):
    """2슬롯 faithful replay. top순위 종목에 lev 배수(일수익) + conc_w 집중(100%내)."""
    portfolio = {}
    sys_nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(2, len(all_dates)):
        date, prev_date = all_dates[i], all_dates[i - 1]
        data = daily_data.get(date, {}); prices = all_prices.get(date, {}); prev_prices = all_prices.get(prev_date, {})
        ticker_ms = {tk: _min_seg(v['nc'], v['n7'], v['n30'], v['n60'], v['n90']) for tk, v in data.items()}
        eligible = [(tk, v['part2_rank']) for tk, v in data.items() if ticker_ms.get(tk, 0) >= -2 and v.get('part2_rank') and tk not in ban]
        eligible.sort(key=lambda x: x[1])
        wgap_rank = {tk: v['part2_rank'] for tk, v in data.items() if v.get('part2_rank')}

        # top순위(보유 중 part2_rank 최소) 식별 → 비중/레버리지 부여
        top_tk = None
        if portfolio:
            held_ranked = sorted(portfolio.keys(), key=lambda t: wgap_rank.get(t, 9999))
            top_tk = held_ranked[0]

        # 일수익 (집중 가중 + top 레버리지)
        day_ret = 0.0
        if portfolio:
            pn = len(portfolio)
            for tk, info in portfolio.items():
                if pn == 1:
                    w = 1.0
                else:
                    w = (conc_w / 100.0) if tk == top_tk else (1 - conc_w / 100.0)
                mult = lev if tk == top_tk else 1.0
                cur, prev = prices.get(tk), prev_prices.get(tk)
                if cur and prev and prev > 0:
                    day_ret += mult * w * (cur - prev) / prev * 100
        sys_nav *= (1 + day_ret / 100)
        peak = max(peak, sys_nav); mdd = min(mdd, sys_nav / peak - 1)

        # 매도 (production 정합): min_seg<-2 즉시 / rank>EXIT_RANK & fwd_PE>=PE_HOLD
        for tk in list(portfolio.keys()):
            info_tk = daily_data.get(date, {}).get(tk)
            if info_tk is None:
                continue
            cp = prices.get(tk)
            if cp is None:
                continue
            rk = wgap_rank.get(tk); ms = ticker_ms.get(tk, 0); nc_tk = info_tk.get('nc')
            sell = False
            if ms < -2:
                sell = True
            elif rk is None or rk > EXIT_RANK:
                pe_tk = (cp / nc_tk) if (cp and nc_tk and nc_tk > 0) else 999
                if pe_tk >= PE_HOLD:
                    sell = True
            if sell:
                del portfolio[tk]

        # 진입: slot 1·2 part2 Top5 + $1B
        if len(portfolio) < 2:
            used = {info['slot_idx'] for info in portfolio.values()}
            free = sorted([s for s in range(2) if s not in used])
            cands = [tk for tk, _ in eligible if tk not in portfolio and ticker_ms.get(tk, -999) >= 0
                     and wgap_rank.get(tk, 999) <= 5 and (daily_data.get(date, {}).get(tk, {}).get('dv') or 0) >= 1000]
            cands.sort(key=lambda t: wgap_rank.get(t, 999))
            for tk in cands:
                if len(portfolio) >= 2:
                    break
                idx = free.pop(0) if free else len(portfolio)
                portfolio[tk] = {'entry_price': prices.get(tk), 'slot_idx': idx}
    cum = (sys_nav - 1) * 100
    cal = (cum / 100) / abs(mdd) if mdd < 0 else 0
    return cum, mdd * 100, cal, sorted(portfolio.keys())


WINNERS = ['SNDK', 'STX', 'MU', 'NVDA', 'LITE', 'COHR']


def lowo(lev, conc_w):
    """winner 하나씩 빼서 최악 delta(=비robust 판정). full=ban없는 우위."""
    base = run(1.0, 50)[0]
    full = run(lev, conc_w)[0] - base
    worst = 999; worst_w = None
    for w in WINNERS:
        b = run(1.0, 50, ban=(w,))[0]
        r = run(lev, conc_w, ban=(w,))[0]
        d = r - b
        if d < worst:
            worst = d; worst_w = w
    return full, worst, worst_w


if __name__ == '__main__':
    b = run(1.0, 50)
    print('=== 정합 게이트: base(lev1, 50/50) ===')
    print(f'  cum {b[0]:+.2f}%  MDD {b[1]:+.1f}%  Cal {b[2]:.2f}  보유 {b[3]}  (production 216.95% 일치 확인)')

    print('\n=== [A] 집중도 스윕 (top순위 종목에 conc_w%, 나머지 100-conc_w%) — 100%내, 레버리지X ===')
    print(f'{"top비중":>8}{"CAGR%":>9}{"MDD%":>8}{"Calmar":>8}')
    for cw in [50, 60, 70, 80, 90, 100]:
        r = run(1.0, cw)
        print(f'{cw:>7}%{r[0]:>+9.1f}{r[1]:>+8.1f}{r[2]:>8.2f}')

    print('\n=== [B] 레버리지 스윕 (top순위 종목 일수익 ×lev = >100% 노출, 50/50 기반) ===')
    print(f'{"lev":>8}{"CAGR%":>9}{"MDD%":>8}{"Calmar":>8}')
    for lv in [1.0, 1.5, 2.0, 3.0]:
        r = run(lv, 50)
        print(f'{lv:>8.1f}{r[0]:>+9.1f}{r[1]:>+8.1f}{r[2]:>8.2f}')

    print('\n=== [C] LOWO (프로젝트 표준: winner 하나 빼서 음수면 비robust 기각) ===')
    print(f'{"설정":>16}{"full Δ":>10}{"worst-LOWO Δ":>14}{"판정":>8}')
    for label, lv, cw in [('집중 80%', 1.0, 80), ('집중 100%', 1.0, 100), ('레버 2x', 2.0, 50), ('레버 3x', 3.0, 50)]:
        full, worst, ww = lowo(lv, cw)
        verdict = '통과' if worst > 0 else f'기각(-{ww})'
        print(f'{label:>16}{full:>+10.1f}{worst:>+14.1f}  {verdict}')
