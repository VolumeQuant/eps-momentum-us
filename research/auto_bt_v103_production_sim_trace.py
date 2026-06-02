# -*- coding: utf-8 -*-
"""V103 — production _get_system_performance trace 정밀 진단

발견 모순:
- production sim V86e+: -31p 손실
- V102 sim V86e+: +23p 우월
- BT V86e+: +92.5p alpha

가설: production sim의 V86e+ 통합 코드에서 메가 carryover 시 weight 100% → 신규 매수 차단

진단:
1. v84 logic만 (use_mega=False) 실행하여 매수/매도 trace
2. V86e+ logic (use_mega=True) 실행하여 매수/매도 trace
3. 두 결과 종목별 비교
"""
import sys, sqlite3, time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]

# production sim 정확 재현 with trace
all_prices = {}
daily_data = {}
for d in dates:
    rows = cur.execute('SELECT ticker, price FROM ntm_screening WHERE date=?', (d,)).fetchall()
    all_prices[d] = {r[0]: r[1] for r in rows}
    rows2 = cur.execute('''SELECT ticker, price, part2_rank, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, rev_growth, adj_gap, num_analysts, rev_up30 FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL''', (d,)).fetchall()
    daily_data[d] = {r[0]: {'price': r[1], 'p2': r[2], 'nc': r[3], 'n7': r[4], 'n30': r[5], 'n60': r[6], 'n90': r[7], 'rg': r[8], 'ag': r[9], 'na': r[10], 'ru': r[11]} for r in rows2}


def _apply_conviction(ag, ru, na, nc, n90, rg):
    ratio = 0
    if na and na > 0 and ru is not None:
        ratio = ru / na
    eps_floor = 0
    if nc is not None and n90 is not None and n90 and abs(n90) > 0.01:
        eps_floor = min(abs((nc - n90) / n90), 3.0)
    base = max(ratio, eps_floor)
    if rg is not None:
        rev_bonus = min(min(rg, 0.5) * 0.6, 0.3)
    else:
        rev_bonus = 0
    conv = base + rev_bonus
    return ag * (1 + conv) if ag is not None else 0


def _min_seg(nc, n7, n30, n60, n90):
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        if b and abs(b) > 0.01:
            segs.append((a-b)/abs(b)*100)
        else:
            segs.append(0)
    return min(segs)


def _w_gap(date_str, all_dates_local):
    """production _w_gap 정확 재현"""
    di = all_dates_local.index(date_str)
    d0 = all_dates_local[di]
    d1 = all_dates_local[di-1] if di >= 1 else None
    d2 = all_dates_local[di-2] if di >= 2 else None
    ds = [d for d in [d2, d1, d0] if d]

    score_by_d = {}
    for d in ds:
        conv = {}
        for tk, info in daily_data.get(d, {}).items():
            if info.get('ag') is None: continue
            conv[tk] = _apply_conviction(info['ag'], info.get('ru'), info.get('na'), info.get('nc'), info.get('n90'), info.get('rg'))
        vals = list(conv.values())
        if len(vals) >= 2:
            mv = sum(vals)/len(vals)
            sv = (sum((v-mv)**2 for v in vals)/len(vals))**0.5
            if sv > 0:
                score_by_d[d] = {tk: max(30.0, 65 + (-(v-mv)/sv)*15) for tk, v in conv.items()}
            else:
                score_by_d[d] = {tk: 65 for tk in conv}
        else:
            score_by_d[d] = {tk: 65 for tk in conv}

    def _cf(tk, idx):
        for j in range(idx-1, -1, -1):
            prev = score_by_d.get(ds[j], {}).get(tk)
            if prev is not None: return prev
        return 30

    result = {}
    tks = set()
    for d in ds:
        if d in score_by_d: tks.update(score_by_d[d].keys())
    wts = [0.5, 0.3, 0.2]
    for tk in tks:
        wg = 0
        for i, d in enumerate([d0, d1, d2]):
            if d:
                score = score_by_d.get(d, {}).get(tk)
                if score is None:
                    score = _cf(tk, ds.index(d) if d in ds else i)
                wg += score * wts[i]
        result[tk] = wg
    return result


def is_mega(info):
    cp = info.get('price'); nc = info.get('nc'); rg = info.get('rg')
    if not (cp and nc and nc > 0 and rg and rg > 0): return False
    peg = (cp/nc) / (rg*100)
    return peg < 0.22


def sim_production_full(use_mega, start_idx=2):
    portfolio = {}
    sys_nav = 1.0
    buys = []; sells = []; carryovers = []
    for i in range(start_idx, len(dates)):
        date = dates[i]; prev_date = dates[i-1]
        data = daily_data.get(date, {})
        prices = all_prices.get(date, {})
        prev_prices = all_prices.get(prev_date, {})

        w_gap = _w_gap(date, dates)
        ticker_ms = {tk: _min_seg(info['nc'], info['n7'], info['n30'], info['n60'], info['n90']) for tk, info in data.items()}

        eligible = [(tk, w_gap.get(tk, 0)) for tk in data if ticker_ms.get(tk, 0) >= -2]
        eligible.sort(key=lambda x: x[1], reverse=True)
        wgap_rank = {tk: r+1 for r, (tk, _) in enumerate(eligible)}

        day_ret = 0
        if portfolio:
            for tk, info_pf in portfolio.items():
                w = info_pf.get('weight', 0) / 100.0
                cp = prices.get(tk); pp = prev_prices.get(tk)
                if cp and pp and pp > 0:
                    day_ret += w * (cp - pp) / pp * 100
        sys_nav *= (1 + day_ret/100)

        # 이탈
        for tk in list(portfolio.keys()):
            ep = portfolio[tk]['entry_price']
            cp = prices.get(tk)
            if cp is None: del portfolio[tk]; sells.append((date, tk, 'no_price')); continue
            rk = wgap_rank.get(tk)
            ms = ticker_ms.get(tk, 0)
            info_tk = data.get(tk, {})
            rg_tk = info_tk.get('rg')
            is_mega_tk = use_mega and is_mega(info_tk)
            sell = False; reason = None
            if ms < -2: sell = True; reason = 'ms<-2'
            elif use_mega and is_mega_tk and rg_tk is not None and rg_tk < 0.25:
                sell = True; reason = 'rev_g<0.25'
            elif (rk is None or rk > 10) and not is_mega_tk:
                sell = True; reason = f'rk>10 ({rk})'
            elif (rk is None or rk > 10) and is_mega_tk:
                carryovers.append((date, tk, rk or 'None'))  # 메가 carryover
            if sell:
                ret = (cp - ep) / ep * 100
                sells.append((date, tk, reason, f'{ret:+.1f}%'))
                del portfolio[tk]

        # 진입
        if len(portfolio) < 2:
            cands = [tk for tk, _ in eligible[:30]
                     if tk not in portfolio and wgap_rank.get(tk, 999) <= 2
                     and ticker_ms.get(tk, -999) >= 0]
            used_idx = {info['slot_idx'] for info in portfolio.values()}
            free_idx = sorted([i for i in range(2) if i not in used_idx])
            if len(portfolio) == 0 and len(eligible) >= 2:
                top1_w = eligible[0][1]; top2_w = eligible[1][1]
                if top1_w > 0:
                    gap = (top1_w - top2_w) / top1_w * 100
                else:
                    gap = 0
                new_weights = [100, 0] if gap >= 15 else [50, 50]
            else:
                new_weights = None
            for slot_idx in free_idx:
                if not cands: break
                tk = cands.pop(0)
                cp = prices.get(tk)
                if cp:
                    if new_weights is not None:
                        w_val = new_weights[slot_idx]
                        if w_val == 0: continue
                        portfolio[tk] = {'entry_price': cp, 'slot_idx': slot_idx, 'weight': w_val}
                        buys.append((date, tk, w_val))
                    else:
                        existing_total = sum(info.get('weight', 0) for info in portfolio.values())
                        remaining = 100 - existing_total
                        if remaining > 0:
                            portfolio[tk] = {'entry_price': cp, 'slot_idx': slot_idx, 'weight': remaining}
                            buys.append((date, tk, remaining))

    return dict(cum=(sys_nav-1)*100, buys=buys, sells=sells, carryovers=carryovers)


print('=' * 100)
print('V103 — production _get_system_performance trace 정밀 진단')
print('=' * 100)

print('\n[1] v84 logic (use_mega=False) full trace')
r84 = sim_production_full(False, 2)
print(f'  cum: {r84["cum"]:+.2f}%')
print(f'  매수: {len(r84["buys"])}회 / 매도: {len(r84["sells"])}회')
buy_tks_84 = set(tk for _, tk, _ in r84['buys'])
print(f'  매수 종목 ({len(buy_tks_84)}): {sorted(buy_tks_84)}')

print('\n[2] V86e+ logic (use_mega=True) full trace')
r86 = sim_production_full(True, 2)
print(f'  cum: {r86["cum"]:+.2f}%')
print(f'  매수: {len(r86["buys"])}회 / 매도: {len(r86["sells"])}회 / 메가 carryover: {len(r86["carryovers"])}회')
buy_tks_86 = set(tk for _, tk, _ in r86['buys'])
print(f'  매수 종목 ({len(buy_tks_86)}): {sorted(buy_tks_86)}')

print(f'\n  V86e+만 매수: {sorted(buy_tks_86 - buy_tks_84)}')
print(f'  v84만 매수: {sorted(buy_tks_84 - buy_tks_86)}')

print('\n[3] 메가 carryover 분석 (V86e+)')
carryover_counter = defaultdict(int)
for d, tk, rk in r86['carryovers']:
    carryover_counter[tk] += 1
for tk, cnt in sorted(carryover_counter.items(), key=lambda x: -x[1])[:10]:
    print(f'  {tk}: {cnt}회 carryover')

print('\n[4] v84 매도 사유 분석')
sell_reason_84 = defaultdict(int)
for entry in r84['sells']:
    if len(entry) >= 3:
        sell_reason_84[entry[2].split(' ')[0] if ' ' in entry[2] else entry[2]] += 1
for r, c in sorted(sell_reason_84.items(), key=lambda x: -x[1]):
    print(f'  {r}: {c}회')

print('\n[5] V86e+ 매도 사유 분석')
sell_reason_86 = defaultdict(int)
for entry in r86['sells']:
    if len(entry) >= 3:
        sell_reason_86[entry[2].split(' ')[0] if ' ' in entry[2] else entry[2]] += 1
for r, c in sorted(sell_reason_86.items(), key=lambda x: -x[1]):
    print(f'  {r}: {c}회')

print('\n[6] 직접 비교: 핵심 winner (MU, SNDK, KEYS, FAF, AEIS, LITE, BE)')
for tk in ['MU', 'SNDK', 'KEYS', 'FAF', 'AEIS', 'LITE', 'BE', 'TER', 'TTMI']:
    v84_buys_t = [(d, w) for d, t, w in r84['buys'] if t == tk]
    v86_buys_t = [(d, w) for d, t, w in r86['buys'] if t == tk]
    v84_sells_t = [(d, *rest) for d, t, *rest in r84['sells'] if t == tk]
    v86_sells_t = [(d, *rest) for d, t, *rest in r86['sells'] if t == tk]
    print(f'  {tk}: v84 매수 {len(v84_buys_t)}회 매도 {len(v84_sells_t)}회 / V86e+ 매수 {len(v86_buys_t)}회 매도 {len(v86_sells_t)}회')

print(f'\n총 소요 {time.time()-t0:.0f}초')
con.close()
