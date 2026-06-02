# -*- coding: utf-8 -*-
"""V102 — production sim vs BT sim 차이 진단

발견: production sim (_get_system_performance)에서
  v84 baseline +252.6% / V86e+ +221.56% (-31p)
BT sim (research/auto_bt_*)에서
  v84 baseline +127.6% / V86e+ +220.1% (+92.5p)

★ production v84 시뮬이 BT v84보다 +125p 큼. 왜?

진단 방향:
1. start point 차이 (production = 처음부터, BT = random start)
2. entry filter 차이
3. eligible/verified 차이
4. weight 차이
5. 시뮬 종료 시점 차이

이번 검증:
A. production v84 logic을 그대로 BT-style random start로 시뮬
B. 그 결과가 BT +127.6%와 같으면 → start point 차이가 원인
C. 다르면 → 다른 logic 차이
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
print(f'전체 dates: {len(dates)} ({dates[0]} ~ {dates[-1]})')

# === production sim의 정확한 logic 재현 ===
# _get_system_performance를 참조하지만 sim 시작점을 변경 가능하게

# 전체 가격 + 데이터 로드
all_prices = {}
daily_data = {}
for d in dates:
    rows = cur.execute('SELECT ticker, price FROM ntm_screening WHERE date=?', (d,)).fetchall()
    all_prices[d] = {r[0]: r[1] for r in rows}
    rows2 = cur.execute('''SELECT ticker, price, part2_rank, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, rev_growth, adj_gap, num_analysts, rev_up30 FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL''', (d,)).fetchall()
    daily_data[d] = {r[0]: {'price': r[1], 'p2': r[2], 'nc': r[3], 'n7': r[4], 'n30': r[5], 'n60': r[6], 'n90': r[7], 'rg': r[8], 'ag': r[9], 'na': r[10], 'ru': r[11]} for r in rows2}


def _min_seg(nc, n7, n30, n60, n90):
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        if b and abs(b) > 0.01:
            segs.append((a-b)/abs(b)*100)
        else:
            segs.append(0)
    return min(segs)


def is_mega(info, peg_thr=0.22):
    cp = info.get('price'); nc = info.get('nc'); rg = info.get('rg')
    if not (cp and nc and nc > 0 and rg and rg > 0): return False
    peg = (cp/nc) / (rg*100)
    return peg < peg_thr


def sim_production_style(use_mega=True, start_idx=2, end_idx=None):
    """production sim 정확 재현 (시작 시점만 변경 가능)"""
    if end_idx is None: end_idx = len(dates)
    portfolio = {}
    sys_nav = 1.0
    for i in range(start_idx, end_idx):
        date = dates[i]; prev_date = dates[i-1]
        data = daily_data.get(date, {})
        prices = all_prices.get(date, {})
        prev_prices = all_prices.get(prev_date, {})

        # 매수 후보 — production logic: part2_rank ≤ 2 (V101 audit에서 발견)
        # NOTE: production uses wgap_rank from _w_gap function, here approximate with part2_rank
        ticker_ms = {}
        for tk, info in data.items():
            ticker_ms[tk] = _min_seg(info['nc'], info['n7'], info['n30'], info['n60'], info['n90'])

        eligible_list = [(tk, info['p2']) for tk, info in data.items() if ticker_ms.get(tk, 0) >= -2 and info['p2'] is not None]
        eligible_list.sort(key=lambda x: x[1])
        wgap_rank = {tk: r+1 for r, (tk, _) in enumerate(eligible_list)}

        # day_ret 누적
        day_ret = 0
        if portfolio:
            for tk, info_pf in portfolio.items():
                w = info_pf.get('weight', 0) / 100.0
                cur_p = prices.get(tk); prev_p = prev_prices.get(tk)
                if cur_p and prev_p and prev_p > 0:
                    day_ret += w * (cur_p - prev_p) / prev_p * 100
        sys_nav *= (1 + day_ret / 100)

        # 이탈
        for tk in list(portfolio.keys()):
            ep = portfolio[tk]['entry_price']
            cp = prices.get(tk)
            if cp is None: del portfolio[tk]; continue
            rk = wgap_rank.get(tk)
            ms = ticker_ms.get(tk, 0)
            info_tk = data.get(tk, {})
            rg_tk = info_tk.get('rg')
            is_mega_tk = use_mega and is_mega(info_tk)
            sell = False
            if ms < -2: sell = True
            elif use_mega and is_mega_tk and rg_tk is not None and rg_tk < 0.25: sell = True
            elif (rk is None or rk > 10) and not is_mega_tk: sell = True
            if sell: del portfolio[tk]

        # 진입
        if len(portfolio) < 2:
            cands = [tk for tk, _ in eligible_list[:30]
                     if tk not in portfolio and wgap_rank.get(tk, 999) <= 2
                     and ticker_ms.get(tk, -999) >= 0]
            used_idx = {info['slot_idx'] for info in portfolio.values()}
            free_idx = sorted([i for i in range(2) if i not in used_idx])
            if len(portfolio) == 0 and len(eligible_list) >= 2:
                top1_w = eligible_list[0][1]; top2_w = eligible_list[1][1]
                # production uses score, not rank. approximate gap
                gap_val = abs(top1_w - top2_w)
                new_weights = [100, 0] if gap_val >= 15 else [50, 50]
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
                    else:
                        existing_total = sum(info.get('weight', 0) for info in portfolio.values())
                        remaining = 100 - existing_total
                        portfolio[tk] = {'entry_price': cp, 'slot_idx': slot_idx, 'weight': remaining}
    return (sys_nav - 1) * 100


print('\n[1] Production sim 정확 재현 (start_idx=2, end=last)')
v84_full = sim_production_style(use_mega=False, start_idx=2)
v86e_full = sim_production_style(use_mega=True, start_idx=2)
print(f'  v84 baseline: {v84_full:+.2f}%')
print(f'  V86e+: {v86e_full:+.2f}%')
print(f'  diff: {v86e_full - v84_full:+.2f}p')

print('\n[2] Random start (BT-style, 100×3 paired)')
elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))

v84_results = []; v86e_results = []
for ch in seeds:
    for s in ch:
        v84_results.append(sim_production_style(use_mega=False, start_idx=s))
        v86e_results.append(sim_production_style(use_mega=True, start_idx=s))

print(f'  v84 random start avg: {statistics.mean(v84_results):+.2f}%')
print(f'  V86e+ random start avg: {statistics.mean(v86e_results):+.2f}%')
print(f'  paired diff avg: {statistics.mean(b-a for a, b in zip(v84_results, v86e_results)):+.2f}p')
wins = sum(1 for a, b in zip(v84_results, v86e_results) if b > a)
print(f'  V86e+ 우월: {wins}/{len(v84_results)}')

print('\n[3] 다양한 start_idx 비교 (production-style full sim)')
print(f'{"start":>6}{"end":>6}{"days":>6}{"v84":>12}{"V86e+":>12}{"diff":>10}')
print('-' * 65)
for s in [2, 10, 20, 30, 40, 50, 60]:
    if s >= len(dates) - MIN_HOLD: continue
    v84_r = sim_production_style(use_mega=False, start_idx=s)
    v86_r = sim_production_style(use_mega=True, start_idx=s)
    print(f'{s:>6}{len(dates):>6}{len(dates)-s:>6}{v84_r:>+10.1f}%{v86_r:>+10.1f}%{v86_r-v84_r:>+9.1f}p')

print(f'\n총 소요 {time.time()-t0:.0f}초')
con.close()
