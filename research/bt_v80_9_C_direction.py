"""C 변형: direction smoothing — X2 base 위에서 12시작일 BT

⚠️ PRODUCTION SAFETY:
- 원본 DB(eps_momentum_data.db)는 절대 수정하지 않음
- 모든 작업은 v80_9_C_dbs/ 하위 복사본에서만 진행
- regenerate()는 복사본 DB만 받아 처리

direction 식 변형:
  baseline (현재): direction = (s1+s2)/2 - (s3+s4)/2  ; cap_hit 시 +9.0 (β1)
  C1: 시간 가중 derivative (recent 70/30, old 50/50)
  C2: 단순 차이 (s1 - s4)
  C3: linear regression slope (4점 균등)
  C4: 시간 가중 평균 - 단순 평균
  C5: divisor 30 → 60 (직접 변경, direction 식 baseline 유지)

direction은 adj_gap에 영향:
  adj_gap = fwd_pe_chg × (1 + dir_factor) × eps_q
  → adj_gap_new = adj_gap_old × (1 + dir_factor_new) / (1 + dir_factor_old)
  fwd_pe_chg sign = sign(adj_gap_old) (eps_q, (1+dir_factor)는 항상 양수)
"""
import sqlite3
import shutil
import sys
import statistics
import math
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import numpy as np
import daily_runner as dr
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'v80_9_C_dbs'
GRID.mkdir(exist_ok=True)
SEG_CAP = 100
DIRECTION_DIVISOR = 30
DIRECTION_CAP = 0.3


def calc_segs(nc, n7, n30, n60, n90):
    if not all(x and abs(x) > 0.01 for x in (n7, n30, n60, n90)):
        return None
    s1 = max(-SEG_CAP, min(SEG_CAP, (nc-n7)/abs(n7)*100))
    s2 = max(-SEG_CAP, min(SEG_CAP, (n7-n30)/abs(n30)*100))
    s3 = max(-SEG_CAP, min(SEG_CAP, (n30-n60)/abs(n60)*100))
    s4 = max(-SEG_CAP, min(SEG_CAP, (n60-n90)/abs(n90)*100))
    return [s1, s2, s3, s4]


def direction_baseline(segs):
    if any(abs(s) >= SEG_CAP for s in segs):
        return 9.0, True
    return (segs[0]+segs[1])/2 - (segs[2]+segs[3])/2, False


def direction_C1(segs):
    """시간 가중 derivative."""
    if any(abs(s) >= SEG_CAP for s in segs):
        return 9.0, True
    recent = segs[0]*0.7 + segs[1]*0.3
    old = segs[2]*0.5 + segs[3]*0.5
    return recent - old, False


def direction_C2(segs):
    """s1 - s4 단순 차이."""
    if any(abs(s) >= SEG_CAP for s in segs):
        return 9.0, True
    return segs[0] - segs[3], False


def direction_C3(segs):
    """linear regression slope (4점 균등 시간 spacing).

    x = [3, 2, 1, 0] (s1=most recent), 정규화 안 한 slope.
    slope formula: sum((x_i - x_mean)(y_i - y_mean)) / sum((x_i - x_mean)^2)
    x_mean = 1.5, sum((x_i - 1.5)^2) = 2.25*4 = ... 사실 단순 가중치로:
    slope ∝ 3*s1 + 1*s2 - 1*s3 - 3*s4 (centered weights)
    direction은 양수 = recent up, 음수 = recent down. baseline scale와 맞추기 위해 /2:
    """
    if any(abs(s) >= SEG_CAP for s in segs):
        return 9.0, True
    return (3*segs[0] + segs[1] - segs[2] - 3*segs[3]) / 4, False


def direction_C4(segs):
    """시간 가중 평균 - 단순 평균."""
    if any(abs(s) >= SEG_CAP for s in segs):
        return 9.0, True
    weighted = segs[0]*0.4 + segs[1]*0.3 + segs[2]*0.2 + segs[3]*0.1
    simple = sum(segs) / 4
    return (weighted - simple) * 4, False  # ×4로 baseline scale 맞춤


DIRECTION_FNS = {
    'baseline': direction_baseline,
    'C1': direction_C1,
    'C2': direction_C2,
    'C3': direction_C3,
    'C4': direction_C4,
}


def reconstruct_dir_factor(adj_gap_old, segs, fn_old, fn_new, divisor_new=DIRECTION_DIVISOR):
    """변형 적용 후 새 dir_factor 반환. fwd_pe_chg sign은 adj_gap sign으로 추정."""
    dir_old, cap_old = fn_old(segs)
    dir_new, cap_new = fn_new(segs)

    # baseline dir_factor
    df_raw_old = max(-DIRECTION_CAP, min(DIRECTION_CAP, dir_old / DIRECTION_DIVISOR))
    df_raw_new = max(-DIRECTION_CAP, min(DIRECTION_CAP, dir_new / divisor_new))

    # opt4 sign flip 결정 (fwd_pe_chg 부호 = sign(adj_gap_old))
    fwd_sign = 1 if adj_gap_old > 0 else -1

    # baseline dir_factor 계산
    if cap_old:
        df_old = DIRECTION_CAP  # β1 +0.3
    elif fwd_sign > 0 and dir_old < 0:
        df_old = -df_raw_old  # opt4 sign flip
    else:
        df_old = df_raw_old

    # new dir_factor 계산 (변형 적용)
    if cap_new:
        df_new = DIRECTION_CAP
    elif fwd_sign > 0 and dir_new < 0:
        df_new = -df_raw_new
    else:
        df_new = df_raw_new

    return df_old, df_new


def regenerate(test_db, direction_fn_name, divisor=DIRECTION_DIVISOR):
    fn_new = DIRECTION_FNS[direction_fn_name]
    fn_old = DIRECTION_FNS['baseline']
    original_path = dr.DB_PATH
    dr.DB_PATH = str(test_db)
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]
        for today in dates:
            rows = cur.execute('''SELECT ticker, adj_gap, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                                  rev_up30, num_analysts, rev_growth
                                  FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL''',
                               (today,)).fetchall()

            # 1) adj_gap 재계산 (direction 변형 적용)
            new_adj_gaps = {}
            for r in rows:
                tk, ag, nc, n7, n30, n60, n90 = r[:7]
                if ag is None:
                    new_adj_gaps[tk] = None
                    continue
                segs = calc_segs(nc, n7, n30, n60, n90)
                if segs is None:
                    new_adj_gaps[tk] = ag  # NTM 미완 — 변형 무관, 원본 유지
                    continue
                df_old, df_new = reconstruct_dir_factor(ag, segs, fn_old, fn_new, divisor)
                # adj_gap = fwd_pe_chg × (1 + dir_factor) × eps_q
                # 비율로 새 adj_gap 산출
                if abs(1 + df_old) < 1e-9:
                    new_adj_gaps[tk] = ag
                else:
                    new_adj_gaps[tk] = ag * (1 + df_new) / (1 + df_old)

            # 2) conviction 적용 → 새 cr 정렬 (X2 conviction 사용 — daily_runner._apply_conviction)
            elig_conv = []
            for r in rows:
                tk, _, nc, _, _, _, n90, ru, na, rg = r
                if (ru or 0) < 3: continue
                # min_seg 계산 (segs로 — cap 제외)
                segs = calc_segs(nc, r[3], r[4], r[5], n90)
                if segs is not None:
                    valid = [s for s in segs if abs(s) < SEG_CAP]
                    ms = min(valid) if valid else 0
                else:
                    ms = 0
                if ms < -2: continue
                ag_new = new_adj_gaps.get(tk)
                if ag_new is None: continue
                cg = dr._apply_conviction(ag_new, ru, na, nc, n90, rev_growth=rg)
                if cg is not None: elig_conv.append((tk, cg, ag_new))
            elig_conv.sort(key=lambda x: x[1])
            new_cr = {tk: i + 1 for i, (tk, _, _) in enumerate(elig_conv)}

            # 3) DB 업데이트 — adj_gap도 갱신해야 _compute_w_gap_map에서 정확히 동작
            cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
            for tk, ag_new in new_adj_gaps.items():
                if ag_new is not None:
                    cur.execute('UPDATE ntm_screening SET adj_gap=? WHERE date=? AND ticker=?', (ag_new, today, tk))
            for tk, cr in new_cr.items():
                cur.execute('UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?', (cr, today, tk))

            # 4) part2_rank: w_gap 기준 Top 30
            tickers = list(new_cr.keys())
            wmap = dr._compute_w_gap_map(cur, today, tickers)
            sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
            top30 = sorted_w[:30]
            cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
            for rk, tk in enumerate(top30, 1):
                cur.execute('UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?', (rk, today, tk))
            conn.commit()
        conn.close()
    finally:
        dr.DB_PATH = original_path


def simulate_full(dates_all, data, entry_top, exit_top, max_slots, start_date):
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    daily_returns = []
    trades = []
    consecutive = defaultdict(int)
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    consecutive[tk] = consecutive.get(tk, 0) + 1
    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consecutive = defaultdict(int)
        for tk in rank_map:
            new_consecutive[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consecutive
        day_ret = 0
        if portfolio:
            for tk in portfolio:
                price = today_data.get(tk, {}).get('price')
                if price and di > 0:
                    prev = data.get(dates[di - 1], {}).get(tk, {}).get('price')
                    if prev and prev > 0:
                        day_ret += (price - prev) / prev * 100
            day_ret /= len(portfolio)
            daily_returns.append(day_ret)
        else:
            daily_returns.append(0)
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            ms = today_data.get(tk, {}).get('min_seg', 0)
            price = today_data.get(tk, {}).get('price')
            should_exit = False
            if rank is None or rank > exit_top: should_exit = True
            if ms < -2: should_exit = True
            if should_exit and price:
                ep = portfolio[tk]['entry_price']
                ret = (price - ep) / ep * 100
                trades.append(ret)
                exited.append(tk)
        for tk in exited: del portfolio[tk]
        vacancies = max_slots - len(portfolio)
        if vacancies > 0:
            cands = []
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > entry_top: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                ms = today_data.get(tk, {}).get('min_seg', 0)
                if ms < 0: continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0: cands.append((tk, price))
            for tk, price in cands[:vacancies]:
                portfolio[tk] = {'entry_price': price, 'entry_date': today}
    return daily_returns, trades


def calc_metrics(daily_returns, trades, n_days):
    cum_ret = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum_ret *= (1 + r / 100)
        peak = max(peak, cum_ret)
        dd = (cum_ret - peak) / peak * 100
        max_dd = min(max_dd, dd)
    total_return = (cum_ret - 1) * 100
    if len(daily_returns) > 1:
        avg_d = sum(daily_returns) / len(daily_returns)
        std_d = statistics.pstdev(daily_returns)
        sharpe = (avg_d / std_d * math.sqrt(252)) if std_d > 0 else 0
    else:
        avg_d = 0; std_d = 0; sharpe = 0
    downside = [r for r in daily_returns if r < 0]
    if len(downside) > 1:
        downside_std = math.sqrt(sum(r**2 for r in downside) / len(downside))
        sortino = (avg_d / downside_std * math.sqrt(252)) if downside_std > 0 else 0
    else:
        sortino = 0
    return {'total_return': total_return, 'max_dd': max_dd, 'sharpe': sharpe, 'sortino': sortino}


VARIANTS = [
    ('★ X2 base (현재)',                     'baseline', DIRECTION_DIVISOR),
    ('C1. 시간가중 derivative (70/30·50/50)', 'C1',       DIRECTION_DIVISOR),
    ('C2. s1 - s4 단순 차이',                'C2',       DIRECTION_DIVISOR),
    ('C3. linear regression slope',         'C3',       DIRECTION_DIVISOR),
    ('C4. weighted avg - simple avg',       'C4',       DIRECTION_DIVISOR),
    ('C5. divisor 30 → 60 (덜 민감)',        'baseline', 60),
]


def main():
    print('=' * 100)
    print('v80.9 (X2) base 위에서 후보 C (direction smoothing) 단건 검증 (12시작일)')
    print('=' * 100)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates_all, _ = bts2.load_data()
    start_dates = dates_all[2:14]
    print(f'시작일 12개\n')

    rows = []
    for name, fn_name, divisor in VARIANTS:
        slug = ''.join(c if c.isalnum() else '_' for c in name)[:25]
        db = GRID / f'{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, fn_name, divisor)
        bts2.DB_PATH = str(db)
        dates_v, data = bts2.load_data()
        per_start = []
        for sd in start_dates:
            sub_dates = [d for d in dates_v if d >= sd]
            n_days = len(sub_dates)
            drs, trades = simulate_full(dates_v, data, 3, 8, 3, start_date=sd)
            per_start.append(calc_metrics(drs, trades, n_days))
        avg_m = {key: sum(p[key] for p in per_start) / len(per_start) for key in per_start[0]}
        rows.append({'name': name, **avg_m})

    base = rows[0]
    print(f'{"변형":<40} {"ret":>8} {"MDD":>8} {"Sharpe":>7} {"Sortino":>8} {"ΔRet":>8} {"ΔMDD":>8}')
    print('-' * 110)
    for r in rows:
        marker = '★' if 'X2 base' in r['name'] else (' ✓' if r['total_return'] - base['total_return'] >= 1 else '  ')
        d_ret = r['total_return'] - base['total_return']
        d_mdd = r['max_dd'] - base['max_dd']
        print(f'{marker} {r["name"]:<38} {r["total_return"]:+7.2f}% {r["max_dd"]:+7.2f}% '
              f'{r["sharpe"]:>6.2f} {r["sortino"]:>7.2f} {d_ret:+7.2f}%p {d_mdd:+7.2f}%p')


if __name__ == '__main__':
    main()
