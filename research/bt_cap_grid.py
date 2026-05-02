"""Cap 그리드 서치 BT — γ 패치의 cap 값 최적화.

bt_segment_fix.py 인프라 기반 + bt_pnl.py 매매 시뮬레이션.
fmt_segments에 cap_value 파라미터 추가한 변형.

비교 대상:
- baseline (control, 변경 없음)
- cap=50, 75, 100(현재), 125, 150, 200, 300, 500 + γ 적용

매매 BT: Top 3 균등비중, T일 part2_rank → T+1 종가 PnL.
"""
import sqlite3
import shutil
import os
import sys
import math
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import daily_runner as dr

ROOT = Path(__file__).parent.parent
# DB_ORIGINAL은 pre-γ (γ 적용 전 production) 사용 — fair compare 위해
# /tmp/db_pre_gamma.db는 git history에서 추출한 def3b4d^ 시점 DB
DB_ORIGINAL = Path('/tmp/db_pre_gamma.db') if Path('/tmp/db_pre_gamma.db').exists() else ROOT / 'eps_momentum_data.db'
GRID_DIR = ROOT / 'research' / 'cap_grid_dbs'
GRID_DIR.mkdir(exist_ok=True)


# ─────────────────── segment + γ (cap 가변) ───────────────────

def fmt_segments_cap(nc, n7, n30, n60, n90, cap):
    if not all(x is not None and x != 0 for x in (n7, n30, n60, n90)):
        return None
    seg1 = max(-cap, min(cap, (nc - n7) / abs(n7) * 100))
    seg2 = max(-cap, min(cap, (n7 - n30) / abs(n30) * 100))
    seg3 = max(-cap, min(cap, (n30 - n60) / abs(n60) * 100))
    seg4 = max(-cap, min(cap, (n60 - n90) / abs(n90) * 100))
    return seg1, seg2, seg3, seg4


def calc_baseline_cap(segs, cap):
    """cap 적용 후 단순 합산 (γ 미적용 control)"""
    s1, s2, s3, s4 = segs
    score = s1 + s2 + s3 + s4
    direction = (s1 + s2) / 2 - (s3 + s4) / 2
    dir_factor = max(-0.3, min(0.3, direction / 30))
    min_seg = min(segs)
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    adj_score = score * (1 + dir_factor)
    return score, dir_factor, eps_q, adj_score


def calc_gamma_cap(segs, cap):
    """γ: cap 발동 시 dir_factor=0"""
    s1, s2, s3, s4 = segs
    score = s1 + s2 + s3 + s4
    caps = [abs(s) >= cap for s in segs]
    if any(caps):
        dir_factor = 0.0
        valid = [s for s, c in zip(segs, caps) if not c]
        min_seg = min(valid) if valid else 0
    else:
        direction = (s1 + s2) / 2 - (s3 + s4) / 2
        dir_factor = max(-0.3, min(0.3, direction / 30))
        min_seg = min(segs)
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    adj_score = score * (1 + dir_factor)
    return score, dir_factor, eps_q, adj_score


# ─────────────────── DB 재생성 (cap_value 가변) ───────────────────

def regenerate(test_db_path, cap, calc_fn):
    """test_db에 cap_value + calc_fn 적용해 재계산"""
    original_path = dr.DB_PATH
    dr.DB_PATH = test_db_path
    try:
        conn = sqlite3.connect(test_db_path)
        cur = conn.cursor()

        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today in dates:
            rows = cur.execute('''
                SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                       adj_gap, rev_up30, num_analysts, rev_growth
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today,)).fetchall()
            if not rows:
                continue

            new_data = []
            for r in rows:
                tk, nc, n7, n30, n60, n90, ag_old, ru, na, rg = r
                # 기존 baseline cap=100 기준으로 fwd_pe_chg 역산
                segs_base = fmt_segments_cap(nc, n7, n30, n60, n90, 100)
                if segs_base is None or ag_old is None:
                    continue
                _, df_old, eq_old, _ = calc_baseline_cap(segs_base, 100)
                denom = (1 + df_old) * eq_old
                if abs(denom) < 1e-6:
                    continue
                fwd_pe_chg = ag_old / denom

                # 새 cap + calc_fn 적용
                segs_new = fmt_segments_cap(nc, n7, n30, n60, n90, cap)
                score_n, df_n, eq_n, asc_n = calc_fn(segs_new, cap)
                ag_n = fwd_pe_chg * (1 + df_n) * eq_n
                new_data.append((tk, score_n, asc_n, ag_n, ru, na, nc, n90, rg))

            for tk, sc, asc, ag, *_ in new_data:
                cur.execute(
                    'UPDATE ntm_screening SET score=?, adj_score=?, adj_gap=? WHERE date=? AND ticker=?',
                    (sc, asc, ag, today, tk)
                )

            # composite_rank 재정렬 (production conviction)
            elig_conv = []
            for tk, _, _, ag, ru, na, nc, n90, rg in new_data:
                cg = dr._apply_conviction(ag, ru, na, nc, n90, rev_growth=rg)
                if cg is not None:
                    elig_conv.append((tk, cg))
            elig_conv.sort(key=lambda x: x[1])
            new_cr = {tk: i + 1 for i, (tk, _) in enumerate(elig_conv)}

            cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
            for tk, cr in new_cr.items():
                cur.execute(
                    'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                    (cr, today, tk)
                )

            # part2_rank w_gap descending
            tickers = list(new_cr.keys())
            wmap = dr._compute_w_gap_map(cur, today, tickers)
            sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
            top30 = sorted_w[:30]
            cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
            for rk, tk in enumerate(top30, 1):
                cur.execute(
                    'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                    (rk, today, tk)
                )
            conn.commit()
        conn.close()
    finally:
        dr.DB_PATH = original_path


# ─────────────────── PnL BT ───────────────────

def load_picks_prices(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT date, ticker, part2_rank FROM ntm_screening
        WHERE part2_rank IS NOT NULL AND part2_rank <= 3
        ORDER BY date, part2_rank
    ''').fetchall()
    picks = defaultdict(list)
    for d, tk, _ in rows:
        picks[d].append(tk)
    prices = {(d, tk): p for d, tk, p in cur.execute(
        'SELECT date, ticker, price FROM ntm_screening WHERE price IS NOT NULL'
    ).fetchall() if p and p > 0}
    conn.close()
    return picks, prices


def simulate_pnl(picks, prices):
    dates = sorted(picks.keys())
    nav = 1.0
    drets = []
    trades = []
    for i in range(len(dates) - 1):
        d, dn = dates[i], dates[i+1]
        pks = picks[d]
        if not pks:
            continue
        rets = []
        for tk in pks:
            pt = prices.get((d, tk))
            pn = prices.get((dn, tk))
            if pt and pn:
                r = pn / pt - 1
                rets.append(r)
                trades.append(r)
        if rets:
            dr_v = sum(rets) / len(rets)
            drets.append(dr_v)
            nav *= (1 + dr_v)
    return nav, drets, trades


def calc_metrics(nav, drets, trades):
    n = len(drets)
    if n == 0: return {}
    avg = sum(drets) / n
    var = sum((r - avg) ** 2 for r in drets) / n
    std = math.sqrt(var)
    sharpe = avg / std * math.sqrt(252) if std > 0 else 0
    cum = 1.0
    peak = 1.0
    mdd = 0
    for r in drets:
        cum *= (1 + r)
        peak = max(peak, cum)
        dd = (cum - peak) / peak
        mdd = min(mdd, dd)
    wins = sum(1 for r in trades if r > 0)
    return {
        'ret': (nav - 1) * 100,
        'mdd': mdd * 100,
        'sharpe': sharpe,
        'days': n,
        'trades': len(trades),
        'winrate': wins / len(trades) * 100 if trades else 0,
    }


def run_variant(name, cap, calc_fn):
    db = GRID_DIR / f'{name}.db'
    shutil.copy(DB_ORIGINAL, db)
    regenerate(db, cap, calc_fn)
    picks, prices = load_picks_prices(db)
    nav, drets, trades = simulate_pnl(picks, prices)
    m = calc_metrics(nav, drets, trades)
    m['name'] = name
    m['cap'] = cap
    return m


def main():
    print('=' * 90)
    print('Cap 그리드 서치 — γ 패치의 최적 cap 값 탐색')
    print('=' * 90)

    print(f'  DB_ORIGINAL: {DB_ORIGINAL}')

    # 1) pre-γ baseline (cap=100, γ 미적용 — 진짜 v80.2 production)
    print('\n[1] Pre-γ baseline (cap=100 γ 미적용, v80.2)')
    picks, prices = load_picks_prices(DB_ORIGINAL)
    nav, drets, trades = simulate_pnl(picks, prices)
    base = calc_metrics(nav, drets, trades)
    base['name'] = 'pre_gamma_v80.2'
    base['cap'] = 100
    print(f'  Ret {base["ret"]:+.2f}%, MDD {base["mdd"]:+.2f}%, Sharpe {base["sharpe"]:.2f}, '
          f'Trades {base["trades"]}, Win {base["winrate"]:.1f}%')

    # 2) cap 그리드 + γ 적용
    print('\n[2] Cap 그리드 (γ 적용)')
    grid = [50, 75, 100, 125, 150, 200, 300, 500]
    rows = [base]
    for cap in grid:
        m = run_variant(f'gamma_cap{cap}', cap, calc_gamma_cap)
        rows.append(m)
        print(f'  cap={cap:>3}: Ret {m["ret"]:+.2f}%, MDD {m["mdd"]:+.2f}%, Sharpe {m["sharpe"]:.2f}, '
              f'Trades {m["trades"]}, Win {m["winrate"]:.1f}%')

    # 3) cap 그리드 + γ 미적용 (control — cap 자체만의 효과 분리)
    print('\n[3] Cap 그리드 (γ 미적용, control)')
    for cap in grid:
        m = run_variant(f'baseline_cap{cap}', cap, calc_baseline_cap)
        m['name'] = f'baseline_cap{cap}'
        rows.append(m)
        print(f'  cap={cap:>3}: Ret {m["ret"]:+.2f}%, MDD {m["mdd"]:+.2f}%, Sharpe {m["sharpe"]:.2f}, '
              f'Trades {m["trades"]}, Win {m["winrate"]:.1f}%')

    # 4) 결과 테이블
    print()
    print('=' * 90)
    print('종합 비교 (Top 3 매매 BT, T+1 종가 기준)')
    print('=' * 90)
    print(f'{"Variant":<22} {"Cap":>5} {"Ret%":>8} {"MDD%":>8} {"Sharpe":>7} {"Trades":>7} {"Win%":>6}')
    print('-' * 90)
    rows.sort(key=lambda x: -x['ret'])
    for r in rows:
        marker = ' ★' if 'production' in r['name'] else '  '
        print(f'{marker}{r["name"]:<20} {r["cap"]:>5} {r["ret"]:+7.2f}% {r["mdd"]:+7.2f}% '
              f'{r["sharpe"]:>6.2f} {r["trades"]:>7} {r["winrate"]:>5.1f}%')

    # 5) 최적 cap 찾기
    gamma_rows = [r for r in rows if r['name'].startswith('gamma_cap')]
    if gamma_rows:
        gamma_rows.sort(key=lambda x: -x['ret'])
        print()
        print(f'γ 적용 최적: {gamma_rows[0]["name"]} ({gamma_rows[0]["ret"]:+.2f}%)')
        # vs production
        d_ret = gamma_rows[0]['ret'] - base['ret']
        d_mdd = gamma_rows[0]['mdd'] - base['mdd']
        d_sharpe = gamma_rows[0]['sharpe'] - base['sharpe']
        print(f'vs production (cap=100 γ): ΔRet {d_ret:+.2f}%p, ΔMDD {d_mdd:+.2f}%p, ΔSharpe {d_sharpe:+.2f}')


if __name__ == '__main__':
    main()
