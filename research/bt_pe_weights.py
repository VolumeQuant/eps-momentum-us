"""가격 가중치 BT — fwd_pe_chg의 시점별 가중치 변형 비교

배경: 현재 가중치 7d 0.4 / 30d 0.3 / 60d 0.2 / 90d 0.1 — 단기 0.4가 가장 큼.
MU 5/1 사례에서 1일 가격 +4.84% 점프가 fwd_pe_chg에 큰 영향. 단기 가중치가
너무 큰지 검증.

비교 변형:
  current      — 7d 0.4 / 30d 0.3 / 60d 0.2 / 90d 0.1   [현행 production]
  uniform      — 0.25 / 0.25 / 0.25 / 0.25              [균등, 단기 영향 균일화]
  long_heavy   — 0.1  / 0.2  / 0.3  / 0.4               [장기 강조]
  midweight    — 0.3  / 0.3  / 0.2  / 0.2               [중간형]
  short_lite   — 0.25 / 0.35 / 0.25 / 0.15              [7d 약간 낮춤, 30d 강조]

방법:
  1. v80.6 DB 복사 (γ 적용 상태)
  2. 각 일자/종목마다 N거래일 전 price 찾아서 fwd_pe_chg 재계산
  3. adj_gap = fwd_pe_chg × (1 + df_γ) × eps_q (γ + opt4)
  4. composite_rank / part2_rank 재정렬
  5. multistart 33시작일 Top3 매매

사용법: python research/bt_pe_weights.py
"""
import sqlite3
import shutil
import sys
import statistics
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import daily_runner as dr
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'pe_weight_dbs'
GRID.mkdir(exist_ok=True)
SEG_CAP = 100


def fmt_segments(nc, n7, n30, n60, n90):
    if not all(x is not None and x != 0 for x in (n7, n30, n60, n90)):
        return None
    return tuple(max(-SEG_CAP, min(SEG_CAP, v)) for v in (
        (nc - n7) / abs(n7) * 100,
        (n7 - n30) / abs(n30) * 100,
        (n30 - n60) / abs(n60) * 100,
        (n60 - n90) / abs(n90) * 100,
    ))


def calc_gamma_opt4(segs, fwd_pe_chg):
    """γ + opt4 (current production v80.6)"""
    score = sum(segs)
    cap_hit = any(abs(s) >= SEG_CAP for s in segs)
    if cap_hit:
        df = 0.0  # γ
    else:
        direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
        df_raw = max(-0.3, min(0.3, direction / 30))
        if fwd_pe_chg is not None and fwd_pe_chg > 0 and direction < 0:
            df = -df_raw  # opt4
        else:
            df = df_raw
    valid = [s for s in segs if abs(s) < SEG_CAP]
    min_seg = min(valid) if valid else 0
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return score, df, eps_q


def build_price_history(cur):
    """ticker별 (date, price) 시계열 — N일 전 가격 검색용"""
    rows = cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL AND price > 0'
    ).fetchall()
    by_ticker = {}
    for tk, d, p in rows:
        by_ticker.setdefault(tk, []).append((d, p))
    for tk in by_ticker:
        by_ticker[tk].sort(key=lambda x: x[0])
    return by_ticker


def find_price_n_days_ago(ticker_history, today_str, n_days):
    """today에서 n_days 달력일 전에 가장 가까운 거래일의 price 반환"""
    target = (datetime.strptime(today_str, '%Y-%m-%d') - timedelta(days=n_days)).date()
    history = ticker_history.get(ticker_history) if isinstance(ticker_history, dict) else ticker_history
    if not history:
        return None
    # binary search would be faster but linear is fine for ~60 dates
    best = None
    best_diff = None
    for d, p in history:
        d_obj = datetime.strptime(d, '%Y-%m-%d').date()
        if d_obj > datetime.strptime(today_str, '%Y-%m-%d').date():
            continue
        diff = abs((d_obj - target).days)
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best = p
    return best


def regenerate(test_db, weights):
    """fwd_pe_chg를 새 weights로 재계산 + adj_gap/cr/p2 재정렬"""
    original = dr.DB_PATH
    dr.DB_PATH = test_db
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        price_history = build_price_history(cur)

        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today in dates:
            rows = cur.execute('''
                SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, price,
                       rev_up30, num_analysts, rev_growth
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today,)).fetchall()
            if not rows:
                continue

            new_data = []
            for r in rows:
                tk, nc, n7, n30, n60, n90, px_now, ru, na, rg = r
                segs = fmt_segments(nc, n7, n30, n60, n90)
                if segs is None or px_now is None or px_now <= 0 or nc is None or nc <= 0:
                    continue

                # N거래일 전 price 검색
                hist = price_history.get(tk, [])
                px_then = {}
                for n_days, key in [(7, '7d'), (30, '30d'), (60, '60d'), (90, '90d')]:
                    target = (datetime.strptime(today, '%Y-%m-%d') - timedelta(days=n_days)).date()
                    today_d = datetime.strptime(today, '%Y-%m-%d').date()
                    best = None
                    best_diff = None
                    for d, p in hist:
                        d_obj = datetime.strptime(d, '%Y-%m-%d').date()
                        if d_obj > today_d:
                            continue
                        diff = abs((d_obj - target).days)
                        if best_diff is None or diff < best_diff:
                            best_diff = diff
                            best = p
                    px_then[key] = best

                # 새 weights로 fwd_pe_chg 재계산
                fwd_pe_now = px_now / nc
                ntm_map = {'7d': n7, '30d': n30, '60d': n60, '90d': n90}
                weighted_sum = 0.0
                total_weight = 0.0
                for key, w in weights.items():
                    pt = px_then.get(key)
                    nt = ntm_map.get(key)
                    if pt and pt > 0 and nt and nt > 0:
                        fwd_pe_then = pt / nt
                        pe_chg = (fwd_pe_now - fwd_pe_then) / fwd_pe_then * 100
                        weighted_sum += w * pe_chg
                        total_weight += w
                if total_weight <= 0:
                    continue
                fwd_pe_chg = weighted_sum / total_weight

                score_n, df_n, eq_n = calc_gamma_opt4(segs, fwd_pe_chg)
                asc_n = score_n * (1 + df_n)
                ag_n = fwd_pe_chg * (1 + df_n) * eq_n
                new_data.append((tk, score_n, asc_n, ag_n, ru, na, nc, n90, rg))

            for tk, sc, asc, ag, *_ in new_data:
                cur.execute(
                    'UPDATE ntm_screening SET score=?, adj_score=?, adj_gap=? WHERE date=? AND ticker=?',
                    (sc, asc, ag, today, tk)
                )

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
        dr.DB_PATH = original


def multistart(db_path, n_starts=33):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    start_dates = dates[:n_starts]
    rets, mdds = [], []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds, start_dates


VARIANTS = [
    ('current',    {'7d': 0.4,  '30d': 0.3,  '60d': 0.2,  '90d': 0.1}),
    ('uniform',    {'7d': 0.25, '30d': 0.25, '60d': 0.25, '90d': 0.25}),
    ('long_heavy', {'7d': 0.1,  '30d': 0.2,  '60d': 0.3,  '90d': 0.4}),
    ('midweight',  {'7d': 0.3,  '30d': 0.3,  '60d': 0.2,  '90d': 0.2}),
    ('short_lite', {'7d': 0.25, '30d': 0.35, '60d': 0.25, '90d': 0.15}),
]


def main():
    print('=' * 110)
    print('가격 가중치 BT — fwd_pe_chg 시점별 가중치 변형 (γ 적용 v80.6 기준)')
    print(f'DB 원본: {DB_ORIGINAL}')
    print('=' * 110)
    print('\n변형:')
    for name, w in VARIANTS:
        print(f'  {name:<14}: {dict(w)}')

    rows = []
    for name, weights in VARIANTS:
        db = GRID / f'{name}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, weights)
        rets, mdds, sds = multistart(db, n_starts=33)
        n = len(rets)
        avg = sum(rets) / n
        med = sorted(rets)[n // 2]
        std = statistics.pstdev(rets)
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        rows.append({
            'name': name, 'avg': avg, 'med': med, 'std': std,
            'min': min(rets), 'max': max(rets),
            'worst_mdd': worst_mdd, 'risk_adj': risk_adj,
        })

    print()
    print(f'{"Variant":<14} {"avg":>8} {"med":>8} {"std":>5} {"min":>8} {"max":>8} '
          f'{"worstMDD":>9} {"risk_adj":>8}')
    print('-' * 90)
    for r in rows:
        marker = ' ★' if r['name'] == 'current' else '  '
        print(f'{marker}{r["name"]:<12} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% {r["std"]:>4.2f} '
              f'{r["min"]:+7.2f}% {r["max"]:+7.2f}% {r["worst_mdd"]:+8.2f}% '
              f'{r["risk_adj"]:>7.2f}')

    base = next((r for r in rows if r['name'] == 'current'), None)
    if base:
        print()
        print('=' * 110)
        print('current (production) 대비 차이')
        print('=' * 110)
        for r in rows:
            if r['name'] == 'current':
                continue
            d_ret = r['avg'] - base['avg']
            d_mdd = r['worst_mdd'] - base['worst_mdd']
            d_ra = r['risk_adj'] - base['risk_adj']
            verdict = '✓ 채택 가능' if d_ret >= -1.0 and d_mdd >= -2.0 else '✗ 성과 손실'
            print(f'  {r["name"]:<14}: ΔRet {d_ret:+6.2f}%p, ΔMDD {d_mdd:+5.2f}%p, '
                  f'Δrisk_adj {d_ra:+5.2f}  {verdict}')


if __name__ == '__main__':
    main()
