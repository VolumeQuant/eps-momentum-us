"""DB-NTM lookback BT — yfinance의 stale 7d/30d/60d 컬럼을 DB의 ntm_current 시계열로 대체

배경:
  BE 5/13 cr 2→56 폭락. 원인 = yfinance `7daysAgo` 컬럼이 14일 지연 후
  마침내 어닝 후 컨센서스 반영. 단기 fwd_pe_chg 신호가 한 번에 소실.
  DB의 ntm_current는 매일 정확히 기록됨 → DB lookup으로 staleness 우회.

방법:
  1. 현재 production DB 복사 (v80.10b 상태)
  2. 각 일자/종목에서 N달력일 전 DB의 ntm_current 조회 → ntm_Nd 대체
  3. 90d는 DB 누적 부족(67거래일) → yf 컬럼 그대로 사용 (하이브리드)
  4. fwd_pe_chg = weighted_avg((px_now/ntm_cur - px_then/ntm_then) / ...)
  5. v80.10b 가중치 (7d 0.30 / 30d 0.10 / 60d 0.10 / 90d 0.50) + γ + opt4
  6. cr / p2 재정렬
  7. multistart 33시작일 Top3/X10/S3 매매 비교

비교:
  baseline_yf       — 현행 production (yf의 ntm_7d/30d/60d/90d 직접 사용)
  db_lookback_3     — 7d, 30d, 60d를 DB에서 lookup. 90d는 yf
  db_lookback_2     — 7d, 30d만 DB lookup. 60d, 90d는 yf
  db_lookback_1     — 7d만 DB lookup. 나머지는 yf
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
GRID = ROOT / 'research' / 'db_ntm_dbs'
GRID.mkdir(exist_ok=True)
SEG_CAP = 100

WEIGHTS = {'7d': 0.30, '30d': 0.10, '60d': 0.10, '90d': 0.50}


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
    score = sum(segs)
    cap_hit = any(abs(s) >= SEG_CAP for s in segs)
    if cap_hit:
        df = 0.0
    else:
        direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
        df_raw = max(-0.3, min(0.3, direction / 30))
        if fwd_pe_chg is not None and fwd_pe_chg > 0 and direction < 0:
            df = -df_raw
        else:
            df = df_raw
    valid = [s for s in segs if abs(s) < SEG_CAP]
    min_seg = min(valid) if valid else 0
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return score, df, eps_q


def build_history(cur):
    """ticker별 (date, price, ntm_current) 시계열"""
    rows = cur.execute(
        'SELECT ticker, date, price, ntm_current FROM ntm_screening '
        'WHERE price IS NOT NULL AND price > 0 AND ntm_current IS NOT NULL'
    ).fetchall()
    by_tk = {}
    for tk, d, p, n in rows:
        by_tk.setdefault(tk, []).append((d, p, n))
    for tk in by_tk:
        by_tk[tk].sort(key=lambda x: x[0])
    return by_tk


def find_n_days_ago(history, today_str, n_days, field_idx):
    """today에서 n_days 달력일 전 가장 가까운 거래일의 (price 또는 ntm)"""
    target = (datetime.strptime(today_str, '%Y-%m-%d') - timedelta(days=n_days)).date()
    today_d = datetime.strptime(today_str, '%Y-%m-%d').date()
    best = None
    best_diff = None
    for row in history:
        d_obj = datetime.strptime(row[0], '%Y-%m-%d').date()
        if d_obj > today_d:
            continue
        diff = abs((d_obj - target).days)
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best = row[field_idx]
    return best, best_diff


def regenerate(test_db, db_lookback_levels):
    """fwd_pe_chg 재계산 — db_lookback_levels = {'7d', '30d', '60d'} 중 어떤 걸 DB lookup으로 대체할지"""
    original = dr.DB_PATH
    dr.DB_PATH = test_db
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        history = build_history(cur)

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
                tk, nc, n7_yf, n30_yf, n60_yf, n90_yf, px_now, ru, na, rg = r
                if px_now is None or px_now <= 0 or nc is None or nc <= 0:
                    continue

                hist = history.get(tk, [])

                # DB lookup으로 ntm_Nd 대체
                ntm_lookup = {}
                px_lookup = {}
                for n_days, key in [(7, '7d'), (30, '30d'), (60, '60d'), (90, '90d')]:
                    if key in db_lookback_levels:
                        ntm_val, diff = find_n_days_ago(hist, today, n_days, 2)
                        # 누적 부족 시 yf fallback (diff > 2배 허용)
                        if ntm_val is None or (diff is not None and diff > n_days * 0.5 + 5):
                            ntm_val = {'7d': n7_yf, '30d': n30_yf, '60d': n60_yf, '90d': n90_yf}[key]
                    else:
                        ntm_val = {'7d': n7_yf, '30d': n30_yf, '60d': n60_yf, '90d': n90_yf}[key]
                    ntm_lookup[key] = ntm_val

                    px_val, _ = find_n_days_ago(hist, today, n_days, 1)
                    px_lookup[key] = px_val

                n7 = ntm_lookup['7d']
                n30 = ntm_lookup['30d']
                n60 = ntm_lookup['60d']
                n90 = ntm_lookup['90d']

                segs = fmt_segments(nc, n7, n30, n60, n90)
                if segs is None:
                    continue

                # fwd_pe_chg 가중 평균
                fwd_pe_now = px_now / nc
                weighted_sum = 0.0
                total_weight = 0.0
                for key, w in WEIGHTS.items():
                    pt = px_lookup.get(key)
                    nt = ntm_lookup.get(key)
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

                # ntm_Nd 컬럼도 DB lookup 결과로 덮어쓰기 (downstream에서 사용)
                new_data.append((tk, score_n, asc_n, ag_n, n7, n30, n60, n90, ru, na, nc, rg))

            for tk, sc, asc, ag, n7, n30, n60, n90, *_ in new_data:
                cur.execute(
                    'UPDATE ntm_screening SET score=?, adj_score=?, adj_gap=?, '
                    'ntm_7d=?, ntm_30d=?, ntm_60d=?, ntm_90d=? WHERE date=? AND ticker=?',
                    (sc, asc, ag, n7, n30, n60, n90, today, tk)
                )

            elig_conv = []
            for tk, _, _, ag, n7, n30, n60, n90, ru, na, nc, rg in new_data:
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


def multistart(db_path, n_starts=6):
    """초반 6시작일 — 각 보유 60~67일, 자기상관 줄임"""
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    start_dates = dates[:n_starts]
    rets, mdds = [], []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 10, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds, start_dates


def inspect_be(db_path, label):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT date, composite_rank, part2_rank, adj_gap, ntm_current, ntm_7d, ntm_30d "
        "FROM ntm_screening WHERE ticker='BE' ORDER BY date DESC LIMIT 7"
    ).fetchall()
    conn.close()
    print(f'\n--- BE under {label} ---')
    print(f'{"date":<12} {"cr":>4} {"p2":>4} {"adj_gap":>9} {"ntm_c":>7} {"ntm_7":>7} {"ntm_30":>7}')
    for r in rows:
        d, cr, p2, ag, nc, n7, n30 = r
        print(f'{d:<12} {cr if cr else "-":>4} {p2 if p2 else "-":>4} {ag:>9.2f} '
              f'{nc:>7.3f} {n7:>7.3f} {n30:>7.3f}')


VARIANTS = [
    ('baseline_yf', set()),
    ('db_1', {'7d'}),
    ('db_2', {'7d', '30d'}),
    ('db_3', {'7d', '30d', '60d'}),
]


def main():
    print('=' * 110)
    print('DB-NTM lookback BT — yfinance staleness 우회')
    print(f'가중치: {WEIGHTS} (v80.10b)')
    print('=' * 110)

    rows = []
    rets_by_variant = {}
    sd_list = None
    for name, levels in VARIANTS:
        db = GRID / f'{name}.db'
        if db.exists():
            db.unlink()
        shutil.copy(DB_ORIGINAL, db)
        print(f'\n[{name}] DB lookup levels: {levels if levels else "(none, pure yf)"}')
        regenerate(db, levels)
        inspect_be(db, name)
        rets, mdds, sds = multistart(db, n_starts=6)
        sd_list = sds
        rets_by_variant[name] = rets
        n = len(rets)
        avg = sum(rets) / n
        med = sorted(rets)[n // 2]
        std = statistics.pstdev(rets)
        worst_mdd = min(mdds)
        rows.append({
            'name': name, 'avg': avg, 'med': med, 'std': std,
            'min': min(rets), 'max': max(rets), 'worst_mdd': worst_mdd,
        })

    print()
    print('=' * 110)
    print(f'시작일 {len(sd_list)}개: {sd_list}')
    print('-' * 110)
    print(f'{"Variant":<14} {"avg":>8} {"med":>8} {"std":>5} {"min":>8} {"max":>8} {"worstMDD":>9}')
    for r in rows:
        marker = ' ★' if r['name'] == 'baseline_yf' else '  '
        print(f'{marker}{r["name"]:<12} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% {r["std"]:>4.2f} '
              f'{r["min"]:+7.2f}% {r["max"]:+7.2f}% {r["worst_mdd"]:+8.2f}%')

    print()
    print('=' * 110)
    print('paired 비교 (같은 시작일 기준 ΔRet vs baseline_yf)')
    print('=' * 110)
    base_rets = rets_by_variant['baseline_yf']
    print(f'{"start":<12} {"base":>8} ' +
          ' '.join(f'{"d_"+v:>10}' for v, _ in VARIANTS if v != 'baseline_yf'))
    for i, sd in enumerate(sd_list):
        row = f'{sd:<12} {base_rets[i]:+7.2f}% '
        for v, _ in VARIANTS:
            if v == 'baseline_yf':
                continue
            d = rets_by_variant[v][i] - base_rets[i]
            row += f'{d:+9.2f}%p '
        print(row)
    print()
    print(f'{"":<12} {"avg":>8} ' +
          ' '.join(f'{"":>10}' for _ in range(len(VARIANTS)-1)))
    for v, _ in VARIANTS:
        if v == 'baseline_yf':
            continue
        diffs = [rets_by_variant[v][i] - base_rets[i] for i in range(len(sd_list))]
        avg_d = sum(diffs) / len(diffs)
        wins = sum(1 for d in diffs if d > 0)
        med_d = sorted(diffs)[len(diffs) // 2]
        verdict = '✓ 양호' if avg_d >= -1.0 else '✗ 손실'
        print(f'  {v:<12}: avg Δ {avg_d:+6.2f}%p  med Δ {med_d:+6.2f}%p  '
              f'wins {wins}/{len(diffs)}  {verdict}')


if __name__ == '__main__':
    main()
