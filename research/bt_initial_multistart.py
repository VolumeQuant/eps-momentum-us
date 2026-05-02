"""초기 시작일 multistart BT — 시작일 운 제거하면서 충분한 기간 보장

전략: 데이터 시작 직후(2/12) ~ 그 다음 5거래일 동안만 시작일로 사용.
모든 시작일에서 50거래일 이상 측정 → 통계적 신뢰도 + 시작일 분산 효과.

비교 변형 4개:
  v80_5b      — β1 + opt4 + 7d 0.4   [현재 production = 메시지 수익률 기준]
  gamma       — γ  + opt4 + 7d 0.4   [β1 제거]
  gamma_mw    — γ  + opt4 + midweight (0.3/0.3/0.2/0.2) [β1 제거 + midweight]
  beta1_mw    — β1 + opt4 + midweight [β1 유지 + midweight]   ← 새 후보

DB 매핑:
  v80_5b   = eps_momentum_data.bak_pre_v80_6.db (마이그레이션 전 production)
  gamma    = research/pe_weight_dbs/current.db  (γ + 7d 0.4)
  gamma_mw = research/pe_weight_dbs/midweight.db (γ + midweight)
  beta1_mw = (새로 생성) bak_pre_v80_6에서 가격 가중치만 midweight로 재계산
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
DB_V80_5B = ROOT / 'eps_momentum_data.bak_pre_v80_6.db'
DB_CURRENT_PE = ROOT / 'research' / 'pe_weight_dbs' / 'current.db'
DB_MIDWEIGHT = ROOT / 'research' / 'pe_weight_dbs' / 'midweight.db'
DB_BETA1_MW = ROOT / 'research' / 'pe_weight_dbs' / 'beta1_midweight.db'
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


def calc_beta1_opt4(segs, fwd_pe_chg):
    """β1 (cap → +0.3) + opt4"""
    score = sum(segs)
    cap_hit = any(abs(s) >= SEG_CAP for s in segs)
    if cap_hit:
        df = 0.3
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


def regenerate_with_weights(test_db, weights, calc_fn):
    """fwd_pe_chg 재계산 + adj_gap/cr/p2 재정렬"""
    original = dr.DB_PATH
    dr.DB_PATH = str(test_db)
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()

        rows = cur.execute(
            'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL AND price > 0'
        ).fetchall()
        price_history = {}
        for tk, d, p in rows:
            price_history.setdefault(tk, []).append((d, p))
        for tk in price_history:
            price_history[tk].sort(key=lambda x: x[0])

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
            today_d = datetime.strptime(today, '%Y-%m-%d').date()
            for r in rows:
                tk, nc, n7, n30, n60, n90, px_now, ru, na, rg = r
                segs = fmt_segments(nc, n7, n30, n60, n90)
                if segs is None or px_now is None or px_now <= 0 or nc is None or nc <= 0:
                    continue

                hist = price_history.get(tk, [])
                px_then = {}
                for n_days, key in [(7, '7d'), (30, '30d'), (60, '60d'), (90, '90d')]:
                    target = today_d - timedelta(days=n_days)
                    best, best_diff = None, None
                    for d, p in hist:
                        d_obj = datetime.strptime(d, '%Y-%m-%d').date()
                        if d_obj > today_d:
                            continue
                        diff = abs((d_obj - target).days)
                        if best_diff is None or diff < best_diff:
                            best_diff = diff
                            best = p
                    px_then[key] = best

                fwd_pe_now = px_now / nc
                ntm_map = {'7d': n7, '30d': n30, '60d': n60, '90d': n90}
                weighted_sum, total_weight = 0.0, 0.0
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

                score_n, df_n, eq_n = calc_fn(segs, fwd_pe_chg)
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


def run_multistart(db_path, start_dates):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    rets, mdds = [], []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds


def main():
    print('=' * 110)
    print('초기 시작일 multistart BT — 시작일 운 제거 + 충분한 기간 보장')
    print('=' * 110)

    # β1+midweight DB 생성 (없으면)
    if not DB_BETA1_MW.exists():
        print(f'\n[준비] β1+midweight DB 생성: {DB_BETA1_MW}')
        shutil.copy(DB_V80_5B, DB_BETA1_MW)
        weights_mw = {'7d': 0.3, '30d': 0.3, '60d': 0.2, '90d': 0.2}
        regenerate_with_weights(DB_BETA1_MW, weights_mw, calc_beta1_opt4)
        print('  완료')

    # 시작일: 데이터 처음 6개 (3일 검증 후 = all_dates[2]부터 6개)
    conn = sqlite3.connect(DB_V80_5B)
    cur = conn.cursor()
    all_dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    conn.close()
    start_dates = all_dates[2:8]  # 2/12 ~ 그 후 5거래일 (총 6개)
    last_date = all_dates[-1]
    print(f'\n시작일 후보 ({len(start_dates)}개): {start_dates[0]} ~ {start_dates[-1]}')
    print(f'데이터 끝: {last_date}')
    for sd in start_dates:
        n_days = len(all_dates) - all_dates.index(sd)
        print(f'  {sd}: {n_days}거래일 측정')

    variants = [
        ('v80_5b (production, 메시지 기준)',  DB_V80_5B),
        ('gamma (β1 제거, 7d 0.4)',           DB_CURRENT_PE),
        ('gamma_mw (β1 제거 + midweight)',     DB_MIDWEIGHT),
        ('beta1_mw (β1 유지 + midweight)',     DB_BETA1_MW),
    ]

    rows = []
    for name, db in variants:
        rets, mdds = run_multistart(db, start_dates)
        avg = sum(rets) / len(rets)
        med = sorted(rets)[len(rets) // 2]
        std = statistics.pstdev(rets)
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        rows.append({
            'name': name, 'rets': rets, 'mdds': mdds,
            'avg': avg, 'med': med, 'std': std,
            'min': min(rets), 'max': max(rets),
            'worst_mdd': worst_mdd, 'risk_adj': risk_adj,
        })

    print()
    print('시작일별 ret (%) — 4개 변형')
    print(f'{"변형":<38}', end='')
    for sd in start_dates:
        print(f' {sd:>9}', end='')
    print()
    print('-' * (40 + 10 * len(start_dates)))
    for r in rows:
        print(f'  {r["name"]:<36}', end='')
        for ret in r['rets']:
            print(f' {ret:>+8.2f}%', end='')
        print()

    print()
    print(f'{"변형":<38} {"avg":>8} {"med":>8} {"std":>5} {"min":>8} {"max":>8} '
          f'{"worstMDD":>9} {"risk_adj":>8}')
    print('-' * 110)
    for r in rows:
        marker = ' ★' if 'production' in r['name'] else '  '
        print(f'{marker}{r["name"]:<36} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% {r["std"]:>4.2f} '
              f'{r["min"]:+7.2f}% {r["max"]:+7.2f}% {r["worst_mdd"]:+8.2f}% {r["risk_adj"]:>7.2f}')

    # 비교
    base = rows[0]  # v80.5b
    print()
    print('=' * 110)
    print(f'{base["name"]} 대비 차이')
    print('=' * 110)
    for r in rows[1:]:
        d_ret = r['avg'] - base['avg']
        d_med = r['med'] - base['med']
        d_mdd = r['worst_mdd'] - base['worst_mdd']
        d_ra = r['risk_adj'] - base['risk_adj']
        verdict = '✓ 개선' if d_ret >= 1.0 and d_mdd >= -1.0 else \
                  '~ 미세' if abs(d_ret) < 1.0 else '✗ 손실'
        print(f'  {r["name"]:<36}: ΔRet평균 {d_ret:+6.2f}%p, ΔRet중앙 {d_med:+6.2f}%p, '
              f'ΔMDD {d_mdd:+5.2f}%p, Δrisk_adj {d_ra:+5.2f}  {verdict}')


if __name__ == '__main__':
    main()
