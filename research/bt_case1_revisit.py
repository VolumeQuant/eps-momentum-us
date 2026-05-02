"""Case 1 보너스 재검증 BT — 6시작일 multistart (β1처럼 BT 한계로 잘못 제거됐을 가능성)

Case 1: NTM 30d > +1% AND 가격 30d < -1% → z-score +8점
v78~v80.4에서 사용, v80.5에서 제거 (메모리: BT +2.84%p 알파지만 cr/p2/score_100 일관성 우선)
v80.5 BT는 5시작일 평균 — β1처럼 짧은 기간 시작일이 평균을 흐렸을 가능성.

비교 변형 (모두 β1 + opt4 유지, current production v80.5b 기준):
  v80_5b      — β1 + opt4 + no_case1   [현재 production]
  case1_on    — β1 + opt4 + case1       [Case 1 복원]

DB:
  v80_5b   = bak_pre_v80_6.db (or 현재 db, 동일)
  case1_on = bak_pre_v80_6.db 복사 후 part2_rank만 재계산 (case1=True)

시작일: 초기 6일 (50거래일+ 보장)
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

import numpy as np
import daily_runner as dr
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB_V80_5B = ROOT / 'eps_momentum_data.db'  # 롤백 후 = v80.5b
DB_CASE1 = ROOT / 'research' / 'pe_weight_dbs' / 'case1_on.db'


def make_w_gap_map_fn(case1_enabled):
    """Case 1 보너스 on/off 가능한 _compute_w_gap_map"""
    def _compute(cursor, today_str, tickers):
        dates = dr._get_recent_dates(cursor, 'composite_rank', today_str, 3)
        dates = sorted(dates)
        MISSING_PENALTY = 30

        score_by_date = {}
        for d in dates:
            rows = cursor.execute(
                'SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, '
                'rev_growth, ntm_30d, price '
                'FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
                (d,)
            ).fetchall()
            conv_gaps = {}
            ntm_px_data = {}
            for r in rows:
                tk = r[0]
                conv_gaps[tk] = dr._apply_conviction(r[1], r[2], r[3], r[4], r[5], rev_growth=r[6])
                ntm_px_data[tk] = (r[4], r[7], r[8])

            vals = list(conv_gaps.values())
            if len(vals) >= 2:
                mean_v = np.mean(vals)
                std_v = np.std(vals)
                if std_v > 0:
                    score_by_date[d] = {
                        tk: max(30.0, 65 + (-(v - mean_v) / std_v) * 15)
                        for tk, v in conv_gaps.items()
                    }
                else:
                    score_by_date[d] = {tk: 65 for tk in conv_gaps}
            else:
                score_by_date[d] = {tk: 65 for tk in conv_gaps}

            if case1_enabled:
                CASE1_PERIOD = 30
                CASE1_NTM_THR = 1.0
                CASE1_PX_THR = -1.0
                CASE1_SCORE_BONUS = 8
                target_30d = (datetime.strptime(d, '%Y-%m-%d') - timedelta(days=CASE1_PERIOD)).strftime('%Y-%m-%d')
                d_30ago = cursor.execute(
                    'SELECT MAX(date) FROM ntm_screening WHERE date <= ?', (target_30d,)
                ).fetchone()
                px_30d_map = {}
                if d_30ago and d_30ago[0]:
                    px_30d_rows = cursor.execute(
                        'SELECT ticker, price FROM ntm_screening WHERE date=? AND price > 0',
                        (d_30ago[0],)
                    ).fetchall()
                    px_30d_map = {r[0]: r[1] for r in px_30d_rows}

                for tk in list(score_by_date[d].keys()):
                    nd = ntm_px_data.get(tk)
                    if not nd: continue
                    ntm_cur, ntm_30d_val, price_now = nd
                    ntm_chg = ((ntm_cur - ntm_30d_val) / ntm_30d_val * 100) \
                        if ntm_30d_val and abs(ntm_30d_val) > 0.01 and ntm_cur else 0
                    px_30d = px_30d_map.get(tk)
                    px_chg = ((price_now - px_30d) / px_30d * 100) \
                        if px_30d and px_30d > 0 and price_now and price_now > 0 else 0
                    if ntm_chg > CASE1_NTM_THR and px_chg < CASE1_PX_THR:
                        score_by_date[d][tk] += CASE1_SCORE_BONUS

        weights = [0.2, 0.3, 0.5]
        if len(dates) == 2:
            weights = [0.4, 0.6]
        elif len(dates) == 1:
            weights = [1.0]

        p2_by_date = {}
        for d in dates:
            rows = cursor.execute(
                'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,)
            ).fetchall()
            p2_by_date[d] = {r[0] for r in rows}

        result = {}
        for tk in tickers:
            wg = 0
            for i, d in enumerate(dates):
                is_today = (d == today_str)
                if not is_today and tk not in p2_by_date.get(d, set()):
                    score = MISSING_PENALTY
                else:
                    score = score_by_date.get(d, {}).get(tk)
                    if score is None:
                        score = MISSING_PENALTY
                wg += score * weights[i]
            result[tk] = wg
        return result
    return _compute


def regenerate_part2_only(test_db, case1_enabled):
    """part2_rank만 재계산 (Case 1 on/off). score/adj_gap/cr는 그대로."""
    original = dr.DB_PATH
    original_w_gap = dr._compute_w_gap_map
    dr.DB_PATH = str(test_db)
    dr._compute_w_gap_map = make_w_gap_map_fn(case1_enabled)
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today in dates:
            tickers = [r[0] for r in cur.execute(
                'SELECT ticker FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
                (today,)
            ).fetchall()]
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
        dr._compute_w_gap_map = original_w_gap


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
    print('Case 1 보너스 재검증 BT — 6시작일 multistart')
    print('=' * 110)

    # case1_on DB 생성
    print(f'\n[준비] case1_on DB 생성: {DB_CASE1}')
    DB_CASE1.parent.mkdir(exist_ok=True)
    shutil.copy(DB_V80_5B, DB_CASE1)
    regenerate_part2_only(DB_CASE1, case1_enabled=True)
    print('  완료')

    # 시작일
    conn = sqlite3.connect(DB_V80_5B)
    cur = conn.cursor()
    all_dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    conn.close()
    start_dates = all_dates[2:8]
    print(f'\n시작일 ({len(start_dates)}개): {start_dates[0]} ~ {start_dates[-1]} (모두 50거래일+ 측정)')

    variants = [
        ('v80_5b (현재, no_case1)', DB_V80_5B),
        ('case1_on (Case 1 복원)',   DB_CASE1),
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
    print(f'{"변형":<28}', end='')
    for sd in start_dates:
        print(f' {sd:>10}', end='')
    print()
    print('-' * (30 + 11 * len(start_dates)))
    for r in rows:
        print(f'  {r["name"]:<26}', end='')
        for ret in r['rets']:
            print(f' {ret:>+9.2f}%', end='')
        print()

    print()
    print(f'{"변형":<28} {"avg":>8} {"med":>8} {"std":>5} {"min":>8} {"max":>8} '
          f'{"worstMDD":>9} {"risk_adj":>8}')
    print('-' * 100)
    for r in rows:
        marker = ' ★' if 'v80_5b' in r['name'] else '  '
        print(f'{marker}{r["name"]:<26} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% {r["std"]:>4.2f} '
              f'{r["min"]:+7.2f}% {r["max"]:+7.2f}% {r["worst_mdd"]:+8.2f}% {r["risk_adj"]:>7.2f}')

    # 비교
    base = rows[0]
    case1 = rows[1]
    d_ret = case1['avg'] - base['avg']
    d_med = case1['med'] - base['med']
    d_mdd = case1['worst_mdd'] - base['worst_mdd']
    d_ra = case1['risk_adj'] - base['risk_adj']
    print()
    print('=' * 110)
    print(f'Case 1 복원 효과')
    print('=' * 110)
    print(f'  ΔRet 평균:    {d_ret:+6.2f}%p')
    print(f'  ΔRet 중앙값:  {d_med:+6.2f}%p')
    print(f'  ΔMDD:         {d_mdd:+5.2f}%p')
    print(f'  Δrisk_adj:    {d_ra:+5.2f}')
    if d_ret >= 1.0 and d_mdd >= -1.0:
        print(f'  → ✓ Case 1 복원 권장 (ret 개선 + MDD 안정)')
    elif d_ret >= 1.0:
        print(f'  → ~ ret 개선이지만 MDD 트레이드오프 — 사용자 결정')
    elif abs(d_ret) < 1.0:
        print(f'  → ~ 효과 미미 — 현행 유지 무방')
    else:
        print(f'  → ✗ Case 1 복원 거부 (손실)')


if __name__ == '__main__':
    main()
