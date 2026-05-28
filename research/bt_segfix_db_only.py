"""DB 데이터만 사용하는 정직한 BT — yfinance 재호출 0번.

사용자 의도:
- production이 매일 적재한 DB의 ntm_screening row만 사용
- ntm_current/ntm_7d/30d/60d/90d (그날 측정한 snapshot) + price (그날 종가) → DB
- DB 시작일(2/10) 이전 데이터 없으면 그 lookback 빼고 weights 재정규화
- yfinance 추가 호출 절대 없음

비교:
- baseline: production 식 그대로 (DB 데이터로 재계산)
- γ:        cap 발동 시 dir_factor=0
- γ'':      cap segment 제외 후 partial direction
"""
import sqlite3
import shutil
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import daily_runner as dr

DB_ORIGINAL = 'eps_momentum_data.db'
DB_START_DATE = '2026-02-10'
SEG_CAP = 100


# ─────────────────────────────────────────────────────────
# Segment 계산 (3가지 변형)
# ─────────────────────────────────────────────────────────

def fmt_segments(nc, n7, n30, n60, n90):
    if not all(x is not None and x != 0 for x in (n7, n30, n60, n90)):
        return None
    seg1 = max(-SEG_CAP, min(SEG_CAP, (nc - n7) / abs(n7) * 100))
    seg2 = max(-SEG_CAP, min(SEG_CAP, (n7 - n30) / abs(n30) * 100))
    seg3 = max(-SEG_CAP, min(SEG_CAP, (n30 - n60) / abs(n60) * 100))
    seg4 = max(-SEG_CAP, min(SEG_CAP, (n60 - n90) / abs(n90) * 100))
    return seg1, seg2, seg3, seg4


def calc_baseline(seg1, seg2, seg3, seg4):
    score = seg1 + seg2 + seg3 + seg4
    direction = (seg1 + seg2) / 2 - (seg3 + seg4) / 2
    dir_factor = max(-0.3, min(0.3, direction / 30))
    min_seg = min(seg1, seg2, seg3, seg4)
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return dir_factor, eps_q


def calc_gamma(seg1, seg2, seg3, seg4):
    segs = [seg1, seg2, seg3, seg4]
    caps = [abs(s) >= SEG_CAP for s in segs]
    if any(caps):
        dir_factor = 0.0
        valid = [s for s, c in zip(segs, caps) if not c]
        min_seg = min(valid) if valid else 0
    else:
        direction = (seg1 + seg2) / 2 - (seg3 + seg4) / 2
        dir_factor = max(-0.3, min(0.3, direction / 30))
        min_seg = min(segs)
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return dir_factor, eps_q


def calc_gamma2(seg1, seg2, seg3, seg4):
    segs = [seg1, seg2, seg3, seg4]
    recent_valid = [s for s in (seg1, seg2) if abs(s) < SEG_CAP]
    old_valid = [s for s in (seg3, seg4) if abs(s) < SEG_CAP]
    if recent_valid and old_valid:
        direction = sum(recent_valid)/len(recent_valid) - sum(old_valid)/len(old_valid)
        dir_factor = max(-0.3, min(0.3, direction / 30))
    else:
        dir_factor = 0.0
    valid_all = [s for s in segs if abs(s) < SEG_CAP]
    min_seg = min(valid_all) if valid_all else 0
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return dir_factor, eps_q


# ─────────────────────────────────────────────────────────
# fwd_pe_chg DB-only (yfinance 안 씀, fallback 안 함)
# ─────────────────────────────────────────────────────────

def calc_fwd_pe_chg_db_only(cursor, ticker, today_str, nc, n7, n30, n60, n90, p_today):
    """Production 식 그대로 + DB 매일 row만 사용.

    각 lookback에 대해:
      - 가격: DB의 가까운 영업일 row의 price (DB 시작일 이전이면 skip)
      - NTM:  4/28 시점 측정한 snapshot (DB의 4/28 row의 ntm_Xd)
    데이터 없는 lookback은 weights 재정규화로 자동 제외.
    """
    if nc is None or nc <= 0 or p_today is None or p_today <= 0:
        return None
    fwd_pe_now = p_today / nc

    weights = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}
    ntms = {'7d': n7, '30d': n30, '60d': n60, '90d': n90}

    today_dt = datetime.strptime(today_str, '%Y-%m-%d')
    weighted_sum = 0.0
    total_weight = 0.0

    for label, w in weights.items():
        days = int(label[:-1])
        target = (today_dt - timedelta(days=days)).strftime('%Y-%m-%d')

        # DB에서 가까운 영업일 row의 price만 (DB 시작일 이상)
        r = cursor.execute(
            'SELECT price FROM ntm_screening WHERE ticker=? AND date <= ? AND date >= ? '
            'ORDER BY date DESC LIMIT 1',
            (ticker, target, DB_START_DATE)
        ).fetchone()

        if r and r[0] is not None and r[0] > 0:
            px_then = r[0]
            ntm_then = ntms[label]
            if ntm_then is not None and ntm_then > 0:
                fwd_pe_then = px_then / ntm_then
                pe_chg = (fwd_pe_now - fwd_pe_then) / fwd_pe_then * 100
                weighted_sum += w * pe_chg
                total_weight += w
        # 데이터 없으면 (DB 시작일 이전) skip → weights 재정규화 자동

    if total_weight <= 0:
        return None
    return weighted_sum / total_weight


# ─────────────────────────────────────────────────────────
# DB 재계산 (yfinance 안 씀)
# ─────────────────────────────────────────────────────────

def regenerate_db_only(test_db_path, segment_fn, label='?'):
    """DB만 사용해서 fwd_pe_chg → adj_gap → composite_rank → part2_rank 재계산"""
    original_path = dr.DB_PATH
    dr.DB_PATH = test_db_path

    try:
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()

        dates = [r[0] for r in cursor.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today_str in dates:
            rows = cursor.execute('''
                SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                       price, rev_up30, num_analysts, rev_growth
                FROM ntm_screening
                WHERE date=? AND composite_rank IS NOT NULL
            ''', (today_str,)).fetchall()

            if not rows:
                continue

            new_data = []
            for r in rows:
                tk, nc, n7, n30, n60, n90, p_today, ru, na, rg = r
                if nc is None or nc <= 0 or p_today is None or p_today <= 0:
                    continue

                segs = fmt_segments(nc, n7, n30, n60, n90)
                if segs is None:
                    continue
                seg1, seg2, seg3, seg4 = segs

                # 1) fwd_pe_chg DB-only 재계산
                fwd_pe_chg = calc_fwd_pe_chg_db_only(
                    cursor, tk, today_str, nc, n7, n30, n60, n90, p_today
                )
                if fwd_pe_chg is None:
                    continue

                # 2) segment_fn으로 dir_factor + eps_q
                dir_factor, eps_q = segment_fn(seg1, seg2, seg3, seg4)

                # 3) adj_gap 새로
                adj_gap_new = fwd_pe_chg * (1 + dir_factor) * eps_q

                # score, adj_score (디스플레이용)
                score = seg1 + seg2 + seg3 + seg4
                adj_score = score * (1 + dir_factor)

                new_data.append((tk, score, adj_score, adj_gap_new,
                                 ru, na, nc, n90, rg))

            # 4) DB 업데이트
            for tk, sc, ascn, ag, *_ in new_data:
                cursor.execute(
                    'UPDATE ntm_screening SET score=?, adj_score=?, adj_gap=? '
                    'WHERE date=? AND ticker=?',
                    (sc, ascn, ag, today_str, tk)
                )

            # 5) composite_rank 재정렬 (production conv 사용)
            eligible_conv = []
            for tk, _, _, ag, ru, na, nc, n90, rg in new_data:
                cg = dr._apply_conviction(ag, ru, na, nc, n90, rev_growth=rg)
                if cg is not None:
                    eligible_conv.append((tk, cg))
            eligible_conv.sort(key=lambda x: x[1])
            new_cr = {tk: i + 1 for i, (tk, _) in enumerate(eligible_conv)}

            cursor.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today_str,))
            for tk, cr in new_cr.items():
                cursor.execute(
                    'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                    (cr, today_str, tk)
                )

            # 6) part2_rank 재계산
            tickers = list(new_cr.keys())
            wgap_map = dr._compute_w_gap_map(cursor, today_str, tickers)
            sorted_w = sorted(tickers, key=lambda t: wgap_map.get(t, 0), reverse=True)
            top30 = sorted_w[:30]

            cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today_str,))
            for rk, tk in enumerate(top30, 1):
                cursor.execute(
                    'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                    (rk, today_str, tk)
                )

            conn.commit()

        conn.close()
    finally:
        dr.DB_PATH = original_path


# ─────────────────────────────────────────────────────────
# 결과 비교
# ─────────────────────────────────────────────────────────

def show_mu_history(db_path, label):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT date, composite_rank, part2_rank, score, adj_score, adj_gap
        FROM ntm_screening WHERE ticker='MU' AND date >= '2026-04-22'
        ORDER BY date
    ''').fetchall()
    conn.close()
    print(f'\n=== MU [{label}] ===')
    print(f"{'date':<12} {'cr':<5} {'p2':<5} {'score':<10} {'adj_score':<11} {'adj_gap':<10}")
    for r in rows:
        d, cr, p2, sc, ascn, ag = r
        print(f"{d:<12} {cr or '-':<5} {p2 or '-':<5} "
              f"{sc:<10.2f} {ascn:<11.2f} {ag:<10.3f}")


def show_top10(db_path, label, today='2026-04-28'):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT composite_rank, ticker, adj_gap, score, adj_score
        FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ORDER BY composite_rank LIMIT 15
    ''', (today,)).fetchall()
    conn.close()
    print(f'\n=== {label} {today} cr 1~15 ===')
    print(f"{'cr':<4} {'ticker':<8} {'adj_gap':<10} {'score':<8} {'adj_score':<10}")
    for r in rows:
        cr, tk, ag, sc, ascn = r
        marker = ' ←' if tk == 'MU' else ''
        print(f"{cr:<4} {tk:<8} {ag:<10.3f} {sc:<8.2f} {ascn:<10.2f}{marker}")


def get_top3_churn(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT date, ticker FROM ntm_screening
        WHERE part2_rank IS NOT NULL AND part2_rank <= 3
        ORDER BY date, part2_rank
    ''').fetchall()
    conn.close()
    by_date = defaultdict(set)
    for d, tk in rows:
        by_date[d].add(tk)
    dates = sorted(by_date.keys())
    if len(dates) < 2:
        return 0
    changes = 0
    for i in range(1, len(dates)):
        diff = by_date[dates[i]].symmetric_difference(by_date[dates[i-1]])
        changes += len(diff)
    return changes


def get_cr_volatility(db_path, top_n=20):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT date, ticker, composite_rank FROM ntm_screening
        WHERE composite_rank IS NOT NULL ORDER BY date
    ''').fetchall()
    conn.close()
    by_date = defaultdict(dict)
    for d, tk, cr in rows:
        by_date[d][tk] = cr
    dates = sorted(by_date.keys())
    if len(dates) < 2:
        return None
    last = by_date[dates[-1]]
    top_tickers = sorted(last.keys(), key=lambda t: last[t])[:top_n]
    deltas = []
    for i in range(1, len(dates)):
        for tk in top_tickers:
            cr_p = by_date[dates[i-1]].get(tk)
            cr_c = by_date[dates[i]].get(tk)
            if cr_p is not None and cr_c is not None:
                deltas.append(abs(cr_c - cr_p))
    if not deltas:
        return None
    return sum(deltas) / len(deltas), max(deltas), len(deltas)


# ─────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────

def main():
    print('=' * 70)
    print('DB-only Segment Fix BT (yfinance 0회 호출)')
    print('=' * 70)

    test_dbs = {
        'baseline': 'eps_test_db_baseline.db',
        'gamma': 'eps_test_db_gamma.db',
        'gamma2': 'eps_test_db_gamma2.db',
    }
    fns = {
        'baseline': calc_baseline,
        'gamma': calc_gamma,
        'gamma2': calc_gamma2,
    }

    for name, db in test_dbs.items():
        if os.path.exists(db):
            os.remove(db)
        shutil.copy(DB_ORIGINAL, db)
        print(f'\n[{name}] DB 복사 → {db} ... 재계산 중')
        regenerate_db_only(db, fns[name], label=name)
        print(f'    완료')

    # MU 4/22~4/28 변화
    print()
    print('=' * 70)
    print('MU 4/22~4/28 (DB-only fwd_pe_chg 재계산 후)')
    print('=' * 70)
    show_mu_history('eps_momentum_data.db', 'production DB (원본)')
    for name, db in test_dbs.items():
        show_mu_history(db, f'BT {name}')

    # 4/28 cr 1~15 비교
    print()
    print('=' * 70)
    print('4/28 cr 1~15 비교 (MU 위치 확인)')
    print('=' * 70)
    for name, db in test_dbs.items():
        show_top10(db, f'BT {name}')

    # 안정성 메트릭
    print()
    print('=' * 70)
    print('안정성 메트릭 비교')
    print('=' * 70)
    print(f"{'Variant':<12} {'mean |Δcr|':<14} {'max |Δcr|':<12} {'Top3 churn':<12}")
    for name, db in test_dbs.items():
        v = get_cr_volatility(db)
        c = get_top3_churn(db)
        if v:
            print(f"{name:<12} {v[0]:<14.2f} {v[1]:<12} {c:<12}")


if __name__ == '__main__':
    main()
