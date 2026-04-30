"""Segment fix BT — γ/γ'' 비교 (lookback shift 노이즈 차단)

핵심 설계:
1. DB 복사 → segment_fn 적용 → adj_gap 재계산 → composite_rank/part2_rank 재정렬
2. fwd_pe_chg는 기존 adj_gap에서 역산해 보존 (이전 BT -20%p 결함 차단)
3. production _apply_conviction + _compute_w_gap_map 그대로 사용
4. 변경되는 건 segment 처리(score/direction/min_seg)만

비교 대상:
- baseline: 현재 production 로직 (control, DB 100% 재현 검증)
- γ: cap 발동 시 dir_factor=0 (가속도 알파 무력화)
- γ'': cap segment 제외 후 partial direction (가속도 알파 부분 살림)

검증 메트릭:
- MU 4/27→4/28 cr/p2 변화
- 전체 종목 일별 cr 변동성
- Top 3 안정성
"""
import sqlite3
import shutil
import os
import sys
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import daily_runner as dr

DB_ORIGINAL = 'eps_momentum_data.db'
SEG_CAP = 100


# ─────────────────────────────────────────────────────────
# Segment 처리 함수들
# ─────────────────────────────────────────────────────────

def fmt_segments(nc, n7, n30, n60, n90):
    """NTM 값들로부터 4 segment 계산 (cap 적용)"""
    if not all(x is not None and x != 0 for x in (n7, n30, n60, n90)):
        return None
    seg1 = max(-SEG_CAP, min(SEG_CAP, (nc - n7) / abs(n7) * 100))
    seg2 = max(-SEG_CAP, min(SEG_CAP, (n7 - n30) / abs(n30) * 100))
    seg3 = max(-SEG_CAP, min(SEG_CAP, (n30 - n60) / abs(n60) * 100))
    seg4 = max(-SEG_CAP, min(SEG_CAP, (n60 - n90) / abs(n90) * 100))
    return seg1, seg2, seg3, seg4


def calc_baseline(seg1, seg2, seg3, seg4):
    """현재 production 로직 (control)"""
    score = seg1 + seg2 + seg3 + seg4
    direction = (seg1 + seg2) / 2 - (seg3 + seg4) / 2
    dir_factor = max(-0.3, min(0.3, direction / 30))
    min_seg = min(seg1, seg2, seg3, seg4)
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    adj_score = score * (1 + dir_factor)
    return score, dir_factor, eps_q, adj_score


def calc_gamma(seg1, seg2, seg3, seg4):
    """γ: cap 발동 시 dir_factor=0 (가속도 알파 차단)"""
    segs = [seg1, seg2, seg3, seg4]
    score = sum(segs)
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
    adj_score = score * (1 + dir_factor)
    return score, dir_factor, eps_q, adj_score


def calc_gamma2(seg1, seg2, seg3, seg4):
    """γ'': cap segment 제외 후 partial direction (가속도 알파 부분 살림)"""
    segs = [seg1, seg2, seg3, seg4]
    score = sum(segs)
    recent_valid = [s for s in (seg1, seg2) if abs(s) < SEG_CAP]
    old_valid = [s for s in (seg3, seg4) if abs(s) < SEG_CAP]
    if recent_valid and old_valid:
        direction = sum(recent_valid) / len(recent_valid) - sum(old_valid) / len(old_valid)
        dir_factor = max(-0.3, min(0.3, direction / 30))
    else:
        dir_factor = 0.0
    valid_all = [s for s in segs if abs(s) < SEG_CAP]
    min_seg = min(valid_all) if valid_all else 0
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    adj_score = score * (1 + dir_factor)
    return score, dir_factor, eps_q, adj_score


def calc_delta(seg1, seg2, seg3, seg4):
    """δ: dir_factor를 adj_gap에서 제거. score/adj_score엔 그대로 적용.

    이유: adj_gap = fwd_pe_chg × (1+dir_factor) × eps_q 구조에서 dir_factor가
    양수 fwd_pe_chg에 곱해질 때 cr 정렬 의도와 거꾸로 작동.
    fix: adj_gap = fwd_pe_chg × eps_q (dir_factor 제거).
    """
    segs = [seg1, seg2, seg3, seg4]
    score = sum(segs)
    direction = (seg1 + seg2) / 2 - (seg3 + seg4) / 2
    dir_factor_display = max(-0.3, min(0.3, direction / 30))  # adj_score 디스플레이용
    min_seg = min(segs)
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    adj_score = score * (1 + dir_factor_display)
    # adj_gap 계산엔 dir_factor=0 사용
    return score, 0.0, eps_q, adj_score


# ─────────────────────────────────────────────────────────
# DB 재계산 (fwd_pe_chg 보존)
# ─────────────────────────────────────────────────────────

def regenerate_with_segment_fix(test_db_path, segment_fn):
    """segment_fn으로 adj_gap 재계산 + composite_rank/part2_rank 재정렬"""
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
                       adj_gap, rev_up30, num_analysts, rev_growth
                FROM ntm_screening
                WHERE date=? AND composite_rank IS NOT NULL
            ''', (today_str,)).fetchall()

            if not rows:
                continue

            new_data = []  # [(tk, score, adj_score, adj_gap, ru, na, nc, n90, rg), ...]
            for r in rows:
                tk, nc, n7, n30, n60, n90, ag_old, ru, na, rg = r
                segs = fmt_segments(nc, n7, n30, n60, n90)
                if segs is None or ag_old is None:
                    continue

                # 1) 기존 dir_factor + eps_q (역산용)
                _, df_old, eq_old, _ = calc_baseline(*segs)
                denom_old = (1 + df_old) * eq_old
                if abs(denom_old) < 1e-6:
                    continue
                fwd_pe_chg = ag_old / denom_old

                # 2) 새 segment 처리 적용
                score_new, df_new, eq_new, adj_score_new = segment_fn(*segs)
                adj_gap_new = fwd_pe_chg * (1 + df_new) * eq_new

                new_data.append((tk, score_new, adj_score_new, adj_gap_new,
                                 ru, na, nc, n90, rg))

            # 3) DB 업데이트 (score/adj_score/adj_gap)
            for tk, sc, ascn, ag, *_ in new_data:
                cursor.execute(
                    'UPDATE ntm_screening SET score=?, adj_score=?, adj_gap=? WHERE date=? AND ticker=?',
                    (sc, ascn, ag, today_str, tk)
                )

            # 4) composite_rank 재정렬 (production conv 사용)
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

            # 5) _compute_w_gap_map → part2_rank
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
# 메트릭 비교
# ─────────────────────────────────────────────────────────

def get_mu_change(db_path, ticker='MU'):
    """MU의 4/27→4/28 cr/p2/score/adj_score/adj_gap 변화"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT date, composite_rank, part2_rank, score, adj_score, adj_gap
        FROM ntm_screening
        WHERE ticker=? AND date IN ('2026-04-27', '2026-04-28')
        ORDER BY date
    ''', (ticker,)).fetchall()
    conn.close()
    return rows


def get_cr_volatility(db_path, ticker_list=None, top_n=20):
    """일별 cr 변동성 (Top N 종목 평균 |Δcr|/day)

    cr이 일관적이면 작은 값 (안정), 흔들리면 큰 값.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT date, ticker, composite_rank
        FROM ntm_screening
        WHERE composite_rank IS NOT NULL
        ORDER BY date
    ''').fetchall()
    conn.close()

    by_date = defaultdict(dict)
    for d, tk, cr in rows:
        by_date[d][tk] = cr

    dates = sorted(by_date.keys())
    if len(dates) < 2:
        return None

    # Top N 종목들 (마지막 날 기준)
    last = by_date[dates[-1]]
    top_tickers = sorted(last.keys(), key=lambda t: last[t])[:top_n]

    deltas = []
    for i in range(1, len(dates)):
        d_prev, d_cur = dates[i-1], dates[i]
        for tk in top_tickers:
            cr_prev = by_date[d_prev].get(tk)
            cr_cur = by_date[d_cur].get(tk)
            if cr_prev is not None and cr_cur is not None:
                deltas.append(abs(cr_cur - cr_prev))

    if not deltas:
        return None
    return {
        'mean_delta': sum(deltas) / len(deltas),
        'max_delta': max(deltas),
        'count': len(deltas),
    }


def get_top3_churn(db_path):
    """일별 Top 3 변경 횟수 (Top 3 진입/이탈)"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT date, ticker
        FROM ntm_screening
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


def compare_top3(db_a, db_b):
    """두 DB의 Top 3 일치율"""
    def get_top3(db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        rows = cur.execute('''
            SELECT date, ticker
            FROM ntm_screening
            WHERE part2_rank IS NOT NULL AND part2_rank <= 3
            ORDER BY date, part2_rank
        ''').fetchall()
        conn.close()
        out = defaultdict(list)
        for d, tk in rows:
            out[d].append(tk)
        return out

    a, b = get_top3(db_a), get_top3(db_b)
    common_dates = sorted(set(a.keys()) & set(b.keys()))
    same = sum(1 for d in common_dates if a[d] == b[d])
    return same, len(common_dates)


# ─────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────

def main():
    print('=' * 70)
    print('Segment Fix BT — γ vs γ'' 비교')
    print('=' * 70)

    test_dbs = {
        'baseline': 'eps_test_baseline.db',
        'gamma': 'eps_test_gamma.db',
        'gamma2': 'eps_test_gamma2.db',
        'delta': 'eps_test_delta.db',
    }

    fns = {
        'baseline': calc_baseline,
        'gamma': calc_gamma,
        'gamma2': calc_gamma2,
        'delta': calc_delta,
    }

    # 1. 각 변형으로 DB 재생성
    for name, db in test_dbs.items():
        if os.path.exists(db):
            os.remove(db)
        shutil.copy(DB_ORIGINAL, db)
        print(f'\n[{name}] DB 복사 → {db}')
        regenerate_with_segment_fix(db, fns[name])
        print(f'    완료')

    # 2. MU 사례 비교 (가장 직접적 검증)
    print()
    print('=' * 70)
    print('MU 4/27 → 4/28 변화 비교')
    print('=' * 70)
    print(f"{'Variant':<12} {'date':<12} {'cr':<5} {'p2':<5} {'score':<10} {'adj_score':<11} {'adj_gap':<10}")
    print('-' * 70)
    for name, db in test_dbs.items():
        rows = get_mu_change(db)
        for r in rows:
            d, cr, p2, sc, ascn, ag = r
            print(f"{name:<12} {d:<12} {cr or 'N/A':<5} {p2 or 'N/A':<5} "
                  f"{sc:<10.2f} {ascn:<11.2f} {ag:<10.3f}")
        print()

    # 3. cr 변동성 비교 (낮을수록 안정)
    print('=' * 70)
    print('일별 cr 변동성 (Top 20 종목, 낮을수록 안정)')
    print('=' * 70)
    print(f"{'Variant':<12} {'mean |Δcr|':<14} {'max |Δcr|':<12} {'samples':<10}")
    print('-' * 50)
    for name, db in test_dbs.items():
        v = get_cr_volatility(db)
        if v:
            print(f"{name:<12} {v['mean_delta']:<14.2f} {v['max_delta']:<12} {v['count']:<10}")

    # 4. Top 3 churn (변경 횟수)
    print()
    print('=' * 70)
    print('Top 3 변경 횟수 (낮을수록 안정)')
    print('=' * 70)
    for name, db in test_dbs.items():
        c = get_top3_churn(db)
        print(f"  {name}: {c}회")

    # 5. baseline vs 변형 Top 3 일치율
    print()
    print('=' * 70)
    print('Baseline vs 변형 Top 3 일치율')
    print('=' * 70)
    for name in ['gamma', 'gamma2', 'delta']:
        same, total = compare_top3(test_dbs['baseline'], test_dbs[name])
        print(f"  baseline vs {name}: {same}/{total} ({same/total*100:.0f}%)")


if __name__ == '__main__':
    main()
