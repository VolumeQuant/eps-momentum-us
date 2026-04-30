"""정확한 백테스트 v3 - DB eligible pool 그대로 + conviction monkey-patch

핵심 설계:
  1. DB의 composite_rank IS NOT NULL = eligible pool (그대로 사용)
  2. _apply_conviction만 monkey-patch
  3. _compute_w_gap_map은 daily_runner의 진짜 함수 직접 호출
  4. 필터 재적용 없음, industry 매핑 없음, cache 시점 문제 없음

표본 검증 목표: base conviction 재계산 = DB 100% 일치
"""
import sqlite3
import shutil
import os
import sys
from collections import defaultdict

DB_ORIGINAL = 'eps_momentum_data.db'

sys.path.insert(0, '.')
import daily_runner as dr


# ─────────────────────────────────────────────────────────
# Conviction 함수 3종
# ─────────────────────────────────────────────────────────

def conv_none(adj_gap, rev_up=None, num_analysts=None, ntm_current=None, ntm_90d=None,
              rev_growth=None):
    """No conviction — raw adj_gap"""
    if adj_gap is None:
        return None
    return adj_gap


def conv_base(adj_gap, rev_up=None, num_analysts=None, ntm_current=None, ntm_90d=None,
              rev_growth=None):
    """현재 v75+ conviction — daily_runner._apply_conviction과 동일.

    v75: rev_growth >= 30%면 +0.3 add 보너스
    """
    if adj_gap is None:
        return None
    ratio = 0
    if num_analysts and num_analysts > 0 and rev_up is not None:
        ratio = rev_up / num_analysts
    eps_floor = 0
    if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    base_conviction = max(ratio, eps_floor)
    rev_bonus = 0.0
    if rev_growth is not None and rev_growth >= 0.30:
        rev_bonus = 0.3
    conviction = base_conviction + rev_bonus
    return adj_gap * (1 + conviction)


def conv_strong(adj_gap, rev_up=None, num_analysts=None, ntm_current=None, ntm_90d=None,
                rev_growth=None):
    """Strong conviction (1~2.5x) — 이중확증 시 tail bonus"""
    if adj_gap is None:
        return None
    ratio = 0
    if num_analysts and num_analysts > 0 and rev_up is not None:
        ratio = rev_up / num_analysts
    eps_floor = 0
    if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 2.0)
    base = max(ratio, min(eps_floor, 1.0))
    tail_bonus = 0
    if ratio >= 0.5 and eps_floor >= 1.0:
        tail_bonus = min((eps_floor - 1.0) * 0.5, 0.5)
    conviction = base + tail_bonus
    return adj_gap * (1 + conviction)


# ─────────────────────────────────────────────────────────
# 정확한 part2_rank 재생성
# ─────────────────────────────────────────────────────────

def regenerate_part2_with_conviction(test_db_path, conviction_fn):
    """
    test_db에 conviction_fn으로 part2_rank 재계산.

    필터 재적용 없음 — DB의 composite_rank IS NOT NULL을 eligible pool로 사용.
    _apply_conviction만 monkey-patch → _compute_w_gap_map이 자동으로 새 함수 사용.
    """
    original_path = dr.DB_PATH
    original_conv = dr._apply_conviction
    dr.DB_PATH = test_db_path
    dr._apply_conviction = conviction_fn

    try:
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()

        # 모든 날짜 (composite_rank가 있는 날짜만)
        dates = [r[0] for r in cursor.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today_str in dates:
            # 1) 그날의 eligible pool 가져오기 (composite_rank가 있는 종목)
            rows = cursor.execute('''
                SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today_str,)).fetchall()

            if not rows:
                continue

            # 2) 새 conviction 함수로 conv_gap 계산
            eligible = []
            for r in rows:
                tk, ag, ru, na, nc, n90 = r
                cg = conviction_fn(ag, ru, na, nc, n90)
                if cg is not None:
                    eligible.append((tk, cg))

            if not eligible:
                continue

            # 3) conv_gap ascending sort → 새 composite_rank
            #    daily_runner와 동일한 정렬: pandas sort_values는 stable
            #    Python sorted도 stable이라 동일 순서
            eligible.sort(key=lambda x: x[1])
            new_composite_ranks = {tk: i + 1 for i, (tk, _) in enumerate(eligible)}

            # 4) DB에 새 composite_rank 저장
            #    _compute_w_gap_map이 cursor에서 composite_rank를 다시 읽으므로
            #    저장이 필요함
            cursor.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?',
                           (today_str,))
            for tk, cr in new_composite_ranks.items():
                cursor.execute(
                    'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                    (cr, today_str, tk)
                )

            # 5) _compute_w_gap_map 호출 (monkey-patched conviction 사용)
            eligible_tickers = list(new_composite_ranks.keys())
            wgap_map = dr._compute_w_gap_map(cursor, today_str, eligible_tickers)

            # 6) w_gap descending sort → part2_rank Top 30
            #    daily_runner와 동일: sorted reverse=True, stable sort
            sorted_by_wgap = sorted(eligible_tickers,
                                     key=lambda tk: wgap_map.get(tk, 0), reverse=True)
            top30 = sorted_by_wgap[:30]

            # 7) part2_rank 저장
            cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?',
                           (today_str,))
            for rank, tk in enumerate(top30, 1):
                cursor.execute(
                    'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                    (rank, today_str, tk)
                )

            conn.commit()

        conn.close()
    finally:
        dr.DB_PATH = original_path
        dr._apply_conviction = original_conv


# ─────────────────────────────────────────────────────────
# 검증
# ─────────────────────────────────────────────────────────

def verify_against_original(test_db_path, original_db_path):
    """두 DB의 part2_rank 일치 검증"""
    conn1 = sqlite3.connect(test_db_path)
    conn2 = sqlite3.connect(original_db_path)
    c1, c2 = conn1.cursor(), conn2.cursor()

    dates = [r[0] for r in c1.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    matches = {'top3': 0, 'top5': 0, 'top12': 0, 'top30': 0}
    diffs = []

    for d in dates:
        rows1 = c1.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank',
            (d,)
        ).fetchall()
        rows2 = c2.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank',
            (d,)
        ).fetchall()
        t1 = [r[0] for r in rows1]
        t2 = [r[0] for r in rows2]

        if t1[:3] == t2[:3]:
            matches['top3'] += 1
        if t1[:5] == t2[:5]:
            matches['top5'] += 1
        if t1[:12] == t2[:12]:
            matches['top12'] += 1
        if t1 == t2:
            matches['top30'] += 1

        if t1[:5] != t2[:5]:
            diffs.append((d, t1[:5], t2[:5]))

    conn1.close()
    conn2.close()

    n = len(dates)
    print(f"=== 정확성 검증 ({n}일) ===")
    print(f"Top 3  일치: {matches['top3']:>3d}/{n} ({matches['top3']/n*100:>3.0f}%)")
    print(f"Top 5  일치: {matches['top5']:>3d}/{n} ({matches['top5']/n*100:>3.0f}%)")
    print(f"Top 12 일치: {matches['top12']:>3d}/{n} ({matches['top12']/n*100:>3.0f}%)")
    print(f"Top 30 일치: {matches['top30']:>3d}/{n} ({matches['top30']/n*100:>3.0f}%)")

    if diffs:
        print(f"\n첫 5개 차이 (Top 5):")
        for d, t1, t2 in diffs[:5]:
            print(f"  {d}:")
            print(f"    regen:    {t1}")
            print(f"    original: {t2}")

    return matches


def main():
    print("=" * 70)
    print("S1: 정확한 sim 구현 + 표본 검증")
    print("=" * 70)

    test_db = 'eps_test.db'
    if os.path.exists(test_db):
        os.remove(test_db)
    shutil.copy(DB_ORIGINAL, test_db)
    print(f"\n[1] DB 복사본 생성: {test_db}")

    print("\n[2] base conviction (현재)으로 part2_rank 재생성...")
    regenerate_part2_with_conviction(test_db, conv_base)
    print("    완료")

    print("\n[3] 표본 검증: base 재계산 vs DB 원본")
    matches = verify_against_original(test_db, DB_ORIGINAL)


if __name__ == '__main__':
    main()
