"""정확한 백테스트 v2 - DataFrame 기반으로 daily_runner 함수 직접 사용

핵심 개선:
  - DB 데이터 → pandas DataFrame (필요 컬럼 모두 추가)
  - daily_runner.get_part2_candidates() 직접 호출 (필터 100% 동일)
  - daily_runner._compute_w_gap_map() 직접 호출
  - conviction 변형은 monkey-patch로 처리
"""
import sqlite3
import shutil
import os
import sys
import json
import pandas as pd
import numpy as np
from collections import defaultdict

DB_ORIGINAL = 'eps_momentum_data.db'

sys.path.insert(0, '.')
import daily_runner as dr

# ticker → industry 매핑 (cache에서 로드)
with open('ticker_info_cache.json', encoding='utf-8') as f:
    TICKER_CACHE = json.load(f)


def conv_none(adj_gap, rev_up=None, num_analysts=None, ntm_current=None, ntm_90d=None):
    if adj_gap is None:
        return None
    return adj_gap


def conv_base(adj_gap, rev_up=None, num_analysts=None, ntm_current=None, ntm_90d=None):
    if adj_gap is None:
        return None
    ratio = 0
    if num_analysts and num_analysts > 0 and rev_up is not None:
        ratio = rev_up / num_analysts
    eps_floor = 0
    if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    conviction = max(ratio, eps_floor)
    return adj_gap * (1 + conviction)


def conv_strong(adj_gap, rev_up=None, num_analysts=None, ntm_current=None, ntm_90d=None):
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


def load_date_as_dataframe(cursor, date_str):
    """DB의 date_str 데이터를 daily_runner가 받는 results_df 형태로 변환"""
    rows = cursor.execute('''
        SELECT ticker, score, adj_score, adj_gap,
               ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
               price, ma60, ma120,
               rev_up30, rev_down30, num_analysts, rev_growth,
               operating_margin, gross_margin, is_turnaround
        FROM ntm_screening WHERE date=?
    ''', (date_str,)).fetchall()

    records = []
    for r in rows:
        (tk, score, adj_score, adj_gap, nc, n7, n30, n60, n90,
         price, ma60, ma120, ru, rd, na, rg, om, gm, is_to) = r

        # fwd_pe
        fwd_pe = (price / nc) if (price and nc and nc > 0) else None

        # eps_change_90d
        if n90 and abs(n90) > 0.01:
            eps_change_90d = (nc - n90) / abs(n90) * 100
        else:
            eps_change_90d = None

        # segments
        nc_, n7_, n30_, n60_, n90_ = (float(x) if x else 0 for x in [nc, n7, n30, n60, n90])
        segs = []
        for a, b in [(nc_, n7_), (n7_, n30_), (n30_, n60_), (n60_, n90_)]:
            if b and abs(b) > 0.01:
                segs.append((a - b) / abs(b) * 100)
            else:
                segs.append(0)
        segs = [max(-100.0, min(100.0, s)) for s in segs]
        seg1, seg2, seg3, seg4 = segs
        # direction
        direction = (seg1 - seg4) if (seg1 is not None and seg4 is not None) else 0

        # industry from cache (한국어, daily_runner와 동일)
        industry = TICKER_CACHE.get(tk, {}).get('industry', '기타')

        records.append({
            'ticker': tk,
            'score': score,
            'adj_score': adj_score,
            'adj_gap': adj_gap,
            'direction': direction,
            'seg1': seg1, 'seg2': seg2, 'seg3': seg3, 'seg4': seg4,
            'ntm_current': nc, 'ntm_7d': n7, 'ntm_30d': n30,
            'ntm_60d': n60, 'ntm_90d': n90,
            'eps_change_90d': eps_change_90d,
            'fwd_pe': fwd_pe,
            'price': price, 'ma60': ma60, 'ma120': ma120,
            'rev_up30': ru, 'rev_down30': rd,
            'num_analysts': na, 'rev_growth': rg,
            'operating_margin': om, 'gross_margin': gm,
            'industry': industry,
            'is_turnaround': is_to,
        })

    return pd.DataFrame(records)


def regenerate_part2_ranks(test_db_path, conviction_fn):
    """test_db에 conviction_fn으로 part2_rank 재생성"""
    original_path = dr.DB_PATH
    original_conv = dr._apply_conviction
    dr.DB_PATH = test_db_path
    dr._apply_conviction = conviction_fn

    try:
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()

        dates = [r[0] for r in cursor.execute(
            'SELECT DISTINCT date FROM ntm_screening ORDER BY date'
        ).fetchall()]

        for di, today_str in enumerate(dates):
            df = load_date_as_dataframe(cursor, today_str)
            if df.empty:
                continue

            # daily_runner.save_part2_ranks 호출
            # 단, 이 함수는 cursor를 닫지 않고 사용. 우리도 같은 conn 사용해야 함
            # save_part2_ranks 내부에서 자기 conn을 만드니까 OK
            try:
                dr.save_part2_ranks(df, today_str)
            except Exception as e:
                print(f"  {today_str} 에러: {e}")

        conn.close()
    finally:
        dr.DB_PATH = original_path
        dr._apply_conviction = original_conv


def verify_sim_accuracy(test_db_path, original_db_path):
    """두 DB의 part2_rank 일치 검증"""
    conn1 = sqlite3.connect(test_db_path)
    conn2 = sqlite3.connect(original_db_path)
    c1, c2 = conn1.cursor(), conn2.cursor()

    dates = [r[0] for r in c1.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    matches_top30 = 0
    matches_top12 = 0
    matches_top5 = 0
    matches_top3 = 0
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
        if t1 == t2:
            matches_top30 += 1
        if t1[:12] == t2[:12]:
            matches_top12 += 1
        if t1[:5] == t2[:5]:
            matches_top5 += 1
        if t1[:3] == t2[:3]:
            matches_top3 += 1
        if t1[:5] != t2[:5]:
            diffs.append((d, t1[:5], t2[:5]))

    conn1.close()
    conn2.close()

    print(f"=== Sim 정확성 검증 ({len(dates)}일) ===")
    print(f"Top 3 일치: {matches_top3}/{len(dates)} ({matches_top3/len(dates)*100:.0f}%)")
    print(f"Top 5 일치: {matches_top5}/{len(dates)} ({matches_top5/len(dates)*100:.0f}%)")
    print(f"Top 12 일치: {matches_top12}/{len(dates)} ({matches_top12/len(dates)*100:.0f}%)")
    print(f"Top 30 완전 일치: {matches_top30}/{len(dates)} ({matches_top30/len(dates)*100:.0f}%)")

    if diffs:
        print(f"\n첫 5개 차이 (Top 5):")
        for d, t1, t2 in diffs[:5]:
            print(f"  {d}:")
            print(f"    regen:    {t1}")
            print(f"    original: {t2}")

    return matches_top30 == len(dates), matches_top5, len(dates)


def main():
    print("=" * 70)
    print("Step 0: Sim 정확성 100% 일치 + 표본 검증 (v2)")
    print("=" * 70)

    test_db = 'eps_test.db'
    if os.path.exists(test_db):
        os.remove(test_db)
    shutil.copy(DB_ORIGINAL, test_db)
    print(f"\n[1] DB 복사본 생성: {test_db}")

    print("\n[2] Base conviction (현재)으로 part2_rank 재생성...")
    regenerate_part2_ranks(test_db, conv_base)
    print("    완료")

    print("\n[3] Sample test: 원본 DB와 일치 검증...")
    is_perfect, top5_matches, total = verify_sim_accuracy(test_db, DB_ORIGINAL)

    if is_perfect:
        print("\n[OK] Sim 정확성 100% — 다음 단계 진행")
    elif top5_matches >= total * 0.9:
        print(f"\n[OK] Sim 정확성 충분 (Top 5 {top5_matches}/{total}일 일치) — 진행")
    else:
        print(f"\n[!] Sim 정확성 부족 — 추가 디버깅 필요")


if __name__ == '__main__':
    main()
