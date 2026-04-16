"""
Case 1 보너스 반영 — 전체 기간 part2_rank 재계산
기존 composite_rank, adj_gap 등 원본 데이터 기반으로
w_gap(보너스 포함) 재계산 → part2_rank Top 30 재할당
"""
import sqlite3
import sys
import time
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'eps_momentum_data.db'

# daily_runner에서 필요한 함수 import
from daily_runner import _compute_w_gap_map, _get_recent_dates

def recompute_all():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 모든 날짜 (composite_rank 존재하는)
    cursor.execute('SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date')
    all_dates = [r[0] for r in cursor.fetchall()]
    print(f"[대상] {len(all_dates)}일: {all_dates[0]} ~ {all_dates[-1]}")

    t0 = time.time()
    total_changed = 0

    for i, d in enumerate(all_dates):
        # eligible tickers
        cursor.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (d,)
        )
        tickers = [r[0] for r in cursor.fetchall()]

        if not tickers:
            continue

        # 기존 part2_rank 저장 (비교용)
        cursor.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank',
            (d,)
        )
        old_ranks = {r[0]: r[1] for r in cursor.fetchall()}

        # w_gap 재계산 (Case 1 보너스 포함)
        wgap = _compute_w_gap_map(cursor, d, tickers)

        # Top 30 재할당
        sorted_by_wgap = sorted(tickers, key=lambda tk: wgap.get(tk, 0), reverse=True)
        top30 = sorted_by_wgap[:30]

        # part2_rank 업데이트
        cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (d,))
        new_ranks = {}
        for rank, tk in enumerate(top30, 1):
            cursor.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rank, d, tk)
            )
            new_ranks[tk] = rank

        # 변경 감지
        old_top3 = set(tk for tk, r in old_ranks.items() if r <= 3)
        new_top3 = set(tk for tk, r in new_ranks.items() if r <= 3)
        if old_top3 != new_top3:
            added = new_top3 - old_top3
            removed = old_top3 - new_top3
            total_changed += 1
            print(f"  [{d}] Top 3 변경: -{','.join(removed) if removed else '없음'} +{','.join(added) if added else '없음'}")

    conn.commit()
    conn.close()

    elapsed = time.time() - t0
    print(f"\n[완료] {len(all_dates)}일 재계산, {elapsed:.1f}초")
    print(f"  Top 3 변경된 날: {total_changed}/{len(all_dates)}일")

if __name__ == '__main__':
    # 안전장치: 백업 확인
    print("⚠️ DB part2_rank를 전체 재계산합니다.")
    print("   백업: eps_momentum_data.db → eps_momentum_data.db.bak_pre_case1")

    import shutil
    shutil.copy2(DB_PATH, DB_PATH + '.bak_pre_case1')
    print("   백업 완료.\n")

    recompute_all()
