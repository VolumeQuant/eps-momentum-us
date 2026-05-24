"""v83 마이그레이션 — C2 boost rank +3 + 80/20 비중

전제:
- daily_runner.py 변경 적용 후 실행
- backup: eps_momentum_data.db.bak_pre_v83 (별도 백업 완료)

변경 사항:
- save_part2_ranks 정렬에 _apply_c2_boost_rerank 적용
- _build_score_100_map / _w_gap 도 동일 rerank 적용

마이그레이션:
- 65일 part2_rank 재계산 (v83 C2 boost 반영)
- composite_rank, w_score 등은 변경 없음 (점수 계산 자체는 동일, 정렬만 reorder)
- 오래된 날부터 순차 적용 (_compute_w_gap_map의 p2_by_date 정합성)
"""
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import daily_runner as dr

ROOT = Path(__file__).parent.parent
DB_TARGET = ROOT / 'eps_momentum_data.db'


def main():
    print('=' * 80)
    print('v83 마이그레이션 — C2 boost rank +3 적용')
    print(f'DB: {DB_TARGET}')
    print('=' * 80)
    if not DB_TARGET.exists():
        print(f'❌ DB 없음: {DB_TARGET}')
        return
    print('backup: eps_momentum_data.db.bak_pre_v83 (별도 완료 가정)')

    dr.DB_PATH = str(DB_TARGET)
    conn = sqlite3.connect(DB_TARGET)
    cur = conn.cursor()

    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    print(f'\n대상 일자: {len(dates)}일 ({dates[0]} ~ {dates[-1]})')

    changed_days = 0
    total_changes = 0
    samples = []
    for today in dates:
        tickers = [r[0] for r in cur.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (today,)
        ).fetchall()]
        if not tickers:
            continue
        wmap = dr._compute_w_gap_map(cur, today, tickers)
        old_p2 = {r[0]: r[1] for r in cur.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
            (today,)
        ).fetchall()}
        sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        # v83 C2 boost rerank
        sorted_v83 = dr._apply_c2_boost_rerank(cur, today, sorted_w)
        top30 = sorted_v83[:30]
        new_p2 = {tk: rk for rk, tk in enumerate(top30, 1)}
        # 변경 감지
        all_tks = set(old_p2.keys()) | set(new_p2.keys())
        day_changes = sum(1 for tk in all_tks if old_p2.get(tk) != new_p2.get(tk))
        if day_changes > 0:
            changed_days += 1
            total_changes += day_changes
            if day_changes >= 5 and len(samples) < 10:
                # Top 5 비교 샘플 저장
                top5_old = sorted([(r, tk) for tk, r in old_p2.items() if r <= 5])
                top5_new = sorted([(r, tk) for tk, r in new_p2.items() if r <= 5])
                samples.append((today, day_changes, top5_old, top5_new))
        # 적용
        cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
        for tk, rk in new_p2.items():
            cur.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rk, today, tk)
            )
        conn.commit()

    print(f'\n[결과] {changed_days}/{len(dates)}일 part2_rank 변경, 총 {total_changes}건')
    if samples:
        print(f'\n[샘플 — Top 5 변경]')
        for d, n, old, new in samples:
            print(f'  {d} ({n}건 변경)')
            print(f'    old: {old}')
            print(f'    new: {new}')

    # 검증
    cur.execute('SELECT MIN(part2_rank), MAX(part2_rank), COUNT(*) FROM ntm_screening WHERE part2_rank IS NOT NULL')
    mn, mx, cnt = cur.fetchone()
    print(f'\n[검증] part2_rank 범위: {mn}~{mx}, 총 {cnt}건')
    cur.execute(
        '''SELECT date, COUNT(*) FROM ntm_screening WHERE part2_rank IS NOT NULL
        GROUP BY date HAVING COUNT(*) != 30 ORDER BY date'''
    )
    issues = cur.fetchall()
    if issues:
        print(f'  ⚠️ 일자별 30개 아닌 경우 {len(issues)}건')
        for d, c in issues[:5]:
            print(f'    {d}: {c}건')
    else:
        print(f'  ✅ 모든 일자 30개 정상')

    conn.close()
    print('\n완료.')


if __name__ == '__main__':
    main()
