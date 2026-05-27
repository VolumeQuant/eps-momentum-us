"""v83.2 마이그레이션 — C2 boost 제거 (no_boost 복귀)

전제: daily_runner.py에서 _apply_c2_boost_rerank 호출 3곳 + 헬퍼 제거 완료.

변경:
- part2_rank를 순수 w_gap 내림차순 Top 30으로 재계산 (boost rerank 제거)
- composite_rank는 변경 없음 (애초에 boost 없었음)
- 오래된 날부터 순차 (이전 날 part2_rank가 _compute_w_gap_map penalty에 영향)

backup: eps_momentum_data.db.bak_pre_c2gate_20260527 (별도 완료)
근거: gate vs no_boost robust 검증 — gate edge 전부 MU 한 종목. MU 제외 시
  동전던지기(239/500) + M24 음수 → boost 비robust → 제거.
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
    print('v83.2 마이그레이션 — C2 boost 제거 (part2_rank = 순수 w_gap Top 30)')
    print(f'DB: {DB_TARGET}')
    print('=' * 80)
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
        # 순수 w_gap 내림차순 (boost 없음)
        sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        top30 = sorted_w[:30]
        new_p2 = {tk: rk for rk, tk in enumerate(top30, 1)}
        all_tks = set(old_p2.keys()) | set(new_p2.keys())
        day_changes = sum(1 for tk in all_tks if old_p2.get(tk) != new_p2.get(tk))
        if day_changes > 0:
            changed_days += 1
            total_changes += day_changes
            if day_changes >= 2 and len(samples) < 12:
                top5_old = sorted([(r, tk) for tk, r in old_p2.items() if r <= 5])
                top5_new = sorted([(r, tk) for tk, r in new_p2.items() if r <= 5])
                samples.append((today, day_changes, top5_old, top5_new))
        cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
        for tk, rk in new_p2.items():
            cur.execute('UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                        (rk, today, tk))
        conn.commit()

    print(f'\n[결과] {changed_days}/{len(dates)}일 part2_rank 변경, 총 {total_changes}건')
    if samples:
        print('\n[샘플 — Top 5 변경 (old → new)]')
        for d, n, old, new in samples:
            print(f'  {d} ({n}건)')
            print(f'    old: {old}')
            print(f'    new: {new}')

    cur.execute('SELECT MIN(part2_rank), MAX(part2_rank), COUNT(*) FROM ntm_screening WHERE part2_rank IS NOT NULL')
    mn, mx, cnt = cur.fetchone()
    print(f'\n[검증] part2_rank 범위 {mn}~{mx}, 총 {cnt}건')
    issues = cur.execute(
        '''SELECT date, COUNT(*) FROM ntm_screening WHERE part2_rank IS NOT NULL
           GROUP BY date HAVING COUNT(*) != 30 ORDER BY date'''
    ).fetchall()
    if issues:
        print(f'  ⚠️ 30개 아닌 일자 {len(issues)}건: {issues[:5]}')
    else:
        print('  ✅ 모든 일자 30개 정상')
    conn.close()
    print('\n완료.')


if __name__ == '__main__':
    main()
