"""v80.6 마이그레이션 — β1 제거 (cap 발동 시 dir=0, γ 복원)

전제:
  - 이전 (v80.5b): β1 (cap → dir=+0.3) + opt4 + no_case1
  - 신규 (v80.6):  γ  (cap → dir=0)    + opt4 + no_case1

영향 범위:
  - cap 발동 종목 (segment 중 1개 이상이 ±100% 이상): adj_score, adj_gap 재계산 (× 1/1.3)
    - score 자체는 segment cap 동일하므로 불변
    - adj_score = score × (1 + df), df: 0.3 → 0
    - adj_gap = fwd_pe_chg × (1 + df) × eps_q, df: 0.3 → 0
  - non-cap 종목: 변경 없음
  - composite_rank, part2_rank: 모든 일자 재정렬 (cap 발동 종목 위치 변동)

방법:
  1. 백업 (bak_pre_v80_6)
  2. 모든 일자 cap 발동 종목 adj_score/adj_gap 재계산 (÷ 1.3)
  3. composite_rank 재정렬 (conviction adj_gap 오름차순)
  4. part2_rank 재정렬 (w_gap 내림차순 Top 30)
  5. 검증
"""
import sqlite3
import shutil
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import daily_runner as dr

ROOT = Path(__file__).parent.parent
DB_TARGET = ROOT / 'eps_momentum_data.db'
DB_BACKUP = ROOT / 'eps_momentum_data.bak_pre_v80_6.db'
SEG_CAP = 100


def is_cap_hit(nc, n7, n30, n60, n90):
    """cap 발동 여부 + segments"""
    if not all(x is not None and x != 0 for x in (n7, n30, n60, n90)):
        return False, None
    segs = (
        max(-SEG_CAP, min(SEG_CAP, (nc - n7) / abs(n7) * 100)),
        max(-SEG_CAP, min(SEG_CAP, (n7 - n30) / abs(n30) * 100)),
        max(-SEG_CAP, min(SEG_CAP, (n30 - n60) / abs(n60) * 100)),
        max(-SEG_CAP, min(SEG_CAP, (n60 - n90) / abs(n90) * 100)),
    )
    return any(abs(s) >= SEG_CAP for s in segs), segs


def main():
    print('=' * 80)
    print('v80.6 마이그레이션 — β1 제거 (cap 발동 시 dir=0, γ 복원)')
    print(f'DB: {DB_TARGET}')
    print('=' * 80)

    if not DB_TARGET.exists():
        print(f'❌ DB 없음: {DB_TARGET}')
        return

    print(f'\n[1] 백업: {DB_BACKUP}')
    shutil.copy(DB_TARGET, DB_BACKUP)

    dr.DB_PATH = str(DB_TARGET)
    conn = sqlite3.connect(DB_TARGET)
    cur = conn.cursor()

    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    print(f'\n[2] 대상 일자: {len(dates)}일')

    cap_total = 0
    cap_dates = 0
    cr_changed = 0
    p2_changed = 0

    for today in dates:
        rows = cur.execute('''
            SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   score, adj_score, adj_gap, rev_up30, num_analysts, rev_growth
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (today,)).fetchall()
        if not rows:
            continue

        # ── 2.1 cap 발동 종목 adj_score/adj_gap 재계산 ──
        cap_today = 0
        new_data = {}  # ticker -> (adj_score, adj_gap, ru, na, nc, n90, rg)
        for r in rows:
            tk, nc, n7, n30, n60, n90, sc, asc, ag, ru, na, rg = r
            cap_hit, segs = is_cap_hit(nc, n7, n30, n60, n90)
            if cap_hit and asc is not None and ag is not None:
                # β1 적용 상태: asc = score × 1.3, ag = fwd_pe_chg × 1.3 × eps_q
                # γ 적용:        asc = score × 1.0, ag = fwd_pe_chg × 1.0 × eps_q
                # 단순 ÷ 1.3
                new_asc = asc / 1.3
                new_ag = ag / 1.3
                cur.execute(
                    'UPDATE ntm_screening SET adj_score=?, adj_gap=? WHERE date=? AND ticker=?',
                    (new_asc, new_ag, today, tk)
                )
                new_data[tk] = (new_asc, new_ag, ru, na, nc, n90, rg)
                cap_today += 1
            else:
                new_data[tk] = (asc, ag, ru, na, nc, n90, rg)

        cap_total += cap_today
        if cap_today > 0:
            cap_dates += 1

        # ── 2.2 composite_rank 재정렬 (conviction 기반 오름차순) ──
        elig_conv = []
        for tk, (asc, ag, ru, na, nc, n90, rg) in new_data.items():
            if ag is None:
                continue
            cg = dr._apply_conviction(ag, ru, na, nc, n90, rev_growth=rg)
            if cg is not None:
                elig_conv.append((tk, cg))
        elig_conv.sort(key=lambda x: x[1])
        new_cr = {tk: i + 1 for i, (tk, _) in enumerate(elig_conv)}

        old_cr = {r[0]: r[1] for r in cur.execute(
            'SELECT ticker, composite_rank FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (today,)
        ).fetchall()}
        if old_cr != new_cr:
            cr_changed += 1

        cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
        for tk, cr in new_cr.items():
            cur.execute(
                'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                (cr, today, tk)
            )

        # ── 2.3 part2_rank 재정렬 (w_gap 내림차순 Top 30) ──
        tickers = list(new_cr.keys())
        wmap = dr._compute_w_gap_map(cur, today, tickers)
        sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        top30 = sorted_w[:30]
        new_p2 = {tk: rk for rk, tk in enumerate(top30, 1)}

        old_p2 = {r[0]: r[1] for r in cur.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
            (today,)
        ).fetchall()}
        if old_p2 != new_p2:
            p2_changed += 1

        cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
        for tk, rk in new_p2.items():
            cur.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rk, today, tk)
            )
        conn.commit()

    print(f'  cap 발동 종목 누적: {cap_total}건 (영향 일자 {cap_dates}/{len(dates)}일)')
    print(f'  composite_rank 변경: {cr_changed}/{len(dates)}일')
    print(f'  part2_rank 변경: {p2_changed}/{len(dates)}일')

    # ── 검증 ──
    print('\n[3] 검증')
    cur.execute('SELECT MIN(part2_rank), MAX(part2_rank), COUNT(*) FROM ntm_screening WHERE part2_rank IS NOT NULL')
    mn, mx, cnt = cur.fetchone()
    print(f'  part2_rank 범위: {mn}~{mx}, 총 {cnt}건')

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
        print(f'  ✓ 모든 일자 30종목')

    # 최근 5일 cap 발동 종목 확인
    print('\n[4] 최근 5일 cap 발동 주요 종목 (β1 제거 영향)')
    last_dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date DESC LIMIT 5'
    ).fetchall()][::-1]
    # MU, SNDK는 어닝 직후 cap 발동
    for tk in ['MU', 'SNDK', 'TER', 'VIRT', 'LRCX']:
        print(f'  {tk}:')
        for d in last_dates:
            row = cur.execute(
                'SELECT part2_rank, composite_rank, adj_score, adj_gap, '
                'ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d '
                'FROM ntm_screening WHERE ticker=? AND date=?',
                (tk, d)
            ).fetchone()
            if row:
                p2, cr, asc, ag, nc, n7, n30, n60, n90 = row
                cap_hit, _ = is_cap_hit(nc, n7, n30, n60, n90)
                marker = ' [CAP]' if cap_hit else ''
                print(f'    {d}: p2={p2 if p2 else "-":>3}, cr={cr if cr else "-":>3}, '
                      f'asc={asc:.2f}, ag={ag:+.2f}{marker}')

    conn.close()
    print('\n✓ 마이그레이션 완료')


if __name__ == '__main__':
    main()
