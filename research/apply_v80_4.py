"""v80.4 (β1 + opt4) DB 재계산.

방법:
  1. /tmp/db_pre_gamma.db (baseline) → eps_momentum_data.db 복사 (γ 롤백)
  2. β1 + opt4 적용해 모든 일자 재계산
  3. composite_rank/part2_rank 재정렬
"""
import sqlite3
import shutil
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import daily_runner as dr

ROOT = Path(__file__).parent.parent
DB_BASELINE = Path('/tmp/db_pre_gamma.db')
DB_TARGET = ROOT / 'eps_momentum_data.db'
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


def calc_v80_4(segs, fwd_pe_chg):
    """β1 + opt4: cap 시 +0.3 보너스, 정상 영역 C4 sign flip"""
    score = sum(segs)
    cap_hit = any(abs(s) >= SEG_CAP for s in segs)
    if cap_hit:
        # β1: cap 발동 = 강한 신호 = +0.3 보너스
        df = +0.3
        valid = [s for s in segs if abs(s) < SEG_CAP]
        min_seg = min(valid) if valid else 0
        # adj_score용 direction = +9 (= 0.3 만들기 위함)
        direction = 9.0
    else:
        direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
        df_raw = max(-0.3, min(0.3, direction / 30))
        # opt4: C4 (fwd>0 + dir<0) sign flip
        if fwd_pe_chg is not None and fwd_pe_chg > 0 and direction < 0:
            df = -df_raw
        else:
            df = df_raw
        min_seg = min(segs)
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    adj_score = score * (1 + df)
    return score, df, eq_q_to_eps_q(eps_q), adj_score, direction


def eq_q_to_eps_q(eps_q):
    return eps_q


def calc_baseline(segs):
    """baseline (γ 적용 전, fwd_pe_chg 역산용)"""
    score = sum(segs)
    direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
    df = max(-0.3, min(0.3, direction / 30))
    min_seg = min(segs)
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return score, df, eps_q, score * (1 + df)


def main():
    print('=' * 80)
    print('v80.4 (β1 + opt4) DB 재계산')
    print('=' * 80)

    # 1) baseline DB → target 복사
    if not DB_BASELINE.exists():
        print(f'❌ baseline DB 없음: {DB_BASELINE}')
        return
    print(f'\n[1] baseline DB 복원: {DB_BASELINE} → {DB_TARGET}')
    shutil.copy(DB_BASELINE, DB_TARGET)

    # 2) 재계산
    print('\n[2] β1 + opt4 적용 재계산...')
    dr.DB_PATH = str(DB_TARGET)
    conn = sqlite3.connect(DB_TARGET)
    cur = conn.cursor()

    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    print(f'  {len(dates)}일 처리...')

    for today in dates:
        rows = cur.execute('''
            SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   adj_gap, rev_up30, num_analysts, rev_growth
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (today,)).fetchall()
        if not rows:
            continue

        new_data = []
        for r in rows:
            tk, nc, n7, n30, n60, n90, ag_old, ru, na, rg = r
            segs = fmt_segments(nc, n7, n30, n60, n90)
            if segs is None or ag_old is None:
                continue
            # baseline 가정으로 fwd_pe_chg 역산
            _, df_base, eq_base, _ = calc_baseline(segs)
            denom = (1 + df_base) * eq_base
            if abs(denom) < 1e-6:
                continue
            fwd_pe_chg = ag_old / denom

            # v80.4 적용
            score_n, df_n, eq_n, asc_n, dir_n = calc_v80_4(segs, fwd_pe_chg)
            ag_n = fwd_pe_chg * (1 + df_n) * eq_n
            new_data.append((tk, score_n, asc_n, ag_n, ru, na, nc, n90, rg))

        for tk, sc, asc, ag, *_ in new_data:
            cur.execute(
                'UPDATE ntm_screening SET score=?, adj_score=?, adj_gap=? WHERE date=? AND ticker=?',
                (sc, asc, ag, today, tk)
            )

        # composite_rank 재정렬 (production conviction)
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

        # part2_rank w_gap 기반
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
    print('  완료.')

    # 3) 검증 — MU 4/27, 4/28
    print('\n[3] MU 변화 검증')
    conn = sqlite3.connect(DB_TARGET)
    cur = conn.cursor()
    for d in ['2026-04-27', '2026-04-28']:
        row = cur.execute(
            'SELECT part2_rank, composite_rank, score, adj_score, adj_gap FROM ntm_screening WHERE ticker=\"MU\" AND date=?',
            (d,)
        ).fetchone()
        if row:
            p2, cr, sc, asc, ag = row
            print(f'  {d}: p2={p2}, cr={cr}, score={sc:.2f}, adj_score={asc:.2f}, adj_gap={ag:+.2f}')
    conn.close()
    print('\n✓ DB 재계산 완료')


if __name__ == '__main__':
    main()
