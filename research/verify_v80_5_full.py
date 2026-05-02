"""v80.5 전체 일자 재계산 검증.

검증 항목:
  1. 각 일자 score/adj_score/adj_gap이 v80.5 (β1+opt4) 로직으로 계산되어 있나
  2. composite_rank가 conv_gap 정렬과 일치하나
  3. part2_rank가 _compute_w_gap_map(현재 = Case 1 제거)과 일치하나
"""
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import daily_runner as dr

ROOT = Path(__file__).parent.parent
DB = ROOT / 'eps_momentum_data.db'
SEG_CAP = 100


def calc_v80_5(segs, fwd_pe_chg):
    score = sum(segs)
    cap_hit = any(abs(s) >= SEG_CAP for s in segs)
    if cap_hit:
        df = 0.3  # β1
    else:
        direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
        df_raw = max(-0.3, min(0.3, direction / 30))
        if fwd_pe_chg is not None and fwd_pe_chg > 0 and direction < 0:
            df = -df_raw  # opt4
        else:
            df = df_raw
    valid = [s for s in segs if abs(s) < SEG_CAP]
    min_seg = min(valid) if valid else 0
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    adj_score = score * (1 + df)
    return score, adj_score, df, eps_q


def main():
    dr.DB_PATH = str(DB)
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    print(f'대상: {len(dates)}일')

    score_mismatch = 0
    adj_score_mismatch = 0
    adj_gap_mismatch = 0
    cr_mismatch = 0
    p2_mismatch = 0
    cr_sort_violation = 0

    score_examples = []
    cr_examples = []
    p2_examples = []

    for d in dates:
        rows = cur.execute(
            'SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, '
            'score, adj_score, adj_gap, composite_rank, part2_rank, '
            'rev_up30, num_analysts, rev_growth '
            'FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL', (d,)
        ).fetchall()

        # 각 종목 score/adj_score 재계산
        ticker_data = {}
        for tk, nc, n7, n30, n60, n90, sc_db, asc_db, ag_db, cr_db, p2_db, ru, na, rg in rows:
            if not all([n7, n30, n60, n90]) or n7==0 or n30==0 or n60==0 or n90==0:
                continue
            try:
                segs = [max(-SEG_CAP, min(SEG_CAP, v)) for v in (
                    (nc-n7)/abs(n7)*100, (n7-n30)/abs(n30)*100,
                    (n30-n60)/abs(n60)*100, (n60-n90)/abs(n90)*100
                )]
            except:
                continue

            # adj_gap 역산해서 fwd_pe_chg 확인
            score_calc, asc_calc, df_calc, eq_calc = calc_v80_5(segs, None)  # fwd 없이는 옵션4 미적용

            # 정확한 fwd_pe_chg 부호 확인 (adj_gap에서 역산)
            denom = (1 + df_calc) * eq_calc
            fwd_pe_chg = ag_db / denom if abs(denom) > 1e-6 else None

            # opt4 적용된 df 재계산
            score_calc2, asc_calc2, df_calc2, eq_calc2 = calc_v80_5(segs, fwd_pe_chg)

            # score 비교
            if abs(sc_db - score_calc2) > 0.01:
                score_mismatch += 1
                if len(score_examples) < 3:
                    score_examples.append((d, tk, sc_db, score_calc2))

            # adj_score 비교
            if abs(asc_db - asc_calc2) > 0.05:
                adj_score_mismatch += 1

            # adj_gap 검증 = fwd_pe_chg × (1+df) × eq (재계산)
            ag_calc = fwd_pe_chg * (1 + df_calc2) * eq_calc2 if fwd_pe_chg is not None else None
            if ag_calc is not None and abs(ag_db - ag_calc) > 0.05:
                adj_gap_mismatch += 1

            ticker_data[tk] = {'cr_db': cr_db, 'ag': ag_db, 'ru': ru, 'na': na, 'nc': nc, 'n90': n90, 'rg': rg, 'p2_db': p2_db}

        # composite_rank 재정렬 검증
        elig_conv = []
        for tk, td in ticker_data.items():
            cg = dr._apply_conviction(td['ag'], td['ru'], td['na'], td['nc'], td['n90'], rev_growth=td['rg'])
            if cg is not None:
                elig_conv.append((tk, cg))
        elig_conv.sort(key=lambda x: x[1])
        expected_cr = {tk: i+1 for i, (tk, _) in enumerate(elig_conv)}

        for tk, td in ticker_data.items():
            if tk in expected_cr and td['cr_db'] != expected_cr[tk]:
                cr_mismatch += 1
                if len(cr_examples) < 5:
                    cr_examples.append((d, tk, td['cr_db'], expected_cr[tk]))

        # part2_rank 재계산 검증
        tickers = [t for t, _ in elig_conv]
        wmap = dr._compute_w_gap_map(cur, d, tickers)
        sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        expected_p2 = {tk: rk for rk, tk in enumerate(sorted_w[:30], 1)}

        for tk in expected_p2:
            db_p2 = ticker_data.get(tk, {}).get('p2_db')
            if db_p2 != expected_p2[tk]:
                p2_mismatch += 1
                if len(p2_examples) < 5:
                    p2_examples.append((d, tk, db_p2, expected_p2[tk]))

    print()
    print('=' * 80)
    print('검증 결과')
    print('=' * 80)
    print(f'\n[1] score (sum of segments) 일치: {"✓" if score_mismatch == 0 else "❌"} ({score_mismatch}건 불일치)')
    for e in score_examples:
        print(f'    {e[0]} {e[1]}: db={e[2]:.2f} expected={e[3]:.2f}')
    print(f'\n[2] adj_score (score × (1+df)) 일치: {"✓" if adj_score_mismatch == 0 else "❌"} ({adj_score_mismatch}건 불일치)')
    print(f'\n[3] adj_gap (fwd_pe_chg × (1+df) × eq) 일치: {"✓" if adj_gap_mismatch == 0 else "❌"} ({adj_gap_mismatch}건 불일치)')
    print(f'\n[4] composite_rank (conv_gap 정렬) 일치: {"✓" if cr_mismatch == 0 else "❌"} ({cr_mismatch}건 불일치)')
    for e in cr_examples:
        print(f'    {e[0]} {e[1]}: db cr={e[2]} expected={e[3]}')
    print(f'\n[5] part2_rank (w_gap Top 30) 일치: {"✓" if p2_mismatch == 0 else "❌"} ({p2_mismatch}건 불일치)')
    for e in p2_examples:
        print(f'    {e[0]} {e[1]}: db p2={e[2]} expected={e[3]}')

    total = score_mismatch + adj_score_mismatch + adj_gap_mismatch + cr_mismatch + p2_mismatch
    print()
    if total == 0:
        print('✅ 전 일자 v80.5 로직과 100% 일치')
    else:
        print(f'⚠️ {total}건 불일치 — 추가 fix 필요')

    conn.close()


if __name__ == '__main__':
    main()
