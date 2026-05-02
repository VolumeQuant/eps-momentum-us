"""저커버리지 컷오프 BT — num_analysts 임계값 변형 비교

배경: 현재 num_analysts ≥ 3 (저커버리지 차단). VIRT 5/1 사례 (n=7로 임계값
경계인데 1일 어닝 점프로 매수 후보 4위 진입). 더 보수적 컷오프가 알파에 미치는
영향 측정.

비교 변형:
  cutoff_3   — 현행 (≥3)
  cutoff_5   — 직관적 추천 (≥5)
  cutoff_7   — 더 보수적
  cutoff_10  — 매우 보수적

방법:
  1. v80.6 마이그레이션된 DB 복사
  2. num_analysts < cutoff 종목의 composite_rank/part2_rank를 NULL 처리
  3. 남은 종목으로 cr 재정렬 (conviction 기반)
  4. w_gap 기반 part2_rank 재정렬 (Top 30)
  5. 5시작일 multistart Top3 매매

사용법: python research/bt_low_coverage.py
"""
import sqlite3
import shutil
import sys
import statistics
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import daily_runner as dr
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'  # v80.6 적용된 DB
GRID = ROOT / 'research' / 'low_cov_dbs'
GRID.mkdir(exist_ok=True)


def regenerate(test_db, cutoff):
    """num_analysts < cutoff 종목 제외하고 cr/p2 재정렬"""
    original = dr.DB_PATH
    dr.DB_PATH = test_db
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today in dates:
            rows = cur.execute('''
                SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, rev_growth
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today,)).fetchall()

            # cutoff 미만 종목은 cr 제외 후 conviction 재계산
            elig_conv = []
            excluded = []
            for r in rows:
                tk, ag, ru, na, nc, n90, rg = r
                if na is None or na < cutoff:
                    excluded.append(tk)
                    continue
                if ag is None:
                    continue
                cg = dr._apply_conviction(ag, ru, na, nc, n90, rev_growth=rg)
                if cg is not None:
                    elig_conv.append((tk, cg))

            elig_conv.sort(key=lambda x: x[1])
            new_cr = {tk: i + 1 for i, (tk, _) in enumerate(elig_conv)}

            # cr 모두 NULL 처리 후 재할당
            cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
            for tk, cr in new_cr.items():
                cur.execute(
                    'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                    (cr, today, tk)
                )

            # part2_rank: w_gap 기반 Top 30 (남은 종목에서)
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
    finally:
        dr.DB_PATH = original


def multistart(db_path, n_starts=33):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    start_dates = dates[:n_starts]
    rets, mdds = [], []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds, start_dates


def count_excluded(db_path, cutoff):
    """cutoff 적용 시 일자별 평균 제외 종목 수"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    excluded_total = 0
    eligible_total = 0
    for d in dates:
        n_excl, n_elig = cur.execute(
            'SELECT '
            'SUM(CASE WHEN num_analysts < ? THEN 1 ELSE 0 END), '
            'COUNT(*) '
            'FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (cutoff, d)
        ).fetchone()
        excluded_total += n_excl or 0
        eligible_total += n_elig or 0
    conn.close()
    return excluded_total, eligible_total, len(dates)


VARIANTS = [3, 5, 7, 10]


def main():
    print('=' * 100)
    print('저커버리지 컷오프 BT — num_analysts 임계값 비교')
    print(f'DB 원본: {DB_ORIGINAL} (v80.6 적용 상태)')
    print('=' * 100)

    rows = []
    for cutoff in VARIANTS:
        db = GRID / f'cutoff_{cutoff}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, cutoff)
        rets, mdds, sds = multistart(db, n_starts=33)
        n = len(rets)
        avg = sum(rets) / n
        med = sorted(rets)[n // 2]
        std = statistics.pstdev(rets)
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        excl, elig, n_dates = count_excluded(DB_ORIGINAL, cutoff)
        rows.append({
            'cutoff': cutoff, 'avg': avg, 'med': med, 'std': std,
            'min': min(rets), 'max': max(rets),
            'worst_mdd': worst_mdd, 'risk_adj': risk_adj,
            'excluded': excl, 'eligible': elig, 'n_dates': n_dates,
        })

    # 결과 표
    print()
    print(f'{"Cutoff":<10} {"avg":>8} {"med":>8} {"std":>5} {"min":>8} {"max":>8} '
          f'{"worstMDD":>9} {"risk_adj":>8} {"excluded%":>10}')
    print('-' * 90)
    for r in rows:
        marker = ' ★' if r['cutoff'] == 3 else '  '
        excl_pct = r['excluded'] / r['eligible'] * 100 if r['eligible'] > 0 else 0
        print(f'{marker}≥{r["cutoff"]:<7} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% {r["std"]:>4.2f} '
              f'{r["min"]:+7.2f}% {r["max"]:+7.2f}% {r["worst_mdd"]:+8.2f}% '
              f'{r["risk_adj"]:>7.2f} {excl_pct:>9.1f}%')

    base = next((r for r in rows if r['cutoff'] == 3), None)
    if base:
        print()
        print('=' * 100)
        print('현행 (≥3) 대비 차이')
        print('=' * 100)
        for r in rows:
            if r['cutoff'] == 3:
                continue
            d_ret = r['avg'] - base['avg']
            d_mdd = r['worst_mdd'] - base['worst_mdd']
            d_ra = r['risk_adj'] - base['risk_adj']
            verdict = '✓ 채택 가능' if d_ret >= -1.0 and d_mdd >= -2.0 else '✗ 성과 손실'
            print(f'  ≥{r["cutoff"]:<3}: ΔRet {d_ret:+6.2f}%p, ΔMDD {d_mdd:+5.2f}%p, '
                  f'Δrisk_adj {d_ra:+5.2f}  {verdict}')


if __name__ == '__main__':
    main()
