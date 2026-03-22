"""v54 마이그레이션: 과거 adj_gap에 eps_quality 보정 적용

기존: adj_gap = fwd_pe_chg × (1 + dir_factor)
변경: adj_gap = fwd_pe_chg × (1 + dir_factor) × eps_quality

eps_quality = 1.0 + 0.3 × clamp(eps_chg_weighted / 10, -1, 1)
eps_chg_weighted = Σ(weight × (ntm_cur - ntm_period) / |ntm_period| × 100)
    weights: 7d=0.4, 30d=0.3, 60d=0.2, 90d=0.1

마이그레이션: new_adj_gap = old_adj_gap × eps_quality
"""
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'eps_momentum_data.db'


def compute_eps_chg_weighted(ntm_cur, ntm_7d, ntm_30d, ntm_60d, ntm_90d):
    """가중평균 EPS 변화율 계산 (daily_runner.py line 551~559와 동일 로직)"""
    if ntm_cur is None:
        return None
    weights = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}
    ntm_vals = {'7d': ntm_7d, '30d': ntm_30d, '60d': ntm_60d, '90d': ntm_90d}

    ew_sum = 0.0
    ew_total = 0.0
    for key, w in weights.items():
        val = ntm_vals[key]
        if val is not None and abs(val) > 0.001:
            ew_sum += w * (ntm_cur - val) / abs(val) * 100
            ew_total += w

    if ew_total > 0:
        return ew_sum / ew_total
    return None


def compute_eps_quality(eps_cw):
    """eps_chg_weighted → eps_quality 팩터 [0.7, 1.3]"""
    if eps_cw is None:
        return 1.0
    eps_norm = max(-1.0, min(1.0, eps_cw / 10.0))
    return 1.0 + 0.3 * eps_norm


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. eps_chg_weighted 컬럼 추가
    try:
        cursor.execute('ALTER TABLE ntm_screening ADD COLUMN eps_chg_weighted REAL')
        print('eps_chg_weighted 컬럼 추가 완료')
    except sqlite3.OperationalError:
        print('eps_chg_weighted 컬럼 이미 존재')

    # 2. 모든 날짜 조회
    dates = cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE adj_gap IS NOT NULL ORDER BY date'
    ).fetchall()
    dates = [d[0] for d in dates]
    print(f'\n처리 대상: {len(dates)}개 날짜 ({dates[0]} ~ {dates[-1]})')

    total_updated = 0

    for date_str in dates:
        rows = cursor.execute('''
            SELECT rowid, ticker, adj_gap, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening
            WHERE date=? AND adj_gap IS NOT NULL
        ''', (date_str,)).fetchall()

        updated = 0
        for rowid, ticker, old_adj_gap, ntm_cur, ntm_7d, ntm_30d, ntm_60d, ntm_90d in rows:
            # eps_chg_weighted 계산
            eps_cw = compute_eps_chg_weighted(ntm_cur, ntm_7d, ntm_30d, ntm_60d, ntm_90d)

            # eps_quality 계산
            eps_q = compute_eps_quality(eps_cw)

            # adj_gap 보정: old × eps_quality
            new_adj_gap = old_adj_gap * eps_q

            cursor.execute('''
                UPDATE ntm_screening
                SET adj_gap=?, eps_chg_weighted=?
                WHERE rowid=?
            ''', (new_adj_gap, eps_cw, rowid))
            updated += 1

        total_updated += updated

        # 변화 샘플 출력 (첫 3종목)
        sample = cursor.execute('''
            SELECT ticker, adj_gap, eps_chg_weighted
            FROM ntm_screening
            WHERE date=? AND adj_gap IS NOT NULL
            ORDER BY adj_gap
            LIMIT 3
        ''', (date_str,)).fetchall()
        sample_str = ', '.join(f'{t}({ag:.1f}%, eq={ecw:.1f})' if ecw else f'{t}({ag:.1f}%)'
                               for t, ag, ecw in sample)
        print(f'  {date_str}: {updated}행 업데이트 | Top3: {sample_str}')

    conn.commit()
    conn.close()

    print(f'\n완료: 총 {total_updated}행 업데이트')
    print('  - adj_gap: old × eps_quality 적용')
    print('  - eps_chg_weighted: ntm 값에서 계산 후 저장')


if __name__ == '__main__':
    main()
