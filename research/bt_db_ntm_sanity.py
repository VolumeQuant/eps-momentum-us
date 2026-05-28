"""sanity check — BE 5/13 단일 일자에서 DB-NTM lookup이 yf 컬럼과 얼마나 다른지"""
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
DB = ROOT / 'eps_momentum_data.db'


def find_n_days_ago(history, today_str, n_days, field_idx):
    target = (datetime.strptime(today_str, '%Y-%m-%d') - timedelta(days=n_days)).date()
    today_d = datetime.strptime(today_str, '%Y-%m-%d').date()
    best = None
    best_diff = None
    best_date = None
    for row in history:
        d_obj = datetime.strptime(row[0], '%Y-%m-%d').date()
        if d_obj > today_d:
            continue
        diff = abs((d_obj - target).days)
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best = row[field_idx]
            best_date = row[0]
    return best, best_diff, best_date


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    today = '2026-05-13'

    # 비교 대상 종목: BE + top 5
    tickers = ['BE', 'SNDK', 'TER', 'WWD', 'HGV', 'FSS']

    for tk in tickers:
        print(f'\n=== {tk} {today} ===')
        # yf 컬럼
        row = cur.execute(
            'SELECT ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, price, '
            'composite_rank, part2_rank, adj_gap '
            'FROM ntm_screening WHERE ticker=? AND date=?',
            (tk, today)
        ).fetchone()
        if not row:
            print('  no data')
            continue
        nc, n7y, n30y, n60y, n90y, px, cr, p2, ag = row
        print(f'  yf: ntm_cur={nc:.3f} 7d={n7y:.3f} 30d={n30y:.3f} 60d={n60y:.3f} 90d={n90y:.3f}')
        print(f'      cr={cr} p2={p2} adj_gap={ag:.2f}')

        # DB lookup
        hist = cur.execute(
            'SELECT date, price, ntm_current FROM ntm_screening '
            "WHERE ticker=? AND price IS NOT NULL AND ntm_current IS NOT NULL ORDER BY date",
            (tk,)
        ).fetchall()
        for n_days, label in [(7, '7d'), (30, '30d'), (60, '60d')]:
            ntm_db, diff_n, date_n = find_n_days_ago(hist, today, n_days, 2)
            px_db, diff_p, date_p = find_n_days_ago(hist, today, n_days, 1)
            yf_val = {'7d': n7y, '30d': n30y, '60d': n60y}[label]
            delta = (ntm_db - yf_val) / yf_val * 100 if yf_val else 0
            print(f'  {label}: DB ntm={ntm_db:.3f} (from {date_n}, Δ{diff_n}d) '
                  f'vs yf={yf_val:.3f} → {delta:+.1f}%')

    conn.close()


if __name__ == '__main__':
    main()
