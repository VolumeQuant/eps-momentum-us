"""rev_up30 추가 그리드 BT — abs ≥ 2/4 세분화 + rev_down30 단독 + 조합

이전 BT(rev_up30_filter)에서 abs ≥ 3 best 확인. 추가 그리드 + rev_down30 메트릭 검증.

추가 변형:
  abs ≥ 2 (그리드 세분화, 1과 3 사이)
  abs ≥ 4 (3과 5 사이)
  abs ≥ 3 AND down=0 (perfect signal)
  abs ≥ 3 AND down ≤ 1 (관대)
  down = 0 단독 (no downgrade)
  abs ≥ 3 AND num_analysts ≥ 5 (이중 컷)
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
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'rev_up30_dbs'
GRID.mkdir(exist_ok=True)


def regenerate(test_db, filter_fn):
    original = dr.DB_PATH
    dr.DB_PATH = str(test_db)
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today in dates:
            rows = cur.execute('''
                SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, rev_growth, rev_down30
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today,)).fetchall()

            elig_conv = []
            for r in rows:
                tk, ag, ru, na, nc, n90, rg, rd = r
                if ag is None or not filter_fn(ru or 0, rd or 0, na or 0):
                    continue
                cg = dr._apply_conviction(ag, ru, na, nc, n90, rev_growth=rg)
                if cg is not None:
                    elig_conv.append((tk, cg))

            elig_conv.sort(key=lambda x: x[1])
            new_cr = {tk: i + 1 for i, (tk, _) in enumerate(elig_conv)}

            cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
            for tk, cr in new_cr.items():
                cur.execute('UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                            (cr, today, tk))

            tickers = list(new_cr.keys())
            wmap = dr._compute_w_gap_map(cur, today, tickers)
            sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
            top30 = sorted_w[:30]
            cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
            for rk, tk in enumerate(top30, 1):
                cur.execute('UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                            (rk, today, tk))
            conn.commit()
        conn.close()
    finally:
        dr.DB_PATH = original


def run_multistart(db_path, start_dates):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    rets, mdds = [], []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds


VARIANTS = [
    ('baseline (no filter)',          lambda ru, rd, na: True),
    ('abs >= 2',                       lambda ru, rd, na: ru >= 2),
    ('abs >= 3 (best from prev)',      lambda ru, rd, na: ru >= 3),
    ('abs >= 4',                       lambda ru, rd, na: ru >= 4),
    ('abs >= 3 AND down = 0',          lambda ru, rd, na: ru >= 3 and rd == 0),
    ('abs >= 3 AND down <= 1',         lambda ru, rd, na: ru >= 3 and rd <= 1),
    ('down = 0 only',                  lambda ru, rd, na: rd == 0),
    ('down <= 1 only',                 lambda ru, rd, na: rd <= 1),
    ('abs >= 3 AND na >= 5',           lambda ru, rd, na: ru >= 3 and na >= 5),
    ('abs >= 3 AND na >= 10',          lambda ru, rd, na: ru >= 3 and na >= 10),
]


def main():
    print('=' * 110)
    print('rev_up30 확장 BT — 추가 그리드 + rev_down30 + 이중 컷')
    print('=' * 110)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates, _ = bts2.load_data()
    start_dates = dates[2:8]
    print(f'시작일: {start_dates[0]} ~ {start_dates[-1]} ({len(start_dates)}개, 모두 50거래일+)')

    rows = []
    for name, fn in VARIANTS:
        slug = name.replace(' ', '_').replace('>=', 'ge').replace('<=', 'le').replace('=', 'eq')[:30]
        db = GRID / f'ext_{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, fn)
        rets, mdds = run_multistart(db, start_dates)
        avg = sum(rets) / len(rets)
        med = sorted(rets)[len(rets) // 2]
        std = statistics.pstdev(rets)
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        rows.append({
            'name': name, 'avg': avg, 'med': med, 'std': std,
            'min': min(rets), 'max': max(rets),
            'worst_mdd': worst_mdd, 'risk_adj': risk_adj,
        })

    print()
    print(f'{"변형":<32} {"avg":>9} {"med":>9} {"std":>5} {"min":>9} {"max":>9} '
          f'{"worstMDD":>10} {"risk_adj":>9}')
    print('-' * 110)
    for r in rows:
        marker = ' ★' if 'baseline' in r['name'] else '  '
        print(f'{marker}{r["name"]:<30} {r["avg"]:+8.2f}% {r["med"]:+8.2f}% {r["std"]:>4.2f} '
              f'{r["min"]:+8.2f}% {r["max"]:+8.2f}% {r["worst_mdd"]:+9.2f}% {r["risk_adj"]:>8.2f}')

    base = next((r for r in rows if 'baseline' in r['name']), None)
    if base:
        print()
        print('=' * 110)
        print('baseline 대비 차이')
        print('=' * 110)
        for r in rows:
            if 'baseline' in r['name']:
                continue
            d_ret = r['avg'] - base['avg']
            d_mdd = r['worst_mdd'] - base['worst_mdd']
            d_ra = r['risk_adj'] - base['risk_adj']
            verdict = '✓ 개선' if d_ret >= 1.0 and d_mdd >= -1.0 else \
                      '~ 미세' if abs(d_ret) < 1.0 else '✗ 손실'
            print(f'  {r["name"]:<32}: ΔRet {d_ret:+7.2f}%p, ΔMDD {d_mdd:+6.2f}%p, '
                  f'Δrisk_adj {d_ra:+5.2f}  {verdict}')


if __name__ == '__main__':
    main()
