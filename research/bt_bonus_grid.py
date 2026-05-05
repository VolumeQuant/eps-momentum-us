"""변형 11 그리드 서치 — '둘 다 강함 보너스' 파라미터 미세 조정

이전 BT: ratio≥0.5 AND eps_floor≥0.3 → +0.2 보너스 = +4.80%p

그리드:
  ratio_thr: 0.4 / 0.5 / 0.6
  eps_thr:   0.2 / 0.3 / 0.4
  bonus:     0.1 / 0.2 / 0.3

= 27변형. 너무 많으면 9개로 축약 (대표 조합).
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
GRID = ROOT / 'research' / 'conviction_dbs'
GRID.mkdir(exist_ok=True)


def calc_min_seg(nc, n7, n30, n60, n90):
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        if b and abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0)
    return min(segs) if segs else 0


def make_conviction_fn(ratio_thr, eps_thr, bonus):
    def _apply_conv(adj_gap, rev_up, num_analysts, ntm_current=None, ntm_90d=None,
                    rev_growth=None):
        ratio = 0
        if num_analysts and num_analysts > 0 and rev_up is not None:
            ratio = rev_up / num_analysts
        eps_floor = 0
        if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
            eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)

        base = max(ratio, eps_floor)
        # 변형 11: 둘 다 강함 보너스 (그리드 서치)
        if ratio_thr is not None and ratio >= ratio_thr and eps_floor >= eps_thr:
            base = min(base + bonus, 1.0)

        rev_bonus = 0.0
        if rev_growth is not None and rev_growth >= 0.30:
            rev_bonus = 0.3
        return adj_gap * (1 + base + rev_bonus)
    return _apply_conv


def regenerate(test_db, conv_fn):
    original_path = dr.DB_PATH
    original_fn = dr._apply_conviction
    dr.DB_PATH = str(test_db)
    dr._apply_conviction = conv_fn
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today in dates:
            rows = cur.execute('''
                SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_7d,
                       ntm_30d, ntm_60d, ntm_90d, rev_growth
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today,)).fetchall()
            elig_conv = []
            for r in rows:
                tk, ag, ru, na, nc, n7, n30, n60, n90, rg = r
                if ag is None:
                    continue
                ms = calc_min_seg(nc or 0, n7 or 0, n30 or 0, n60 or 0, n90 or 0)
                if ms < -2:
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
        dr.DB_PATH = original_path
        dr._apply_conviction = original_fn


def run_multistart(db_path, start_dates):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    rets, mdds = [], []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds


# 9 그리드 + baseline = 10
VARIANTS = [
    ('baseline (no bonus)',          None, None, None),
    ('r0.4 e0.3 b+0.2',              0.4,  0.3,  0.2),
    ('r0.5 e0.3 b+0.2 (이전 best)',   0.5,  0.3,  0.2),
    ('r0.6 e0.3 b+0.2',              0.6,  0.3,  0.2),
    ('r0.5 e0.2 b+0.2',              0.5,  0.2,  0.2),
    ('r0.5 e0.4 b+0.2',              0.5,  0.4,  0.2),
    ('r0.5 e0.3 b+0.1',              0.5,  0.3,  0.1),
    ('r0.5 e0.3 b+0.3',              0.5,  0.3,  0.3),
    ('r0.4 e0.2 b+0.3 (관대+큰보너스)', 0.4,  0.2,  0.3),
    ('r0.6 e0.4 b+0.3 (엄격+큰보너스)', 0.6,  0.4,  0.3),
]


def main():
    print('=' * 110)
    print('변형 11 그리드 서치 — 둘다강함 보너스 파라미터 (10변형)')
    print('=' * 110)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates, _ = bts2.load_data()
    start_dates = dates[2:8]
    print(f'시작일 6개, 모두 50거래일+')

    rows = []
    for name, rt, et, b in VARIANTS:
        slug = name.split(' ')[0].replace('+', 'p').replace('.', '')
        db = GRID / f'grid_{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        fn = make_conviction_fn(rt, et, b)
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
    print(f'{"변형":<35} {"avg":>8} {"med":>8} {"min":>8} {"max":>8} {"MDD":>8} {"risk_adj":>9}')
    print('-' * 100)
    for r in rows:
        marker = ' ★' if 'baseline' in r['name'] else '  '
        print(f'{marker}{r["name"]:<33} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% '
              f'{r["min"]:+7.2f}% {r["max"]:+7.2f}% {r["worst_mdd"]:+7.2f}% {r["risk_adj"]:>8.2f}')

    base = rows[0]
    print()
    print('baseline 대비 차이')
    print('-' * 100)
    for r in rows[1:]:
        d_ret = r['avg'] - base['avg']
        d_mdd = r['worst_mdd'] - base['worst_mdd']
        verdict = '✓ 개선' if d_ret >= 1.0 else '~ 미세' if abs(d_ret) < 1.0 else '✗ 손실'
        print(f'  {r["name"]:<35}: ΔRet {d_ret:+6.2f}%p, ΔMDD {d_mdd:+5.2f}%p  {verdict}')


if __name__ == '__main__':
    main()
