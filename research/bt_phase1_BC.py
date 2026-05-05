"""Phase 1: 그룹 BC 통합 그리드 — Base = rev_up30 ≥ 3 위에서

결합방식 × eps_cap × 보너스 = 4 × 2 × 2 = 16 조합
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
GRID = ROOT / 'research' / 'phase1_dbs'
GRID.mkdir(exist_ok=True)


def calc_min_seg(nc, n7, n30, n60, n90):
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        if b and abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0)
    return min(segs) if segs else 0


def make_conv(combine='max', eps_cap=1.0, bonus=False):
    def fn(adj_gap, ru, na, nc, n90, rev_growth=None):
        ru = ru or 0
        na = na or 0
        rg = rev_growth or 0
        ratio = ru / na if na > 0 else 0
        eps_floor = 0
        if nc is not None and n90 and abs(n90) > 0.01:
            eps_floor = min(abs((nc - n90) / n90), eps_cap)

        if combine == 'max':
            base = max(ratio, eps_floor)
        elif combine == 'avg':
            base = (ratio + eps_floor) / 2
        elif combine == 'sum':
            base = min(ratio + eps_floor, max(eps_cap, 1.0))
        elif combine == 'w73':
            base = 0.7 * ratio + 0.3 * eps_floor
        else:
            base = max(ratio, eps_floor)

        if bonus and ratio >= 0.5 and eps_floor >= 0.3:
            base = min(base + 0.2, max(eps_cap, 1.0))

        rb = 0.3 if rg >= 0.30 else 0
        return adj_gap * (1 + base + rb)
    return fn


def regenerate(test_db, fn, rev_up_min=3):  # base = rev_up30 ≥ 3
    original_path = dr.DB_PATH
    original_fn = dr._apply_conviction
    dr.DB_PATH = str(test_db)
    dr._apply_conviction = fn
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
                if ag is None: continue
                ms = calc_min_seg(nc or 0, n7 or 0, n30 or 0, n60 or 0, n90 or 0)
                if ms < -2: continue
                if rev_up_min > 0 and (ru or 0) < rev_up_min: continue
                cg = fn(ag, ru, na, nc, n90, rev_growth=rg)
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


# 4 결합 × 2 cap × 2 보너스 = 16
VARIANTS = []
for combine in ['max', 'avg', 'sum', 'w73']:
    for cap in [1.0, 3.0]:
        for bonus in [False, True]:
            name = f'{combine} cap={cap} bonus={"on" if bonus else "off"}'
            VARIANTS.append((name, combine, cap, bonus))


def main():
    print('=' * 100)
    print('Phase 1: 그룹 BC 통합 그리드 (16 조합) — Base: rev_up30 ≥ 3')
    print('=' * 100)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates, _ = bts2.load_data()
    start_dates = dates[2:8]

    rows = []
    for name, combine, cap, bonus in VARIANTS:
        slug = name.replace(' ', '_').replace('=', '_').replace('.', '')
        db = GRID / f'p1_{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        fn = make_conv(combine, cap, bonus)
        regenerate(db, fn, rev_up_min=3)
        rets, mdds = run_multistart(db, start_dates)
        avg = sum(rets) / len(rets)
        med = sorted(rets)[len(rets) // 2]
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        rows.append({
            'name': name, 'avg': avg, 'med': med, 'worst_mdd': worst_mdd,
            'risk_adj': risk_adj,
        })

    # baseline = max + cap=1.0 + bonus=off (현재 production + rev_up30 컷)
    base = next(r for r in rows if 'max cap=1.0 bonus=off' in r['name'])

    print()
    print(f'{"변형":<35} {"avg":>8} {"med":>8} {"MDD":>8} {"risk_adj":>9} {"ΔRet":>8}')
    print('-' * 100)
    for r in sorted(rows, key=lambda x: -x['avg']):
        marker = ' ★' if r is base else ('  ✓' if r['avg'] - base['avg'] >= 1 else '   ')
        d_ret = r['avg'] - base['avg']
        print(f'{marker}{r["name"]:<33} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% '
              f'{r["worst_mdd"]:+7.2f}% {r["risk_adj"]:>8.2f} {d_ret:+7.2f}%p')

    # best 3 출력
    sorted_rows = sorted(rows, key=lambda x: -x['avg'])
    print()
    print('=' * 100)
    print('Phase 1 best')
    print('=' * 100)
    for i, r in enumerate(sorted_rows[:5]):
        print(f'{i+1}. {r["name"]} → avg {r["avg"]:+.2f}% (vs base {r["avg"]-base["avg"]:+.2f}%p)')


if __name__ == '__main__':
    main()
