"""최종 조합 BT — 발견된 모든 알파 단계별 누적 적용

스택:
  Stage 0: baseline
  Stage 1: + rev_up30 ≥ 3 (eligible 컷)              | 어제 +8.51%p
  Stage 2: + 맹점 4: eps_cap = 3.0                   | +6.22%p
  Stage 3: + 맹점 11: 둘 다 강함 보너스 (r0.5 e0.3 b+0.2) | +4.80%p
  Stage 4: + 맹점 5: rev_bonus 비례화                  | +3.64%p

각 단계 누적 효과 측정.
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


def make_conv(eps_cap=1.0, bonus=False, rev_bonus_proportional=False):
    def fn(adj_gap, ru, na, nc, n90, rev_growth=None):
        ru = ru or 0
        na = na or 0
        rg = rev_growth or 0

        ratio = ru / na if na > 0 else 0

        eps_floor = 0
        if nc is not None and n90 and abs(n90) > 0.01:
            eps_floor = min(abs((nc - n90) / n90), eps_cap)

        base = max(ratio, eps_floor)

        # 변형 11
        if bonus and ratio >= 0.5 and eps_floor >= 0.3:
            base = min(base + 0.2, max(eps_cap, 1.0))

        # rev_bonus
        if rev_bonus_proportional:
            rb = min(min(rg, 0.5) * 0.6, 0.3)
        else:
            rb = 0.3 if rg >= 0.30 else 0

        return adj_gap * (1 + base + rb)
    return fn


def regenerate(test_db, fn, rev_up_min=0):
    """rev_up_min: rev_up30 ≥ rev_up_min 종목만 통과 (eligible 컷)"""
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
                # rev_up30 컷오프 (Stage 1+)
                if rev_up_min > 0 and (ru or 0) < rev_up_min:
                    continue
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


# 단계별 누적 + 개별 검증
VARIANTS = [
    # 이름, fn 인자, rev_up_min
    ('Stage 0: baseline (현재)',                {}, 0),
    ('Stage 1: + rev_up30≥3',                  {}, 3),
    ('Stage 2: Stage1 + eps_cap=3.0',          {'eps_cap': 3.0}, 3),
    ('Stage 3: Stage2 + 변형11 보너스',          {'eps_cap': 3.0, 'bonus': True}, 3),
    ('Stage 4: Stage3 + rev_bonus 비례',         {'eps_cap': 3.0, 'bonus': True, 'rev_bonus_proportional': True}, 3),
    # 개별 비교 (각 효과 단독)
    ('단독: eps_cap=3.0',                       {'eps_cap': 3.0}, 0),
    ('단독: 변형11 보너스',                       {'bonus': True}, 0),
    ('단독: rev_bonus 비례',                      {'rev_bonus_proportional': True}, 0),
]


def main():
    print('=' * 100)
    print('최종 조합 BT — 모든 알파 단계별 누적')
    print('=' * 100)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates, _ = bts2.load_data()
    start_dates = dates[2:8]

    rows = []
    for name, kw, rev_up_min in VARIANTS:
        slug = name.split(':')[0].strip().replace(' ', '_')
        db = GRID / f'final_{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        fn = make_conv(**kw)
        regenerate(db, fn, rev_up_min=rev_up_min)
        rets, mdds = run_multistart(db, start_dates)
        avg = sum(rets) / len(rets)
        med = sorted(rets)[len(rets) // 2]
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        rows.append({
            'name': name, 'avg': avg, 'med': med,
            'min': min(rets), 'max': max(rets),
            'worst_mdd': worst_mdd, 'risk_adj': risk_adj,
        })

    print()
    print(f'{"변형":<42} {"avg":>8} {"med":>8} {"min":>8} {"max":>8} {"MDD":>8} {"risk_adj":>9}')
    print('-' * 100)
    for r in rows:
        marker = ' ★' if 'Stage 0' in r['name'] else '  '
        print(f'{marker}{r["name"]:<40} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% '
              f'{r["min"]:+7.2f}% {r["max"]:+7.2f}% {r["worst_mdd"]:+7.2f}% {r["risk_adj"]:>8.2f}')

    base = rows[0]
    print()
    print('Stage 0 (baseline) 대비 누적 효과')
    for r in rows[1:]:
        d_ret = r['avg'] - base['avg']
        d_mdd = r['worst_mdd'] - base['worst_mdd']
        verdict = '✓ 개선' if d_ret >= 1.0 else '~ 미세' if abs(d_ret) < 1.0 else '✗ 손실'
        print(f'  {r["name"]:<42}: ΔRet {d_ret:+7.2f}%p, ΔMDD {d_mdd:+5.2f}%p  {verdict}')


if __name__ == '__main__':
    main()
