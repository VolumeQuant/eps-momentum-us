"""맹점 1 BT — conviction의 ratio에 num_analysts confidence 가중

문제: 현재 ratio = rev_up30 / num_analysts
- WELL (1/3 = 33%) ≈ TER (5/17 = 29%) 거의 동등 취급
- 하지만 1명 의존 vs 5명 합의는 신뢰도 천지차이

가설:
  ratio_adj = ratio × min(num_analysts / N_REF, 1.0)
  N_REF = 5, 10, 15, 20

→ num_analysts < N_REF인 종목은 ratio 디스카운트
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


def make_conviction_fn(n_ref):
    """N_REF에 따른 confidence-weighted conviction"""
    def _apply_conviction_v2(adj_gap, rev_up, num_analysts, ntm_current=None, ntm_90d=None,
                              rev_growth=None):
        ratio = 0
        if num_analysts and num_analysts > 0 and rev_up is not None:
            ratio = rev_up / num_analysts
            if n_ref is not None and n_ref > 0:
                confidence = min(num_analysts / n_ref, 1.0)
                ratio = ratio * confidence
        eps_floor = 0
        if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
            eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
        base_conviction = max(ratio, eps_floor)
        rev_bonus = 0.0
        if rev_growth is not None and rev_growth >= 0.30:
            rev_bonus = 0.3
        conviction = base_conviction + rev_bonus
        return adj_gap * (1 + conviction)
    return _apply_conviction_v2


def regenerate(test_db, n_ref):
    """conviction 공식 변경 → cr/p2 재계산"""
    original_path = dr.DB_PATH
    original_fn = dr._apply_conviction
    dr.DB_PATH = str(test_db)
    if n_ref is not None:
        dr._apply_conviction = make_conviction_fn(n_ref)
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

            elig_conv = []
            for r in rows:
                tk, ag, ru, na, nc, n90, rg = r
                if ag is None:
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


VARIANTS = [
    ('baseline (no confidence)', None),
    ('N_REF = 5',                5),
    ('N_REF = 10',               10),
    ('N_REF = 15',               15),
    ('N_REF = 20',               20),
    ('N_REF = 30',               30),
]


def main():
    print('=' * 110)
    print('맹점 1 BT — confidence-weighted ratio (num_analysts/N_REF 가중)')
    print('=' * 110)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates, _ = bts2.load_data()
    start_dates = dates[2:8]
    print(f'시작일: {start_dates[0]} ~ {start_dates[-1]} ({len(start_dates)}개, 모두 50거래일+)')

    rows = []
    for name, n_ref in VARIANTS:
        slug = name.replace(' ', '_').replace('=', 'eq').replace('(', '').replace(')', '')[:30]
        db = GRID / f'{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, n_ref)
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
