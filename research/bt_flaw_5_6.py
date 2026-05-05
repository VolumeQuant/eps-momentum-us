"""맹점 5, 6 BT — rev_bonus cliff 제거 + rev_down30 활용"""
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


def conv_full(adj_gap, ru, rd, na, nc, n90, rg, variant):
    """모든 변형 처리"""
    ru = ru or 0
    rd = rd or 0
    na = na or 0
    rg = rg or 0
    if na > 0:
        up_ratio = ru / na
        down_ratio = rd / na
    else:
        up_ratio = 0
        down_ratio = 0

    # ratio 계산 (맹점 6)
    if variant == '6a':
        ratio = max(0, (ru - rd) / na) if na > 0 else 0
    elif variant == '6b':
        ratio = max(0, up_ratio - 0.5 * down_ratio)
    elif variant == '6c':
        ratio = max(0, up_ratio - 0.3 * down_ratio)
    else:
        ratio = up_ratio

    # eps_floor
    eps_floor = 0
    if nc is not None and n90 and abs(n90) > 0.01:
        eps_floor = min(abs((nc - n90) / n90), 1.0)

    base = max(ratio, eps_floor)

    # 변형 11 보너스 (조합 시)
    if variant == '7b':
        if up_ratio >= 0.5 and eps_floor >= 0.3:
            base = min(base + 0.2, 1.0)

    # rev_bonus (맹점 5)
    if variant == '5a':
        rb = min(min(rg, 0.5) * 1.0, 0.3)
    elif variant in ('5b', '7a', '7b'):
        rb = min(min(rg, 0.5) * 0.6, 0.3)
    elif variant == '5c':
        rb = 0.3 * min(rg / 0.5, 1.0) if rg else 0
    else:
        rb = 0.3 if rg >= 0.30 else 0

    return adj_gap * (1 + base + rb)


def regenerate(test_db, variant):
    original_path = dr.DB_PATH
    original_fn = dr._apply_conviction
    dr.DB_PATH = str(test_db)

    # _compute_w_gap_map은 _apply_conviction 호출. 5인자라 rev_down 못 씀.
    # 단순화: _apply_conviction을 variant로 patch (rev_down=0 가정)
    # → 6a/6b/6c는 _compute_w_gap_map에서 rev_down 정보 없으므로 부분적 적용
    def patched_conv(ag, ru, na, nc, n90, rev_growth=None):
        return conv_full(ag, ru, 0, na, nc, n90, rev_growth, variant)
    dr._apply_conviction = patched_conv

    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today in dates:
            rows = cur.execute('''
                SELECT ticker, adj_gap, rev_up30, rev_down30, num_analysts, ntm_current, ntm_7d,
                       ntm_30d, ntm_60d, ntm_90d, rev_growth
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today,)).fetchall()
            elig_conv = []
            for r in rows:
                tk, ag, ru, rd, na, nc, n7, n30, n60, n90, rg = r
                if ag is None:
                    continue
                ms = calc_min_seg(nc or 0, n7 or 0, n30 or 0, n60 or 0, n90 or 0)
                if ms < -2:
                    continue
                cg = conv_full(ag, ru, rd, na, nc, n90, rg, variant)
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
    ('baseline',                    'baseline'),
    ('5a. rev_bonus rg×1.0',        '5a'),
    ('5b. rev_bonus rg×0.6',        '5b'),
    ('5c. rev_bonus sigmoid',       '5c'),
    ('6a. ratio=(up-down)/N',       '6a'),
    ('6b. up/N - 0.5×down/N',       '6b'),
    ('6c. up/N - 0.3×down/N',       '6c'),
    ('7a. 5b + 6a 조합',             '7a'),
    ('7b. 5b + 변형11 보너스',         '7b'),
]


def main():
    print('=' * 100)
    print('맹점 5, 6 BT — rev_bonus cliff 제거 + rev_down30 활용')
    print('=' * 100)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates, _ = bts2.load_data()
    start_dates = dates[2:8]

    rows = []
    for name, var in VARIANTS:
        slug = name.split('.')[0].strip()
        db = GRID / f'flaw56_{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, var)
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
    print(f'{"변형":<32} {"avg":>8} {"med":>8} {"min":>8} {"max":>8} {"MDD":>8} {"risk_adj":>9}')
    print('-' * 100)
    for r in rows:
        marker = ' ★' if 'baseline' in r['name'] else '  '
        print(f'{marker}{r["name"]:<30} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% '
              f'{r["min"]:+7.2f}% {r["max"]:+7.2f}% {r["worst_mdd"]:+7.2f}% {r["risk_adj"]:>8.2f}')

    base = rows[0]
    print()
    print('baseline 대비')
    for r in rows[1:]:
        d_ret = r['avg'] - base['avg']
        d_mdd = r['worst_mdd'] - base['worst_mdd']
        verdict = '✓ 개선' if d_ret >= 1.0 else '~ 미세' if abs(d_ret) < 1.0 else '✗ 손실'
        print(f'  {r["name"]:<32}: ΔRet {d_ret:+6.2f}%p, ΔMDD {d_mdd:+5.2f}%p  {verdict}')


if __name__ == '__main__':
    main()
