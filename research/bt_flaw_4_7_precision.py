"""맹점 4, 7 + 변형 11 정밀 그리드 통합 BT"""
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


def make_conv(eps_cap=1.0, na_zero_handle='zero', bonus_ratio_thr=None, bonus_eps_thr=None, bonus_amt=0):
    """
    eps_cap: eps_floor cap (1.0=현재, 1.5/2.0=완화)
    na_zero_handle: 'zero' (현재) or 'eps_only' (na=0면 eps_floor만)
    bonus_*: 변형 11 보너스
    """
    def fn(adj_gap, ru, na, nc, n90, rev_growth=None):
        ru = ru or 0
        na = na or 0
        rg = rev_growth or 0

        ratio = 0
        if na > 0:
            ratio = ru / na
        # 맹점 7: num_analysts=0 처리
        elif na_zero_handle == 'eps_only':
            ratio = 0  # 명시적, 사실 default와 같음
        # else: ratio=0 default

        eps_floor = 0
        if nc is not None and n90 and abs(n90) > 0.01:
            eps_floor = min(abs((nc - n90) / n90), eps_cap)  # 맹점 4: cap 변경

        base = max(ratio, eps_floor)

        # 변형 11 보너스
        if bonus_ratio_thr is not None and ratio >= bonus_ratio_thr and eps_floor >= bonus_eps_thr:
            base = min(base + bonus_amt, max(eps_cap, 1.0))

        rb = 0.3 if rg >= 0.30 else 0
        return adj_gap * (1 + base + rb)
    return fn


def regenerate(test_db, fn):
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


VARIANTS = [
    ('baseline (eps_cap=1.0)',                    {}),
    # 맹점 4: eps_floor cap 변경
    ('맹점4: eps_cap=1.5',                         {'eps_cap': 1.5}),
    ('맹점4: eps_cap=2.0',                         {'eps_cap': 2.0}),
    ('맹점4: eps_cap=3.0',                         {'eps_cap': 3.0}),
    ('맹점4: eps_cap=무제한',                       {'eps_cap': 99.0}),
    # 변형 11 정밀 그리드
    ('정밀: r0.45 e0.30 b+0.2',                    {'bonus_ratio_thr': 0.45, 'bonus_eps_thr': 0.30, 'bonus_amt': 0.2}),
    ('정밀: r0.55 e0.35 b+0.2',                    {'bonus_ratio_thr': 0.55, 'bonus_eps_thr': 0.35, 'bonus_amt': 0.2}),
    ('정밀: r0.65 e0.30 b+0.2',                    {'bonus_ratio_thr': 0.65, 'bonus_eps_thr': 0.30, 'bonus_amt': 0.2}),
    ('정밀: r0.7 e0.4 b+0.2 (엄격)',                {'bonus_ratio_thr': 0.70, 'bonus_eps_thr': 0.40, 'bonus_amt': 0.2}),
    ('정밀: r0.8 e0.5 b+0.3 (매우엄격)',             {'bonus_ratio_thr': 0.80, 'bonus_eps_thr': 0.50, 'bonus_amt': 0.3}),
    ('정밀: r0.4 e0.3 b+0.05 (관대+소보너스)',        {'bonus_ratio_thr': 0.40, 'bonus_eps_thr': 0.30, 'bonus_amt': 0.05}),
    ('정밀: r0.5 e0.3 b+0.5 (큰보너스)',             {'bonus_ratio_thr': 0.50, 'bonus_eps_thr': 0.30, 'bonus_amt': 0.5}),
    # 맹점 7: num_analysts=0 처리 (이미 ≥3 컷오프라 영향 미미 예상)
    ('맹점7: na=0 명시 처리',                       {'na_zero_handle': 'eps_only'}),
]


def main():
    print('=' * 100)
    print('맹점 4, 7 + 변형 11 정밀 그리드 통합 BT')
    print('=' * 100)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates, _ = bts2.load_data()
    start_dates = dates[2:8]

    rows = []
    for name, kw in VARIANTS:
        slug = name.split(':')[0].strip().replace(' ', '_')[:25]
        db = GRID / f'pre_{slug}_{abs(hash(name))%10000}.db'
        shutil.copy(DB_ORIGINAL, db)
        fn = make_conv(**kw)
        regenerate(db, fn)
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
    print(f'{"변형":<40} {"avg":>8} {"med":>8} {"MDD":>8} {"risk_adj":>9}')
    print('-' * 90)
    for r in rows:
        marker = ' ★' if 'baseline' in r['name'] else '  '
        print(f'{marker}{r["name"]:<38} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% '
              f'{r["worst_mdd"]:+7.2f}% {r["risk_adj"]:>8.2f}')

    base = rows[0]
    print()
    print('baseline 대비')
    for r in rows[1:]:
        d_ret = r['avg'] - base['avg']
        d_mdd = r['worst_mdd'] - base['worst_mdd']
        verdict = '✓ 개선' if d_ret >= 1.0 else '~ 미세' if abs(d_ret) < 1.0 else '✗ 손실'
        print(f'  {r["name"]:<40}: ΔRet {d_ret:+6.2f}%p, ΔMDD {d_mdd:+5.2f}%p  {verdict}')


if __name__ == '__main__':
    main()
