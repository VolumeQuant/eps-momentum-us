"""의존성 그래프대로 conviction 공식 변형 BT — min_seg 필터 포함

수정사항:
- regenerate에 min_seg < -2% 필터 추가 (production과 일치)
- 의존성 그래프대로 12변형 검증

변형 (12개):
  맹점 1+2 결합:
    1. max + no_confidence (baseline)
    2. max + N=10 (맹점 1만, max에 가려져 무력 검증)
    3. avg + no_confidence (맹점 2만)
    4. avg + N=10 (맹점 1+2 결합)
    5. avg + N=15
    6. sum + no_confidence
    7. sum + N=10
    8. sum + N=15
    9. 0.7r+0.3e + no_confidence
   10. 0.7r+0.3e + N=10
  맹점 3 (둘 다 강함 보너스):
   11. max + 둘 다 강함 보너스 (ratio≥0.5 AND eps_floor≥0.3 → +0.2)
   12. sum + 둘 다 강함 보너스
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
SEG_CAP = 100


def calc_min_seg(nc, n7, n30, n60, n90):
    """daily_runner와 동일 로직"""
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        if b and abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0)
    return min(segs) if segs else 0


def make_conviction_fn(combine, n_ref, both_strong_bonus=False):
    """combine: 'max' / 'avg' / 'sum' / 'w73' / 'w37'
       n_ref: None or int
       both_strong_bonus: ratio≥0.5 AND eps_floor≥0.3 시 +0.2
    """
    def _apply_conv(adj_gap, rev_up, num_analysts, ntm_current=None, ntm_90d=None,
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

        if combine == 'max':
            base = max(ratio, eps_floor)
        elif combine == 'avg':
            base = (ratio + eps_floor) / 2
        elif combine == 'sum':
            base = min(ratio + eps_floor, 1.0)
        elif combine == 'w73':
            base = 0.7 * ratio + 0.3 * eps_floor
        elif combine == 'w37':
            base = 0.3 * ratio + 0.7 * eps_floor
        else:
            base = max(ratio, eps_floor)

        # 맹점 3: 둘 다 강함 보너스
        if both_strong_bonus and ratio >= 0.5 and eps_floor >= 0.3:
            base = min(base + 0.2, 1.0)

        rev_bonus = 0.0
        if rev_growth is not None and rev_growth >= 0.30:
            rev_bonus = 0.3
        conviction = base + rev_bonus
        return adj_gap * (1 + conviction)
    return _apply_conv


def regenerate(test_db, conv_fn):
    """v2 — min_seg < -2% 필터 추가 (production save_part2_ranks와 일치)"""
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
                # production과 일치: min_seg < -2% 제외
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


VARIANTS = [
    # (이름, combine, n_ref, both_strong_bonus)
    ('1.  max + no_conf (baseline)',     'max', None, False),
    ('2.  max + N=10',                    'max', 10,   False),
    ('3.  avg + no_conf',                 'avg', None, False),
    ('4.  avg + N=10',                    'avg', 10,   False),
    ('5.  avg + N=15',                    'avg', 15,   False),
    ('6.  sum + no_conf',                 'sum', None, False),
    ('7.  sum + N=10',                    'sum', 10,   False),
    ('8.  sum + N=15',                    'sum', 15,   False),
    ('9.  0.7r+0.3e + no_conf',           'w73', None, False),
    ('10. 0.7r+0.3e + N=10',              'w73', 10,   False),
    ('11. max + 둘다강함 보너스',          'max', None, True),
    ('12. sum + 둘다강함 보너스',          'sum', None, True),
]


def main():
    print('=' * 110)
    print('의존성 그래프 BT — conviction 공식 변형 12개 (min_seg 필터 포함)')
    print('=' * 110)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates, _ = bts2.load_data()
    start_dates = dates[2:8]
    print(f'시작일: {start_dates[0]} ~ {start_dates[-1]} ({len(start_dates)}개, 모두 50거래일+)')

    rows = []
    for name, combine, n_ref, bsb in VARIANTS:
        slug = name.split('.')[0].strip()
        db = GRID / f'dep_{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        fn = make_conviction_fn(combine, n_ref, bsb)
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
        marker = ' ★' if '1.' in r['name'].split()[0] else '  '
        print(f'{marker}{r["name"]:<30} {r["avg"]:+8.2f}% {r["med"]:+8.2f}% {r["std"]:>4.2f} '
              f'{r["min"]:+8.2f}% {r["max"]:+8.2f}% {r["worst_mdd"]:+9.2f}% {r["risk_adj"]:>8.2f}')

    base = rows[0]
    print()
    print('=' * 110)
    print(f'1 (max + no_conf, baseline) 대비 차이')
    print('=' * 110)
    for r in rows[1:]:
        d_ret = r['avg'] - base['avg']
        d_mdd = r['worst_mdd'] - base['worst_mdd']
        d_ra = r['risk_adj'] - base['risk_adj']
        verdict = '✓ 개선' if d_ret >= 1.0 and d_mdd >= -1.0 else \
                  '~ 미세' if abs(d_ret) < 1.0 else '✗ 손실'
        print(f'  {r["name"]:<32}: ΔRet {d_ret:+7.2f}%p, ΔMDD {d_mdd:+6.2f}%p, '
              f'Δrisk_adj {d_ra:+5.2f}  {verdict}')


if __name__ == '__main__':
    main()
