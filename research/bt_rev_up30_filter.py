"""rev_up30 필터 가설 검증 BT — 합의 강도 컷오프 다양한 변형

배경: WELL 5/4 사례 (num_analysts=3, rev_up30=1, 합의 33%) — REIT 특성상 EPS estimate
분석가가 적고 단일 분석가 의존도 위험. 현재 정책은 rev_down/total > 30%만 차단,
rev_up30 자체엔 컷오프 없음. 다양한 컷오프 가설로 알파 영향 측정.

귀무가설들:
  H1 (절대값): rev_up30 ≥ N → 작은 N부터 큰 N까지
  H2 (비율):   rev_up30 / num_analysts ≥ X%
  H3 (순상향): (rev_up30 - rev_down30) ≥ N
  H4 (조합):   절대 + 비율 동시 만족
  H5 (현재):   필터 없음 (baseline)

방법:
  - 각 변형마다 DB 복사 → composite_rank/part2_rank 재계산
  - 컷오프 미달 종목은 eligible 제외 (cr/p2 NULL)
  - 6시작일 multistart Top3/Exit8/Slot3
"""
import sqlite3
import shutil
import sys
import statistics
from collections import defaultdict
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
    """filter_fn(rev_up, rev_down, num_analysts) → True면 통과, False면 제외"""
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
            excluded = 0
            for r in rows:
                tk, ag, ru, na, nc, n90, rg, rd = r
                if ag is None:
                    continue
                # 필터 적용
                if not filter_fn(ru or 0, rd or 0, na or 0):
                    excluded += 1
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


# 필터 함수들 (rev_up, rev_down, num_analysts) → bool
def f_baseline(ru, rd, na): return True  # 현재
def f_abs1(ru, rd, na): return ru >= 1
def f_abs3(ru, rd, na): return ru >= 3
def f_abs5(ru, rd, na): return ru >= 5
def f_abs10(ru, rd, na): return ru >= 10
def f_ratio20(ru, rd, na): return na > 0 and ru / na >= 0.20
def f_ratio30(ru, rd, na): return na > 0 and ru / na >= 0.30
def f_ratio50(ru, rd, na): return na > 0 and ru / na >= 0.50
def f_ratio70(ru, rd, na): return na > 0 and ru / na >= 0.70
def f_net1(ru, rd, na): return (ru - rd) >= 1
def f_net3(ru, rd, na): return (ru - rd) >= 3
def f_combo3_30(ru, rd, na): return ru >= 3 and na > 0 and ru / na >= 0.30
def f_combo5_50(ru, rd, na): return ru >= 5 and na > 0 and ru / na >= 0.50


VARIANTS = [
    ('baseline (no filter)',    f_baseline),
    # H1: 절대값
    ('H1 abs >= 1',             f_abs1),
    ('H1 abs >= 3',             f_abs3),
    ('H1 abs >= 5',             f_abs5),
    ('H1 abs >= 10',            f_abs10),
    # H2: 비율
    ('H2 ratio >= 20%',         f_ratio20),
    ('H2 ratio >= 30%',         f_ratio30),
    ('H2 ratio >= 50%',         f_ratio50),
    ('H2 ratio >= 70%',         f_ratio70),
    # H3: 순상향
    ('H3 (up-down) >= 1',       f_net1),
    ('H3 (up-down) >= 3',       f_net3),
    # H4: 조합
    ('H4 abs>=3 AND ratio>=30%', f_combo3_30),
    ('H4 abs>=5 AND ratio>=50%', f_combo5_50),
]


def main():
    print('=' * 110)
    print('rev_up30 컷오프 BT — 합의 강도 필터 가설 검증 (6시작일 multistart)')
    print('=' * 110)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates, _ = bts2.load_data()
    start_dates = dates[2:8]
    print(f'시작일: {start_dates[0]} ~ {start_dates[-1]} ({len(start_dates)}개, 모두 50거래일+)')

    rows = []
    for name, fn in VARIANTS:
        slug = name.replace(' ', '_').replace('>=', 'ge').replace('%', 'pct')[:30]
        db = GRID / f'{slug}.db'
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
                      '~ 미세' if abs(d_ret) < 1.0 else \
                      '~ 트레이드오프' if d_ret > 0 else '✗ 손실'
            print(f'  {r["name"]:<32}: ΔRet {d_ret:+7.2f}%p, ΔMDD {d_mdd:+6.2f}%p, '
                  f'Δrisk_adj {d_ra:+5.2f}  {verdict}')


if __name__ == '__main__':
    main()
