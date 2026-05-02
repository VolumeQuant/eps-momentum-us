"""SNDK가 세상에 없는 시장 시뮬레이션.

각 변형 DB의 SNDK 행을 composite_rank/part2_rank NULL 처리 후
나머지 종목들로 part2_rank 재정렬. multistart 5시작일 비교.

목적: SNDK 한 종목 의존성 제거 후 변형별 진짜 알파 측정.
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
SRC_DIR = ROOT / 'research' / 'gv2_dbs'
DST_DIR = ROOT / 'research' / 'no_sndk_dbs'
DST_DIR.mkdir(exist_ok=True)


def remove_sndk_and_recompute(db_path):
    """SNDK를 eligible/part2 모두에서 제외 후 part2_rank 재계산"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dr.DB_PATH = str(db_path)

    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    for today in dates:
        # 1) SNDK composite_rank, part2_rank NULL
        cur.execute(
            "UPDATE ntm_screening SET composite_rank=NULL, part2_rank=NULL WHERE date=? AND ticker='SNDK'",
            (today,)
        )
        # 2) 나머지 종목 composite_rank 1부터 재정렬 (그 날 conv_gap 순)
        rows = cur.execute('''
            SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, rev_growth
            FROM ntm_screening
            WHERE date=? AND adj_gap IS NOT NULL AND ticker != 'SNDK'
        ''', (today,)).fetchall()
        if not rows:
            continue

        elig = []
        for tk, ag, ru, na, nc, n90, rg in rows:
            cg = dr._apply_conviction(ag, ru, na, nc, n90, rev_growth=rg)
            if cg is not None:
                elig.append((tk, cg))
        elig.sort(key=lambda x: x[1])

        # SNDK 제외한 종목들에 composite_rank 1~N 재부여
        cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
        for i, (tk, _) in enumerate(elig, 1):
            cur.execute(
                'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                (i, today, tk)
            )

        # 3) part2_rank w_gap 기반 재계산
        tickers = [tk for tk, _ in elig]
        wmap = dr._compute_w_gap_map(cur, today, tickers)
        sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        top30 = sorted_w[:30]
        cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
        for rk, tk in enumerate(top30, 1):
            cur.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rk, today, tk)
            )
        conn.commit()

    conn.close()


def multistart(db_path, n_starts=5):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    start_dates = dates[:n_starts]
    rets, mdds = [], []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds, start_dates


def main():
    print('=' * 100)
    print('SNDK가 세상에 없다고 가정한 BT (multistart 5시작일, 룰 3/8/3)')
    print('=' * 100)

    variants = ['baseline', 'gamma', 'gamma_v2', 'opt2', 'gv2_opt2', 'base_opt2']

    rows = []
    for name in variants:
        src = SRC_DIR / f'{name}.db'
        dst = DST_DIR / f'{name}_no_sndk.db'
        if not src.exists():
            print(f'  {name}: src DB 없음 ({src}), skip')
            continue
        shutil.copy(src, dst)
        remove_sndk_and_recompute(dst)
        rets, mdds, sds = multistart(dst)
        n = len(rets)
        avg = sum(rets) / n
        med = sorted(rets)[n // 2]
        std = statistics.pstdev(rets)
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        rows.append({
            'name': name, 'avg': avg, 'med': med, 'std': std,
            'min': min(rets), 'max': max(rets),
            'worst_mdd': worst_mdd, 'risk_adj': risk_adj, 'rets': rets,
        })

    print()
    print(f'{"Variant":<14}', end='')
    for sd in sds:
        print(f' {sd:>10}', end='')
    print(f' {"avg":>8} {"std":>5} {"worstMDD":>9} {"risk_adj":>8}')
    print('-' * (16 + 11 * len(sds) + 35))
    for r in rows:
        print(f'  {r["name"]:<12}', end='')
        for ret in r['rets']:
            print(f' {ret:>9.2f}%', end='')
        print(f' {r["avg"]:+7.2f}% {r["std"]:>4.2f} {r["worst_mdd"]:+8.2f}% {r["risk_adj"]:>7.2f}')

    base = next((r for r in rows if r['name'] == 'baseline'), None)
    if base:
        print()
        print('=' * 100)
        print('비교 (baseline 대비, SNDK 없는 시장)')
        print('=' * 100)
        sorted_rows = sorted(rows, key=lambda x: -x['avg'])
        for r in sorted_rows:
            d_ret = r['avg'] - base['avg']
            d_mdd = r['worst_mdd'] - base['worst_mdd']
            d_ra = r['risk_adj'] - base['risk_adj']
            marker = ''
            if r['name'] == 'baseline':
                marker = ' (control)'
            elif d_ret > 0:
                marker = ' ← 우월'
            print(f'  {r["name"]:<12} avg {r["avg"]:+6.2f}%, MDD {r["worst_mdd"]:+6.2f}%, '
                  f'risk_adj {r["risk_adj"]:.2f} | ΔRet {d_ret:+.2f}%p, ΔMDD {d_mdd:+.2f}%p{marker}')


if __name__ == '__main__':
    main()
