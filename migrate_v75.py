"""v74 → v75 마이그레이션: 모든 일자별 part2_rank를 v75 conviction으로 재계산

핵심:
  - v75 변경: _apply_conviction에 rev_growth >= 30%면 +0.3 add 보너스
  - 영향: composite_rank, part2_rank 모두 재계산 필요
  - daily_runner.py의 진짜 함수 (save_part2_ranks, _compute_w_gap_map) 사용

안전 절차:
  1. DB 복사본에 적용
  2. SNDK 등 표본 검증
  3. sanity check 통과 시 production 교체
"""
import sqlite3
import shutil
import os
import sys
from pathlib import Path

sys.path.insert(0, '.')
import daily_runner as dr

DB_ORIGINAL = 'eps_momentum_data.db'
DB_TEST = 'eps_test_v75_migration.db'


def load_date_as_dataframe(cursor, date_str):
    """DB의 date_str 데이터를 daily_runner가 받는 results_df 형태로 변환"""
    import pandas as pd
    rows = cursor.execute('''
        SELECT ticker, score, adj_score, adj_gap,
               ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
               price, ma60, ma120,
               rev_up30, rev_down30, num_analysts, rev_growth,
               operating_margin, gross_margin, is_turnaround
        FROM ntm_screening WHERE date=?
    ''', (date_str,)).fetchall()

    # ticker_info_cache에서 industry 로드 (한국어)
    import json
    with open('ticker_info_cache.json', encoding='utf-8') as f:
        ticker_cache = json.load(f)

    records = []
    for r in rows:
        (tk, score, adj_score, adj_gap, nc, n7, n30, n60, n90,
         price, ma60, ma120, ru, rd, na, rg, om, gm, is_to) = r

        fwd_pe = (price / nc) if (price and nc and nc > 0) else None

        if n90 and abs(n90) > 0.01:
            eps_change_90d = (nc - n90) / abs(n90) * 100
        else:
            eps_change_90d = None

        # segments
        nc_, n7_, n30_, n60_, n90_ = (float(x) if x else 0 for x in [nc, n7, n30, n60, n90])
        segs = []
        for a, b in [(nc_, n7_), (n7_, n30_), (n30_, n60_), (n60_, n90_)]:
            if b and abs(b) > 0.01:
                segs.append((a - b) / abs(b) * 100)
            else:
                segs.append(0)
        segs = [max(-100.0, min(100.0, s)) for s in segs]
        seg1, seg2, seg3, seg4 = segs
        direction = (seg1 - seg4) if (seg1 is not None and seg4 is not None) else 0

        # industry from cache
        industry = ticker_cache.get(tk, {}).get('industry', '기타')

        records.append({
            'ticker': tk, 'score': score, 'adj_score': adj_score,
            'adj_gap': adj_gap, 'direction': direction,
            'seg1': seg1, 'seg2': seg2, 'seg3': seg3, 'seg4': seg4,
            'ntm_current': nc, 'ntm_7d': n7, 'ntm_30d': n30,
            'ntm_60d': n60, 'ntm_90d': n90,
            'eps_change_90d': eps_change_90d,
            'fwd_pe': fwd_pe,
            'price': price, 'ma60': ma60, 'ma120': ma120,
            'rev_up30': ru, 'rev_down30': rd,
            'num_analysts': na, 'rev_growth': rg,
            'operating_margin': om, 'gross_margin': gm,
            'industry': industry,
            'is_turnaround': is_to,
        })

    return pd.DataFrame(records)


def regenerate_v75(test_db_path):
    """v75 적용된 daily_runner를 사용해서 모든 날짜 재계산

    save_part2_ranks를 직접 호출하면 _apply_conviction (v75)이 자동 적용됨.
    """
    # daily_runner.DB_PATH를 test_db로 변경
    original_path = dr.DB_PATH
    dr.DB_PATH = test_db_path

    try:
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()

        # 모든 날짜
        dates = [r[0] for r in cursor.execute(
            'SELECT DISTINCT date FROM ntm_screening ORDER BY date'
        ).fetchall()]

        print(f"총 {len(dates)}일 마이그레이션 시작...")

        for i, today_str in enumerate(dates):
            df = load_date_as_dataframe(cursor, today_str)
            if df.empty:
                continue

            try:
                # save_part2_ranks가 v75 _apply_conviction 사용해서 새로 저장
                dr.save_part2_ranks(df, today_str)
                if (i + 1) % 10 == 0 or i == len(dates) - 1:
                    print(f"  {i+1}/{len(dates)} 완료 ({today_str})")
            except Exception as e:
                print(f"  ERROR {today_str}: {e}")

        conn.close()
        print("마이그레이션 완료")
    finally:
        dr.DB_PATH = original_path


def verify_performance(test_db_path):
    """v74 vs v75 마이그레이션 후 성과 비교"""
    print("\n" + "=" * 70)
    print("성과 검증 - v74 (백업) vs v75 (마이그레이션)")
    print("=" * 70)

    from bt_engine import load_data, simulate

    HOLD_STRICT = {'lookback_days': 20, 'price_threshold': 25, 'rev_up_ratio': 0.4,
                   'check_ma60': True, 'max_grace': 2}

    def multistart(db_path):
        dates, data = load_data(db_path)
        starts = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
        rets, mdds, holds = [], [], []
        for sd in starts:
            r = simulate(dates, data, 3, 11, 3, start_date=sd, hold_params=HOLD_STRICT)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            holds.append(r['breakout_holds'])
        n = len(rets)
        avg = sum(rets) / n
        sorted_r = sorted(rets)
        std = (sum((r - avg) ** 2 for r in rets) / n) ** 0.5
        return {
            'n': n, 'avg': round(avg, 2),
            'med': round(sorted_r[n//2], 2),
            'min': round(min(rets), 2),
            'max': round(max(rets), 2),
            'std': round(std, 2),
            'mdd_avg': round(sum(mdds)/n, 2),
            'mdd_worst': round(min(mdds), 2),
            'risk_adj': round(avg / abs(min(mdds)), 2) if min(mdds) != 0 else 0,
            'avg_holds': round(sum(holds)/n, 1),
        }

    # v74 backup
    backup_files = sorted(Path('.').glob('eps_momentum_data.db.v74_backup_*'))
    if backup_files:
        v74_db = str(backup_files[-1])
        print(f"\n[v74 baseline]: {v74_db}")
        m74 = multistart(v74_db)
        print(f"  평균: {m74['avg']:+.2f}% (med {m74['med']:+.2f}%, min {m74['min']:+.2f}%, max {m74['max']:+.2f}%)")
        print(f"  std {m74['std']}, MDD avg {m74['mdd_avg']:+.2f}% / worst {m74['mdd_worst']:+.2f}%")
        print(f"  위험조정: {m74['risk_adj']}, 평균 hold: {m74['avg_holds']}")
    else:
        m74 = None
        print("\n[v74 backup 없음]")

    # v75 migrated
    print(f"\n[v75 migrated]: {test_db_path}")
    m75 = multistart(test_db_path)
    print(f"  평균: {m75['avg']:+.2f}% (med {m75['med']:+.2f}%, min {m75['min']:+.2f}%, max {m75['max']:+.2f}%)")
    print(f"  std {m75['std']}, MDD avg {m75['mdd_avg']:+.2f}% / worst {m75['mdd_worst']:+.2f}%")
    print(f"  위험조정: {m75['risk_adj']}, 평균 hold: {m75['avg_holds']}")

    # 차이
    if m74:
        print(f"\n[차이 (v75 - v74)]")
        print(f"  평균 수익: {m75['avg'] - m74['avg']:+.2f}%p")
        print(f"  최저: {m75['min'] - m74['min']:+.2f}%p")
        print(f"  MDD worst: {m75['mdd_worst'] - m74['mdd_worst']:+.2f}%p")
        print(f"  위험조정: {m75['risk_adj'] - m74['risk_adj']:+.2f}")

        # 백테스트 예측 vs 실제
        print(f"\n[백테스트 예측 vs 마이그레이션 실측]")
        print(f"  예측: 평균 +1.69%p, MDD +1.5%p, 위험조정 +0.25")
        print(f"  실측: 평균 {m75['avg']-m74['avg']:+.2f}%p, MDD {m75['mdd_worst']-m74['mdd_worst']:+.2f}%p, 위험조정 {m75['risk_adj']-m74['risk_adj']:+.2f}")


def verify_migration(test_db_path):
    """SNDK 등 표본 검증"""
    print("\n=== 검증 ===")
    conn = sqlite3.connect(test_db_path)

    # SNDK 추적
    print("\n[SNDK 일자별 순위 (v75 적용 후)]")
    rows = conn.execute('''
        SELECT date, composite_rank, part2_rank, adj_gap, rev_growth
        FROM ntm_screening WHERE ticker='SNDK' AND date >= '2026-04-06'
        ORDER BY date
    ''').fetchall()
    for r in rows:
        rg = r[4]*100 if r[4] else 0
        print(f"  {r[0]}: composite={r[1]} part2={r[2]} adj_gap={r[3]:+.2f} rev={rg:.0f}%")

    # 4/10 Top 10
    print("\n[4/10 part2_rank Top 10 (v75)]")
    rows = conn.execute('''
        SELECT ticker, composite_rank, part2_rank, rev_growth
        FROM ntm_screening WHERE date='2026-04-10' AND part2_rank IS NOT NULL
        ORDER BY part2_rank LIMIT 10
    ''').fetchall()
    for r in rows:
        rg = r[3]*100 if r[3] else 0
        bonus = '✓' if r[3] and r[3] >= 0.30 else ''
        print(f"  rank {r[2]}: {r[0]:6s} composite={r[1]} rev={rg:>3.0f}% {bonus}")

    # MU, FIVE, BE 추적
    print("\n[보유 후보 종목 일자별 part2_rank]")
    for tk in ['MU', 'FIVE', 'BE', 'SNDK', 'FTAI']:
        rows = conn.execute(f'''
            SELECT date, part2_rank FROM ntm_screening
            WHERE ticker=? AND date >= '2026-04-06' AND part2_rank IS NOT NULL
            ORDER BY date
        ''', (tk,)).fetchall()
        ranks = ' → '.join(f'{d[5:]}:{r}' for d, r in rows)
        print(f"  {tk}: {ranks}")

    conn.close()


def main():
    print("=" * 70)
    print("v74 → v75 마이그레이션")
    print("=" * 70)

    # 1. 복사본 생성
    if os.path.exists(DB_TEST):
        os.remove(DB_TEST)
    shutil.copy(DB_ORIGINAL, DB_TEST)
    print(f"\n[1] 복사본 생성: {DB_TEST}")

    # 2. 재계산
    print(f"\n[2] v75 conviction으로 재계산")
    regenerate_v75(DB_TEST)

    # 3. 종목 검증
    verify_migration(DB_TEST)

    # 4. 성과 검증
    verify_performance(DB_TEST)


if __name__ == '__main__':
    main()
