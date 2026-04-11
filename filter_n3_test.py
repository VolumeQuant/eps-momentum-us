"""N<3 (저커버리지) 필터 효과 검증

V75 conviction 고정, N<3 컷 ON/OFF 비교:
- V75 + N≥3 (현재 production): 분석가 3명 미만 종목 제외
- V75 + N≥1 (필터 제거):       N<3 종목도 후보에 포함

monkey-patch:
  dr.get_part2_candidates → patched 버전 (N<3 라인만 제거)
  dr._apply_conviction → conv_v75
"""
import sys
import os
import shutil
import sqlite3
import json

sys.path.insert(0, '.')
import daily_runner as dr
import pandas as pd
import numpy as np

V74_BACKUP = sorted([f for f in os.listdir('.') if f.startswith('eps_momentum_data.db.v74_backup_')])[-1]
print(f"V74 backup: {V74_BACKUP}")


def conv_v75(adj_gap, rev_up, num_analysts, ntm_current=None, ntm_90d=None, rev_growth=None):
    """V75 현재 production conviction"""
    if adj_gap is None:
        return None
    ratio = 0
    if num_analysts and num_analysts > 0 and rev_up is not None:
        ratio = rev_up / num_analysts
    eps_floor = 0
    if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    base = max(ratio, eps_floor)
    bonus = 0.3 if (rev_growth is not None and rev_growth >= 0.30) else 0
    return adj_gap * (1 + base + bonus)


# ===== Patched get_part2_candidates: N<3 컷 제거 =====
# daily_runner.get_part2_candidates 코드 복사 후 N<3 라인만 제거

def patched_get_part2_candidates_no_n3(df, top_n=None, return_counts=False):
    """get_part2_candidates와 동일하지만 N<3 컷만 제거"""
    if 'ma120' in df.columns:
        ma_col = df['ma120'].where(df['ma120'].notna(), df['ma60'])
    else:
        ma_col = df['ma60']
    filtered = df[
        (df['adj_score'] > 9) &
        (df['adj_gap'].notna()) &
        (df['fwd_pe'].notna()) & (df['fwd_pe'] > 0) &
        (df['eps_change_90d'] > 0) &
        (df['price'].notna()) & (df['price'] >= 10) &
        (ma_col.notna()) & (df['price'] > ma_col)
    ].copy()
    eps_screened = len(filtered)

    has_rev = 'rev_growth' in filtered.columns and filtered['rev_growth'].notna().sum() >= 10
    if has_rev:
        filtered = filtered[filtered['rev_growth'].notna()].copy()
        filtered = filtered[filtered['rev_growth'] >= 0.10].copy()

    # === N<3 컷 제거 (이게 차이) ===
    # if 'num_analysts' in filtered.columns:
    #     filtered = filtered[filtered['num_analysts'].fillna(0) >= 3].copy()

    if 'rev_up30' in filtered.columns and 'rev_down30' in filtered.columns:
        up = filtered['rev_up30'].fillna(0)
        dn = filtered['rev_down30'].fillna(0)
        total = up + dn
        down_ratio = dn / total.replace(0, float('nan'))
        filtered = filtered[~(down_ratio > 0.3)].copy()

    if 'operating_margin' in filtered.columns and 'gross_margin' in filtered.columns:
        om = filtered['operating_margin']
        gm = filtered['gross_margin']
        filtered = filtered[~(om.notna() & gm.notna() & (om < 0.10) & (gm < 0.30))].copy()

    if 'operating_margin' in filtered.columns:
        om = filtered['operating_margin']
        filtered = filtered[~(om.notna() & (om < 0.05))].copy()

    if 'industry' in filtered.columns:
        filtered = filtered[~filtered['industry'].isin(dr.COMMODITY_INDUSTRIES)].copy()
    filtered = filtered[~filtered['ticker'].isin(dr.COMMODITY_TICKERS)].copy()

    if return_counts:
        return filtered, {'eps_screened': eps_screened, 'quality_filtered': len(filtered)}
    return filtered


def regenerate(test_db_path, conv_fn, get_part2_fn=None):
    """V75 conviction + (선택) patched get_part2_candidates로 재계산"""
    with open('ticker_info_cache.json', encoding='utf-8') as f:
        ticker_cache = json.load(f)

    original_path = dr.DB_PATH
    original_conv = dr._apply_conviction
    original_get_part2 = dr.get_part2_candidates

    dr.DB_PATH = test_db_path
    dr._apply_conviction = conv_fn
    if get_part2_fn is not None:
        dr.get_part2_candidates = get_part2_fn

    try:
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()
        dates = [r[0] for r in cursor.execute(
            'SELECT DISTINCT date FROM ntm_screening ORDER BY date'
        ).fetchall()]

        for today_str in dates:
            rows = cursor.execute('''
                SELECT ticker, score, adj_score, adj_gap,
                       ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                       price, ma60, ma120,
                       rev_up30, rev_down30, num_analysts, rev_growth,
                       operating_margin, gross_margin, is_turnaround
                FROM ntm_screening WHERE date=?
            ''', (today_str,)).fetchall()

            records = []
            for r in rows:
                (tk, score, adj_score, adj_gap, nc, n7, n30, n60, n90,
                 price, ma60, ma120, ru, rd, na, rg, om, gm, is_to) = r

                fwd_pe = (price / nc) if (price and nc and nc > 0) else None
                eps_change_90d = ((nc - n90) / abs(n90) * 100) if (n90 and abs(n90) > 0.01) else None

                nc_, n7_, n30_, n60_, n90_ = (float(x) if x else 0 for x in [nc, n7, n30, n60, n90])
                segs = []
                for a, b in [(nc_, n7_), (n7_, n30_), (n30_, n60_), (n60_, n90_)]:
                    if b and abs(b) > 0.01:
                        segs.append((a - b) / abs(b) * 100)
                    else:
                        segs.append(0)
                segs = [max(-100.0, min(100.0, s)) for s in segs]
                seg1, seg2, seg3, seg4 = segs
                direction = (seg1 - seg4)
                industry = ticker_cache.get(tk, {}).get('industry', '기타')

                records.append({
                    'ticker': tk, 'score': score, 'adj_score': adj_score,
                    'adj_gap': adj_gap, 'direction': direction,
                    'seg1': seg1, 'seg2': seg2, 'seg3': seg3, 'seg4': seg4,
                    'ntm_current': nc, 'ntm_7d': n7, 'ntm_30d': n30,
                    'ntm_60d': n60, 'ntm_90d': n90,
                    'eps_change_90d': eps_change_90d, 'fwd_pe': fwd_pe,
                    'price': price, 'ma60': ma60, 'ma120': ma120,
                    'rev_up30': ru, 'rev_down30': rd,
                    'num_analysts': na, 'rev_growth': rg,
                    'operating_margin': om, 'gross_margin': gm,
                    'industry': industry, 'is_turnaround': is_to,
                })

            df = pd.DataFrame(records)
            if df.empty:
                continue

            try:
                dr.save_part2_ranks(df, today_str)
            except Exception as e:
                print(f"  ERROR {today_str}: {e}")

        conn.close()
    finally:
        dr.DB_PATH = original_path
        dr._apply_conviction = original_conv
        dr.get_part2_candidates = original_get_part2


def make_test_db(suffix, conv_fn, get_part2_fn=None):
    db = f'eps_test_n3_{suffix}.db'
    if os.path.exists(db):
        os.remove(db)
    shutil.copy(V74_BACKUP, db)
    print(f"  [{suffix}] 재계산 중...")
    regenerate(db, conv_fn, get_part2_fn=get_part2_fn)
    return db


def multistart(db_path):
    from bt_engine import load_data, simulate
    HOLD_STRICT = {'lookback_days': 20, 'price_threshold': 25, 'rev_up_ratio': 0.4,
                   'check_ma60': True, 'max_grace': 2}
    dates, data = load_data(db_path)
    starts = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    rets, mdds = [], []
    for sd in starts:
        r = simulate(dates, data, 3, 11, 3, start_date=sd, hold_params=HOLD_STRICT)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    n = len(rets)
    avg = sum(rets) / n
    sorted_r = sorted(rets)
    std = (sum((r - avg) ** 2 for r in rets) / n) ** 0.5
    return {
        'n': n, 'avg': round(avg, 2),
        'med': round(sorted_r[n // 2], 2),
        'min': round(min(rets), 2), 'max': round(max(rets), 2),
        'std': round(std, 2),
        'mdd_avg': round(sum(mdds) / n, 2),
        'mdd_worst': round(min(mdds), 2),
        'risk_adj': round(avg / abs(min(mdds)), 2) if min(mdds) != 0 else 0,
    }


def count_n3_tickers(db_path):
    """N<3 종목이 part2_rank 받은 횟수 (날짜·종목)"""
    conn = sqlite3.connect(db_path)
    n_below = conn.execute('''
        SELECT COUNT(*) FROM ntm_screening
        WHERE part2_rank IS NOT NULL AND num_analysts < 3
    ''').fetchone()[0]
    n_total = conn.execute('''
        SELECT COUNT(*) FROM ntm_screening
        WHERE part2_rank IS NOT NULL
    ''').fetchone()[0]
    conn.close()
    return n_below, n_total


def main():
    print("=" * 100)
    print("N<3 (저커버리지) 필터 효과 검증 — V75 conviction 고정")
    print("=" * 100)

    print("\n[1] V75 + N≥3 (현재 production)")
    db_with = make_test_db('v75_with_n3', conv_v75, get_part2_fn=None)
    m_with = multistart(db_with)
    nb_w, nt_w = count_n3_tickers(db_with)
    print(f"  avg {m_with['avg']:+.2f}% | min {m_with['min']:+.2f}% | "
          f"std {m_with['std']} | MDD worst {m_with['mdd_worst']:+.2f}% | risk_adj {m_with['risk_adj']}")
    print(f"  N<3 part2 진입: {nb_w}/{nt_w}회")

    print("\n[2] V75 + N≥1 (저커버리지 필터 제거)")
    db_no = make_test_db('v75_no_n3', conv_v75, get_part2_fn=patched_get_part2_candidates_no_n3)
    m_no = multistart(db_no)
    nb_n, nt_n = count_n3_tickers(db_no)
    print(f"  avg {m_no['avg']:+.2f}% | min {m_no['min']:+.2f}% | "
          f"std {m_no['std']} | MDD worst {m_no['mdd_worst']:+.2f}% | risk_adj {m_no['risk_adj']}")
    print(f"  N<3 part2 진입: {nb_n}/{nt_n}회")

    print()
    print("=" * 100)
    print(f"{'변형':<24s} {'avg':>8s} {'med':>8s} {'min':>8s} {'max':>8s} "
          f"{'std':>6s} {'MDD avg':>9s} {'MDD worst':>10s} {'risk_adj':>9s}")
    print("-" * 100)
    for name, m in [('V75 + N≥3 (현재)', m_with), ('V75 + N≥1 (필터제거)', m_no)]:
        print(f"{name:<24s} {m['avg']:+7.2f}% {m['med']:+7.2f}% {m['min']:+7.2f}% "
              f"{m['max']:+7.2f}% {m['std']:>5.2f} {m['mdd_avg']:+8.2f}% "
              f"{m['mdd_worst']:+9.2f}% {m['risk_adj']:>8.2f}")

    print()
    print("[차분 (N≥1 − N≥3)]")
    print(f"  avg:       {m_no['avg'] - m_with['avg']:+.2f}%p")
    print(f"  med:       {m_no['med'] - m_with['med']:+.2f}%p")
    print(f"  min:       {m_no['min'] - m_with['min']:+.2f}%p")
    print(f"  MDD worst: {m_no['mdd_worst'] - m_with['mdd_worst']:+.2f}%p")
    print(f"  risk_adj:  {m_no['risk_adj'] - m_with['risk_adj']:+.2f}")

    print()
    print(f"[N<3 종목 채택 활동]")
    print(f"  V75 + N≥3:  {nb_w}회 / {nt_w}회 (Top30 진입)  ← 0이 정상")
    print(f"  V75 + N≥1:  {nb_n}회 / {nt_n}회 (Top30 진입)")
    print(f"  → N<3 종목이 실제로 Top30에 들어간 빈도: {nb_n - nb_w}회 (filter 제거 시 추가됨)")


if __name__ == '__main__':
    main()
