"""seg-style fwd_pe_chg BT — 시간축 일관성 검증

⚠️ PRODUCTION SAFETY:
- 원본 DB(eps_momentum_data.db)는 절대 수정하지 않음
- pe_seg_dbs/ 하위 복사본만 사용

핵심 가설:
  Production fwd_pe_chg는 누적 lookback (4개 모두 현재 vs T일 전)
  Score (seg1~seg4)는 인접 구간

  → fwd_pe_chg를 seg-style (인접 구간)로 바꾸면 score와 시간축 일관됨
  → 4D BT의 long-tail 알파가 시간축 mismatch artefact인지 검증

비교 변형:
  C_prod  cumulative (0.4, 0.3, 0.2, 0.1)  ← production
  C_4dbest cumulative (0.1, 0.1, 0.3, 0.5)  ← 4D BT Top 1 (참고)
  S_prod  seg-style  (0.4, 0.3, 0.2, 0.1)  ← 같은 weight, 인접 구간
  S_uniform seg-style (0.25, 0.25, 0.25, 0.25) ← score와 완전 일관 (균등)
  S_long   seg-style (0.1, 0.2, 0.3, 0.4)   ← seg-style long-tail
  S_4dbest seg-style (0.1, 0.1, 0.3, 0.5)   ← 4D BT best 분포 seg-style 적용
  S_recent seg-style (0.5, 0.3, 0.1, 0.1)   ← seg-style 최근 강조
"""
import sqlite3
import shutil
import sys
import statistics
import math
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import daily_runner as dr
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'pe_seg_dbs'
GRID.mkdir(exist_ok=True)
SEG_CAP = 100


def fmt_segments(nc, n7, n30, n60, n90):
    if not all(x is not None and x != 0 for x in (n7, n30, n60, n90)):
        return None
    return tuple(max(-SEG_CAP, min(SEG_CAP, v)) for v in (
        (nc - n7) / abs(n7) * 100,
        (n7 - n30) / abs(n30) * 100,
        (n30 - n60) / abs(n60) * 100,
        (n60 - n90) / abs(n90) * 100,
    ))


def calc_gamma_opt4(segs, fwd_pe_chg):
    """β1 (cap) + opt4 (C4 sign flip) — 현재 production 동일."""
    cap_hit = any(abs(s) >= SEG_CAP for s in segs)
    if cap_hit:
        df = 0.3  # β1 (v80.4+ — cap 시 +0.3 보너스)
    else:
        direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
        df_raw = max(-0.3, min(0.3, direction / 30))
        if fwd_pe_chg is not None and fwd_pe_chg > 0 and direction < 0:
            df = -df_raw  # opt4
        else:
            df = df_raw
    valid = [s for s in segs if abs(s) < SEG_CAP]
    min_seg = min(valid) if valid else 0
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return df, eps_q


def build_price_history(cur):
    rows = cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL AND price > 0'
    ).fetchall()
    by_ticker = {}
    for tk, d, p in rows:
        by_ticker.setdefault(tk, []).append((d, p))
    for tk in by_ticker:
        by_ticker[tk].sort(key=lambda x: x[0])
    return by_ticker


def find_price_then(history, today_str, n_days):
    target = (datetime.strptime(today_str, '%Y-%m-%d') - timedelta(days=n_days)).date()
    today_d = datetime.strptime(today_str, '%Y-%m-%d').date()
    best = None
    best_diff = None
    for d, p in history:
        d_obj = datetime.strptime(d, '%Y-%m-%d').date()
        if d_obj > today_d:
            continue
        diff = abs((d_obj - target).days)
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best = p
    return best


def calc_fwd_pe_cumulative(px_now, nc, px_then_map, ntm_map, weights):
    """현재 production: 누적 lookback (모두 현재 vs T일 전)."""
    if px_now <= 0 or nc <= 0:
        return None
    fwd_pe_now = px_now / nc
    weighted_sum = 0.0
    total = 0.0
    for key in ('7d', '30d', '60d', '90d'):
        pt = px_then_map.get(key)
        nt = ntm_map.get(key)
        if pt and pt > 0 and nt and nt > 0:
            fwd_pe_then = pt / nt
            pe_chg = (fwd_pe_now - fwd_pe_then) / fwd_pe_then * 100
            weighted_sum += weights[key] * pe_chg
            total += weights[key]
    return weighted_sum / total if total > 0 else None


def calc_fwd_pe_seg_style(px_now, nc, px_then_map, ntm_map, weights):
    """seg-style: 인접 구간 4개 (0~7, 7~30, 30~60, 60~90)."""
    pe_now = px_now / nc if (px_now > 0 and nc > 0) else None
    pe_7d = (px_then_map['7d'] / ntm_map['7d']) if (
        px_then_map.get('7d') and ntm_map.get('7d', 0) > 0 and px_then_map['7d'] > 0
    ) else None
    pe_30d = (px_then_map['30d'] / ntm_map['30d']) if (
        px_then_map.get('30d') and ntm_map.get('30d', 0) > 0 and px_then_map['30d'] > 0
    ) else None
    pe_60d = (px_then_map['60d'] / ntm_map['60d']) if (
        px_then_map.get('60d') and ntm_map.get('60d', 0) > 0 and px_then_map['60d'] > 0
    ) else None
    pe_90d = (px_then_map['90d'] / ntm_map['90d']) if (
        px_then_map.get('90d') and ntm_map.get('90d', 0) > 0 and px_then_map['90d'] > 0
    ) else None

    pairs = []
    if pe_now is not None and pe_7d is not None and pe_7d != 0:
        pairs.append(('s1', (pe_now - pe_7d) / abs(pe_7d) * 100, weights['s1']))
    if pe_7d is not None and pe_30d is not None and pe_30d != 0:
        pairs.append(('s2', (pe_7d - pe_30d) / abs(pe_30d) * 100, weights['s2']))
    if pe_30d is not None and pe_60d is not None and pe_60d != 0:
        pairs.append(('s3', (pe_30d - pe_60d) / abs(pe_60d) * 100, weights['s3']))
    if pe_60d is not None and pe_90d is not None and pe_90d != 0:
        pairs.append(('s4', (pe_60d - pe_90d) / abs(pe_90d) * 100, weights['s4']))

    if not pairs:
        return None
    weighted_sum = sum(w * v for _, v, w in pairs)
    total = sum(w for _, _, w in pairs)
    return weighted_sum / total if total > 0 else None


def regenerate(test_db, mode, weights):
    """mode: 'cumulative' or 'seg'"""
    original = dr.DB_PATH
    dr.DB_PATH = str(test_db)
    fn = calc_fwd_pe_cumulative if mode == 'cumulative' else calc_fwd_pe_seg_style
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        price_history = build_price_history(cur)
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]
        for today in dates:
            rows = cur.execute('''
                SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, price,
                       rev_up30, num_analysts, rev_growth
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today,)).fetchall()
            new_data = []
            for r in rows:
                tk, nc, n7, n30, n60, n90, px_now, ru, na, rg = r
                segs = fmt_segments(nc, n7, n30, n60, n90)
                if segs is None or px_now is None or px_now <= 0 or nc is None or nc <= 0:
                    continue
                hist = price_history.get(tk, [])
                px_then = {}
                for n_days, key in [(7, '7d'), (30, '30d'), (60, '60d'), (90, '90d')]:
                    px_then[key] = find_price_then(hist, today, n_days)
                ntm_map = {'7d': n7, '30d': n30, '60d': n60, '90d': n90}
                fwd_pe_chg = fn(px_now, nc, px_then, ntm_map, weights)
                if fwd_pe_chg is None:
                    continue
                df_n, eq_n = calc_gamma_opt4(segs, fwd_pe_chg)
                ag_n = fwd_pe_chg * (1 + df_n) * eq_n
                new_data.append((tk, ag_n, ru, na, nc, n90, rg))

            for tk, ag, *_ in new_data:
                cur.execute('UPDATE ntm_screening SET adj_gap=? WHERE date=? AND ticker=?',
                            (ag, today, tk))

            elig_conv = []
            for tk, ag, ru, na, nc, n90, rg in new_data:
                cg = dr._apply_conviction(ag, ru, na, nc, n90, rev_growth=rg)
                if cg is not None:
                    elig_conv.append((tk, cg))
            elig_conv.sort(key=lambda x: x[1])
            new_cr = {tk: i + 1 for i, (tk, _) in enumerate(elig_conv)}
            cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
            for tk, cr in new_cr.items():
                cur.execute(
                    'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                    (cr, today, tk)
                )

            tickers = list(new_cr.keys())
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
    finally:
        dr.DB_PATH = original


def multistart(db_path, n_starts=12):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    rets, mdds = [], []
    for sd in dates[:n_starts]:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds


VARIANTS = [
    ('★ C_prod (production cumulative)',  'cumulative', {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}),
    ('  C_4dbest (cumulative long-tail)', 'cumulative', {'7d': 0.1, '30d': 0.1, '60d': 0.3, '90d': 0.5}),
    ('  S_prod (seg-style 0.4/0.3/0.2/0.1)', 'seg', {'s1': 0.4, 's2': 0.3, 's3': 0.2, 's4': 0.1}),
    ('  S_uniform (seg-style 균등)',       'seg', {'s1': 0.25, 's2': 0.25, 's3': 0.25, 's4': 0.25}),
    ('  S_long (seg-style 0.1/0.2/0.3/0.4)', 'seg', {'s1': 0.1, 's2': 0.2, 's3': 0.3, 's4': 0.4}),
    ('  S_4dbest (seg-style 0.1/0.1/0.3/0.5)', 'seg', {'s1': 0.1, 's2': 0.1, 's3': 0.3, 's4': 0.5}),
    ('  S_recent (seg-style 0.5/0.3/0.1/0.1)', 'seg', {'s1': 0.5, 's2': 0.3, 's3': 0.1, 's4': 0.1}),
]


def calc_metrics(rets, mdds):
    n = len(rets)
    avg = sum(rets) / n
    med = sorted(rets)[n // 2]
    worst_mdd = min(mdds)
    risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
    return {'avg': avg, 'med': med, 'min': min(rets), 'max': max(rets),
            'std': statistics.pstdev(rets), 'mdd': worst_mdd, 'risk': risk_adj}


def main():
    print('=' * 110)
    print('seg-style fwd_pe_chg BT — 시간축 일관성 검증 (12시작일)')
    print('=' * 110)

    rows = []
    for name, mode, w in VARIANTS:
        slug = ''.join(c if c.isalnum() else '_' for c in name)[:30]
        db = GRID / f'{slug}.db'
        if not db.exists():
            shutil.copy(DB_ORIGINAL, db)
            regenerate(db, mode, w)
        rets, mdds = multistart(db, n_starts=12)
        m = calc_metrics(rets, mdds)
        rows.append({'name': name, 'mode': mode, **m})
        print(f'  {name:<42} avg={m["avg"]:+7.2f}% MDD={m["mdd"]:+6.2f}% risk={m["risk"]:.2f}')

    base = rows[0]
    print()
    print('=' * 110)
    print('비교 (★ C_prod 기준)')
    print('=' * 110)
    print(f'{"변형":<42} {"avg":>8} {"MDD":>7} {"risk":>5} {"ΔRet":>9} {"ΔMDD":>9}')
    print('-' * 110)
    for r in rows:
        d_ret = r['avg'] - base['avg']
        d_mdd = r['mdd'] - base['mdd']
        marker = '★' if 'C_prod' in r['name'] else (' ✓' if d_ret >= 1 else '  ')
        print(f'{marker} {r["name"]:<40} {r["avg"]:+7.2f}% {r["mdd"]:+6.2f}% '
              f'{r["risk"]:>4.2f} {d_ret:+8.2f}%p {d_mdd:+8.2f}%p')


if __name__ == '__main__':
    main()
