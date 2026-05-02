"""SNDK 4/29 보정 v2 — adj_gap 정확히 재계산 (가격 lookback 포함).

이전 v1: score 비례로 adj_gap 추정 (부정확)
v2: yfinance 가격 + 보정된 ntm_*d → fwd_pe_chg 정확히 계산
"""
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import yfinance as yf
import daily_runner as dr

ROOT = Path(__file__).parent.parent
DB = ROOT / 'eps_momentum_data.db'

SNDK_NTM = {
    'ntm_current': 107.69,
    'ntm_7d': 97.60,
    'ntm_30d': 75.83,
    'ntm_60d': 67.28,
    'ntm_90d': 21.24,
}

TODAY = datetime(2026, 4, 29)


def main():
    dr.DB_PATH = str(DB)
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    print('=' * 80)
    print('SNDK 4/29 adj_gap 정확 재계산')
    print('=' * 80)

    # 1) yfinance에서 SNDK 가격 1년치 가져옴
    print('\n[1] yfinance SNDK 1년치 가격')
    sndk = yf.Ticker('SNDK')
    hist = sndk.history(period='1y', auto_adjust=False)
    if hist.empty:
        print('❌ yfinance 데이터 없음')
        return
    hist_dt = hist.index.tz_localize(None) if hist.index.tz else hist.index
    print(f'  데이터 기간: {hist_dt[0].date()} ~ {hist_dt[-1].date()}, {len(hist)}일')

    # 2) 4/29 시점의 lookback 가격 추출 (calendar days)
    # 단 4/29 시점에는 4/29 close가 latest. yfinance 데이터에서 4/29 row까지만 사용.
    cutoff = TODAY
    hist_until = hist[hist_dt <= cutoff]
    if hist_until.empty:
        print('❌ 4/29 이전 데이터 없음')
        return
    p_now = float(hist_until['Close'].iloc[-1])
    actual_today = hist_until.index[-1]
    print(f'  4/29 가격: ${p_now:.2f} ({actual_today.date()})')

    prices = {}
    hist_dt_filt = hist_until.index.tz_localize(None) if hist_until.index.tz else hist_until.index
    for days, key in [(7, '7d'), (30, '30d'), (60, '60d'), (90, '90d')]:
        target = TODAY - timedelta(days=days)
        idx = (hist_dt_filt - target).map(lambda x: abs(x.days)).argmin()
        prices[key] = float(hist_until['Close'].iloc[idx])
        print(f'  {key}: ${prices[key]:.2f} ({hist_dt_filt[idx].date()})')

    # 3) fwd_pe_chg 계산 (보정된 ntm_*d 사용)
    print('\n[2] fwd_pe_chg 계산 (보정된 ntm_*d 사용)')
    nc = SNDK_NTM['ntm_current']
    fwd_pe_now = p_now / nc
    print(f'  fwd_pe_now = ${p_now:.2f} / ${nc:.2f} = {fwd_pe_now:.2f}')

    weights = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}
    weighted_sum = 0.0
    total_weight = 0.0
    for key, w in weights.items():
        ntm_val = SNDK_NTM[f'ntm_{key}']
        if ntm_val > 0 and prices[key] > 0:
            fwd_pe_then = prices[key] / ntm_val
            pe_chg = (fwd_pe_now - fwd_pe_then) / fwd_pe_then * 100
            weighted_sum += w * pe_chg
            total_weight += w
            print(f'  {key}: fwd_pe_then = {prices[key]:.2f}/{ntm_val:.2f} = {fwd_pe_then:.2f}, '
                  f'chg = {pe_chg:+.2f}%, w={w}')
    fwd_pe_chg = weighted_sum / total_weight
    print(f'  fwd_pe_chg (가중) = {fwd_pe_chg:+.2f}%')

    # 4) score, direction, dir_factor, eps_q 계산 (β1+opt4 v80.5 로직)
    print('\n[3] score / direction / dir_factor / eps_q')
    SEG_CAP = 100
    n7, n30, n60, n90 = SNDK_NTM['ntm_7d'], SNDK_NTM['ntm_30d'], SNDK_NTM['ntm_60d'], SNDK_NTM['ntm_90d']
    seg1 = max(-SEG_CAP, min(SEG_CAP, (nc - n7) / abs(n7) * 100))
    seg2 = max(-SEG_CAP, min(SEG_CAP, (n7 - n30) / abs(n30) * 100))
    seg3 = max(-SEG_CAP, min(SEG_CAP, (n30 - n60) / abs(n60) * 100))
    seg4 = max(-SEG_CAP, min(SEG_CAP, (n60 - n90) / abs(n90) * 100))
    print(f'  segs: {seg1:.1f} {seg2:.1f} {seg3:.1f} {seg4:.1f}')
    score = seg1 + seg2 + seg3 + seg4

    cap_hit = any(abs(s) >= SEG_CAP for s in [seg1, seg2, seg3, seg4])
    if cap_hit:
        direction = 9.0  # β1
        df = 0.3
    else:
        direction = (seg1 + seg2) / 2 - (seg3 + seg4) / 2
        df_raw = max(-0.3, min(0.3, direction / 30))
        # opt4: C4 sign flip
        if fwd_pe_chg > 0 and direction < 0:
            df = -df_raw
        else:
            df = df_raw
    print(f'  cap_hit={cap_hit}, direction={direction}, df={df}')

    valid_segs = [s for s in [seg1,seg2,seg3,seg4] if abs(s) < SEG_CAP]
    min_seg = min(valid_segs) if valid_segs else 0
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    print(f'  min_seg={min_seg:.1f}, eps_q={eps_q:.3f}')

    adj_score = score * (1 + df)
    adj_gap = fwd_pe_chg * (1 + df) * eps_q

    print(f'\n  score = {score:.2f}')
    print(f'  adj_score = score × (1 + df) = {score:.2f} × {1+df:.2f} = {adj_score:.2f}')
    print(f'  adj_gap = fwd_pe_chg × (1+df) × eps_q = {fwd_pe_chg:.2f} × {1+df:.2f} × {eps_q:.3f} = {adj_gap:+.2f}')

    # 5) DB에 update
    print('\n[4] DB 업데이트')
    cur.execute(
        'UPDATE ntm_screening SET score=?, adj_score=?, adj_gap=? WHERE ticker="SNDK" AND date="2026-04-29"',
        (score, adj_score, adj_gap)
    )

    # 6) 4/29 composite_rank 재정렬
    print('\n[5] 4/29 composite_rank 재정렬')
    rows = cur.execute(
        'SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, rev_growth '
        'FROM ntm_screening WHERE date="2026-04-29" AND composite_rank IS NOT NULL'
    ).fetchall()
    elig_conv = []
    for tk, ag, ru_, na, nc_, n90_, rg_ in rows:
        cg = dr._apply_conviction(ag, ru_, na, nc_, n90_, rev_growth=rg_)
        if cg is not None:
            elig_conv.append((tk, cg))
    elig_conv.sort(key=lambda x: x[1])
    cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date="2026-04-29"')
    for i, (tk, _) in enumerate(elig_conv):
        cur.execute(
            'UPDATE ntm_screening SET composite_rank=? WHERE date="2026-04-29" AND ticker=?',
            (i + 1, tk)
        )
    print(f'  {len(elig_conv)}종목 재정렬')

    # 7) part2_rank 재계산
    print('\n[6] 4/29 part2_rank 재계산')
    tickers = [t for t, _ in elig_conv]
    wmap = dr._compute_w_gap_map(cur, '2026-04-29', tickers)
    sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
    top30 = sorted_w[:30]
    cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date="2026-04-29"')
    for rk, tk in enumerate(top30, 1):
        cur.execute(
            'UPDATE ntm_screening SET part2_rank=? WHERE date="2026-04-29" AND ticker=?',
            (rk, tk)
        )

    conn.commit()

    # 8) 검증
    print('\n[7] SNDK 최종')
    r = cur.execute(
        'SELECT part2_rank, composite_rank, score, adj_score, adj_gap, ntm_current, ntm_90d '
        'FROM ntm_screening WHERE ticker="SNDK" AND date="2026-04-29"'
    ).fetchone()
    p2, cr, sc, asc, ag, nc_, n90_ = r
    print(f'  SNDK: p2={p2}, cr={cr}, score={sc:.2f}, adj_score={asc:.2f}, adj_gap={ag:+.2f}')
    print(f'    ntm_current={nc_}, ntm_90d={n90_}')

    print('\n[8] 4/29 Top 10')
    for r in cur.execute(
        'SELECT part2_rank, ticker, composite_rank, adj_gap FROM ntm_screening '
        'WHERE date="2026-04-29" AND part2_rank<=10 ORDER BY part2_rank'
    ).fetchall():
        print(f'  p2={r[0]} {r[1]:<6} cr={r[2]:>3} adj_gap={r[3]:+.2f}')

    conn.close()
    print('\n✓ 정확 재계산 완료')


if __name__ == '__main__':
    main()
