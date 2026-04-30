"""SNDK 4/29 v3 재적용 + 4/30 part2_rank 재계산.

배경: cron이 4/29 v1 fix 상태에서 돌아서 4/30 메시지는 비정확했음.
이제 4/29 v3 다시 적용 후 4/30 part2_rank 재계산 (3일 가중 윈도우 갱신).
4/30 row의 score/adj_gap/cr는 그대로 유지 (yfinance 4/30 close 데이터 valid).
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

SNDK_429 = {
    'ntm_current': 107.69,
    'ntm_7d': 97.6,
    'ntm_30d': 68.0,
    'ntm_60d': 60.0,
    'ntm_90d': 21.24,
}


def main():
    dr.DB_PATH = str(DB)
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    print('=' * 80)
    print('SNDK 4/29 v3 재적용 + 4/30 part2_rank 재계산')
    print('=' * 80)

    # 1) yfinance SNDK 가격
    sndk = yf.Ticker('SNDK')
    hist = sndk.history(period='1y', auto_adjust=False)
    hist_dt = hist.index.tz_localize(None) if hist.index.tz else hist.index
    cutoff = datetime(2026, 4, 29)
    hist_until = hist[hist_dt <= cutoff]
    p_now = float(hist_until['Close'].iloc[-1])
    hist_dt_filt = hist_until.index.tz_localize(None) if hist_until.index.tz else hist_until.index

    prices = {}
    for days, key in [(7,'7d'),(30,'30d'),(60,'60d'),(90,'90d')]:
        target = cutoff - timedelta(days=days)
        idx = (hist_dt_filt - target).map(lambda x: abs(x.days)).argmin()
        prices[key] = float(hist_until['Close'].iloc[idx])

    # 2) SNDK 4/29 v3 적용
    print('\n[1] SNDK 4/29 ntm + score/adj_gap 재계산')
    nc, n7, n30, n60, n90 = (SNDK_429[k] for k in ['ntm_current','ntm_7d','ntm_30d','ntm_60d','ntm_90d'])

    SEG_CAP = 100
    seg1 = max(-SEG_CAP, min(SEG_CAP, (nc - n7) / abs(n7) * 100))
    seg2 = max(-SEG_CAP, min(SEG_CAP, (n7 - n30) / abs(n30) * 100))
    seg3 = max(-SEG_CAP, min(SEG_CAP, (n30 - n60) / abs(n60) * 100))
    seg4 = max(-SEG_CAP, min(SEG_CAP, (n60 - n90) / abs(n90) * 100))
    score = seg1 + seg2 + seg3 + seg4

    fwd_pe_now = p_now / nc
    weights = {'7d':0.4,'30d':0.3,'60d':0.2,'90d':0.1}
    ntm_map = {'7d':n7,'30d':n30,'60d':n60,'90d':n90}
    total = sum(w * ((fwd_pe_now - prices[k]/ntm_map[k]) / (prices[k]/ntm_map[k]) * 100) for k, w in weights.items())
    fwd_pe_chg = total / sum(weights.values())

    cap_hit = any(abs(s) >= SEG_CAP for s in [seg1, seg2, seg3, seg4])
    if cap_hit:
        df = 0.3
    else:
        direction = (seg1 + seg2) / 2 - (seg3 + seg4) / 2
        df_raw = max(-0.3, min(0.3, direction / 30))
        if fwd_pe_chg > 0 and direction < 0:
            df = -df_raw
        else:
            df = df_raw

    valid = [s for s in [seg1,seg2,seg3,seg4] if abs(s) < SEG_CAP]
    min_seg = min(valid) if valid else 0
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    adj_score = score * (1 + df)
    adj_gap = fwd_pe_chg * (1 + df) * eps_q

    print(f'  fwd_pe_chg = {fwd_pe_chg:+.2f}%')
    print(f'  score={score:.2f}, adj_score={adj_score:.2f}, adj_gap={adj_gap:+.2f}')

    cur.execute(
        'UPDATE ntm_screening SET ntm_current=?, ntm_7d=?, ntm_30d=?, ntm_60d=?, ntm_90d=?, '
        'score=?, adj_score=?, adj_gap=? WHERE ticker="SNDK" AND date="2026-04-29"',
        (nc, n7, n30, n60, n90, score, adj_score, adj_gap)
    )

    # 3) 4/29 composite_rank + part2_rank 재정렬
    print('\n[2] 4/29 composite_rank + part2_rank 재정렬')
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

    tickers = [t for t, _ in elig_conv]
    wmap_429 = dr._compute_w_gap_map(cur, '2026-04-29', tickers)
    sorted_w = sorted(tickers, key=lambda t: wmap_429.get(t, 0), reverse=True)
    cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date="2026-04-29"')
    for rk, tk in enumerate(sorted_w[:30], 1):
        cur.execute(
            'UPDATE ntm_screening SET part2_rank=? WHERE date="2026-04-29" AND ticker=?',
            (rk, tk)
        )
    conn.commit()

    # 4) 4/30 part2_rank 재계산 (4/29 변경됐으므로 3일 가중 윈도우 영향)
    print('\n[3] 4/30 part2_rank 재계산 (4/29 변경 반영)')
    tickers_430 = [r[0] for r in cur.execute(
        'SELECT ticker FROM ntm_screening WHERE date="2026-04-30" AND composite_rank IS NOT NULL'
    ).fetchall()]
    wmap_430 = dr._compute_w_gap_map(cur, '2026-04-30', tickers_430)
    sorted_w_430 = sorted(tickers_430, key=lambda t: wmap_430.get(t, 0), reverse=True)
    cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date="2026-04-30"')
    for rk, tk in enumerate(sorted_w_430[:30], 1):
        cur.execute(
            'UPDATE ntm_screening SET part2_rank=? WHERE date="2026-04-30" AND ticker=?',
            (rk, tk)
        )
    conn.commit()

    # 5) 검증
    print('\n[4] 검증')
    print('\n4/29 Top 12:')
    for r in cur.execute(
        'SELECT part2_rank, ticker, composite_rank, adj_gap FROM ntm_screening '
        'WHERE date="2026-04-29" AND part2_rank<=12 ORDER BY part2_rank'
    ).fetchall():
        marker = ' ←' if r[1] == 'SNDK' else ''
        print(f'  p2={r[0]:>2} {r[1]:<6} cr={r[2]:>3} adj_gap={r[3]:+.2f}{marker}')

    print('\n4/30 Top 12:')
    for r in cur.execute(
        'SELECT part2_rank, ticker, composite_rank, adj_gap FROM ntm_screening '
        'WHERE date="2026-04-30" AND part2_rank<=12 ORDER BY part2_rank'
    ).fetchall():
        marker = ' ←' if r[1] == 'SNDK' else ''
        print(f'  p2={r[0]:>2} {r[1]:<6} cr={r[2]:>3} adj_gap={r[3]:+.2f}{marker}')

    print('\n전 일자 누락 체크:')
    for d in ['2026-04-22','2026-04-23','2026-04-24','2026-04-27','2026-04-28','2026-04-29','2026-04-30']:
        cnt = cur.execute('SELECT COUNT(*) FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,)).fetchone()[0]
        elig = cur.execute('SELECT COUNT(*) FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL', (d,)).fetchone()[0]
        print(f'  {d}: eligible={elig}, p2_rank={cnt}')

    conn.close()
    print('\n✓ 완료')


if __name__ == '__main__':
    main()
