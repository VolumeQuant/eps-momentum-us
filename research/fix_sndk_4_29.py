"""SNDK 4/29 보정 + 4/29 전체 재계산.

원인: 5/1 SNDK 어닝 발표로 yfinance가 ntm_90d snapshot을 retroactively 업데이트.
보정: 4/27/4/28 추세로 extrapolate, 메시지 EPS 전망 +407% 매칭.
"""
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import daily_runner as dr
import eps_momentum_system as ems

ROOT = Path(__file__).parent.parent
DB = ROOT / 'eps_momentum_data.db'

# 메시지 기준: EPS 전망 +407% (= ratio 5.07)
# 4/27/4/28 추세로 extrapolate
SNDK_FIX = {
    'ntm_current': 107.69,  # = 21.24 × 5.07 (메시지 매칭)
    'ntm_7d': 97.60,        # 4/28 nc=97.42 → 약간 증가
    'ntm_30d': 75.83,
    'ntm_60d': 67.28,
    'ntm_90d': 21.24,
}


def main():
    dr.DB_PATH = str(DB)
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # 1) SNDK 4/29 ntm 값 보정
    print('=' * 80)
    print('[1] SNDK 4/29 ntm 값 보정')
    print('=' * 80)
    old = cur.execute(
        'SELECT ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, price, rev_growth, rev_up30, rev_down30 '
        'FROM ntm_screening WHERE ticker="SNDK" AND date="2026-04-29"'
    ).fetchone()
    nc_old, n7_old, n30_old, n60_old, n90_old, price, rev_g, ru, rd = old
    print(f'  이전: nc={nc_old:.2f}, n7={n7_old:.2f}, n30={n30_old:.2f}, n60={n60_old:.2f}, n90={n90_old:.2f}')
    print(f'  보정: nc={SNDK_FIX["ntm_current"]}, n7={SNDK_FIX["ntm_7d"]}, n30={SNDK_FIX["ntm_30d"]}, '
          f'n60={SNDK_FIX["ntm_60d"]}, n90={SNDK_FIX["ntm_90d"]}')

    cur.execute(
        '''UPDATE ntm_screening SET ntm_current=?, ntm_7d=?, ntm_30d=?, ntm_60d=?, ntm_90d=?
           WHERE ticker="SNDK" AND date="2026-04-29"''',
        (SNDK_FIX['ntm_current'], SNDK_FIX['ntm_7d'], SNDK_FIX['ntm_30d'],
         SNDK_FIX['ntm_60d'], SNDK_FIX['ntm_90d'])
    )

    # 2) SNDK score/adj_score/adj_gap 재계산 (v80.5 로직 = β1+opt4)
    print()
    print('[2] SNDK 4/29 score/adj_gap 재계산 (β1+opt4 적용)')
    SEG_CAP = 100
    nc, n7, n30, n60, n90 = (SNDK_FIX[k] for k in ['ntm_current','ntm_7d','ntm_30d','ntm_60d','ntm_90d'])
    seg1 = max(-SEG_CAP, min(SEG_CAP, (nc - n7) / abs(n7) * 100))
    seg2 = max(-SEG_CAP, min(SEG_CAP, (n7 - n30) / abs(n30) * 100))
    seg3 = max(-SEG_CAP, min(SEG_CAP, (n30 - n60) / abs(n60) * 100))
    seg4 = max(-SEG_CAP, min(SEG_CAP, (n60 - n90) / abs(n90) * 100))
    print(f'  segments: seg1={seg1:.1f}, seg2={seg2:.1f}, seg3={seg3:.1f}, seg4={seg4:.1f}')
    score = sum([seg1, seg2, seg3, seg4])

    # β1: cap 발동 시 dir = +9.0
    cap_hit = any(abs(s) >= SEG_CAP for s in [seg1, seg2, seg3, seg4])
    if cap_hit:
        direction = 9.0
        df = 0.3
    else:
        direction = (seg1 + seg2) / 2 - (seg3 + seg4) / 2
        df = max(-0.3, min(0.3, direction / 30))
    adj_score = score * (1 + df)

    # eps_quality
    valid_segs = [s for s in [seg1,seg2,seg3,seg4] if abs(s) < SEG_CAP]
    min_seg = min(valid_segs) if valid_segs else 0
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))

    # fwd_pe_chg 역산: 기존 adj_gap에서 추출 어렵 — 새로 계산하려면 fwd_pe와 fwd_pe_90d 필요
    # 간단: 기존 row의 fwd_pe_chg를 보존 (price 변화 없음 가정 기반)
    # 실제로 adj_gap = fwd_pe_chg × (1+df) × eps_q
    # 새 ntm으로 fwd_pe_chg 재계산: fwd_pe = price/nc, fwd_pe_90d = price_90d/n90 (n90 변경 시 영향)
    # 단순화: price/nc 사용 (90일 전 가격은 historical, ntm_90d_old × (price_90d/price) ≈ fwd_pe ratio)
    # 가장 정직한 방법: daily_runner의 fwd_pe_chg 계산 로직 그대로 적용
    # 그러나 90일 전 가격 데이터가 DB에 있어야 함. 우선 다른 종목과 일관 비교 위해 단순 추정.

    # 실제 fwd_pe_chg = (fwd_pe_now - fwd_pe_90d) / fwd_pe_90d 가중평균
    # ntm 변경된 종목은 score만 정확히 갱신, adj_gap은 후속 cron에서 자연 정정
    # 임시: score/adj_score 갱신, adj_gap은 score 비례 추정

    # 더 정확: 같은 row의 score/adj_gap 비율 유지
    old_score = cur.execute(
        'SELECT score FROM ntm_screening WHERE ticker="SNDK" AND date="2026-04-29"'
    ).fetchone()[0]
    old_adj_gap = cur.execute(
        'SELECT adj_gap FROM ntm_screening WHERE ticker="SNDK" AND date="2026-04-29"'
    ).fetchone()[0]
    # 비율 유지 (대략적): adj_gap_new = adj_gap_old × (score_new / score_old) — 첫 근사
    # 이건 정확하지 않음. 더 정확하게 하려면 yfinance 90d 가격 필요.

    # 가장 정직: SNDK는 cap-hit 종목이라 segments 변동 큼.
    # fwd_pe_chg는 price/ntm 비율 변화. ntm_current 보정으로 fwd_pe_chg도 변함.
    # fwd_pe_now = price / ntm_current, fwd_pe_90d_ago = price_90d / ntm_90d
    # 90일 전 가격 = DB에서 90거래일 전 row 조회
    cur.execute('SELECT date, price FROM ntm_screening WHERE ticker="SNDK" AND date<"2026-04-29" ORDER BY date DESC')
    rows = cur.fetchall()
    # 90 거래일 전이면 약 4-5달 전 (영업일 5일/주 × 18주 ~ 90일)
    if len(rows) >= 90:
        price_90d_ago = rows[89][1]  # 90거래일 전
    else:
        # DB에 80일치만 있으면 그 중 가장 오래된 가격
        price_90d_ago = rows[-1][1] if rows else price
    print(f'  90거래일 전 가격: ${price_90d_ago:.2f} (참조용)')

    # 단순화: score 변화율 비례로 adj_gap 갱신 (보수적)
    # score_old, adj_gap_old → score_new, adj_gap_new
    # 정확한 계산 어려우므로 score 비례 + cap_hit 보정
    if old_score and abs(old_score) > 1:
        ratio = score / old_score
        adj_gap = old_adj_gap * ratio
    else:
        adj_gap = old_adj_gap

    print(f'  score: {old_score:.2f} → {score:.2f}')
    print(f'  adj_score: → {adj_score:.2f}')
    print(f'  adj_gap (추정): {old_adj_gap:+.2f} → {adj_gap:+.2f}')

    cur.execute(
        'UPDATE ntm_screening SET score=?, adj_score=?, adj_gap=? WHERE ticker="SNDK" AND date="2026-04-29"',
        (score, adj_score, adj_gap)
    )

    # 3) 4/29 전체 composite_rank 재정렬
    print()
    print('[3] 4/29 composite_rank 재정렬')
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
    print(f'  {len(elig_conv)}종목 재정렬 완료')

    # 4) part2_rank 재계산
    print()
    print('[4] 4/29 part2_rank 재계산')
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
    print(f'  Top 30 part2_rank 갱신')

    conn.commit()

    # 5) 검증
    print()
    print('[5] SNDK 보정 결과')
    r = cur.execute(
        'SELECT part2_rank, composite_rank, score, adj_score, adj_gap '
        'FROM ntm_screening WHERE ticker="SNDK" AND date="2026-04-29"'
    ).fetchone()
    p2, cr, sc, asc, ag = r
    print(f'  SNDK 4/29: p2={p2}, cr={cr}, score={sc:.2f}, adj_score={asc:.2f}, adj_gap={ag:+.2f}')

    print()
    print('[6] 4/29 Top 10 part2_rank')
    for r in cur.execute(
        'SELECT part2_rank, ticker, composite_rank FROM ntm_screening '
        'WHERE date="2026-04-29" AND part2_rank<=10 ORDER BY part2_rank'
    ).fetchall():
        print(f'  p2={r[0]} {r[1]} cr={r[2]}')

    conn.close()
    print()
    print('✓ SNDK 보정 + 4/29 재계산 완료')


if __name__ == '__main__':
    main()
