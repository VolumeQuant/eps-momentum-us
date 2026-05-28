"""segment fix BT 결과 검증 — MU 4/28 cr=14/15가 진짜인지

확인:
1. 4/28 종목별 adj_gap/conv_gap 분포 (MU 위/아래 종목 비교)
2. fwd_pe_chg 역산 정확성 (가격 vs NTM 비율로 직접 계산해서 비교)
3. baseline DB vs BT baseline DB 차이 (재계산 정확도)
"""
import sqlite3
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
import daily_runner as dr


def show_top20(db_path, label, today='2026-04-28'):
    """그날 cr 1~20 종목 + adj_gap + adj_score"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT composite_rank, ticker, adj_gap, score, adj_score, part2_rank,
               ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, price
        FROM ntm_screening
        WHERE date=? AND composite_rank IS NOT NULL
        ORDER BY composite_rank ASC
        LIMIT 20
    ''', (today,)).fetchall()
    conn.close()
    print(f'\n=== {label} {today} cr 1~20 ===')
    print(f'{"cr":<4} {"p2":<4} {"ticker":<8} {"adj_gap":<10} {"score":<8} {"adj_score":<10}')
    for r in rows:
        cr, tk, ag, sc, ascn, p2, *_ = r
        print(f'{cr:<4} {p2 or "-":<4} {tk:<8} {ag:<10.3f} {sc:<8.2f} {ascn:<10.2f}')
    return rows


def verify_fwd_pe_chg(db_path, ticker='MU', today='2026-04-28'):
    """fwd_pe_chg 역산 vs production 식 직접 계산 비교"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT date, ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
               price, adj_gap, score, adj_score
        FROM ntm_screening
        WHERE ticker=? AND date BETWEEN '2026-01-28' AND ?
        ORDER BY date
    ''', (ticker, today)).fetchall()
    conn.close()

    if len(rows) < 2:
        print(f'  {ticker}: 데이터 부족')
        return

    # 마지막 날 (today)
    today_row = rows[-1]
    d, tk, nc, n7, n30, n60, n90, p_now, ag_db, sc, ascn = today_row

    # 7d/30d/60d/90d 가격: 영업일 기준 7/30/60/90일 전
    # daily_runner는 가격을 같은 자리수에서 가져오는데, BT에선 dates 기반 lookback
    print(f'\n=== {ticker} {today} fwd_pe_chg 역산 검증 ===')
    print(f'NTM: nc={nc:.2f} n7={n7:.2f} n30={n30:.2f} n60={n60:.2f} n90={n90:.2f}')
    print(f'price today: ${p_now:.2f}')

    # production 식 — 가격은 N일 전 cycle row에서 가져와야 함
    # 각 lookback마다 row 찾기
    prices = {'7d': None, '30d': None, '60d': None, '90d': None}
    cycle_offsets = {'7d': 7, '30d': 30, '60d': 60, '90d': 90}
    for key, off in cycle_offsets.items():
        if len(rows) > off:
            prices[key] = rows[-1 - off][7]  # price index 7
        else:
            prices[key] = rows[0][7]  # earliest available

    print(f'prices (lookback): 7d=${prices["7d"]} 30d=${prices["30d"]} 60d=${prices["60d"]} 90d=${prices["90d"]}')

    # fwd_pe_now
    fwd_pe_now = p_now / nc if nc > 0 else 0
    print(f'fwd_pe_now = {fwd_pe_now:.4f}')

    # weighted average
    weights = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}
    ntm_keys = {'7d': n7, '30d': n30, '60d': n60, '90d': n90}
    weighted_sum = 0.0
    total_weight = 0.0
    print('  per-period:')
    for key, w in weights.items():
        ntm_val = ntm_keys[key]
        px = prices[key]
        if nc > 0 and ntm_val and ntm_val > 0 and px and px > 0:
            fwd_pe_then = px / ntm_val
            pe_chg = (fwd_pe_now - fwd_pe_then) / fwd_pe_then * 100
            weighted_sum += w * pe_chg
            total_weight += w
            print(f'    {key}: pe_then={fwd_pe_then:.4f}, pe_chg={pe_chg:+.2f}%, w={w}')

    fwd_pe_chg_direct = weighted_sum / total_weight if total_weight > 0 else 0
    print(f'fwd_pe_chg (direct): {fwd_pe_chg_direct:+.4f}')

    # 역산 비교
    # adj_gap = fwd_pe_chg × (1+dir_factor) × eps_q
    SEG_CAP = 100
    seg1 = max(-SEG_CAP, min(SEG_CAP, (nc - n7) / abs(n7) * 100))
    seg2 = max(-SEG_CAP, min(SEG_CAP, (n7 - n30) / abs(n30) * 100))
    seg3 = max(-SEG_CAP, min(SEG_CAP, (n30 - n60) / abs(n60) * 100))
    seg4 = max(-SEG_CAP, min(SEG_CAP, (n60 - n90) / abs(n90) * 100))
    direction = (seg1+seg2)/2 - (seg3+seg4)/2
    dir_factor = max(-0.3, min(0.3, direction / 30))
    min_seg = min(seg1, seg2, seg3, seg4)
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    denom = (1 + dir_factor) * eps_q
    fwd_pe_chg_inv = ag_db / denom if abs(denom) > 1e-6 else 0
    print(f'segments: [{seg1:.2f}, {seg2:.2f}, {seg3:.2f}, {seg4:.2f}]')
    print(f'direction={direction:.2f}, dir_factor={dir_factor:.3f}, min_seg={min_seg:.2f}, eps_q={eps_q:.3f}')
    print(f'fwd_pe_chg (inverse from adj_gap): {fwd_pe_chg_inv:+.4f}')
    print(f'difference: {abs(fwd_pe_chg_direct - fwd_pe_chg_inv):.4f}')


def show_mu_neighbors(db_path, label, today='2026-04-28'):
    """4/28 cr 10~20 부근 종목 + 가격 7일 변화율 + NTM 7일 변화율"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT composite_rank, ticker, adj_gap, score, adj_score, ntm_current,
               ntm_7d, price, rev_up30, num_analysts, rev_growth, ntm_90d
        FROM ntm_screening
        WHERE date=? AND composite_rank IS NOT NULL AND composite_rank <= 20
        ORDER BY composite_rank
    ''', (today,)).fetchall()

    # 7일 전 가격 가져오기
    price_7d_ago = {}
    for r in rows:
        tk = r[1]
        # 7 영업일 전: 4/17 (4/18-4/24 영업일 4/21-4/24)
        prev = cur.execute(
            'SELECT price FROM ntm_screening WHERE ticker=? AND date <= "2026-04-17" ORDER BY date DESC LIMIT 1',
            (tk,)
        ).fetchone()
        price_7d_ago[tk] = prev[0] if prev else None
    conn.close()

    print(f'\n=== {label} {today} cr 1~20 + 7일 가격/NTM 변화 ===')
    print(f'{"cr":<4} {"ticker":<8} {"adj_gap":<10} {"price":<10} {"7d ago":<10} {"px Δ%":<8} {"nc":<8} {"n7":<8} {"NTM Δ%":<8} {"conv_gap":<10}')
    for r in rows:
        cr, tk, ag, sc, ascn, nc, n7, px, ru, na, rg, n90 = r
        px_7d = price_7d_ago.get(tk)
        px_chg = (px / px_7d - 1) * 100 if px_7d else None
        ntm_chg = (nc / n7 - 1) * 100 if n7 else None
        # conv_gap (production 식)
        conv = dr._apply_conviction(ag, ru, na, nc, n90, rev_growth=rg)
        marker = ' ←' if tk == 'MU' else ''
        print(f'{cr:<4} {tk:<8} {ag:<10.3f} ${px:<8.2f} '
              f'${px_7d if px_7d else "-":<8} '
              f'{px_chg:<+8.2f} ' if px_chg is not None else f'{"-":<8} ',
              end='')
        print(f'{nc:<8.2f} {n7:<8.2f} {ntm_chg:<+8.2f} {conv:<10.3f}{marker}')


def main():
    print('=' * 80)
    print('Verify: BT result vs production DB + fwd_pe_chg 정확성')
    print('=' * 80)

    # 1. production DB의 4/28 cr 1~20
    show_top20('eps_momentum_data.db', 'production DB (현재)', '2026-04-28')

    # 2. baseline BT DB의 4/28 cr 1~20
    show_top20('eps_test_baseline.db', 'BT baseline (재계산)', '2026-04-28')

    # 3. γ'' BT DB의 4/28 cr 1~20
    show_top20('eps_test_gamma2.db', 'BT γ"" (cap segment 제외)', '2026-04-28')

    # 4. fwd_pe_chg 역산 vs 직접계산 (MU 케이스)
    verify_fwd_pe_chg('eps_momentum_data.db', 'MU', '2026-04-28')

    # 5. cr 1~20 종목 가격/NTM 변화 분포
    show_mu_neighbors('eps_test_gamma2.db', 'BT γ""', '2026-04-28')


if __name__ == '__main__':
    main()
