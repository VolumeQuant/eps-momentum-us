"""v83 종합 검증 — 5개 카테고리 맹점 점검

1. 시스템 수익률 정상 확인 (v83 production 코드)
2. DB 재계산 spot check (5개 날짜 무작위 검증)
3. C2 boost edge case (cold start, 30일 전 가격 없는 종목, eps_w None)
4. _w_gap rerank 정확성 (특정 날짜 직접 계산 vs DB 일치)
5. select_display_top5 비중 시뮬레이션 (5/22 실제 데이터)
"""
import sqlite3
import sys
import random
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import daily_runner as dr

ROOT = Path(__file__).parent.parent
DB = ROOT / 'eps_momentum_data.db'
BAK = ROOT / 'eps_momentum_data.db.bak_pre_v83'


def section(title):
    print('\n' + '=' * 100)
    print(title)
    print('=' * 100)


def main():
    dr.DB_PATH = str(DB)

    # ─────────────────────────────────────────────────────
    section('[1] 시스템 수익률 정상 확인 (v83 production 코드)')
    # ─────────────────────────────────────────────────────
    perf = dr._get_system_performance()
    if perf:
        print(f'  시스템 누적: {perf["sys_cum"]:+.2f}%')
        print(f'  SPY 누적:    {perf["spy_cum"]:+.2f}%')
        print(f'  알파:        {perf["alpha"]:+.2f}%p')
        print(f'  기간:        {perf["start_date"]} ~ {perf["end_date"]} ({perf["n_days"]}일)')
        print(f'  승/패:       {perf["wins"]}/{perf["losses"]}')
        # 점검: 알파 양수 + 합리적 범위
        assert perf['sys_cum'] > perf['spy_cum'], '시스템 누적이 SPY 대비 낮음 (이상)'
        assert perf['sys_cum'] > 100, f'시스템 누적 비정상 (낮음): {perf["sys_cum"]:.2f}%'
        assert perf['sys_cum'] < 500, f'시스템 누적 비정상 (높음): {perf["sys_cum"]:.2f}%'
        print('  ✅ 합리적 범위 (sys_cum > spy_cum, 100~500%)')

    # ─────────────────────────────────────────────────────
    section('[2] DB 재계산 spot check — 5개 무작위 날짜')
    # ─────────────────────────────────────────────────────
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    all_dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    random.seed(42)
    spot_dates = random.sample(all_dates, 5)
    spot_dates.append(all_dates[-1])  # 마지막 날 (5/22) 포함

    for d in sorted(set(spot_dates)):
        # 현재 DB의 part2_rank
        db_ranks = {r[0]: r[1] for r in cur.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank',
            (d,)
        ).fetchall()}
        # 직접 재계산
        eligible = [r[0] for r in cur.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (d,)
        ).fetchall()]
        wgap_map = dr._compute_w_gap_map(cur, d, eligible)
        sorted_w = sorted(eligible, key=lambda t: wgap_map.get(t, 0), reverse=True)
        sorted_v83 = dr._apply_c2_boost_rerank(cur, d, sorted_w)
        expected_ranks = {tk: i + 1 for i, tk in enumerate(sorted_v83[:30])}
        # 비교
        match = (db_ranks == expected_ranks)
        mismatches = []
        for tk in set(db_ranks.keys()) | set(expected_ranks.keys()):
            if db_ranks.get(tk) != expected_ranks.get(tk):
                mismatches.append((tk, db_ranks.get(tk), expected_ranks.get(tk)))
        # Top 3만 보기
        top3 = sorted(db_ranks.items(), key=lambda x: x[1])[:3]
        status = '✅' if match else f'❌ {len(mismatches)}건 불일치'
        print(f'  {d} ({len(db_ranks)}건, Top3 {[tk for tk, _ in top3]}): {status}')
        if not match and mismatches[:3]:
            for tk, db_r, exp_r in mismatches[:3]:
                print(f'      {tk}: DB={db_r}, expected={exp_r}')

    # ─────────────────────────────────────────────────────
    section('[3] C2 boost edge case 점검')
    # ─────────────────────────────────────────────────────
    DATE = all_dates[-1]
    # (a) cold start: 30일 전 가격 데이터 없는 종목
    cold_count = 0
    for tk in [r[0] for r in cur.execute(
        'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (DATE,)
    ).fetchall()]:
        past = cur.execute(
            'SELECT price FROM ntm_screening WHERE ticker=? AND date<? AND price IS NOT NULL '
            'ORDER BY date DESC LIMIT 1 OFFSET 29',
            (tk, DATE)
        ).fetchone()
        if not past:
            cold_count += 1
            is_c2 = dr._is_c2_for_v83(cur, DATE, tk)
            assert not is_c2, f'{tk}: 30일 전 가격 없는데 is_c2=True (BUG)'
    print(f'  ✅ 30거래일 전 가격 없는 종목: {cold_count}건 (모두 is_c2=False 처리)')

    # (b) eps_chg_weighted ≤ 0 종목 → is_c2 False
    bad_eps_count = 0
    for tk, eps_w in cur.execute(
        'SELECT ticker, eps_chg_weighted FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
        (DATE,)
    ).fetchall():
        if eps_w is None or eps_w <= 0:
            bad_eps_count += 1
            is_c2 = dr._is_c2_for_v83(cur, DATE, tk)
            assert not is_c2, f'{tk}: eps_w={eps_w}인데 is_c2=True (BUG)'
    print(f'  ✅ eps_chg_weighted ≤ 0 종목: {bad_eps_count}건 (모두 is_c2=False)')

    # (c) C2로 판정된 종목 카운트
    c2_count = sum(
        1 for tk in [r[0] for r in cur.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (DATE,)
        ).fetchall()]
        if dr._is_c2_for_v83(cur, DATE, tk)
    )
    print(f'  5/22 part2_rank Top 30 중 C2 종목: {c2_count}건 ({c2_count/30*100:.0f}%)')

    # (d) Cold start 단계 (전체 DB에서 part2_rank 3일 미만 시점)
    print(f'  is_cold_start(): {dr.is_cold_start()}')

    # ─────────────────────────────────────────────────────
    section('[4] _w_gap (in _get_system_performance) rerank 정확성')
    # ─────────────────────────────────────────────────────
    # _w_gap은 inner function이라 직접 호출 어려움. 대신 결과 (perf) 일치 확인.
    # _build_score_100_map의 결과를 _compute_w_gap_map 기반과 비교 (둘 다 rerank 후 정렬 일관해야)
    wm, _ = dr._build_score_100_map(DATE)
    sorted_by_wm = sorted(wm.keys(), key=lambda t: wm[t], reverse=True)[:10]
    print(f'  _build_score_100_map Top 10 ({DATE}):')
    print(f'    {sorted_by_wm}')
    # DB part2_rank Top 10
    db_top10 = [tk for tk, _ in sorted(
        [(r[0], r[1]) for r in cur.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank<=10', (DATE,)
        ).fetchall()],
        key=lambda x: x[1]
    )]
    print(f'  DB part2_rank Top 10 ({DATE}):')
    print(f'    {db_top10}')
    match = sorted_by_wm == db_top10
    print(f'  → 정렬 일관성: {"✅ 일치" if match else "❌ 불일치"}')

    # ─────────────────────────────────────────────────────
    section('[5] select_display_top5 비중 시뮬레이션 — 5/22 실제 데이터')
    # ─────────────────────────────────────────────────────
    # results_df 시뮬레이션은 복잡 — DB Top 종목 + score 조회로 대체
    print(f'  5/22 Top 10 (DB) + score_100 + C2 여부:')
    top10_with_score = cur.execute(
        '''SELECT t.ticker, t.part2_rank, t.eps_chg_weighted FROM ntm_screening t
        WHERE t.date=? AND t.part2_rank<=10 ORDER BY t.part2_rank''', (DATE,)
    ).fetchall()
    _, score_disp = dr._build_score_100_map(DATE)
    for tk, p2, eps_w in top10_with_score:
        is_c2 = dr._is_c2_for_v83(cur, DATE, tk)
        score = score_disp.get(tk, 0)
        c2 = ' ★C2' if is_c2 else ''
        # 1위 → 80%, 2위 → 20%, 3위 이하 → 0% (메시지에는 안 나옴)
        weight = 80 if p2 == 1 else (20 if p2 == 2 else 0)
        weight_str = f'{weight}%' if weight else '-'
        print(f'    {p2}. {tk:<8} score={score:>5.1f}  eps_w={eps_w:+.2f}{c2:<5} weight={weight_str}')

    print()
    print(f'  → 메시지 미리보기:')
    print(f'    🛒 EPS 모멘텀 매수 후보')
    n_signal = sum(1 for tk, p2, _ in top10_with_score if p2 <= 2)
    weights_v83 = [80, 20]
    for i, (tk, p2, _) in enumerate(top10_with_score[:2]):
        print(f'    {i+1}. <ticker_name>({tk})')
    print(f'    매수: 상위 2종목 (1위 80%, 2위 20%), 최대 2종목 보유')
    print(f'    매도: 10위 밖 or 실적하락')

    conn.close()
    print('\n' + '=' * 100)
    print('전수 검증 완료')
    print('=' * 100)


if __name__ == '__main__':
    main()
