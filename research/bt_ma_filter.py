"""MA 필터 변형 BT — price > MA120 (fallback MA60) vs MA60/MA120/strict/no MA

배경:
  production v80.10c는 get_part2_candidates()에서 price > MA120 (NULL이면 MA60 fallback)
  필터를 적용. 사용자 검증 요청: MA60이나 다른 선이 더 좋은 성과를 내는지?

DB에 저장된 컬럼: ma60, ma120 (둘 다 daily snapshot).
  → MA60 / MA120 조합 변형은 즉시 가능
  → MA20 / MA200 는 daily snapshot 65일 한계로 본 BT에서는 제외

비교 변형:
  current        — price > MA120 (NULL이면 MA60 fallback)            [production]
  ma60_only      — price > MA60                                     [더 짧은 추세]
  ma120_strict   — price > MA120 (NULL이면 제외, fallback 없음)        [엄격]
  ma60_and_ma120 — price > MA60 AND price > MA120                   [둘 다 통과]
  no_ma          — MA 필터 제거                                       [컨트롤]

방법:
  1. production DB 복제 (변형별)
  2. 각 일자별: 전체 ntm_screening에서 모든 필터(MA 변형 포함) 다시 적용
     → composite_rank, part2_rank 재계산하여 DB에 덮어쓰기
  3. bt_breakout_hold.simulate_hold(hold_days=0, entry=3, exit=10, slots=3) 실행
  4. 100 seed × 3 starts paired (v80.10 검증과 동일)
"""
import sys
import shutil
import sqlite3
import random
import statistics
import time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import daily_runner as dr
import bt_breakout_hold as bth

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'
GRID.mkdir(exist_ok=True)

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10

# v80.10c production params
ENTRY_TOP = 3
EXIT_TOP = 10
MAX_SLOTS = 3
HOLD_DAYS = 0  # v80.10c: ⏸️ 룰 제거


def ma_pass(price, ma60, ma120, variant):
    """변형별 MA 필터 통과 여부"""
    if price is None or price <= 0:
        return False
    if variant == 'no_ma':
        return True
    if variant == 'ma60_only':
        return ma60 is not None and price > ma60
    if variant == 'ma120_strict':
        return ma120 is not None and price > ma120
    if variant == 'ma60_and_ma120':
        ok60 = ma60 is not None and price > ma60
        ok120 = ma120 is not None and price > ma120
        return ok60 and ok120
    if variant == 'current':
        # MA120 우선, NULL이면 MA60 fallback
        if ma120 is not None:
            return price > ma120
        return ma60 is not None and price > ma60
    raise ValueError(f'unknown variant: {variant}')


def regenerate_for_variant(test_db, variant):
    """test_db에서 variant별 MA 필터를 적용해 composite_rank, part2_rank 재계산"""
    conn = sqlite3.connect(test_db)
    cur = conn.cursor()

    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening ORDER BY date'
    ).fetchall()]

    # 변형 적용을 위해 모든 date의 composite_rank, part2_rank를 일단 NULL로 리셋
    cur.execute('UPDATE ntm_screening SET composite_rank=NULL, part2_rank=NULL')
    conn.commit()

    for today in dates:
        # 1) 전체 ticker 로드 (제외 사유 판별용)
        rows = cur.execute('''
            SELECT ticker, adj_score, adj_gap, eps_chg_weighted, price, ma60, ma120,
                   ntm_current, ntm_90d, rev_growth, num_analysts, rev_up30, rev_down30,
                   operating_margin, gross_margin, free_cashflow, roe
            FROM ntm_screening WHERE date=?
        ''', (today,)).fetchall()
        if not rows:
            continue

        eligible = []
        for r in rows:
            (tk, asc, ag, eps_w, px, m60, m120,
             nc, n90, rg, na, ru, rd, om, gm, fcf, roe) = r

            # 기본 필터
            if asc is None or asc <= 9:
                continue
            if ag is None:
                continue
            # fwd_pe > 0 ↔ price > 0 AND ntm_current > 0
            if px is None or px < 10:
                continue
            if nc is None or nc <= 0:
                continue
            if eps_w is None or eps_w <= 0:
                continue
            # MA 필터 (변형)
            if not ma_pass(px, m60, m120, variant):
                continue
            # rev_growth >= 10% (가용 시)
            if rg is None:
                continue
            if rg < 0.10:
                continue
            # 애널리스트 필터
            if na is None or na < 3:
                continue
            # v80.8 rev_up30 ≥ 3
            if ru is None or ru < 3:
                continue
            # 하향 과다 (>30%)
            total = (ru or 0) + (rd or 0)
            if total > 0 and (rd or 0) / total > 0.3:
                continue
            # 구조적 저마진 (OM<10% AND GM<30%)
            if om is not None and gm is not None and om < 0.10 and gm < 0.30:
                continue
            # OP < 5%
            if om is not None and om < 0.05:
                continue
            # FCF<0 AND ROE<0
            if fcf is not None and roe is not None and fcf < 0 and roe < 0:
                continue

            eligible.append({
                'ticker': tk, 'adj_gap': ag, 'rev_up30': ru, 'num_analysts': na,
                'ntm_current': nc, 'ntm_90d': n90, 'rev_growth': rg, 'price': px,
            })

        # min_seg < -2 제외 (segs 계산)
        def _min_seg(tk_row):
            r2 = cur.execute(
                'SELECT ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d '
                'FROM ntm_screening WHERE date=? AND ticker=?',
                (today, tk_row['ticker'])
            ).fetchone()
            if not r2 or any(x is None for x in r2):
                return 0
            nc, n7, n30, n60, n90 = r2
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b is not None and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a - b) / abs(b) * 100)))
                else:
                    segs.append(0)
            return min(segs)

        eligible = [e for e in eligible if _min_seg(e) >= -2]

        if not eligible:
            continue

        # conviction adj_gap (composite_rank용 — 오름차순, 가장 음수가 1위)
        for e in eligible:
            e['_conv_gap'] = dr._apply_conviction(
                e['adj_gap'], e['rev_up30'], e['num_analysts'],
                e['ntm_current'], e['ntm_90d'], rev_growth=e['rev_growth']
            )
        eligible.sort(key=lambda e: e['_conv_gap'])

        # composite_rank 저장
        for i, e in enumerate(eligible, 1):
            cur.execute(
                'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                (i, today, e['ticker'])
            )
        conn.commit()

        # w_gap 계산 (production 함수 사용)
        tickers = [e['ticker'] for e in eligible]
        wmap = dr._compute_w_gap_map(cur, today, tickers)

        # w_gap 내림차순 Top 30 → part2_rank
        sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        top30 = sorted_w[:30]
        for rk, tk in enumerate(top30, 1):
            cur.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rk, today, tk)
            )
        conn.commit()

    conn.close()


def run_bt(db_path):
    """변형 DB로 BT 실행 (100 seed × 3 starts paired)"""
    bth.DB_PATH = db_path
    dates, data, price_series = bth.load_data_ext()
    if len(dates) <= MIN_HOLD_DAYS:
        return None

    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        seed_rets = []
        for sd in chosen:
            r = bth.simulate_hold(
                dates, data, price_series, hold_days=HOLD_DAYS,
                entry_top=ENTRY_TOP, exit_top=EXIT_TOP,
                max_slots=MAX_SLOTS, start_date=sd
            )
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            seed_rets.append(r['total_return'])
        seed_avgs.append(sum(seed_rets) / len(seed_rets))
    return {
        'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs,
        'dates_n': len(dates), 'starts_n': len(eligible_starts),
    }


VARIANTS = [
    'current',         # production: MA120 with MA60 fallback
    'ma60_only',
    'ma120_strict',
    'ma60_and_ma120',
    'no_ma',
]


def main():
    print('=' * 110)
    print('MA 필터 변형 BT — production (MA120 fallback MA60) vs 4 변형')
    print(f'DB: {DB_ORIGINAL}')
    print(f'params: entry={ENTRY_TOP}, exit={EXIT_TOP}, slots={MAX_SLOTS}, hold={HOLD_DAYS} (v80.10c)')
    print(f'seeds: {N_SEEDS} × samples {SAMPLES_PER_SEED} = {N_SEEDS*SAMPLES_PER_SEED} 시뮬/변형')
    print('=' * 110)

    all_results = {}
    for variant in VARIANTS:
        db = GRID / f'{variant}.db'
        print(f'\n[{variant}] DB 복제 + regenerate...')
        t0 = time.time()
        shutil.copy(DB_ORIGINAL, db)
        regenerate_for_variant(db, variant)
        print(f'  regenerate: {time.time()-t0:.1f}s')

        t1 = time.time()
        res = run_bt(db)
        if res is None:
            print('  데이터 부족 — skip')
            continue
        all_results[variant] = res
        avg = sum(res['rets']) / len(res['rets'])
        med = sorted(res['rets'])[len(res['rets']) // 2]
        worst_mdd = min(res['mdds'])
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        print(f'  BT: {time.time()-t1:.1f}s | avg={avg:+6.2f}% med={med:+6.2f}% '
              f'mdd={worst_mdd:+6.2f}% risk_adj={risk_adj:+.2f}')

    # 결과 분포
    print()
    print('=' * 110)
    print(f'결과 분포 ({N_SEEDS * SAMPLES_PER_SEED}개 시뮬/변형)')
    print('=' * 110)
    print(f'{"variant":<18} {"avg":>9} {"median":>9} {"std":>6} '
          f'{"p25":>9} {"p75":>9} {"min":>9} {"max":>9} {"MDD":>8} {"risk_adj":>9}')
    print('-' * 110)
    for v in VARIANTS:
        if v not in all_results:
            continue
        r = all_results[v]
        rets = sorted(r['rets'])
        n = len(rets)
        avg = sum(rets) / n
        med = rets[n // 2]
        std = statistics.pstdev(rets)
        p25 = rets[n // 4]
        p75 = rets[3 * n // 4]
        mdd = min(r['mdds'])
        ra = avg / abs(mdd) if mdd < 0 else 0
        marker = ' ★' if v == 'current' else '  '
        print(f'{marker}{v:<16} {avg:+8.2f}% {med:+8.2f}% {std:>5.2f} '
              f'{p25:+8.2f}% {p75:+8.2f}% {min(rets):+8.2f}% {max(rets):+8.2f}% '
              f'{mdd:+7.2f}% {ra:+8.2f}')

    # paired current 대비
    if 'current' in all_results:
        print()
        print('=' * 110)
        print('current (production) 대비 paired 비교 (seed별 동일 시작일)')
        print('=' * 110)
        base = all_results['current']['seed_avgs']
        print(f'{"vs":<18} {"avg lift":>10} {"min lift":>10} {"max lift":>10} '
              f'{"#wins":>7} {"#losses":>8} {"#ties":>6}')
        print('-' * 80)
        for v in VARIANTS:
            if v == 'current' or v not in all_results:
                continue
            new = all_results[v]['seed_avgs']
            lifts = [b - a for a, b in zip(base, new)]
            wins = sum(1 for l in lifts if l > 0)
            losses = sum(1 for l in lifts if l < 0)
            ties = sum(1 for l in lifts if l == 0)
            avg_lift = sum(lifts) / len(lifts)
            verdict = '✓ 우월' if wins > losses + 30 else '✗ 열세' if losses > wins + 30 else '~ 동등'
            print(f'  {v:<16} {avg_lift:+9.2f}%p {min(lifts):+9.2f}%p {max(lifts):+9.2f}%p '
                  f'{wins:>6} {losses:>7} {ties:>5}  {verdict}')


if __name__ == '__main__':
    main()
