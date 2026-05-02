"""사분면별 진짜 알파 측정 — production 룰 시뮬 trade 결과 분류.

방법:
  1. baseline DB로 multistart simulate (production 룰 3/8/3)
  2. 각 trade의 entry_date에 그 종목의 fwd_pe_chg/direction 부호 분류
  3. 사분면별 trade 수익률 평균/승률/표본 측정

이게 "production 룰 적용했을 때 어느 사분면이 진짜 알파인가" 정확 측정.
"""
import sqlite3
import sys
import statistics
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB = Path('/tmp/db_pre_gamma.db') if Path('/tmp/db_pre_gamma.db').exists() else ROOT / 'eps_momentum_data.db'
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


def classify_trade(cur, entry_date, ticker):
    """trade entry 시점 종목 사분면 분류"""
    row = cur.execute('''
        SELECT ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, adj_gap
        FROM ntm_screening WHERE date=? AND ticker=?
    ''', (entry_date, ticker)).fetchone()
    if not row:
        return None
    nc, n7, n30, n60, n90, ag = row
    segs = fmt_segments(nc, n7, n30, n60, n90)
    if segs is None or ag is None:
        return None
    cap_hit = any(abs(s) >= SEG_CAP for s in segs)
    direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
    df = max(-0.3, min(0.3, direction / 30))
    min_seg = min(segs)
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    denom = (1 + df) * eps_q
    if abs(denom) < 1e-6:
        return None
    fwd_pe_chg = ag / denom

    if cap_hit:
        return 'CAP_발동', fwd_pe_chg, direction
    if fwd_pe_chg < 0 and direction > 0:
        return 'C1_저평가+가속', fwd_pe_chg, direction
    if fwd_pe_chg < 0 and direction < 0:
        return 'C2_저평가+둔화', fwd_pe_chg, direction
    if fwd_pe_chg > 0 and direction > 0:
        return 'C3_고평가+가속', fwd_pe_chg, direction
    if fwd_pe_chg > 0 and direction < 0:
        return 'C4_고평가+둔화', fwd_pe_chg, direction
    return 'C0_zero', fwd_pe_chg, direction


def main():
    print('=' * 100)
    print(f'사분면별 진짜 알파 — production 룰 시뮬 trade 분류')
    print(f'DB: {DB}')
    print('=' * 100)

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    bts2.DB_PATH = str(DB)
    dates, data = bts2.load_data()
    start_dates = dates[:5]

    # 모든 시작일 trade 수집
    all_trades = []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        for t in r['trades']:
            t['start_sim'] = sd
            all_trades.append(t)
    print(f'\n총 trade (5 시작일 합산): {len(all_trades)}건')

    # trade 사분면 분류
    by_case = defaultdict(list)
    for t in all_trades:
        cls = classify_trade(cur, t['entry_date'], t['ticker'])
        if cls is None:
            by_case['UNKNOWN'].append(t)
            continue
        case, fwd, direction = cls
        t['fwd_pe_chg'] = fwd
        t['direction'] = direction
        by_case[case].append(t)

    # 사분면별 통계
    print()
    print('=' * 100)
    print('사분면별 trade 통계 (entry 시점 분류)')
    print('=' * 100)
    print(f'{"Case":<22} {"N":>4} {"avg":>8} {"med":>7} {"min":>7} {"max":>8} '
          f'{"win%":>5} {"avg_hold":>8} {"avg_ag":>7} {"avg_dir":>7}')
    print('-' * 100)

    for case in sorted(by_case.keys()):
        trades = by_case[case]
        rets = [t['return'] for t in trades]
        if not rets:
            continue
        n = len(trades)
        avg = sum(rets) / n
        med = sorted(rets)[n // 2]
        wins = sum(1 for r in rets if r > 0)
        # 보유 일수 (영업일 기준 근사)
        holds = []
        for t in trades:
            try:
                e_idx = dates.index(t['entry_date'])
                x_idx = dates.index(t['exit_date'])
                holds.append(x_idx - e_idx)
            except ValueError:
                pass
        avg_hold = sum(holds) / len(holds) if holds else 0
        ags = [t.get('fwd_pe_chg', 0) for t in trades if 'fwd_pe_chg' in t]
        dirs = [t.get('direction', 0) for t in trades if 'direction' in t]
        avg_ag = sum(ags)/len(ags) if ags else 0
        avg_dir = sum(dirs)/len(dirs) if dirs else 0
        print(f'  {case:<20} {n:>4} {avg:+7.2f}% {med:+6.2f}% {min(rets):+6.2f}% {max(rets):+7.2f}% '
              f'{wins*100/n:>4.0f}% {avg_hold:>7.1f}일 {avg_ag:+6.1f} {avg_dir:+6.1f}')

    # 종목별 trade 표본
    print()
    print('=' * 100)
    print('주요 종목별 trade 분포')
    print('=' * 100)
    by_ticker = defaultdict(list)
    for t in all_trades:
        by_ticker[t['ticker']].append(t)
    sorted_tk = sorted(by_ticker.items(), key=lambda x: -len(x[1]))
    for tk, ts in sorted_tk[:10]:
        rets = [t['return'] for t in ts]
        avg = sum(rets) / len(rets)
        print(f'  {tk:<6}: {len(ts)} trade, avg {avg:+.2f}%, '
              f'returns: {[f"{r:+.1f}" for r in rets]}')

    # 사분면별 종목 분포
    print()
    print('=' * 100)
    print('사분면별 자주 거래된 종목 Top 5')
    print('=' * 100)
    for case in sorted(by_case.keys()):
        if not by_case[case]:
            continue
        ticker_count = defaultdict(int)
        for t in by_case[case]:
            ticker_count[t['ticker']] += 1
        top = sorted(ticker_count.items(), key=lambda x: -x[1])[:5]
        print(f'  {case}: {top}')

    conn.close()


if __name__ == '__main__':
    main()
