"""S2: 파라미터 그리드 서치 (DB part2_rank 직접 사용)

데이터 소스: DB 원본 part2_rank (100% 정확)
변형: Entry × Exit × Slot 조합
검증: Multistart로 시작일 효과 제거
"""
import sqlite3
from collections import defaultdict

DB_PATH = 'eps_momentum_data.db'


def load_data():
    """DB에서 시뮬레이션 필요 데이터 로드"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    data = {}
    for d in dates:
        rows = cursor.execute('''
            SELECT ticker, part2_rank, price, composite_rank,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append((a - b) / abs(b) * 100)
                else:
                    segs.append(0)
            segs = [max(-100, min(100, s)) for s in segs]
            min_seg = min(segs) if segs else 0
            data[d][tk] = {
                'p2': r[1], 'price': r[2],
                'comp_rank': r[3], 'min_seg': min_seg,
            }
    conn.close()
    return dates, data


def simulate(dates_all, data, entry_top, exit_top, max_slots, start_date=None):
    """단일 시뮬레이션"""
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    portfolio = {}
    daily_returns = []
    trades = []
    consecutive = defaultdict(int)

    if start_date:
        for d in dates_all:
            if d >= start_date:
                break
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    consecutive[tk] = consecutive.get(tk, 0) + 1

    for di, today in enumerate(dates):
        if today not in data:
            continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}

        new_consecutive = defaultdict(int)
        for tk in rank_map:
            new_consecutive[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consecutive

        # v80.7 (2026-05-02): day_ret을 이탈/진입 전 어제 portfolio 기준 계산.
        # 이전 코드: 이탈→진입→day_ret 순서 → 진입한 종목의 매수 전 변동(어제→오늘)이
        # day_ret에 잘못 누적 + 이탈한 종목의 마지막 변동(어제→오늘)이 day_ret에서 누락.
        # 사용자 운영(메시지 받고 그 종가에 애프터마켓 매수/매도)과 일치하려면 어제 portfolio
        # 기준 day_ret 계산이 정확. (메모리 v80.6 rollback 참조)
        day_ret = 0
        if portfolio:
            for tk in portfolio:
                price = today_data.get(tk, {}).get('price')
                if price and di > 0:
                    prev = data.get(dates[di - 1], {}).get(tk, {}).get('price')
                    if prev and prev > 0:
                        day_ret += (price - prev) / prev * 100
            day_ret /= len(portfolio)
            daily_returns.append(day_ret)
        else:
            daily_returns.append(0)

        # 이탈
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            price = today_data.get(tk, {}).get('price')
            should_exit = False
            if rank is None or rank > exit_top:
                should_exit = True
            if min_seg < -2:
                should_exit = True
            if should_exit and price:
                entry_price = portfolio[tk]['entry_price']
                ret = (price - entry_price) / entry_price * 100
                trades.append({
                    'ticker': tk, 'entry_date': portfolio[tk]['entry_date'],
                    'exit_date': today, 'entry_price': entry_price,
                    'exit_price': price, 'return': ret,
                })
                exited.append(tk)
        for tk in exited:
            del portfolio[tk]

        # 진입
        vacancies = max_slots - len(portfolio)
        if vacancies > 0:
            candidates = []
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > entry_top:
                    break
                if tk in portfolio:
                    continue
                if consecutive.get(tk, 0) < 3:
                    continue
                min_seg = today_data.get(tk, {}).get('min_seg', 0)
                if min_seg < 0:
                    continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0:
                    candidates.append((tk, price))
            for tk, price in candidates[:vacancies]:
                portfolio[tk] = {'entry_price': price, 'entry_date': today}

    cum_ret = 1.0
    peak = 1.0
    max_dd = 0
    for dr_ in daily_returns:
        cum_ret *= (1 + dr_ / 100)
        peak = max(peak, cum_ret)
        dd = (cum_ret - peak) / peak * 100
        max_dd = min(max_dd, dd)

    closed = [t['return'] for t in trades]
    n_trades = len(closed)
    win_rate = (sum(1 for t in closed if t > 0) / n_trades * 100
                if n_trades > 0 else 0)

    # 미실현 수익
    unrealized = []
    for tk, pos in portfolio.items():
        last_price = data.get(dates[-1], {}).get(tk, {}).get('price')
        if last_price and pos['entry_price']:
            unrealized.append((last_price - pos['entry_price']) / pos['entry_price'] * 100)

    return {
        'total_return': round((cum_ret - 1) * 100, 2),
        'max_dd': round(max_dd, 2),
        'n_trades': n_trades,
        'n_open': len(portfolio),
        'win_rate': round(win_rate, 1),
        'avg_unrealized': round(sum(unrealized) / len(unrealized), 2) if unrealized else 0,
        'best_trade': round(max(closed), 2) if closed else 0,
        'worst_trade': round(min(closed), 2) if closed else 0,
        'trades': trades,
    }


def sample_test(dates, data):
    """표본 테스트: 현재 설정으로 sanity check"""
    print("=" * 70)
    print("표본 테스트: E5/X12/S3 (현재 설정) sanity check")
    print("=" * 70)

    r = simulate(dates, data, entry_top=5, exit_top=12, max_slots=3)
    print(f"\n수익률: {r['total_return']:+.2f}%")
    print(f"MDD: {r['max_dd']:.2f}%")
    print(f"거래 횟수: {r['n_trades']}")
    print(f"승률: {r['win_rate']:.0f}%")
    print(f"오픈 포지션: {r['n_open']}")
    print(f"평균 미실현: {r['avg_unrealized']:+.2f}%")

    print(f"\n거래 내역:")
    for t in r['trades']:
        print(f"  {t['ticker']:6s} {t['entry_date']} -> {t['exit_date']} "
              f"${t['entry_price']:>7.1f} -> ${t['exit_price']:>7.1f} = {t['return']:+6.1f}%")

    # sanity check: 이전 backtest_v2.py와 비교
    print(f"\n예상값 (이전 backtest_v2 결과):")
    print(f"  수익률: +30.7% / MDD: -9.9% / 거래: 6")
    print(f"\n현재 결과 일치: {r['total_return']:.1f}% vs 30.7%")

    return abs(r['total_return'] - 30.7) < 2.0


def grid_search(dates, data):
    """전체 그리드 + multistart"""
    print("\n" + "=" * 70)
    print("그리드 서치: Entry × Exit × Slot")
    print("=" * 70)

    entry_range = [2, 3, 4, 5]
    exit_range = [10, 12, 14, 15, 18, 20]
    slot_range = [2, 3, 4]

    results = []
    for entry in entry_range:
        for exit_th in exit_range:
            if exit_th <= entry:
                continue
            for slots in slot_range:
                r = simulate(dates, data, entry, exit_th, slots)
                r['config'] = f"E{entry}/X{exit_th}/S{slots}"
                r['entry'] = entry
                r['exit'] = exit_th
                r['slots'] = slots
                results.append(r)

    print(f"\n{'Config':>12s} {'Ret':>7s} {'MDD':>7s} {'Trd':>4s} {'WR':>4s} "
          f"{'Open':>4s} {'AvgUnr':>7s}")
    print("-" * 60)
    sorted_r = sorted(results, key=lambda x: -x['total_return'])
    for r in sorted_r[:15]:
        print(f"{r['config']:>12s} {r['total_return']:+6.1f}% {r['max_dd']:+6.1f}% "
              f"{r['n_trades']:>4d} {r['win_rate']:>3.0f}% {r['n_open']:>4d} "
              f"{r['avg_unrealized']:+6.1f}%")

    return results


def multistart_test(dates, data):
    """Multistart: 시작일 변동"""
    print("\n" + "=" * 70)
    print("Multistart Test: 시작일 변동 효과")
    print("=" * 70)

    # 시작일 후보: 3일 검증 후 ~ 끝 전 5일
    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    # 조밀하게: 매일 (가능한 경우)
    samples = start_dates[::2]  # 격일

    print(f"\n시작일 샘플 ({len(samples)}개): {samples[0]} ~ {samples[-1]}")

    # 비교할 변형 (S2 인사이트 기반 추가 변형)
    variants = [
        ('A: E5/X12/S3 (현재)', 5, 12, 3),
        ('B: E3/X12/S3', 3, 12, 3),
        ('D: E3/X10/S3', 3, 10, 3),
        ('F: E3/X12/S2', 3, 12, 2),
        ('G: E3/X10/S2 (D+F조합)', 3, 10, 2),
        ('H: E3/X12/S4', 3, 12, 4),
        ('I: E3/X8/S2 (매우 빠른 이탈)', 3, 8, 2),
        ('J: E3/X10/S4', 3, 10, 4),
    ]

    print(f"\n시작일별 수익률 (각 변형):")
    print(f"{'시작일':<12s} ", end='')
    for name, *_ in variants:
        print(f"{name[:14]:>14s}", end='')
    print()
    print("-" * (12 + 14 * len(variants)))

    results_by_variant = {name: [] for name, *_ in variants}
    for sd in samples:
        print(f"{sd:<12s} ", end='')
        for name, e, x, s in variants:
            r = simulate(dates, data, e, x, s, start_date=sd)
            print(f"{r['total_return']:+8.1f}% (T{r['n_trades']:>2d})", end='')
            results_by_variant[name].append(r['total_return'])
        print()

    print("\n=== 통계 (Multistart 평균) ===")
    print(f"{'Variant':<22s} {'평균':>7s} {'중앙값':>7s} {'표준편차':>8s} {'최저':>7s} {'최고':>7s}")
    print("-" * 65)
    for name in results_by_variant:
        rets = sorted(results_by_variant[name])
        avg = sum(rets) / len(rets)
        median = rets[len(rets) // 2]
        std = (sum((r - avg) ** 2 for r in rets) / len(rets)) ** 0.5
        print(f"{name:<22s} {avg:+6.2f}% {median:+6.2f}% {std:>7.2f} "
              f"{min(rets):+6.1f}% {max(rets):+6.1f}%")

    return results_by_variant


def main():
    print("S2: 파라미터 그리드 서치")
    print("데이터 소스: DB 원본 part2_rank (100% 정확)\n")

    dates, data = load_data()
    print(f"기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")

    # Step 1: 표본 테스트
    if not sample_test(dates, data):
        print("\n[!] 표본 테스트 실패 — 디버깅 필요")
        return

    print("\n[OK] 표본 테스트 통과")

    # Step 2: 전체 그리드
    grid_results = grid_search(dates, data)

    # Step 3: Multistart
    ms_results = multistart_test(dates, data)


if __name__ == '__main__':
    main()
