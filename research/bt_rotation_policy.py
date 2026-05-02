"""회전 정책 BT — 약한 보유 종목 정리 옵션 비교

5/1 메시지 발견된 약점: FIVE 18거래일 +1.76% (같은 기간 SNDK +67%) — Top 3 진입 후
4~8위에 머물면서 슬롯 차지하는 약한 종목 기회 비용 큼.

비교 변형 (모두 진입 Top 3 / 슬롯 3 / min_seg<-2 이탈 / Breakout Hold strict):
  current        — 이탈 part2_rank > 8 [현재 production]
  B_exit5        — 이탈 > 5 (강화)
  B_exit6        — 이탈 > 6
  B_exit7        — 이탈 > 7
  C_relative_5   — 보유 part2_rank - 신규 1위 part2_rank > 5 차이면 교체
  C_relative_3   — 차이 > 3
  D_time_10_3    — 10일 보유 + 수익 < 3% 면 매도
  D_time_15_5    — 15일 보유 + 수익 < 5% 면 매도

시작일: 초기 6일 (50거래일+ 보장)
"""
import sqlite3
import sys
import statistics
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

ROOT = Path(__file__).parent.parent
DB = ROOT / 'eps_momentum_data.db'


def load_data():
    conn = sqlite3.connect(DB)
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


def simulate(dates_all, data, *, entry_top=3, exit_top=8, max_slots=3,
             relative_diff=None, time_days=None, time_min_ret=None,
             start_date=None):
    """변형 BT 시뮬레이션 — v80.7 day_ret 순서 적용 (어제 portfolio 기준)

    Args:
        relative_diff: 보유 p2 - 신규1위 p2 차이가 이 값보다 크면 매도 (C 정책)
        time_days, time_min_ret: 보유 N일 + 수익률 X% 미만이면 매도 (D 정책)
    """
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    portfolio = {}  # tk -> {entry_price, entry_idx}
    daily_returns = []
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

        # day_ret 먼저 (어제 portfolio 기준, v80.7)
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

        # 신규 1위 part2_rank (relative_diff 정책용)
        top1_p2 = None
        for tk, rk in sorted(rank_map.items(), key=lambda x: x[1]):
            if tk not in portfolio:
                top1_p2 = rk
                break

        # 이탈 결정
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            price = today_data.get(tk, {}).get('price')
            should_exit = False

            # 기본: rank > exit_top OR min_seg < -2
            if rank is None or rank > exit_top:
                should_exit = True
            if min_seg < -2:
                should_exit = True

            # C: 상대 매도 (보유 p2 - 신규 1위 p2 > relative_diff)
            if not should_exit and relative_diff is not None and rank is not None and top1_p2 is not None:
                if rank - top1_p2 > relative_diff:
                    should_exit = True

            # D: 시간 기반 (N일 보유 + 수익률 < X%)
            if not should_exit and time_days is not None and time_min_ret is not None:
                held = di - portfolio[tk]['entry_idx']
                if held >= time_days and price:
                    ret = (price - portfolio[tk]['entry_price']) / portfolio[tk]['entry_price'] * 100
                    if ret < time_min_ret:
                        should_exit = True

            if should_exit:
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
                portfolio[tk] = {'entry_price': price, 'entry_idx': di}

    cum_ret = 1.0
    peak = 1.0
    max_dd = 0
    for dr_ in daily_returns:
        cum_ret *= (1 + dr_ / 100)
        peak = max(peak, cum_ret)
        dd = (cum_ret - peak) / peak * 100
        max_dd = min(max_dd, dd)

    return {
        'total_return': round((cum_ret - 1) * 100, 2),
        'max_dd': round(max_dd, 2),
    }


def run_multistart(dates, data, params, start_dates):
    rets, mdds = [], []
    for sd in start_dates:
        r = simulate(dates, data, start_date=sd, **params)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds


def main():
    print('=' * 110)
    print('회전 정책 BT — 약한 보유 종목 정리 옵션 비교 (6시작일 multistart)')
    print('=' * 110)

    dates, data = load_data()
    start_dates = dates[2:8]
    print(f'시작일: {start_dates[0]} ~ {start_dates[-1]} ({len(start_dates)}개, 모두 50거래일+)')

    variants = [
        ('current (exit>8)',          {'exit_top': 8}),
        ('B exit>5',                  {'exit_top': 5}),
        ('B exit>6',                  {'exit_top': 6}),
        ('B exit>7',                  {'exit_top': 7}),
        ('C relative diff>5',         {'exit_top': 8, 'relative_diff': 5}),
        ('C relative diff>3',         {'exit_top': 8, 'relative_diff': 3}),
        ('C relative diff>7',         {'exit_top': 8, 'relative_diff': 7}),
        ('D time>=10d & ret<3%',      {'exit_top': 8, 'time_days': 10, 'time_min_ret': 3}),
        ('D time>=15d & ret<5%',      {'exit_top': 8, 'time_days': 15, 'time_min_ret': 5}),
        ('D time>=10d & ret<5%',      {'exit_top': 8, 'time_days': 10, 'time_min_ret': 5}),
        ('D time>=20d & ret<10%',     {'exit_top': 8, 'time_days': 20, 'time_min_ret': 10}),
    ]

    rows = []
    for name, params in variants:
        rets, mdds = run_multistart(dates, data, params, start_dates)
        avg = sum(rets) / len(rets)
        med = sorted(rets)[len(rets) // 2]
        std = statistics.pstdev(rets)
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        rows.append({
            'name': name, 'rets': rets, 'mdds': mdds,
            'avg': avg, 'med': med, 'std': std,
            'min': min(rets), 'max': max(rets),
            'worst_mdd': worst_mdd, 'risk_adj': risk_adj,
        })

    print()
    print(f'{"변형":<28} {"avg":>9} {"med":>9} {"std":>5} {"min":>9} {"max":>9} '
          f'{"worstMDD":>10} {"risk_adj":>9}')
    print('-' * 100)
    for r in rows:
        marker = ' ★' if 'current' in r['name'] else '  '
        print(f'{marker}{r["name"]:<26} {r["avg"]:+8.2f}% {r["med"]:+8.2f}% {r["std"]:>4.2f} '
              f'{r["min"]:+8.2f}% {r["max"]:+8.2f}% {r["worst_mdd"]:+9.2f}% {r["risk_adj"]:>8.2f}')

    base = next((r for r in rows if 'current' in r['name']), None)
    if base:
        print()
        print('=' * 110)
        print('current (exit>8) 대비 차이')
        print('=' * 110)
        for r in rows:
            if 'current' in r['name']:
                continue
            d_ret = r['avg'] - base['avg']
            d_med = r['med'] - base['med']
            d_mdd = r['worst_mdd'] - base['worst_mdd']
            d_ra = r['risk_adj'] - base['risk_adj']
            verdict = '✓ 개선' if d_ret >= 1.0 and d_mdd >= -1.0 else \
                      '~ 미세' if abs(d_ret) < 1.0 else \
                      '~ 트레이드오프' if d_ret > 0 else '✗ 손실'
            print(f'  {r["name"]:<28}: ΔRet평균 {d_ret:+7.2f}%p, ΔRet중앙 {d_med:+7.2f}%p, '
                  f'ΔMDD {d_mdd:+6.2f}%p, Δrisk_adj {d_ra:+5.2f}  {verdict}')


if __name__ == '__main__':
    main()
