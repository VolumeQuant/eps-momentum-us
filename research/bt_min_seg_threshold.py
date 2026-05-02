"""min_seg 임계값 BT — 진입 차단 임계값 변형 비교

배경: 현재 진입 차단 = round(min_seg, 1) < 0. VIRT 5/1 (seg4 -0.52%),
JAZZ 5/1 (seg1 -0.06%) 같은 노이즈 수준 음수가 종목을 차단. 사용자 의도
"꾸준하지 않으면 버린다"는 유지하되 노이즈 폭만 약간 완화하는 게 합리적인지
6시작일 multistart로 검증.

비교 변형:
  thr_0    — round(min_seg, 1) <  0    [현행 production = ≥0% 요구]
  thr_n05  — round(min_seg, 1) <= -0.5 [-0.5%까지 노이즈로 허용]
  thr_n10  — round(min_seg, 1) <= -1.0 [-1.0%까지 허용]
  thr_n20  — round(min_seg, 1) <= -2.0 [-2%까지 허용 = 이탈선과 동일]

이탈 임계값(< -2%)은 모든 변형 동일. 진입 임계값만 변경.
DB: 현재 production (v80.5b) 그대로.
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
    """bts2.load_data와 동일 로직"""
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


def simulate(dates_all, data, entry_top, exit_top, max_slots,
             min_seg_entry_thr, min_seg_exit_thr=-2, start_date=None):
    """bts2.simulate 변형 — min_seg 진입 임계값 가변"""
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    portfolio = {}
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

        # 이탈
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            should_exit = False
            if rank is None or rank > exit_top:
                should_exit = True
            if min_seg < min_seg_exit_thr:
                should_exit = True
            if should_exit:
                exited.append(tk)
        for tk in exited:
            del portfolio[tk]

        # 진입 — min_seg_entry_thr 적용
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
                # round(min_seg, 1) < threshold 차단 (production 일치)
                if round(min_seg, 1) < min_seg_entry_thr:
                    continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0:
                    candidates.append((tk, price))
            for tk, price in candidates[:vacancies]:
                portfolio[tk] = {'entry_price': price, 'entry_date': today}

        # 일간 수익
        if portfolio:
            day_ret = 0
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


def run_multistart(dates, data, threshold, start_dates):
    rets, mdds = [], []
    for sd in start_dates:
        r = simulate(dates, data, 3, 8, 3, threshold, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds


def count_eligible_blocked(dates, data, threshold):
    """각 임계값에서 part2_rank ≤ 3 진입 가능했는데 임계값으로 차단된 종목 수"""
    blocked = 0
    total_top3 = 0
    for d in dates:
        for tk, v in data[d].items():
            if v.get('p2') and v['p2'] <= 3:
                total_top3 += 1
                ms = v.get('min_seg', 0)
                if round(ms, 1) < threshold:
                    blocked += 1
    return blocked, total_top3


def main():
    print('=' * 110)
    print('min_seg 임계값 BT — 진입 차단 기준 변형 비교 (이탈은 < -2% 동일)')
    print('=' * 110)

    dates, data = load_data()
    start_dates = dates[2:8]
    print(f'\n시작일 ({len(start_dates)}개): {start_dates[0]} ~ {start_dates[-1]} (모두 50거래일+)')

    variants = [
        ('thr_0    (현행, ≥0%)',     0.0),
        ('thr_n05  (≥-0.5%)',        -0.5),
        ('thr_n10  (≥-1.0%)',        -1.0),
        ('thr_n20  (≥-2.0%, 이탈과 동일)', -2.0),
    ]

    rows = []
    for name, thr in variants:
        rets, mdds = run_multistart(dates, data, thr, start_dates)
        avg = sum(rets) / len(rets)
        med = sorted(rets)[len(rets) // 2]
        std = statistics.pstdev(rets)
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        blocked, top3 = count_eligible_blocked(dates, data, thr)
        rows.append({
            'name': name, 'thr': thr, 'rets': rets, 'mdds': mdds,
            'avg': avg, 'med': med, 'std': std,
            'min': min(rets), 'max': max(rets),
            'worst_mdd': worst_mdd, 'risk_adj': risk_adj,
            'blocked': blocked, 'top3': top3,
        })

    print()
    print(f'{"변형":<32}', end='')
    for sd in start_dates:
        print(f' {sd:>10}', end='')
    print()
    print('-' * (34 + 11 * len(start_dates)))
    for r in rows:
        print(f'  {r["name"]:<30}', end='')
        for ret in r['rets']:
            print(f' {ret:>+9.2f}%', end='')
        print()

    print()
    print(f'{"변형":<32} {"avg":>8} {"med":>8} {"std":>5} {"min":>8} {"max":>8} '
          f'{"worstMDD":>9} {"risk_adj":>8} {"차단":>10}')
    print('-' * 110)
    for r in rows:
        marker = ' ★' if r['thr'] == 0.0 else '  '
        block_pct = r['blocked'] / r['top3'] * 100 if r['top3'] > 0 else 0
        print(f'{marker}{r["name"]:<30} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% {r["std"]:>4.2f} '
              f'{r["min"]:+7.2f}% {r["max"]:+7.2f}% {r["worst_mdd"]:+8.2f}% '
              f'{r["risk_adj"]:>7.2f} {r["blocked"]:>4}/{r["top3"]:<3} ({block_pct:.0f}%)')

    base = next((r for r in rows if r['thr'] == 0.0), None)
    if base:
        print()
        print('=' * 110)
        print('현행 (≥0%) 대비 차이')
        print('=' * 110)
        for r in rows:
            if r['thr'] == 0.0:
                continue
            d_ret = r['avg'] - base['avg']
            d_mdd = r['worst_mdd'] - base['worst_mdd']
            d_ra = r['risk_adj'] - base['risk_adj']
            verdict = '✓ 개선' if d_ret >= 1.0 and d_mdd >= -1.0 else \
                      '~ 미세 차이' if abs(d_ret) < 1.0 and abs(d_mdd) < 1.0 else \
                      '~ 트레이드오프' if d_ret > 0 else '✗ 손실'
            print(f'  {r["name"]:<32}: ΔRet {d_ret:+6.2f}%p, ΔMDD {d_mdd:+5.2f}%p, '
                  f'Δrisk_adj {d_ra:+5.2f}  {verdict}')


if __name__ == '__main__':
    main()
