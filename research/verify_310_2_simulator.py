"""Phase 4 - simulator 정확성 검증 (v81 사태 학습)

목적: (3,10,2) 80/20 boost=3 결과가 환상이 아닌지 확인.

검증 항목:
  1. trade 진입/이탈 가격이 DB와 일치하는가
  2. C2 분류가 정확한가
  3. 같은 시나리오를 다른 코드로 재구현해서 같은 결과 나오는지
  4. 부분 데이터 (다른 시작일)에서도 같은 alpha 패턴인지
"""
import sys
import sqlite3
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
LOOKBACK = 30


def load_db_raw(db_path):
    """전체 데이터 raw 로드 (검증용)"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening ORDER BY date'
    ).fetchall()]
    # 전체 price + ntm 데이터 (no part2_rank filter)
    raw = defaultdict(dict)
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, composite_rank, price, eps_chg_weighted,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=?
        ''', (d,)).fetchall()
        for r in rows:
            tk = r[0]
            raw[d][tk] = {
                'p2': r[1], 'cr': r[2], 'price': r[3], 'eps_w': r[4],
                'nc': r[5], 'n7': r[6], 'n30': r[7], 'n60': r[8], 'n90': r[9],
            }
    conn.close()
    return dates, raw


def verify_trade_prices(dates, raw):
    """analyze_310_2_80_20_boost3.log의 trade들 가격을 DB와 직접 대조"""
    # log에서 추출한 진입/이탈 (수동 입력)
    trades = [
        ('LITE', '2026-02-17', 600.42, '2026-03-13', 622.50),
        ('SNDK', '2026-02-17', 590.59, '2026-03-18', 753.69),
        ('STX',  '2026-03-13', 383.71, '2026-04-02', 429.36),
        ('MOD',  '2026-03-18', 200.42, '2026-03-30', 202.18),
        ('MU',   '2026-03-30', 321.80, '2026-05-08', 746.81),
        ('TTMI', '2026-04-02',  97.48, '2026-04-10', 121.49),
        ('FIVE', '2026-04-10', 217.37, '2026-04-22', 240.23),
        ('SNDK', '2026-04-22', 979.07, '2026-05-21', 1542.24),
        ('BE',   '2026-05-08', 261.03, '2026-05-13', 289.76),
        ('FIVE', '2026-05-13', 209.99, '2026-05-19', 214.39),
        ('AEIS', '2026-05-19', 302.84, '2026-05-21',  323.79),
    ]
    print('=' * 100)
    print('Trade 가격 DB 대조')
    print('=' * 100)
    print(f'{"ticker":<7} {"entry":<12} {"entry_p (sim)":>14} {"entry_p (DB)":>14} {"exit":<12} {"exit_p (sim)":>14} {"exit_p (DB)":>14} {"check":>6}')
    ok = 0
    for tk, ed, ep, xd, xp in trades:
        db_ep = raw.get(ed, {}).get(tk, {}).get('price')
        db_xp = raw.get(xd, {}).get(tk, {}).get('price')
        match = (abs(db_ep - ep) < 0.01 if db_ep else False) and (abs(db_xp - xp) < 0.01 if db_xp else False)
        check = '✓' if match else '✗'
        if match: ok += 1
        db_ep_str = f'{db_ep:.2f}' if db_ep else 'NULL'
        db_xp_str = f'{db_xp:.2f}' if db_xp else 'NULL'
        print(f'{tk:<7} {ed:<12} {ep:>13.2f} {db_ep_str:>14} {xd:<12} {xp:>13.2f} {db_xp_str:>14} {check:>6}')
    print(f'\n매칭: {ok}/{len(trades)}')


def verify_c2_classification(dates, raw):
    """trade들 진입 시점에서 C2 분류 검증"""
    # C2 표시된 trades (log 기준)
    c2_trades = [
        ('MU',   '2026-03-30'),
        ('TTMI', '2026-04-02'),
        ('FIVE', '2026-04-10'),
        ('FIVE', '2026-05-13'),
        ('AEIS', '2026-05-19'),
    ]
    c1_trades = [
        ('LITE', '2026-02-17'),
        ('SNDK', '2026-02-17'),
        ('STX',  '2026-03-13'),
        ('MOD',  '2026-03-18'),
        ('SNDK', '2026-04-22'),
        ('BE',   '2026-05-08'),
    ]
    print()
    print('=' * 100)
    print('C2 분류 검증 — eps_w > 0 AND price 30d < 0')
    print('=' * 100)
    print(f'{"ticker":<7} {"date":<12} {"eps_w":>8} {"price 30d":>12} {"expected":>10} {"actual":>8} {"check":>6}')

    def calc_p30(tk, today):
        if today not in dates: return None
        di = dates.index(today)
        if di < LOOKBACK: return None
        past_d = dates[di - LOOKBACK]
        past_p = raw.get(past_d, {}).get(tk, {}).get('price')
        cur_p = raw.get(today, {}).get(tk, {}).get('price')
        if past_p and cur_p and past_p > 0:
            return (cur_p - past_p) / past_p * 100
        return None

    all_trades = [(tk, d, 'C2') for tk, d in c2_trades] + [(tk, d, 'C1') for tk, d in c1_trades]
    for tk, d, expected in all_trades:
        info = raw.get(d, {}).get(tk, {})
        eps_w = info.get('eps_w')
        p30 = calc_p30(tk, d)
        if eps_w is None or p30 is None:
            print(f'{tk:<7} {d:<12} (데이터 없음)')
            continue
        actual = 'C2' if (eps_w > 0 and p30 < 0) else ('C1' if eps_w > 0 else 'C3/C4')
        check = '✓' if actual == expected else '✗'
        print(f'{tk:<7} {d:<12} {eps_w:>7.2f} {p30:>11.2f}% {expected:>10} {actual:>8} {check:>6}')


def cross_check_simple_simulator(dates, raw):
    """간단한 독립 simulator로 결과 재현 — 시작일=처음"""
    print()
    print('=' * 100)
    print('독립 simulator로 cross-check — 시작일=처음 ({}={})'.format(dates[0], dates[0]))
    print('=' * 100)

    # part2_rank 있는 날짜만
    p2_dates = [d for d in dates if any(info.get('p2') for info in raw.get(d, {}).values())]

    # simulate with config: (3,10,2), 80/20, boost=3
    weights = [80, 20]
    entry = 3; exit_ = 10; slots = 2; boost = 3

    portfolio = {}  # {tk: {ep, slot_idx, was_c2}}
    consecutive = defaultdict(int)
    nav = 1.0; peak = 1.0; max_dd = 0

    def p30(tk, today):
        if today not in dates: return None
        di = dates.index(today)
        if di < LOOKBACK: return None
        past_d = dates[di - LOOKBACK]
        past_p = raw.get(past_d, {}).get(tk, {}).get('price')
        cur_p = raw.get(today, {}).get(tk, {}).get('price')
        if past_p and cur_p and past_p > 0:
            return (cur_p - past_p) / past_p * 100
        return None

    for di, today in enumerate(p2_dates):
        today_data = raw.get(today, {})
        # composite_rank IS NOT NULL인 종목들만 (production 시스템 후보 풀)
        eligible = {tk: info for tk, info in today_data.items() if info.get('cr') is not None}
        # rerank with boost
        cands = []
        c2_set = set()
        for tk, info in eligible.items():
            p2 = info.get('p2')
            if p2 is None: continue
            eps_w = info.get('eps_w'); p30v = p30(tk, today)
            is_c2 = (eps_w is not None and eps_w > 0 and p30v is not None and p30v < 0)
            if is_c2: c2_set.add(tk)
            score = (31 - p2) + (boost if is_c2 else 0)
            cands.append((-score, tk))
        cands.sort()
        new_ranks = {tk: i+1 for i, (_, tk) in enumerate(cands)}

        # consecutive
        new_c = defaultdict(int)
        for tk, r in new_ranks.items():
            if r <= 30:
                new_c[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_c

        # day return
        if portfolio and di > 0:
            prev_d = p2_dates[di-1]
            day_ret = 0
            for tk, info in portfolio.items():
                cur_p = today_data.get(tk, {}).get('price')
                prev_p = raw.get(prev_d, {}).get(tk, {}).get('price')
                if cur_p and prev_p and prev_p > 0:
                    w = weights[info['slot_idx']] / 100.0
                    day_ret += w * (cur_p - prev_p) / prev_p * 100
            nav *= (1 + day_ret/100)
            peak = max(peak, nav)
            dd = (nav - peak) / peak * 100
            max_dd = min(max_dd, dd)

        # min_seg
        def min_seg(info):
            nc, n7, n30, n60, n90 = (float(info.get(k, 0) or 0) for k in ['nc','n7','n30','n60','n90'])
            segs = []
            for a, b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else: segs.append(0)
            return min(segs) if segs else 0

        # exit
        exited = []
        for tk in list(portfolio.keys()):
            rank = new_ranks.get(tk)
            info = today_data.get(tk, {})
            ms = min_seg(info)
            if ms < -2 or rank is None or rank > exit_:
                exited.append(tk)
        for tk in exited:
            del portfolio[tk]

        # entry
        if len(portfolio) < slots:
            used = {info['slot_idx'] for info in portfolio.values()}
            free = sorted([i for i in range(slots) if i not in used])
            possible = []
            for tk, r in sorted(new_ranks.items(), key=lambda x: x[1]):
                if r > entry: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                info = today_data.get(tk, {})
                if min_seg(info) < 0: continue
                price = info.get('price')
                if price and price > 0:
                    possible.append((tk, price))
            for slot_idx in free:
                if not possible: break
                tk, price = possible.pop(0)
                portfolio[tk] = {'ep': price, 'slot_idx': slot_idx, 'was_c2': tk in c2_set}

    print(f'독립 simulator 결과: 누적 {(nav-1)*100:+.2f}%, MDD {max_dd:+.2f}%')
    print(f'(Phase 3 analyze_310_2_80_20_boost3 결과: +241.34%, MDD -19.82%)')


def main():
    dates, raw = load_db_raw(DB_PATH)
    verify_trade_prices(dates, raw)
    verify_c2_classification(dates, raw)
    cross_check_simple_simulator(dates, raw)


if __name__ == '__main__':
    main()
