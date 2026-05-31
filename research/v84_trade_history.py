"""v84 적용 trade history — 역대 매수 종목, 비중, 수익률, 보유 수익률

v84 production 환경:
- entry=2, exit=10, slot=2
- 진입 필터: dd_30_25 추가
- 진입 시점 weight: 2step_t15 (gap≥15 → [100,0], gap<15 → [50,50])
- entry_fixed simulator

시작일: 73일 BT 첫 날 (2026-02-12)
종료일: 최근 (2026-05-29)
"""
import sys
import sqlite3
import pandas as pd
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')
import daily_runner as dr

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'


def load_data():
    """v84 적용 DB 로드 (high30 이미 채워짐)"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, high30
            FROM ntm_screening WHERE date=?
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[3:8])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else:
                    segs.append(0)
            data[d][tk] = {
                'p2': r[1], 'price': r[2],
                'high30': r[8],
                'min_seg': min(segs) if segs else 0,
            }
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px

    # score_100 precompute (v84 dynamic weight용)
    scores = {}
    for d in dates:
        tickers = [r[0] for r in cur.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank LIMIT 5',
            (d,)).fetchall()]
        if len(tickers) >= 2:
            wmap = dr._compute_w_gap_map(cur, d, tickers)
            sorted_t = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
            top_w = wmap.get(sorted_t[0], 0)
            if top_w > 0:
                s1 = 100.0
                s2 = wmap.get(sorted_t[1], 0) / top_w * 100
                gap = s1 - s2
            else:
                s1, s2, gap = 100.0, 100.0, 0.0
        else:
            s1, s2, gap = 100.0, 0.0, 100.0
        scores[d] = (s1, s2, gap)
    conn.close()
    return dates, data, price_full, scores


def get_weight_v84(gap):
    """2step_t15"""
    if gap >= 15:
        return [100, 0]
    return [50, 50]


def simulate_with_trades(dates, data, price_full, scores,
                         entry=2, exit_=10, max_slots=2,
                         dd_filter_threshold=25):
    """v84 entry_fixed simulator + trade history 기록 + 보유 종목 미실현 수익률"""

    # dd_30_25 필터: high30 정보로 매수 후보에서 미리 컷
    # (이미 production에서 적용되지만 BT 정합성 위해 여기서도)
    # 단, part2_rank 자체는 production이 채운 거 사용 (사용자 의도)

    INIT_CAP = 100.0
    total_cash = INIT_CAP
    slot_cash = [0.0] * max_slots
    slot_holding = [None] * max_slots  # [(ticker, shares, entry_price, entry_date, weight)]
    current_weights = None
    daily_returns = []
    consecutive = defaultdict(int)
    trades = []  # 매도된 trade 기록
    prev_pv = INIT_CAP

    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consec = defaultdict(int)
        for tk in rank_map: new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        # PV
        pv = total_cash
        for i in range(max_slots):
            if slot_holding[i] is None:
                pv += slot_cash[i]
            else:
                tk, shares, ep, ed, w = slot_holding[i]
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk) or ep
                pv += shares * p
        if prev_pv > 0:
            daily_returns.append((pv - prev_pv) / prev_pv * 100)
        prev_pv = pv

        # 이탈
        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, ep, ed, w = slot_holding[i]
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            reason = None
            if min_seg < -2:
                reason = 'min_seg'
            elif rank is None or rank > exit_:
                reason = 'rank_exit'
            if reason:
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk) or ep
                ret = (p - ep) / ep * 100
                trades.append({
                    'ticker': tk, 'weight': w,
                    'entry_date': ed, 'entry_price': ep,
                    'exit_date': today, 'exit_price': p,
                    'return': ret, 'reason': reason,
                    'hold_days': dates.index(today) - dates.index(ed) if ed in dates else 0,
                })
                slot_cash[i] = shares * p
                slot_holding[i] = None

        # cash 통합
        if all(h is None for h in slot_holding):
            total_cash += sum(slot_cash)
            slot_cash = [0.0] * max_slots
            current_weights = None

        # 진입 (v84: dd_30_25 적용 + dynamic weight)
        cands = []
        for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
            if rank > entry: break
            if any(h is not None and h[0] == tk for h in slot_holding): continue
            if consecutive.get(tk, 0) < 3: continue
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < 0: continue
            price = today_data.get(tk, {}).get('price')
            if not price or price <= 0: continue
            # dd_30_25 진입 필터
            high30 = today_data.get(tk, {}).get('high30')
            if high30 is not None:
                dd = (price - high30) / high30 * 100
                if dd <= -dd_filter_threshold:
                    continue
            cands.append((tk, price))

        # weight 결정
        if cands and current_weights is None and total_cash > 0:
            _, _, gap = scores.get(today, (100, 0, 100))
            ws = get_weight_v84(gap)
            current_weights = ws
            slot_cash = [w / 100 * total_cash for w in ws]
            total_cash = 0

        # 슬롯 진입
        for slot_idx in range(max_slots):
            if slot_holding[slot_idx] is not None: continue
            if not cands: break
            if slot_cash[slot_idx] <= 0:
                cands.pop(0); continue  # weight 0 슬롯은 진입 X (cand만 소비)
            tk, price = cands.pop(0)
            shares = slot_cash[slot_idx] / price
            w = current_weights[slot_idx] if current_weights else 0
            slot_holding[slot_idx] = (tk, shares, price, today, w)
            slot_cash[slot_idx] = 0

    # 마감 시 미실현 (open 포지션)
    open_positions = []
    if dates:
        last = dates[-1]
        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, ep, ed, w = slot_holding[i]
            p = data.get(last, {}).get(tk, {}).get('price') or price_full.get(last, {}).get(tk) or ep
            ret = (p - ep) / ep * 100
            open_positions.append({
                'ticker': tk, 'weight': w,
                'entry_date': ed, 'entry_price': ep,
                'current_date': last, 'current_price': p,
                'unrealized_return': ret,
                'hold_days': dates.index(last) - dates.index(ed) if ed in dates else 0,
            })

    # 누적
    cum = 1.0
    for r in daily_returns:
        cum *= (1 + r/100)
    cum_pct = (cum - 1) * 100

    return {
        'trades': trades,
        'open_positions': open_positions,
        'cum_return': cum_pct,
        'n_days': len(daily_returns),
    }


def main():
    print('=' * 100)
    print('★ v84 적용 시 역대 매수 종목 + 비중 + 수익률 + 보유 미실현')
    print(f'  환경: entry=2, exit=10, slot=2, dd_30_25 진입필터, 2step_t15 dynamic weight')
    print('=' * 100)

    dates, data, price_full, scores = load_data()
    print(f'\n기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)')

    res = simulate_with_trades(dates, data, price_full, scores)

    # 모든 trade를 시간순 정렬
    all_events = []
    for t in res['trades']:
        all_events.append({**t, 'status': 'CLOSED'})
    for p in res['open_positions']:
        all_events.append({**p, 'status': 'OPEN', 'return': p['unrealized_return'],
                           'exit_date': p['current_date'], 'exit_price': p['current_price'],
                           'reason': 'open'})
    all_events.sort(key=lambda x: x['entry_date'])

    print()
    print('=' * 100)
    print(f'역대 매수 trade ({len(all_events)}건)')
    print('=' * 100)
    print(f'{"#":>3} {"ticker":<8} {"비중":>5} {"entry":<12} {"exit":<12} {"hold":>5} {"진입가":>8} {"청산가":>8} {"수익":>9} {"상태":<8} {"사유":<10}')
    print('-' * 100)
    for i, e in enumerate(all_events, 1):
        ret = e['return']
        ret_str = f'{ret:+7.2f}%'
        if e['status'] == 'OPEN':
            ret_str = f'{ret:+7.2f}% ★'
        print(f'{i:>3} {e["ticker"]:<8} {e["weight"]:>4}% {e["entry_date"]:<12} {e["exit_date"]:<12} '
              f'{e["hold_days"]:>4}일 ${e["entry_price"]:>7.2f} ${e["exit_price"]:>7.2f} {ret_str:>10} '
              f'{e["status"]:<8} {e["reason"]:<10}')

    # 종합 통계
    print()
    print('=' * 100)
    print('종합 통계')
    print('=' * 100)
    closed = [e for e in all_events if e['status'] == 'CLOSED']
    opens = [e for e in all_events if e['status'] == 'OPEN']
    wins = sum(1 for t in closed if t['return'] > 0)
    losses = sum(1 for t in closed if t['return'] <= 0)
    print(f'완료 trade: {len(closed)}건 ({wins}승 {losses}패)')
    if closed:
        avg_ret = sum(t['return'] for t in closed) / len(closed)
        print(f'평균 trade 수익률: {avg_ret:+.2f}%')
        print(f'  최고: {max(t["return"] for t in closed):+.2f}% ({max(closed, key=lambda x: x["return"])["ticker"]})')
        print(f'  최저: {min(t["return"] for t in closed):+.2f}% ({min(closed, key=lambda x: x["return"])["ticker"]})')

    print(f'\n현재 보유 포지션 (OPEN): {len(opens)}건')
    for p in opens:
        print(f'  {p["ticker"]} ({p["weight"]}%): 진입 {p["entry_date"]} @ ${p["entry_price"]:.2f}')
        print(f'    현재 {p["current_date"]} @ ${p["current_price"]:.2f}, 미실현 {p["return"]:+.2f}%, 보유 {p["hold_days"]}일')

    print(f'\nv84 시뮬 누적 수익률: {res["cum_return"]:+.2f}% ({res["n_days"]}일)')


if __name__ == '__main__':
    main()
