"""슬롯 채움 vs 빈 슬롯 BT.

현재 동작(baseline): ✅이지만 min_seg<0/flags 탈락 → 카운트 소진, 빈 슬롯.
대체 모드(replace): ✅ + 탈락 시 다음 ✅ 후보로 슬롯 채움 (4위/5위 등).

54일 DB로 실측해서 어느 게 성과가 좋은지 측정.
"""

import sys
import sqlite3
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
DB = Path(__file__).parent.parent / 'eps_momentum_data.db'


def load_data():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    data = defaultdict(dict)
    seg_map = defaultdict(dict)
    for r in cur.execute("""
        SELECT date, ticker, composite_rank, part2_rank, num_analysts,
               price, adj_gap, score, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
               rev_up30, rev_down30
        FROM ntm_screening WHERE date >= '2026-02-10'
    """).fetchall():
        d, tk, cr, p2, na, px, ag, sc, c, n7, n30, n60, n90, up, dn = r
        data[d][tk] = {
            'cr': cr, 'p2': p2, 'num_analysts': na, 'price': px,
            'adj_gap': ag, 'score': sc, 'rev_up30': up, 'rev_down30': dn,
        }
        if all(x is not None and x > 0 for x in (c, n7, n30, n60, n90)):
            seg1 = (c - n7) / abs(n7) * 100
            seg2 = (n7 - n30) / abs(n30) * 100
            seg3 = (n30 - n60) / abs(n60) * 100
            seg4 = (n60 - n90) / abs(n90) * 100
            seg_map[d][tk] = min(seg1, seg2, seg3, seg4)
        else:
            seg_map[d][tk] = None
    conn.close()
    return data, seg_map


def get_status(data, dates, today, ticker):
    """✅ = cr Top 20 3일 연속, ⏳ = 2일, 🆕 = 1일."""
    idx = dates.index(today)
    cnt = 0
    for i in range(min(3, idx + 1)):
        d = dates[idx - i]
        e = data[d].get(ticker)
        if e and e['cr'] is not None and e['cr'] <= 20:
            cnt += 1
        else:
            break
    return '✅' if cnt >= 3 else ('⏳' if cnt == 2 else '🆕')


def is_disqualified(row, min_seg):
    """현재 코드의 진입 차단 조건들."""
    if min_seg is not None and round(min_seg, 1) < 0:
        return True
    up = row.get('rev_up30', 0) or 0
    dn = row.get('rev_down30', 0) or 0
    if (up + dn) > 0 and dn / (up + dn) > 0.3:
        return True
    if dn >= up and dn >= 2:
        return True
    if (row.get('num_analysts', 0) or 0) < 3:
        return True
    return False


def select_picks(data, seg_map, dates, today, mode='baseline'):
    """오늘의 Signal pick 종목 선정.

    baseline: ✅ but 탈락 → 카운트 소진, 빈 슬롯
    replace : ✅ but 탈락 → 다음 ✅로 슬롯 채움 (ENTRY_THRESHOLD 무한)
    """
    # p2 순으로 후보 정렬
    candidates = []
    for tk, e in data[today].items():
        if e['p2'] is not None:
            candidates.append((e['p2'], tk, e))
    candidates.sort()

    selected = []
    verified_count = 0
    MAX_SLOTS = 3
    ENTRY_THRESHOLD = 3

    for p2, tk, e in candidates:
        if len(selected) >= MAX_SLOTS:
            break
        status = get_status(data, dates, today, tk)
        if status != '✅':
            continue
        verified_count += 1
        if mode == 'baseline' and verified_count > ENTRY_THRESHOLD:
            break
        ms = seg_map[today].get(tk)
        if is_disqualified(e, ms):
            continue
        selected.append((tk, e['price']))

    return selected


def simulate(data, seg_map, dates, mode='baseline'):
    holdings = {}
    nav = 1.0

    for i, d in enumerate(dates):
        # 1) 퇴출: p2 > 8 OR p2 NULL OR min_seg < -2%
        sells = []
        for tk in list(holdings.keys()):
            e = data[d].get(tk)
            ms = seg_map[d].get(tk)
            if (not e or e['p2'] is None or e['p2'] > 8 or
                (ms is not None and ms < -2.0)):
                sells.append(tk)
        for tk in sells:
            del holdings[tk]

        # 2) 신규: select_picks 결과로 현재 보유 외 채움
        picks = select_picks(data, seg_map, dates, d, mode=mode)
        for tk, px in picks:
            if len(holdings) >= 3:
                break
            if tk in holdings:
                continue
            if px and px > 0:
                holdings[tk] = px

        # 3) NAV 업데이트
        if i > 0 and holdings:
            prev_d = dates[i - 1]
            rets = []
            for tk in list(holdings.keys()):
                cur_e = data[d].get(tk)
                prev_e = data[prev_d].get(tk)
                if cur_e and prev_e and cur_e['price'] and prev_e['price']:
                    rets.append(cur_e['price'] / prev_e['price'] - 1)
            if rets:
                nav *= (1 + sum(rets) / len(rets))

    return nav


def count_slot_events(data, seg_map, dates):
    """빈 슬롯 발생 케이스 수집."""
    events = []
    for d in dates:
        candidates = []
        for tk, e in data[d].items():
            if e['p2'] is not None:
                candidates.append((e['p2'], tk, e))
        candidates.sort()
        verified_in_top3 = 0
        empty_slot_caused_by = []
        for p2, tk, e in candidates[:10]:
            if verified_in_top3 >= 3:
                break
            status = get_status(data, dates, d, tk)
            if status != '✅':
                continue
            verified_in_top3 += 1
            ms = seg_map[d].get(tk)
            if is_disqualified(e, ms):
                reason = 'min_seg<0' if (ms is not None and round(ms, 1) < 0) else 'flags'
                empty_slot_caused_by.append((tk, p2, ms, reason))
        if empty_slot_caused_by:
            events.append((d, empty_slot_caused_by))
    return events


def main():
    data, seg_map = load_data()
    dates = sorted(data.keys())
    print(f'Date range: {dates[0]} ~ {dates[-1]} ({len(dates)} days)')
    print()

    nav_base = simulate(data, seg_map, dates, mode='baseline')
    nav_repl = simulate(data, seg_map, dates, mode='replace')

    print(f'{"Mode":<25} {"NAV":<10} {"Return%":<10}')
    print('-' * 50)
    print(f'{"baseline (현재 — 빈 슬롯)":<22} {nav_base:<10.4f} {(nav_base-1)*100:<10.2f}')
    print(f'{"replace (4위 자동 대체)":<22} {nav_repl:<10.4f} {(nav_repl-1)*100:<10.2f}')
    print()

    print('=== 빈 슬롯 발생 케이스 (baseline) ===')
    events = count_slot_events(data, seg_map, dates)
    if events:
        print(f"총 {len(events)}일 발생")
        for d, items in events[-15:]:
            for tk, p2, ms, reason in items:
                ms_str = f'{ms:+.2f}%' if ms is not None else 'N/A'
                print(f'  {d}: p2={p2} {tk} ({reason}, min_seg={ms_str})')
    else:
        print('(no occurrences)')


if __name__ == '__main__':
    main()
