"""SNDK 제외 시 MA20 + (2,10,2) 알파 유지되나 — 슈퍼위너 의존성 검증

사용자 가설:
  (2,10,2)가 BT에서 best였던 건 SNDK가 81일간 슬롯1 고정 +164.54% 폭주했기 때문일 수 있음.
  slots=2면 SNDK + 1종목 → 평균 끌어올림
  slots=3이면 SNDK + 2종목 → 다른 종목이 평균 끌어내림

검증:
  SNDK 제외 후 (3,10,3) vs (2,10,2) paired 비교.
  만약 (2,10,2)가 여전히 우월 → robust
  만약 lift 사라짐 → 사용자 의심 정답 (single-stock alpha)

추가로 LITE, MU 등 다른 슈퍼위너도 같이 제외 BT.
"""
import sys
import random
import statistics
import time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import sqlite3
import bt_breakout_hold as bth

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'ma_filter_dbs'
DB_MA20 = GRID / 'ext_ma20.db'

N_SEEDS = 500
SAMPLES = 5
MIN_HOLD_DAYS = 10


def load_data_ext_ban(banned):
    """bt_breakout_hold.load_data_ext에서 banned ticker 제거"""
    bth.DB_PATH = DB_MA20
    dates, data, price_series = bth.load_data_ext()
    for d in dates:
        for tk in list(data[d].keys()):
            if tk in banned:
                # p2를 None으로 설정 → ranking에서 빠짐
                # 보유 중인 종목 매수도 차단
                data[d][tk]['p2'] = None
                data[d][tk]['comp_rank'] = None
    return dates, data, price_series


def run(entry, exit_, slots, seed_starts, banned):
    dates, data, price_series = load_data_ext_ban(banned)
    rets, mdds, seed_avgs = [], [], []
    sndk_holdings = 0  # 진입한 sndk-like 사례 카운트
    for chosen in seed_starts:
        seed_rets = []
        for sd in chosen:
            r = bth.simulate_hold(
                dates, data, price_series, hold_days=0,
                entry_top=entry, exit_top=exit_,
                max_slots=slots, start_date=sd
            )
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            seed_rets.append(r['total_return'])
        seed_avgs.append(sum(seed_rets) / len(seed_rets))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 100)
    print('SNDK 제외 시 (2,10,2) vs (3,10,3) paired — 슈퍼위너 의존성 검증')
    print(f'{N_SEEDS} seed × {SAMPLES} samples = {N_SEEDS*SAMPLES} sim/조합')
    print('=' * 100)

    bth.DB_PATH = DB_MA20
    dates, _, _ = bth.load_data_ext()
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    # 추가: SNDK 외 슈퍼위너 카운트 (BT 기간 +50% 이상)
    conn = sqlite3.connect(DB_MA20)
    cur = conn.cursor()
    # 각 ticker의 BT 기간 시작/끝 price 비교
    print('\n[참고] BT 기간 +50% 이상 폭주 종목 (potential super-winner):')
    rows = cur.execute('''
        SELECT t1.ticker, t1.price as p_start, t2.price as p_end,
               (t2.price - t1.price) / t1.price * 100 as ret_pct
        FROM ntm_screening t1
        JOIN ntm_screening t2 ON t1.ticker = t2.ticker
        WHERE t1.date = (SELECT MIN(date) FROM ntm_screening WHERE part2_rank IS NOT NULL)
          AND t2.date = (SELECT MAX(date) FROM ntm_screening)
          AND t1.price > 0 AND t2.price > 0
          AND (t2.price - t1.price) / t1.price > 0.5
        ORDER BY ret_pct DESC LIMIT 15
    ''').fetchall()
    for tk, ps, pe, ret in rows:
        print(f'  {tk:<6} {ps:6.2f} → {pe:7.2f}  {ret:+7.1f}%')
    conn.close()

    scenarios = [
        ('전체', set()),
        ('SNDK 제외', {'SNDK'}),
        ('Top 3 super-winner 제외', {r[0] for r in rows[:3]}),
        ('Top 5 super-winner 제외', {r[0] for r in rows[:5]}),
        ('Top 10 super-winner 제외', {r[0] for r in rows[:10]}),
    ]

    print()
    for label, banned in scenarios:
        print('=' * 100)
        print(f'시나리오: {label}  (제외 종목: {sorted(banned) if banned else "없음"})')
        print('=' * 100)
        results = {}
        for entry, exit_, slots, name in [
            (3, 10, 3, 'production'),
            (2, 10, 2, 'grid_best'),
            (3, 10, 2, 'slots2_only'),
            (2, 10, 3, 'entry2_only'),
        ]:
            spec = f'({entry},{exit_},{slots})'
            t0 = time.time()
            res = run(entry, exit_, slots, seed_starts, banned)
            avg = sum(res['rets']) / len(res['rets'])
            mdd = min(res['mdds'])
            ra = avg / abs(mdd) if mdd < 0 else 0
            results[spec] = res
            print(f'  [{time.time()-t0:>5.1f}s] {spec:<10} {name:<14} '
                  f'avg={avg:+6.2f}% mdd={mdd:+6.2f}% ra={ra:+5.2f}')

        # paired (2,10,2) vs (3,10,3)
        base = results['(3,10,3)']['seed_avgs']
        for spec in ['(2,10,2)', '(3,10,2)', '(2,10,3)']:
            new = results[spec]['seed_avgs']
            lifts = [b - a for a, b in zip(base, new)]
            wins = sum(1 for l in lifts if l > 0)
            losses = sum(1 for l in lifts if l < 0)
            avg_l = sum(lifts) / len(lifts)
            print(f'    {spec} vs (3,10,3): lift {avg_l:+.2f}%p, {wins}/{N_SEEDS} wins')
        print()


if __name__ == '__main__':
    main()
