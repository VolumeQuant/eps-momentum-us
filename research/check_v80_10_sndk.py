"""v80.10이 SNDK를 진짜 못 잡았는지 / 보유 중인지 진단"""
import sys
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import backtest_s2_params as bts2
from collections import defaultdict

ROOT = Path(__file__).parent.parent

for db_path, label in [
    (ROOT / 'eps_momentum_data.bak_pre_v80_10.db', 'v80.9'),
    (ROOT / 'eps_momentum_data.db', 'v80.10'),
]:
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()

    # simulate 내부 로직 모방하며 SNDK 상태 추적
    portfolio = {}
    consecutive = defaultdict(int)
    sndk_history = []

    for di, today in enumerate(dates):
        td = data.get(today, {})
        rank_map = {tk: v['p2'] for tk, v in td.items() if v.get('p2') is not None}
        new_cons = defaultdict(int)
        for tk in rank_map:
            new_cons[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_cons

        # SNDK 정보 수집
        sndk = td.get('SNDK', {})
        sndk_p2 = rank_map.get('SNDK')
        sndk_ms = sndk.get('min_seg', 0)
        sndk_cons = consecutive.get('SNDK', 0)
        in_portfolio = 'SNDK' in portfolio
        sndk_history.append({
            'date': today, 'p2': sndk_p2, 'min_seg': sndk_ms,
            'consec': sndk_cons, 'in_portfolio': in_portfolio,
        })

        # 이탈 (>8 또는 ms<-2)
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            ms = td.get(tk, {}).get('min_seg', 0)
            if rank is None or rank > 8 or ms < -2:
                exited.append(tk)
        for tk in exited:
            del portfolio[tk]

        # 진입
        vacancies = 3 - len(portfolio)
        if vacancies > 0:
            cands = []
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > 3: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                ms = td.get(tk, {}).get('min_seg', 0)
                if ms < 0: continue
                cands.append(tk)
            for tk in cands[:vacancies]:
                portfolio[tk] = {'entry_date': today}

    # SNDK 등장 이력 출력
    print(f'\n========== {label} ==========')
    print(f'{"date":<12} {"p2":>4} {"min_seg":>8} {"consec":>7} {"포트?":<8} {"진입가능?":<10}')
    print('-' * 60)
    prev_in = False
    for h in sndk_history:
        if h['p2'] is None and not h['in_portfolio']:
            continue
        # 진입 가능 여부 (cr <=3 + cons>=3 + ms>=0)
        ok = (h['p2'] and h['p2'] <= 3 and h['consec'] >= 3 and h['min_seg'] >= 0)
        in_str = '✓ IN' if h['in_portfolio'] else ''
        if h['in_portfolio'] != prev_in:
            mark = '⬆ ENTER' if h['in_portfolio'] else '⬇ EXIT'
        else:
            mark = ''
        cond_str = '진입가능' if ok else ''
        if h['p2'] is not None:
            print(f'{h["date"]:<12} {h["p2"]:>4} {h["min_seg"]:>+7.2f}% {h["consec"]:>7} {in_str:<8} {cond_str:<10} {mark}')
        prev_in = h['in_portfolio']

    if 'SNDK' in portfolio:
        ed = portfolio['SNDK']['entry_date']
        print(f'\n→ {label}: SNDK 5/8까지 보유 중 (진입일 {ed})')
    else:
        print(f'\n→ {label}: SNDK 5/8 시점 미보유')
