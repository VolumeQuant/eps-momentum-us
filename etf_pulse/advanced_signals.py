"""ETF Pulse 고급 신호 — 추적오차, 변동성, Sharpe, RSI 등

같은 지수 추적 ETF끼리 30일 daily return spread/correlation으로
운용사 운영 효율 측정.
"""
import sys
import sqlite3
import statistics
import math
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))
from compare import COMPARE_GROUPS

DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def get_returns(ticker, days=30):
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    rows = cur.execute('''
        SELECT date, day_return FROM etf_daily
        WHERE ticker=? AND day_return IS NOT NULL
        ORDER BY date DESC LIMIT ?
    ''', (ticker, days)).fetchall()
    conn.close()
    rows.reverse()
    return [(r[0], r[1]) for r in rows]


def tracking_error(group_name, tickers, lookback=30):
    """같은 카테고리 ETF끼리 daily return 차이 std (낮을수록 추적 좋음)"""
    rets = {tk: dict(get_returns(tk, lookback)) for tk in tickers}
    # 공통 날짜만
    common_dates = set.intersection(*[set(r.keys()) for r in rets.values() if r])
    if len(common_dates) < 5:
        return None
    common = sorted(common_dates)

    # 평균 return (벤치마크 대용)
    avg_rets = []
    for d in common:
        day_rets = [rets[tk][d] for tk in tickers if d in rets[tk]]
        avg_rets.append(statistics.mean(day_rets))

    # 각 ETF별 vs 평균 차이의 std
    result = []
    for tk in tickers:
        diffs = []
        for d, avg in zip(common, avg_rets):
            if d in rets[tk]:
                diffs.append(rets[tk][d] - avg)
        if len(diffs) >= 5:
            te = statistics.pstdev(diffs)
            result.append({
                'ticker': tk,
                'tracking_error': te,
                'n_days': len(diffs),
                'cumulative_return': sum(rets[tk].values()),
            })
    result.sort(key=lambda x: x['tracking_error'])
    return {'group': group_name, 'etfs': result, 'n_dates': len(common)}


def etf_metrics(ticker, lookback=30):
    """단일 ETF 통계 — Sharpe, MDD, RSI, 변동성"""
    rets = get_returns(ticker, lookback)
    if len(rets) < 5:
        return None
    daily_rets = [r[1] for r in rets]
    cum = 1.0
    cums = [cum]
    for r in daily_rets:
        cum *= (1 + r/100)
        cums.append(cum)

    # MDD
    peak = cums[0]; max_dd = 0
    for c in cums:
        peak = max(peak, c)
        dd = (c - peak) / peak * 100
        max_dd = min(max_dd, dd)

    # Sharpe (annualized, no rf)
    mu = statistics.mean(daily_rets)
    sd = statistics.pstdev(daily_rets)
    sharpe = (mu * 252) / (sd * math.sqrt(252)) if sd > 0 else 0

    # 변동성 (annualized)
    vol_annual = sd * math.sqrt(252)

    # 단순 RSI (14일 윈도우, 가능하면)
    rsi = None
    if len(daily_rets) >= 14:
        recent = daily_rets[-14:]
        gains = [r for r in recent if r > 0]
        losses = [-r for r in recent if r < 0]
        avg_gain = sum(gains) / 14 if gains else 0
        avg_loss = sum(losses) / 14 if losses else 0
        if avg_loss > 0:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
        else:
            rsi = 100 if avg_gain > 0 else 50

    return {
        'ticker': ticker,
        'lookback': lookback,
        'cumulative_return': (cums[-1] - 1) * 100,
        'avg_daily_return': mu,
        'volatility_daily': sd,
        'volatility_annual': vol_annual,
        'sharpe': sharpe,
        'mdd': max_dd,
        'rsi_14d': rsi,
    }


def portfolio_bt(tickers, weights=None, lookback=30):
    """포트폴리오 가상 BT — N일 수익률 시뮬"""
    if weights is None:
        weights = [1/len(tickers)] * len(tickers)
    rets_map = {tk: dict(get_returns(tk, lookback)) for tk in tickers}
    common_dates = set.intersection(*[set(r.keys()) for r in rets_map.values() if r])
    if len(common_dates) < 5:
        return None
    common = sorted(common_dates)

    cum = 1.0
    daily = []
    for d in common:
        day_ret = 0
        for tk, w in zip(tickers, weights):
            if d in rets_map[tk]:
                day_ret += w * rets_map[tk][d]
        daily.append(day_ret)
        cum *= (1 + day_ret/100)

    peak = 1.0; max_dd = 0
    cums = [1.0]
    for r in daily:
        c = cums[-1] * (1 + r/100)
        cums.append(c)
        peak = max(peak, c)
        max_dd = min(max_dd, (c-peak)/peak*100)

    mu = statistics.mean(daily)
    sd = statistics.pstdev(daily)
    sharpe = (mu*252)/(sd*math.sqrt(252)) if sd > 0 else 0

    return {
        'tickers': tickers, 'weights': weights, 'n_days': len(common),
        'cumulative_return': (cum-1)*100,
        'mdd': max_dd, 'sharpe': sharpe,
        'volatility_annual': sd * math.sqrt(252),
    }


def gen_tracking_error_md():
    """추적오차 콘텐츠 — 같은 그룹 내 ETF 비교"""
    lines = ['# 📐 동일 카테고리 ETF 추적오차 분석', '']
    lines.append('30일 daily return 평균 대비 각 ETF 차이의 표준편차.')
    lines.append('낮을수록 그룹 평균(벤치마크)에 가까움 = 안정적.')
    lines.append('')
    groups_to_test = ['S&P 500', 'Nasdaq 100', 'Semiconductor', 'Gold',
                      'Long Treasury', 'High Yield', 'AI/Robot', 'China']
    for group in groups_to_test:
        tks = COMPARE_GROUPS.get(group)
        if not tks: continue
        r = tracking_error(group, tks)
        if not r: continue
        lines.append(f'## {group}')
        lines.append('')
        lines.append(f'| Ticker | Tracking Error | 30일 누적 | 비고 |')
        lines.append('|--------|----------------|-----------|------|')
        for e in r['etfs']:
            note = '🥇 최저' if e == r['etfs'][0] else ''
            lines.append(f'| {e["ticker"]} | {e["tracking_error"]:.3f}% | {e["cumulative_return"]:+.2f}% | {note} |')
        lines.append('')
    return '\n'.join(lines)


if __name__ == '__main__':
    print('=== 추적오차 분석 ===\n')
    md = gen_tracking_error_md()
    out = Path(__file__).parent / 'content' / 'tracking_error.md'
    out.parent.mkdir(exist_ok=True)
    out.write_text(md, encoding='utf-8')
    print(md[:2000])
    print('...')
    print(f'\n저장: {out}')

    print('\n=== ETF 메트릭 샘플 ===\n')
    for tk in ['VOO', 'QQQ', 'ARKK', 'TLT', 'GLD']:
        m = etf_metrics(tk)
        if m:
            print(f'{tk}: 30d cum {m["cumulative_return"]:+.2f}%, '
                  f'Sharpe {m["sharpe"]:.2f}, MDD {m["mdd"]:.2f}%, '
                  f'Vol(연) {m["volatility_annual"]:.2f}%, '
                  f'RSI {m["rsi_14d"]:.1f}' if m["rsi_14d"] else '')

    print('\n=== 포트폴리오 BT 샘플 ===')
    bt = portfolio_bt(['VOO', 'QQQ', 'SOXX', 'GLD', 'TLT'], [0.4, 0.2, 0.15, 0.15, 0.10])
    if bt:
        print(f'  포트폴리오: VOO 40% / QQQ 20% / SOXX 15% / GLD 15% / TLT 10%')
        print(f'  30일 누적: {bt["cumulative_return"]:+.2f}%')
        print(f'  Sharpe: {bt["sharpe"]:.2f}')
        print(f'  MDD: {bt["mdd"]:.2f}%')
        print(f'  변동성(연): {bt["volatility_annual"]:.2f}%')
