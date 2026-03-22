"""전략 변형 비교 백테스트 프레임워크

DB 데이터 1회 로드 → 여러 전략 config로 백테스트 실행 → 비교표 출력.
진입/이탈 규칙, 손절 방식, 포지션 사이징 등을 config로 조합하여 비교.

조합 가능한 옵션:
  - 진입: top_n (Top N 진입), min_seg_entry (진입 min_seg 기준)
  - 이탈: exit_rank (순위 이탈선), min_seg_exit, fixed_stop, trailing_stop
  - 사이징: equal (동일비중), inverse_vol (역변동성 비중)
  - 포지션: max_positions (최대 보유 종목 수)

Usage:
    python backtest/backtest_compare.py

검증 결과 (25거래일, 2026-02-12~03-20):
  - 트레일링 스탑: 현행보다 MDD 악화 (휘핑 문제) → 채택 안 함
  - 역변동성(5일): MDD -13.9%→-10.1%, 수익 +12.5%→+14.7% → 적용 완료
  - VIX 국면별 포지션: 추가 검증 중
"""
import math
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = Path(__file__).parent.parent / 'eps_momentum_data.db'


# ── 공통 유틸 ──

def calc_min_seg(nc, n7, n30, n60, n90):
    """NTM EPS 4구간 변화율 중 최소값 (%)"""
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        if b and abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0)
    return min(segs)


def calc_pairwise_corr(price_history, tk1, tk2, lookback=20):
    """두 종목의 최근 N일 일간수익률 상관계수

    상관 제약 진입 필터용. 상관이 높으면 동일 방향 리스크 집중.
    데이터 부족 시 0.0 반환 (진입 허용).
    """
    h1 = price_history.get(tk1, [])
    h2 = price_history.get(tk2, [])
    if len(h1) < 3 or len(h2) < 3:
        return 0.0
    # 최근 lookback+1 가격 → lookback 수익률
    p1 = h1[-(lookback + 1):] if len(h1) >= lookback + 1 else h1
    p2 = h2[-(lookback + 1):] if len(h2) >= lookback + 1 else h2
    # 공통 길이 맞추기 (뒤에서부터)
    n = min(len(p1), len(p2))
    p1 = p1[-n:]
    p2 = p2[-n:]
    r1 = [(p1[j] - p1[j - 1]) / p1[j - 1] * 100 for j in range(1, n) if p1[j - 1] > 0]
    r2 = [(p2[j] - p2[j - 1]) / p2[j - 1] * 100 for j in range(1, n) if p2[j - 1] > 0]
    if len(r1) < 3 or len(r2) < 3 or len(r1) != len(r2):
        return 0.0
    m1 = sum(r1) / len(r1)
    m2 = sum(r2) / len(r2)
    cov = sum((a - m1) * (b - m2) for a, b in zip(r1, r2)) / (len(r1) - 1)
    v1 = sum((a - m1) ** 2 for a in r1) / (len(r1) - 1)
    v2 = sum((b - m2) ** 2 for b in r2) / (len(r2) - 1)
    denom = math.sqrt(v1) * math.sqrt(v2)
    if denom < 1e-12:
        return 0.0
    return cov / denom


def calc_ticker_vol(price_history, ticker, lookback=10):
    """종목의 최근 N일 일간수익률 표준편차 (일간%, 연환산X)

    역변동성 비중 계산용. 변동성이 클수록 비중을 줄인다.
    """
    hist = price_history.get(ticker, [])
    if len(hist) < 3:
        return None
    prices = hist[-lookback:] if len(hist) >= lookback else hist
    rets = []
    for j in range(1, len(prices)):
        if prices[j - 1] > 0:
            rets.append((prices[j] - prices[j - 1]) / prices[j - 1] * 100)
    if len(rets) < 2:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    return math.sqrt(var) if var > 0 else None


# ── 데이터 로드 ──

def load_data():
    """DB에서 백테스트용 데이터 1회 로드

    Returns:
        dict with keys: all_dates, gap_dates, gap_by_date, daily_data, all_prices
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    all_dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    gap_dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE adj_gap IS NOT NULL ORDER BY date'
    ).fetchall()]

    gap_by_date = {}
    for d in gap_dates:
        rows = cursor.execute(
            'SELECT ticker, adj_gap FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL', (d,)
        ).fetchall()
        gap_by_date[d] = {r[0]: r[1] for r in rows}

    daily_data = {}
    for d in all_dates:
        rows = cursor.execute('''
            SELECT ticker, price, part2_rank, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, adj_gap
            FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL
        ''', (d,)).fetchall()
        daily_data[d] = {
            r[0]: {
                'price': r[1], 'part2_rank': r[2],
                'ntm_current': r[3], 'ntm_7d': r[4], 'ntm_30d': r[5],
                'ntm_60d': r[6], 'ntm_90d': r[7], 'adj_gap': r[8]
            } for r in rows
        }

    all_prices = {}
    for d in all_dates:
        rows = cursor.execute(
            'SELECT ticker, price FROM ntm_screening WHERE date=?', (d,)
        ).fetchall()
        all_prices[d] = {r[0]: r[1] for r in rows}

    conn.close()

    # VIX 데이터 로드 (yfinance)
    vix_by_date = {}
    try:
        import yfinance as yf
        vix_df = yf.download('^VIX', start=all_dates[0], end=all_dates[-1],
                             progress=False)
        for _, row in vix_df.iterrows():
            d = str(row.name.date()) if hasattr(row.name, 'date') else str(row.name)[:10]
            c = float(row['Close'].iloc[0]) if hasattr(row['Close'], 'iloc') else float(row['Close'])
            vix_by_date[d] = c
    except Exception as e:
        print(f'VIX 로드 실패: {e}')

    return {
        'all_dates': all_dates, 'gap_dates': gap_dates,
        'gap_by_date': gap_by_date, 'daily_data': daily_data,
        'all_prices': all_prices, 'vix_by_date': vix_by_date,
    }


def compute_w_gap_fast(date_str, gap_dates, gap_by_date):
    """w_gap = 3일 가중 adj_gap (T0×0.5 + T1×0.3 + T2×0.2)"""
    di = gap_dates.index(date_str) if date_str in gap_dates else -1
    if di < 0:
        return {}
    d0 = gap_dates[di]
    d1 = gap_dates[di - 1] if di >= 1 else None
    d2 = gap_dates[di - 2] if di >= 2 else None

    all_tickers = set()
    for d in [d0, d1, d2]:
        if d and d in gap_by_date:
            all_tickers.update(gap_by_date[d].keys())

    result = {}
    for tk in all_tickers:
        wg = gap_by_date.get(d0, {}).get(tk, 0) * 0.5
        if d1:
            wg += gap_by_date.get(d1, {}).get(tk, 0) * 0.3
        if d2:
            wg += gap_by_date.get(d2, {}).get(tk, 0) * 0.2
        result[tk] = wg
    return result


# ── 백테스트 엔진 ──

def run_backtest(db, config):
    """전략 변형 백테스트 실행

    config keys:
        label: str           — 전략 이름 (표시용)
        top_n: int           — 진입 순위 (Top N, default 3)
        exit_rank: int       — 이탈 순위선 (default 15)
        fixed_stop: float    — 고정 손절 % (e.g. -10, None=미사용)
        trailing_stop: float — 트레일링 스탑 % (고점 대비, e.g. -7, None=미사용)
        min_seg_entry: float — 진입 min_seg 기준 (default 0)
        min_seg_exit: float  — 이탈 min_seg 기준 (default -2)
        max_positions: int   — 최대 보유 종목 수 (default 3)
        sizing: str          — 'equal' (동일비중) or 'inverse_vol' (역변동성)
        vol_lookback: int    — 변동성 계산 기간 (default 5)
        vix_regime: bool     — VIX 국면별 포지션 수 조절 (default False)
        vix_thresholds: list — [안정/경계, 경계/높음] VIX 임계값 (default [20, 25])
        corr_constraint: bool — 진입 시 기존 보유 종목과 상관 제약 (default False)
        corr_threshold: float — 상관 임계값 (default 0.65, 이상이면 진입 스킵)
        portfolio_dd_limit: float — 포트폴리오 전체 드로다운 한도 (e.g. -15, None=미사용)
        dd_cooldown: int     — DD 한도 도달 후 진입 금지 기간 (거래일, default 5)

    Returns:
        (daily_returns, trade_log, portfolio)
    """
    all_dates = db['all_dates']
    gap_dates = db['gap_dates']
    gap_by_date = db['gap_by_date']
    daily_data = db['daily_data']
    all_prices = db['all_prices']

    top_n = config.get('top_n', 3)
    exit_rank = config.get('exit_rank', 15)
    fixed_stop = config.get('fixed_stop', None)
    trailing_stop = config.get('trailing_stop', None)
    min_seg_entry = config.get('min_seg_entry', 0)
    min_seg_exit = config.get('min_seg_exit', -2)
    max_pos = config.get('max_positions', 3)
    sizing = config.get('sizing', 'equal')
    vol_lookback = config.get('vol_lookback', 5)
    vix_regime = config.get('vix_regime', False)
    vix_thresholds = config.get('vix_thresholds', [20, 25])
    vix_by_date = db.get('vix_by_date', {})
    # VIX 노출 조절: 종목 수 유지, 총 투자 비율만 축소
    vix_exposure = config.get('vix_exposure', False)
    vix_exp_levels = config.get('vix_exp_levels', [1.0, 0.7, 0.4])  # [안정, 경계, 높음]
    # 상관 제약: 기존 보유 종목과 높은 상관 시 진입 스킵
    corr_constraint = config.get('corr_constraint', False)
    corr_threshold = config.get('corr_threshold', 0.65)
    # 포트폴리오 드로다운 한도
    portfolio_dd_limit = config.get('portfolio_dd_limit', None)
    dd_cooldown = config.get('dd_cooldown', 5)

    start_idx = 2  # 3일 검증 시작
    portfolio = {}  # {ticker: {entry_date, entry_price, peak_price}}
    trade_log = []
    daily_returns = []

    # 포트폴리오 NAV 추적 (DD 한도용)
    port_nav = 1.0
    port_peak_nav = 1.0
    dd_cooldown_remaining = 0  # 남은 쿨다운 일수

    # 변동성 계산용 가격 히스토리
    price_history = defaultdict(list)
    for j in range(start_idx):
        for tk, p in all_prices[all_dates[j]].items():
            if p and p > 0:
                price_history[tk].append(p)

    for i in range(start_idx, len(all_dates)):
        date = all_dates[i]
        prev_date = all_dates[i - 1]
        data = daily_data[date]
        prices = all_prices[date]
        prev_prices = all_prices[prev_date]

        # 가격 히스토리 갱신
        for tk, p in prices.items():
            if p and p > 0:
                price_history[tk].append(p)

        # w_gap 순위 계산
        w_gap = compute_w_gap_fast(date, gap_dates, gap_by_date)
        ticker_min_seg = {}
        for tk, info in data.items():
            ticker_min_seg[tk] = calc_min_seg(
                info['ntm_current'], info['ntm_7d'], info['ntm_30d'],
                info['ntm_60d'], info['ntm_90d']
            )
        eligible = [(tk, w_gap.get(tk, 0)) for tk in data.keys()
                     if ticker_min_seg.get(tk, 0) >= min_seg_exit]
        eligible.sort(key=lambda x: x[1])
        wgap_rank = {tk: rank + 1 for rank, (tk, _) in enumerate(eligible)}

        # 고점 갱신
        for tk in portfolio:
            cur = prices.get(tk)
            if cur and cur > portfolio[tk]['peak_price']:
                portfolio[tk]['peak_price'] = cur

        # ── 이탈 체크 ──
        exits = []
        for tk in list(portfolio.keys()):
            entry_price = portfolio[tk]['entry_price']
            peak_price = portfolio[tk]['peak_price']
            cur_price = prices.get(tk)
            if cur_price is None:
                exits.append((tk, 'delisted'))
                continue

            rank = wgap_rank.get(tk)
            ms = ticker_min_seg.get(tk, 0)
            ret_from_entry = (cur_price - entry_price) / entry_price * 100
            ret_from_peak = (cur_price - peak_price) / peak_price * 100

            # 이탈 우선순위: 순위밀림 > EPS추세 > 트레일링 > 고정손절
            if rank is None or rank > exit_rank:
                exits.append((tk, '순위밀림'))
            elif ms < min_seg_exit:
                exits.append((tk, 'EPS↓'))
            elif trailing_stop is not None and ret_from_peak <= trailing_stop:
                exits.append((tk, f'트레일링{trailing_stop}%'))
            elif fixed_stop is not None and ret_from_entry <= fixed_stop:
                exits.append((tk, '손절'))

        for tk, reason in exits:
            cur_price = prices.get(tk, portfolio[tk]['entry_price'])
            ret = (cur_price - portfolio[tk]['entry_price']) / portfolio[tk]['entry_price'] * 100
            trade_log.append({
                'ticker': tk, 'entry_date': portfolio[tk]['entry_date'],
                'exit_date': date, 'entry_price': portfolio[tk]['entry_price'],
                'exit_price': cur_price, 'return': ret, 'reason': reason,
            })
            del portfolio[tk]

        # ── 포트폴리오 드로다운 한도 체크 ──
        if portfolio_dd_limit is not None and dd_cooldown_remaining <= 0:
            dd_pct = (port_nav - port_peak_nav) / port_peak_nav * 100 if port_peak_nav > 0 else 0
            if dd_pct <= portfolio_dd_limit and portfolio:
                # 전 종목 강제 매도
                for tk in list(portfolio.keys()):
                    cur_price = prices.get(tk, portfolio[tk]['entry_price'])
                    ret = (cur_price - portfolio[tk]['entry_price']) / portfolio[tk]['entry_price'] * 100
                    trade_log.append({
                        'ticker': tk, 'entry_date': portfolio[tk]['entry_date'],
                        'exit_date': date, 'entry_price': portfolio[tk]['entry_price'],
                        'exit_price': cur_price, 'return': ret, 'reason': 'DD한도',
                    })
                    del portfolio[tk]
                dd_cooldown_remaining = dd_cooldown

        # 쿨다운 차감
        if dd_cooldown_remaining > 0:
            dd_cooldown_remaining -= 1

        # ── VIX 국면별 포지션 수 결정 ──
        if vix_regime and date in vix_by_date:
            vix = vix_by_date[date]
            if vix >= vix_thresholds[1]:    # 높음/위기
                cur_max = 1
            elif vix >= vix_thresholds[0]:  # 경계
                cur_max = 2
            else:                           # 안정
                cur_max = max_pos
        else:
            cur_max = max_pos

        # VIX 상승으로 초과 포지션 → 순위 낮은 것부터 강제 이탈
        if vix_regime and len(portfolio) > cur_max:
            ranked_holds = sorted(portfolio.keys(),
                                  key=lambda tk: wgap_rank.get(tk, 999),
                                  reverse=True)
            while len(portfolio) > cur_max:
                tk = ranked_holds.pop(0)
                cur_price = prices.get(tk, portfolio[tk]['entry_price'])
                ret = (cur_price - portfolio[tk]['entry_price']) / portfolio[tk]['entry_price'] * 100
                trade_log.append({
                    'ticker': tk, 'entry_date': portfolio[tk]['entry_date'],
                    'exit_date': date, 'entry_price': portfolio[tk]['entry_price'],
                    'exit_price': cur_price, 'return': ret, 'reason': 'VIX축소',
                })
                del portfolio[tk]

        # ── 진입 ──
        # DD 쿨다운 중이면 신규 진입 금지
        in_dd_cooldown = (portfolio_dd_limit is not None and dd_cooldown_remaining > 0)
        slots = cur_max - len(portfolio)
        if slots > 0 and not in_dd_cooldown:
            candidates = []
            for tk, wg in eligible[:30]:
                if tk in portfolio:
                    continue
                if wgap_rank.get(tk, 999) > top_n:
                    continue
                if ticker_min_seg.get(tk, -999) < min_seg_entry:
                    continue
                candidates.append(tk)
            # 상관 제약 + 슬롯 제한을 함께 적용
            entered = 0
            for tk in candidates:
                if entered >= slots:
                    break
                cur_price = prices.get(tk)
                if not cur_price:
                    continue
                # 상관 제약: 기존 보유 종목과 높은 상관이면 스킵
                if corr_constraint and portfolio:
                    skip = False
                    for held_tk in portfolio:
                        corr = calc_pairwise_corr(price_history, tk, held_tk)
                        if corr >= corr_threshold:
                            skip = True
                            break
                    if skip:
                        continue
                portfolio[tk] = {
                    'entry_date': date,
                    'entry_price': cur_price,
                    'peak_price': cur_price,
                }
                entered += 1

        # ── VIX 노출 비율 결정 ──
        if vix_exposure and date in vix_by_date:
            vix = vix_by_date[date]
            if vix >= vix_thresholds[1]:
                exposure = vix_exp_levels[2]    # 높음
            elif vix >= vix_thresholds[0]:
                exposure = vix_exp_levels[1]    # 경계
            else:
                exposure = vix_exp_levels[0]    # 안정
        else:
            exposure = 1.0

        # ── 일간 수익률 (비중 방식에 따라) ──
        if portfolio:
            tk_rets = {}
            for tk in portfolio:
                cur = prices.get(tk)
                prev = prev_prices.get(tk)
                if cur and prev and prev > 0:
                    tk_rets[tk] = (cur - prev) / prev * 100

            if not tk_rets:
                daily_returns.append(0)
            elif sizing == 'inverse_vol' and len(tk_rets) > 1:
                daily_returns.append(
                    _calc_inverse_vol_return(tk_rets, price_history, vol_lookback) * exposure
                )
            else:
                daily_returns.append(sum(tk_rets.values()) / len(tk_rets) * exposure)
        else:
            daily_returns.append(0)

        # ── 포트폴리오 NAV 갱신 (DD 한도 추적용) ──
        if portfolio_dd_limit is not None:
            port_nav *= (1 + daily_returns[-1] / 100)
            if port_nav > port_peak_nav:
                port_peak_nav = port_nav

    return daily_returns, trade_log, portfolio


def _calc_inverse_vol_return(tk_rets, price_history, vol_lookback):
    """역변동성 비중 일간 수익률 계산

    변동성 높은 종목 → 낮은 비중, 변동성 낮은 종목 → 높은 비중.
    vol 계산 불가 종목은 동일비중 fallback.
    """
    inv_vols = {}
    for tk in tk_rets:
        vol = calc_ticker_vol(price_history, tk, vol_lookback)
        inv_vols[tk] = 1.0 / vol if vol and vol > 0 else None

    has_vol = {tk: iv for tk, iv in inv_vols.items() if iv is not None}
    no_vol = [tk for tk, iv in inv_vols.items() if iv is None]

    if not has_vol:
        return sum(tk_rets.values()) / len(tk_rets)

    total_inv = sum(has_vol.values())
    if no_vol:
        n_total = len(tk_rets)
        no_vol_weight = 1.0 / n_total
        remaining = 1.0 - no_vol_weight * len(no_vol)
        weights = {tk: no_vol_weight for tk in no_vol}
        for tk, iv in has_vol.items():
            weights[tk] = (iv / total_inv) * remaining
    else:
        weights = {tk: iv / total_inv for tk, iv in has_vol.items()}

    return sum(tk_rets[tk] * weights[tk] for tk in tk_rets)


# ── 출력 ──

def print_trades(trade_log, portfolio, all_prices, last_date, label):
    """거래 내역 + 미청산 + 이탈사유 요약"""
    print(f'\n--- {label} ---')
    for t in trade_log:
        status = '✅' if t['return'] > 0 else '❌'
        print(f"  {status} {t['ticker']:6s} {t['entry_date']}→{t['exit_date']} "
              f"{t['return']:+.1f}% [{t['reason']}]")

    if portfolio:
        last_prices = all_prices[last_date]
        parts = []
        for tk, pos in portfolio.items():
            cur = last_prices.get(tk, pos['entry_price'])
            ret = (cur - pos['entry_price']) / pos['entry_price'] * 100
            parts.append(f'{tk}({ret:+.1f}%)')
        print(f'  미청산: {", ".join(parts)}')

    if trade_log:
        reasons = defaultdict(list)
        for t in trade_log:
            reasons[t['reason']].append(t['return'])
        parts = [f'{r} {len(v)}건({sum(v)/len(v):+.1f}%)'
                 for r, v in sorted(reasons.items())]
        print(f'  이탈사유: {" | ".join(parts)}')


# ── 전략 프리셋 ──

PRESETS = {
    # === 손절 방식 비교 ===
    'stop_loss': [
        {
            'label': '현행(고정-10%)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'equal',
        },
        {
            'label': '트레일링-5%',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': None, 'trailing_stop': -5,
            'sizing': 'equal',
        },
        {
            'label': '트레일링-7%',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': None, 'trailing_stop': -7,
            'sizing': 'equal',
        },
        {
            'label': '트레일링-10%',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': None, 'trailing_stop': -10,
            'sizing': 'equal',
        },
    ],
    # === 포지션 사이징 비교 ===
    'sizing': [
        {
            'label': '현행(동일비중)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'equal',
        },
        {
            'label': '역변동성(5일)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'inverse_vol', 'vol_lookback': 5,
        },
        {
            'label': '역변동성(10일)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'inverse_vol', 'vol_lookback': 10,
        },
        {
            'label': '역변동성(20일)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'inverse_vol', 'vol_lookback': 20,
        },
    ],
    # === VIX 국면별 포지션 비교 ===
    'vix_regime': [
        {
            'label': '현행(항상3종목)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'inverse_vol', 'vol_lookback': 5,
        },
        {
            'label': 'VIX국면(20/25)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'inverse_vol', 'vol_lookback': 5,
            'vix_regime': True, 'vix_thresholds': [20, 25],
        },
        {
            'label': 'VIX국면(22/28)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'inverse_vol', 'vol_lookback': 5,
            'vix_regime': True, 'vix_thresholds': [22, 28],
        },
        {
            'label': 'VIX국면(18/23)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'inverse_vol', 'vol_lookback': 5,
            'vix_regime': True, 'vix_thresholds': [18, 23],
        },
    ],
    # === VIX 노출 비율 비교 (종목 수 유지, 투자 비율만 축소) ===
    'vix_exposure': [
        {
            'label': '역변(VIX무시)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'inverse_vol', 'vol_lookback': 5,
        },
        {
            'label': '역변+노출(100/70/40)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'inverse_vol', 'vol_lookback': 5,
            'vix_exposure': True, 'vix_thresholds': [20, 25],
            'vix_exp_levels': [1.0, 0.7, 0.4],
        },
        {
            'label': '역변+노출(100/80/50)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'inverse_vol', 'vol_lookback': 5,
            'vix_exposure': True, 'vix_thresholds': [20, 25],
            'vix_exp_levels': [1.0, 0.8, 0.5],
        },
        {
            'label': '역변+노출(100/60/30)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'inverse_vol', 'vol_lookback': 5,
            'vix_exposure': True, 'vix_thresholds': [20, 25],
            'vix_exp_levels': [1.0, 0.6, 0.3],
        },
    ],
    # === 상관 제약 비교 ===
    'correlation': [
        {
            'label': '현행(상관무시)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'sizing': 'inverse_vol', 'vol_lookback': 5,
        },
        {
            'label': '상관<0.65제약',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'sizing': 'inverse_vol', 'vol_lookback': 5,
            'corr_constraint': True, 'corr_threshold': 0.65,
        },
        {
            'label': '상관<0.5제약',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'sizing': 'inverse_vol', 'vol_lookback': 5,
            'corr_constraint': True, 'corr_threshold': 0.5,
        },
    ],
    # === 포트폴리오 드로다운 한도 비교 ===
    'portfolio_dd': [
        {
            'label': '현행(DD무제한)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'sizing': 'inverse_vol', 'vol_lookback': 5,
        },
        {
            'label': 'DD-15%한도',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'sizing': 'inverse_vol', 'vol_lookback': 5,
            'portfolio_dd_limit': -15, 'dd_cooldown': 5,
        },
        {
            'label': 'DD-10%한도',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'sizing': 'inverse_vol', 'vol_lookback': 5,
            'portfolio_dd_limit': -10, 'dd_cooldown': 5,
        },
        {
            'label': 'DD-20%한도',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'sizing': 'inverse_vol', 'vol_lookback': 5,
            'portfolio_dd_limit': -20, 'dd_cooldown': 3,
        },
    ],
    # === 종합 최적 조합 ===
    'best': [
        {
            'label': '원래(동일비중)',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'equal',
        },
        {
            'label': '+역변동성',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'inverse_vol', 'vol_lookback': 5,
        },
        {
            'label': '+역변+VIX노출',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'trailing_stop': None,
            'sizing': 'inverse_vol', 'vol_lookback': 5,
            'vix_exposure': True, 'vix_thresholds': [20, 25],
            'vix_exp_levels': [1.0, 0.7, 0.4],
        },
        {
            'label': '+역변+상관+DD15',
            'top_n': 3, 'exit_rank': 15,
            'fixed_stop': -10, 'sizing': 'inverse_vol', 'vol_lookback': 5,
            'corr_constraint': True, 'corr_threshold': 0.65,
            'portfolio_dd_limit': -15, 'dd_cooldown': 5,
        },
    ],
}


def main():
    from bt_metrics import compare, report

    print('데이터 로딩...')
    db = load_data()
    dates = db['all_dates']
    print(f'데이터: {len(dates)}거래일 ({dates[0]} ~ {dates[-1]})')

    # 실행할 프리셋 선택 (커맨드라인 인자 또는 기본값)
    preset_name = sys.argv[1] if len(sys.argv) > 1 else 'all'

    if preset_name == 'all':
        presets_to_run = list(PRESETS.items())
    elif preset_name in PRESETS:
        presets_to_run = [(preset_name, PRESETS[preset_name])]
    else:
        print(f'사용 가능한 프리셋: {", ".join(PRESETS.keys())}, all')
        return

    for name, strategies in presets_to_run:
        print(f'\n{"="*80}')
        print(f'  비교: {name}')
        print(f'{"="*80}')

        results = []
        for cfg in strategies:
            daily_rets, trades, port = run_backtest(db, cfg)
            results.append((cfg['label'], daily_rets, trades))
            print_trades(trades, port, db['all_prices'], dates[-1], cfg['label'])

        compare(results)

    # 현행 vs 개선안 상세 리포트
    print(f'\n{"="*80}')
    print(f'  상세 리포트: 현행 vs 개선안')
    print(f'{"="*80}')
    for cfg in PRESETS['best']:
        daily_rets, trades, port = run_backtest(db, cfg)
        report(daily_rets, trades, label=cfg['label'])


if __name__ == '__main__':
    main()
