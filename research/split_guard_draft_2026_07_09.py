# -*- coding: utf-8 -*-
"""스플릿 가드 — 독립 초안 (2026-07-09).

★ 이 파일은 research/ 전용 초안이다. daily_runner.py나 DB를 import/수정하지 않는다.
  단독 실행: `python research/split_guard_draft_2026_07_09.py` (unittest 3+2종 실행).

설계서: research/SPLIT_GUARD_DESIGN_2026_07_09.md 참고.

핵심 아이디어 (실측 근거는 설계서 §1.1/§1.2):
  - DB(ntm_screening.price)는 "그날 수집 시점의 raw 종가"를 그대로 저장 — 소급 조정 안 함.
  - 같은 날 fetch한 yfinance hist(1년 윈도우)는 그 안에서는 auto_adjust로 항상 정합
    (야후 eps_trend도 동일 — 90일 전 EPS 추정치도 오늘 기준 최신 주식수로 리스케일되어 온다,
    MLI 2026-07-01 실측: ntm_current/7d/30d/60d/90d 전부 동시에 정확히 0.5배).
  - 문제는 오직 "다른 날짜에 저장된 raw price끼리 직접 비교"하는 코드
    (_vm_paper_state NAV, _get_system_performance NAV, 편입가 대비 수익률, high30/dd_30_25).
  - 해법: 저장가 시계열에서 스플릿 시그니처(하루 가격비 이상치)를 값싸게 스크린 →
    yfinance Ticker.splits로 확인(윈도우 매칭, 정확 일치 강제 금지 — POWL 4일 오차 실측) →
    확인된 비율만 조정계수로 적용. 미확인이면 절대 임의 보정하지 않고 경고만
    (ASGN/CAR 실측: 분할급 절벽인데 yf splits엔 기록이 없는 케이스가 실제로 존재).
"""
from __future__ import annotations

import unittest
from typing import Callable, Optional


# ---------------------------------------------------------------------------
# 1. 값싼 1차 스크린 — 연속 저장일 가격비가 이상치인 지점을 후보로 표시
# ---------------------------------------------------------------------------

def detect_split_candidates(price_series: dict, ratio_low: float = 0.75,
                              ratio_high: float = 1.34) -> list:
    """{date_str(YYYY-MM-DD 오름차순 정렬 가능): price} → [(prev_date, date, ratio), ...]

    임계값 근거(설계서 §2.3): 3-for-2 분할(0.667)까지 잡되, 이 유니버스(대형/주도주)의
    정상적인 어닝발 하루 변동과는 여유를 둠. 이 함수는 "API를 부를지 말지"를 정하는
    스크린일 뿐 최종 판정이 아니므로 넓게 잡아도 비용은 API 호출 1회뿐 — false negative
    (진짜 분할을 놓치는 것)가 false positive보다 훨씬 위험하다는 원칙.
    """
    dates = sorted(price_series.keys())
    out = []
    prev_d = None
    for d in dates:
        p = price_series.get(d)
        if prev_d is not None and p:
            pp = price_series.get(prev_d)
            if pp and pp > 0:
                ratio = p / pp
                if ratio <= ratio_low or ratio >= ratio_high:
                    out.append((prev_d, d, ratio))
        prev_d = d
    return out


# ---------------------------------------------------------------------------
# 2. 확인 — yfinance Ticker.splits (주입 가능한 fetcher로 테스트/합성 데이터 지원)
# ---------------------------------------------------------------------------

def _yf_splits_fetcher(ticker: str) -> Optional[list]:
    """실제 프로덕션용 fetcher. 반환: [(date_str, ratio), ...] 또는 조회 실패 시 None.

    None과 빈 리스트[]는 의미가 다르다:
      None = 조회 자체 실패(네트워크/삭제된 티커 등, 실측: ASGN "possibly delisted") → 재시도 여지.
      []   = 조회 성공했으나 분할 이력 없음(실측: CAR/KD도 이 케이스 — 절벽은 있는데 분할기록 없음)
             → 이 경우 절벽은 분할이 아닐 수 있으므로 보정하면 안 됨(단, 사람에게 경고는 필요).
    """
    try:
        import yfinance as yf
        s = yf.Ticker(ticker).splits
        return [(ts.strftime('%Y-%m-%d'), float(v)) for ts, v in s.items()]
    except Exception:
        return None


def confirm_split_ratio(ticker: str, candidate_date: str,
                          splits_fetcher: Callable[[str], Optional[list]],
                          window_days: int = 7,
                          _cache: Optional[dict] = None) -> Optional[float]:
    """candidate_date ± window_days 안에 확정 분할이 있으면 그 비율(들의 곱)을 반환.

    window_days 근거: POWL 실측 — DB 절벽은 2026-04-01→04-02인데 yfinance가 기록한
    ex-date는 04-06 (4일 차이). 정확 일치(`==`)로 매칭하면 진짜 분할도 놓친다.

    _cache: {ticker: splits_list_or_None} — 호출자가 세션 내 재사용하려면 전달(같은 티커에
      대해 API를 여러 후보 날짜마다 반복 호출하지 않도록). 프로덕션에서는 여기에 더해
      data_cache/split_events.json 같은 영속 캐시를 씌우는 것을 권장(설계서 §4).
    """
    from datetime import datetime, timedelta
    cache = _cache if _cache is not None else {}
    if ticker not in cache:
        cache[ticker] = splits_fetcher(ticker)
    splits = cache[ticker]
    if not splits:
        return None  # None(조회 실패) 또는 []([]=분할 없음 확인됨) 둘 다 "확정 불가"로 취급

    cd = datetime.strptime(candidate_date, '%Y-%m-%d')
    lo, hi = cd - timedelta(days=window_days), cd + timedelta(days=window_days)
    matched = [ratio for (sd, ratio) in splits
               if lo <= datetime.strptime(sd, '%Y-%m-%d') <= hi]
    if not matched:
        return None
    ratio = 1.0
    for r in matched:
        ratio *= r
    return ratio


# ---------------------------------------------------------------------------
# 3. 조정계수 계산 — 메인 진입점 (DB에 아무것도 쓰지 않는다)
# ---------------------------------------------------------------------------

def compute_split_adjustments(ticker: str, price_series: dict,
                                splits_fetcher: Callable[[str], Optional[list]] = _yf_splits_fetcher,
                                ratio_low: float = 0.75, ratio_high: float = 1.34,
                                window_days: int = 7) -> tuple:
    """{date: raw_price} → ({date: factor}, [unconfirmed_candidate_date, ...])

    factor: 그 날짜의 raw_price에 곱하면 "가장 최근 주식수 기준"으로 정규화된 가격이 되는 배수.
      - 확정된 분할이 없는 날짜(또는 시계열 전체) → factor=1.0
      - 분할일 이전 날짜 → factor = 1 / (그 날짜 이후 발생한 모든 확정 분할 비율의 누적곱)
        (yfinance Ticker.splits 관례: ratio>1=정분할[주가 축소, 예 2.0=2-for-1],
         ratio<1=역분할[주가 확대, 예 0.333≈1-for-3] — 실측: MLI 2.0/DD 0.333/POWL 3.0.
         양방향 모두 동일 공식 factor=1/ratio로 처리됨.)

    unconfirmed: 시그니처는 감지됐지만 splits_fetcher로 확정 못 한 날짜들 — 이 경우 factor는
      1.0으로 둔 채(임의 보정 금지) 호출자가 알림/수동검토 큐에 올리는 데 쓴다
      (실측: ASGN·CAR — 분할급 절벽인데 yfinance 분할기록이 없던 실제 사례).
    """
    dates = sorted(price_series.keys())
    candidates = detect_split_candidates(price_series, ratio_low, ratio_high)
    _fetch_cache: dict = {}
    confirmed = {}  # split_date -> cumulative ratio (같은 날 여러 후보면 곱)
    unconfirmed = []
    for prev_d, d, _raw_ratio in candidates:
        ratio = confirm_split_ratio(ticker, d, splits_fetcher, window_days, _fetch_cache)
        if ratio is None:
            unconfirmed.append(d)
            continue
        confirmed[d] = confirmed.get(d, 1.0) * ratio

    if not confirmed:
        return {d: 1.0 for d in dates}, unconfirmed

    # 확정된 분할일들을 오름차순으로 두고, 각 날짜 이전 구간에 누적 역비율을 적용.
    split_dates = sorted(confirmed.keys())
    adjustments = {}
    for d in dates:
        cum = 1.0
        for sd in split_dates:
            if d < sd:
                cum *= confirmed[sd]
        adjustments[d] = 1.0 / cum
    return adjustments, unconfirmed


# ---------------------------------------------------------------------------
# 4. 리플레이 통합용 래퍼 — _vm_paper_state의 px_map / _replay_holdings의 pxh와
#    동일 구조({ticker: {date: price}})를 받아 동일 구조로 반환한다.
#    실제 통합 시: 해당 딕셔너리를 이 함수의 리턴값으로 교체하기만 하면 된다
#    (daily_runner.py L3964-3968 px_map, L4232-4234 pxh, L6039-6047 all_prices 부근).
# ---------------------------------------------------------------------------

def build_adjusted_lookup(pxh: dict,
                            splits_fetcher: Callable[[str], Optional[list]] = _yf_splits_fetcher,
                            **kwargs) -> tuple:
    """{ticker: {date: raw_price}} → ({ticker: {date: adjusted_price}}, {ticker: [unconfirmed...]})"""
    out = {}
    unconfirmed_all = {}
    for tk, series in pxh.items():
        adj, unconfirmed = compute_split_adjustments(tk, series, splits_fetcher, **kwargs)
        out[tk] = {d: (series[d] * adj.get(d, 1.0)) for d in series}
        if unconfirmed:
            unconfirmed_all[tk] = unconfirmed
    return out, unconfirmed_all


# ============================================================================
# 단위테스트 — 합성 데이터 (실측된 실패모드를 재현)
# ============================================================================

class TestSplitGuard(unittest.TestCase):

    def test_2for1_split(self):
        """2:1 분할 (MLI 2026-07-01 실측 패턴 재현): 조정 후 day-over-day 수익률이
        분할 경계에서 정상 범위로 복원되어야 한다."""
        prices = {
            '2026-06-25': 134.39, '2026-06-26': 128.20, '2026-06-29': 122.83,
            '2026-06-30': 122.93, '2026-07-01': 57.42, '2026-07-02': 56.50,
            '2026-07-06': 56.84, '2026-07-07': 55.67,
        }

        def fake_fetcher(tk):
            return [('2026-07-01', 2.0)]

        adj, unconfirmed = compute_split_adjustments('MLI', prices, fake_fetcher)
        self.assertEqual(unconfirmed, [])
        # 분할일 이전 = factor 0.5, 이후 = factor 1.0
        self.assertAlmostEqual(adj['2026-06-30'], 0.5)
        self.assertAlmostEqual(adj['2026-06-25'], 0.5)
        self.assertAlmostEqual(adj['2026-07-01'], 1.0)
        self.assertAlmostEqual(adj['2026-07-07'], 1.0)
        # 조정가로 day-over-day 수익률 재계산 → 분할일 경계에 절벽이 없어야 함
        adjusted = {d: prices[d] * adj[d] for d in prices}
        r = adjusted['2026-07-01'] / adjusted['2026-06-30'] - 1
        self.assertLess(abs(r), 0.10)  # 절벽(-53%) 사라지고 정상 변동폭 이내

    def test_10for1_split(self):
        """10:1 분할 — 훨씬 극단적인 비율에서도 동일 로직이 성립해야 한다."""
        prices = {
            '2026-01-01': 1000.0, '2026-01-02': 1010.0, '2026-01-03': 990.0,
            '2026-01-06': 99.0, '2026-01-07': 100.5,
        }

        def fake_fetcher(tk):
            return [('2026-01-06', 10.0)]

        adj, unconfirmed = compute_split_adjustments('SYN10', prices, fake_fetcher)
        self.assertEqual(unconfirmed, [])
        self.assertAlmostEqual(adj['2026-01-03'], 0.1)
        self.assertAlmostEqual(adj['2026-01-06'], 1.0)
        adjusted = {d: prices[d] * adj[d] for d in prices}
        r = adjusted['2026-01-06'] / adjusted['2026-01-03'] - 1
        self.assertLess(abs(r), 0.05)

    def test_reverse_split(self):
        """역분할 (DD 2026-06-24 실측 패턴: ratio=0.333, 주가 약 3배 급등)."""
        prices = {
            '2026-06-22': 48.19, '2026-06-23': 46.67,
            '2026-06-24': 137.82, '2026-06-25': 137.80, '2026-06-26': 137.22,
        }

        def fake_fetcher(tk):
            return [('2026-06-24', 1.0 / 3.0)]

        adj, unconfirmed = compute_split_adjustments('DD', prices, fake_fetcher)
        self.assertEqual(unconfirmed, [])
        self.assertAlmostEqual(adj['2026-06-23'], 3.0, places=3)
        self.assertAlmostEqual(adj['2026-06-24'], 1.0)
        adjusted = {d: prices[d] * adj[d] for d in prices}
        r = adjusted['2026-06-24'] / adjusted['2026-06-23'] - 1
        self.assertLess(abs(r), 0.10)

    def test_dividend_2pct_not_flagged(self):
        """배당락 2% 하락 — 시그니처 임계값(±25~34%)에 전혀 안 걸려야 하며,
        splits_fetcher가 아예 호출되지 않아야 한다(비용 절약 + 오탐 방지)."""
        prices = {
            '2026-03-01': 100.0, '2026-03-02': 100.5, '2026-03-03': 98.0,  # ex-div -2.5%
            '2026-03-04': 98.8, '2026-03-05': 99.5,
        }
        calls = []

        def fake_fetcher(tk):
            calls.append(tk)
            return []

        adj, unconfirmed = compute_split_adjustments('DIVCO', prices, fake_fetcher)
        self.assertEqual(calls, [])  # 스크린 단계에서 이미 걸러져 API 호출 자체가 없어야 함
        self.assertEqual(unconfirmed, [])
        for d in prices:
            self.assertAlmostEqual(adj[d], 1.0)

    def test_unconfirmed_cliff_not_auto_corrected(self):
        """미확인 절벽 (ASGN/CAR 2026-04-22→23 실측 패턴: 분할급 절벽이나 yf splits에 기록 없음).
        임의로 보정하면 안 되고(진짜 폭락일 수도 있음), unconfirmed 목록에 잡혀야 한다."""
        prices = {
            '2026-04-20': 40.0, '2026-04-21': 40.43, '2026-04-22': 40.43,
            '2026-04-23': 19.53, '2026-04-24': 19.30,
        }

        def fake_fetcher_none(tk):
            return None  # ASGN 실측: "possibly delisted", 조회 자체 실패

        def fake_fetcher_empty(tk):
            return []  # CAR 실측: 조회는 되나 분할 이력 없음

        for fetcher in (fake_fetcher_none, fake_fetcher_empty):
            adj, unconfirmed = compute_split_adjustments('ASGN', prices, fetcher)
            self.assertIn('2026-04-23', unconfirmed)
            # 확정 못 했으므로 factor는 그대로 1.0 (임의 보정 금지)
            for d in prices:
                self.assertAlmostEqual(adj[d], 1.0)

    def test_date_window_slop_powl(self):
        """분할 확인일이 절벽 날짜와 정확히 안 맞는 경우 (POWL 실측: DB 절벽 04-01→04-02,
        yfinance ex-date 04-06, 4일 오차). 정확 일치가 아니라 ±window_days로 잡혀야 한다."""
        prices = {
            '2026-03-30': 554.05, '2026-04-01': 554.05, '2026-04-02': 182.60,
            '2026-04-03': 183.00, '2026-04-06': 185.00,
        }

        def fake_fetcher(tk):
            return [('2026-04-06', 3.0)]  # ex-date가 절벽(04-02)보다 4일 늦게 기록됨

        adj, unconfirmed = compute_split_adjustments('POWL', prices, fake_fetcher, window_days=7)
        self.assertEqual(unconfirmed, [])
        self.assertAlmostEqual(adj['2026-04-01'], 1.0 / 3.0)
        self.assertAlmostEqual(adj['2026-04-02'], 1.0)

        # window_days를 좁히면(예: 2일) 놓쳐야 정상 — 회귀 방지용 대조 테스트
        adj2, unconfirmed2 = compute_split_adjustments('POWL', prices, fake_fetcher, window_days=2)
        self.assertIn('2026-04-02', unconfirmed2)

    def test_build_adjusted_lookup_multi_ticker(self):
        """_vm_paper_state의 px_map / _replay_holdings의 pxh와 동일한 구조 통합 테스트."""
        pxh = {
            'MLI': {'2026-06-30': 122.93, '2026-07-01': 57.42, '2026-07-02': 56.50},
            'CLEAN': {'2026-06-30': 100.0, '2026-07-01': 101.0, '2026-07-02': 99.5},
        }

        def fake_fetcher(tk):
            if tk == 'MLI':
                return [('2026-07-01', 2.0)]
            return []

        out, unconfirmed_all = build_adjusted_lookup(pxh, fake_fetcher)
        self.assertAlmostEqual(out['MLI']['2026-06-30'], 122.93 * 0.5)
        self.assertAlmostEqual(out['MLI']['2026-07-01'], 57.42)
        # 분할 없는 종목은 완전히 원본과 동일해야 함 (no-op 보장 — 설계서 §4 1차 합격 기준)
        for d, p in pxh['CLEAN'].items():
            self.assertAlmostEqual(out['CLEAN'][d], p)
        self.assertEqual(unconfirmed_all, {})


if __name__ == '__main__':
    unittest.main(verbosity=2)
