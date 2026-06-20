# -*- coding: utf-8 -*-
"""DEPLOY-READY (미배포): 섹터 브레드스 조기경보 레그 — daily_runner.py get_market_regime() 통합용.

검증: research/early_warn/FINDINGS_2026_06_20.md
  섹터 브레드스(11 SPDR 중 자기 200DMA 위 비율 < 0.45, 3일확인/15일확인) OR 현행 게이트
  → MDD −36.5→−27.4, Calmar 0.36→0.44, WF최소 0.34→0.40, LOWO 0.37→0.46 (인접CV 0.070, OOS·LOBO 통과).

★ 이 파일은 독립 모듈로 검증/재현용. 프로덕션 적용은 사용자 승인 후 아래 [통합지침]대로.
★ 기본 OFF(REGIME_BREADTH_DISABLE 기본 '1'). v121 REGIME_TS_DISABLE과 동일 staging.
"""
import os

REGIME_BREADTH_DISABLE = os.environ.get('REGIME_BREADTH_DISABLE', '1') == '1'
BREADTH_THR = float(os.environ.get('REGIME_BREADTH_THR', '0.45'))   # plateau 0.35~0.55 중앙
BREADTH_NE = int(os.environ.get('REGIME_BREADTH_NE', '3'))          # 진입 확인일
BREADTH_NX = int(os.environ.get('REGIME_BREADTH_NX', '15'))         # 퇴출 확인일
SECTOR_ETFS = ['XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLB', 'XLP', 'XLU', 'XLY', 'XLRE', 'XLC']


def _confirm(raw_seq, ne, nx):
    """오래된→최신 bool 시퀀스 → 최신 defense 여부 (진입 ne·퇴출 nx 연속, 히스테리시스)."""
    st = False; sd = sb = 0
    for d in raw_seq:
        sd = sd + 1 if d else 0
        sb = 0 if d else sb + 1
        if not st and sd >= ne:
            st = True
        elif st and sb >= nx:
            st = False
    return st


def compute_sector_breadth():
    """11개 SPDR 섹터의 '자기 200DMA 위 비율' 시계열 + 현재 방어신호.

    Returns dict {breadth_now: 0~1, defense: bool, frac_series: pd.Series} 또는 실패시 None.
    배치 다운로드 1회(threads 제어)로 rate-limit 영향 최소화(CLAUDE.md 수집안정화 정합).
    stateless: ~2년 히스토리서 매 실행 재계산(상태파일 불필요, get_market_regime과 동일).
    """
    if REGIME_BREADTH_DISABLE:
        return None
    try:
        import yfinance as yf
        import pandas as pd
        import numpy as np
        df = yf.download(SECTOR_ETFS, period='2y', auto_adjust=True, progress=False, threads=2)
        close = df['Close'] if 'Close' in df else df
        avail = above = None
        for c in close.columns:
            s = close[c].dropna()
            if len(s) < 200:
                continue
            ma200 = s.rolling(200).mean()
            a = (s > ma200)
            ok = s.notna() & ma200.notna()
            above = a.astype(float).where(ok, 0) if above is None else above.add(a.astype(float).where(ok, 0), fill_value=0)
            avail = ok.astype(float) if avail is None else avail.add(ok.astype(float), fill_value=0)
        frac = (above / avail.replace(0, np.nan)).dropna()
        raw = (frac < BREADTH_THR)
        defense = _confirm(list(raw.values[-300:]), BREADTH_NE, BREADTH_NX)
        return {'breadth_now': float(frac.iloc[-1]), 'defense': bool(defense), 'frac_series': frac}
    except Exception:
        return None  # 실패 시 None → 현행 게이트만(폴백, 안전)


# ============================ [통합지침] daily_runner.py get_market_regime() ============================
#
# 1) 모듈 상단 상수 옆(REGIME_TS_DISABLE 근처)에:
#       REGIME_BREADTH_DISABLE = os.environ.get('REGIME_BREADTH_DISABLE', '1') == '1'  # 기본 OFF
#       BREADTH_THR=0.45; BREADTH_NE=3; BREADTH_NX=15
#       SECTOR_ETFS = ['XLK','XLF','XLE','XLV','XLI','XLB','XLP','XLU','XLY','XLRE','XLC']
#    + 위 _confirm/compute_sector_breadth 함수 이식(이미 _confirm_regime 존재 → 재사용 가능).
#
# 2) get_market_regime() 내부, ma_defense/vix_defense 계산 직후:
#       breadth_defense = False; breadth_now = None
#       if not REGIME_BREADTH_DISABLE:
#           bd = compute_sector_breadth()
#           if bd: breadth_defense, breadth_now = bd['defense'], bd['breadth_now']
#    그리고 최종 regime 판정에 OR:
#       'regime': 'defense' if (ma_defense or vix_defense or breadth_defense) else 'boost'
#    reason에 추가: if breadth_defense: reasons.append(f'섹터 참여 협소(브레드스 {breadth_now*100:.0f}%<{BREADTH_THR*100:.0f}%)')
#
# 3) ★1차 권장 = 표시전용: 위 OR 라인은 켜지 말고, early_warn(Day-1)에만 항상 표시:
#       if breadth_now is not None and breadth_now < 0.55:
#           warn.append(f'섹터 브레드스 <b>{breadth_now*100:.0f}%</b> (45%↓ 지속 시 조기방어 후보)')
#    → 매매 무변경으로 1~2 약세장 관찰 후, OR-leg 활성화 결정.
#
# 4) _get_system_performance 시뮬은 라이브 게이트와 동일 OR로 맞춰야 BT==production 정합.
#    (활성화 시에만. 표시전용 단계에선 시뮬 무변경.)
#
# 롤백: REGIME_BREADTH_DISABLE=1 (기본값). 또는 OR 라인 제거.
# 리스크: 메가캡 협소장(2023류) 위양성 — FINDINGS 한계#2. 표시전용 staging이 이 리스크를 흡수.
# =====================================================================================================

if __name__ == '__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    os.environ['REGIME_BREADTH_DISABLE'] = '0'  # 로컬 테스트용 강제 ON
    globals()['REGIME_BREADTH_DISABLE'] = False
    r = compute_sector_breadth()
    print('현재 섹터 브레드스:', r)
