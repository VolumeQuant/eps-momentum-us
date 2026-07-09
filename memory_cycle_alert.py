# -*- coding: utf-8 -*-
"""메모리 사이클 경보 — 알림 전용(매매 무관), 2026-07-09 검증분의 1단계 배선.

경보 2종 (research/CYCLE_DETECTOR_FINDINGS_2026_07_09.md):
  ①주가: 6종(MU·SNDK·WDC·STX·삼성·하이닉스) 중 4종+가 자기 MA60 아래 3일 연속 → 발동, 해소 15일 연속 → 해제
  ②수출: 한국 반도체 수출 YoY(ECOS 403Y001/30911AA) 3개월 연속 하락 → 발동
발동 중 행동(권고): 메모리 보유 노출 25~0%로 축소 (10년 프록시: 최악 -54%→-23%, 수익 보존)
무상태(stateless): 매 실행 2년치 가격으로 상태기계 재계산 → 어느 머신에서든 동일 답.
사용: python memory_cycle_alert.py [--send]  (--send 시 개인봇 발송, 기본은 출력만)
"""
import sys
import numpy as np
import pandas as pd

CLUSTER = ['MU', 'SNDK', 'WDC', 'STX', '005930.KS', '000660.KS']
MA, K_FIRE, N_CONFIRM, N_CLEAR = 60, 4, 3, 15


def price_alarm():
    import yfinance as yf
    px = yf.download(CLUSTER, period='2y', progress=False, auto_adjust=True, threads=2)['Close']
    px = px.dropna(how='all').ffill()
    below = px.lt(px.rolling(MA).mean())
    breadth_n = below.sum(axis=1)
    raw = breadth_n >= K_FIRE
    fire = raw.rolling(N_CONFIRM).sum() == N_CONFIRM
    off = (~raw).rolling(N_CLEAR).sum() == N_CLEAR
    on = False
    for d in raw.index:
        if not on and fire.loc[d]:
            on = True
        elif on and off.loc[d]:
            on = False
    last = px.index[-1]
    names = [t for t in CLUSTER if bool(below[t].iloc[-1])]
    return on, int(breadth_n.iloc[-1]), names, last.date()


def export_alarm():
    sys.path.insert(0, r'C:\dev')
    import requests
    from config import ECOS_API_KEY
    url = (f'https://ecos.bok.or.kr/api/StatisticSearch/{ECOS_API_KEY}'
           f'/json/kr/1/10000/403Y001/M/201301/209912/30911AA')
    rows = requests.get(url, timeout=30).json().get('StatisticSearch', {}).get('row', [])
    s = pd.Series({pd.Period(r['TIME'], freq='M'): float(r['DATA_VALUE']) for r in rows}).sort_index()
    yoy = (s / s.shift(12) - 1) * 100
    falling3 = bool((yoy.diff().iloc[-1] < 0) and (yoy.diff().iloc[-2] < 0) and (yoy.diff().iloc[-3] < 0))
    return falling3, float(yoy.iloc[-1]), str(yoy.index[-1])


def build_message():
    p_on, n, names, asof = price_alarm()
    try:
        e_on, e_yoy, e_month = export_alarm()
        exp_line = (f'{"🚨 발동" if e_on else "✅ 정상"} — 수출 YoY {e_yoy:+.0f}% ({e_month}, 확정치 기준)')
    except Exception as ex:
        e_on, exp_line = False, f'⚠️ 수출 데이터 조회 실패({ex}) — 주가 경보만 유효'
    fired = p_on or e_on
    head = '🚨 <b>메모리 사이클 경보 발동</b>' if fired else '🧭 메모리 사이클 경보: 꺼짐'
    lines = [head, '',
             f'주가 경보: {"🚨 발동" if p_on else "꺼짐"} — {n}/6 이탈 ({asof})',
             ('  이탈: ' + ', '.join(names)) if names else '  이탈 종목 없음',
             f'수출 경보: {exp_line}']
    if fired:
        lines += ['', '권고: 메모리 보유 노출 25~0%로 축소',
                  '(10년 검증: 최악 -54%→-23%, 수익 보존)', '해제 알림 올 때까지 유지']
    return '\n'.join(lines), fired


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    msg, fired = build_message()
    print(msg.replace('<b>', '').replace('</b>', ''))
    if '--send' in sys.argv:
        import requests
        sys.path.insert(0, r'C:\dev')
        from config import TELEGRAM_BOT_TOKEN, TELEGRAM_PRIVATE_ID
        requests.post(f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage',
                      data={'chat_id': TELEGRAM_PRIVATE_ID, 'text': msg, 'parse_mode': 'HTML'},
                      timeout=20)
        print('[sent]')
