# -*- coding: utf-8 -*-
"""메모리 사이클 경보 — 알림 전용(매매 무관), 2026-07-09 검증분의 1단계 배선.

경보 2종 (research/CYCLE_DETECTOR_FINDINGS_2026_07_09.md):
  ①주가: 6종(MU·SNDK·WDC·STX·삼성·하이닉스) 중 4종+가 자기 MA60 아래 1일 → 발동, 해소 5일 연속 → 해제
  ②수출: 한국 반도체 수출 YoY(ECOS 403Y001/30911AA) 3개월 연속 하락 → 발동
발동 중 행동(권고): 메모리 보유 노출 25~0%로 축소 (10년 프록시: 최악 -54%→-23%, 수익 보존)
무상태(stateless): 매 실행 2년치 가격으로 상태기계 재계산 → 어느 머신에서든 동일 답.
사용: python memory_cycle_alert.py [--send]  (--send 시 개인봇 발송, 기본은 출력만)
"""
import sys
import numpy as np
import pandas as pd

CLUSTER = ['MU', 'SNDK', 'WDC', 'STX', '005930.KS', '000660.KS']
# 2026-07-09 사용자 도전으로 재캘리브레이션: 진입3일/해제15일은 브레드스 관행 상속(미검증)이었고
# 10년 그리드(E0~5×X3~15×배치) 실측서 빠른 진입(0~1일)·빠른 해제(3~5일)가 전 구간 우월(고원).
# E1/X5: Calmar 1.21→1.49~1.52, MDD −35.7→−30.8. 배치는 0%(전량 현금) 채택(2026-07-09 사용자):
#   빠른 타이밍에선 0% vs 25%가 동전던지기(Calmar 1.49 vs 1.52, MDD는 0%가 우위 −30.8 vs −31.5)
#   → 단순성·철학 일관(방어=현금)·준수 가능성으로 0% 확정. 상세: research/CYCLE_TIMING_SWEEP_2026_07_09.md
MA, K_FIRE, N_CONFIRM, N_CLEAR = 60, 4, 1, 5


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
        exp_line = f'한국 반도체 수출 {"석 달 연속 감소 🔴" if e_on else f"전년비 {e_yoy:+.0f}% 호조"}'
    except Exception:
        e_on, exp_line = False, '수출 데이터 조회 실패 (주가 감시만 유효)'
    fired = p_on or e_on
    KRN = {'005930.KS': '삼성전자', '000660.KS': 'SK하이닉스'}
    disp = [KRN.get(t, t) for t in names]
    if fired:
        lines = ['🚦 <b>메모리 위험 감시등: 🔴 켜짐</b>',
                 f'메모리 대표주 {n}/6이 동반 하락 추세입니다.',
                 f'({", ".join(disp)})' if disp else '',
                 exp_line, '',
                 '→ 메모리 종목은 전부 팔고',
                 '  판 돈은 현금으로 보관하세요.',
                 '  (다른 종목은 그대로 둡니다)',
                 '  (다른 종목으로 갈아타지 않기)',
                 '  초록불 알림이 올 때까지 유지.',
                 '  근거: 지난 10년 큰 하락 5번 전부 감지,',
                 '  따랐다면 최악 손실 -54%→-23%']
    else:
        detail = f'약세 신호 {n}/6' + (f' ({", ".join(disp)})' if disp else '')
        lines = ['🚦 메모리 위험 감시등: 🟢 정상',
                 f'{detail} — 4/6부터 빨간불 · {exp_line}']
    return '\n'.join([l for l in lines if l]), fired


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
