# -*- coding: utf-8 -*-
"""재캘리브레이션(전수 gap1.5 + Top5) 적용 TOP5 개인봇 발송 (2026-07-09 일회성, 사용자 지시).

표시용으로 에폭을 과거로 밀어 7/8 데이터에 새 규칙을 적용해 렌더 — production 에폭(7/9)은 불변."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr

dr.VM_GATE_FULL_FROM = '2026-07-01'  # 표시 전용: 7/8 데이터에 새 규칙 적용해 미리보기
picks = dr._vm_pick('2026-07-08')

lines = [
    '🧪 <b>새 규칙 적용 TOP5 미리보기</b>',
    '(7/8 종가 데이터 기준)',
    '',
    '오늘 밤 고친 것 2가지:',
    '① 이익성장 검사를 141종목만 하던',
    '  구멍을 막고 <b>전종목 검사</b>로',
    '  (기준 2.5배→1.5배, 2.5는 141개',
    '  안에서만 맞던 잘못된 선)',
    '② 종목 수 4→<b>5개</b> (각 20%)',
    '  — 새 게이트에선 5개가 두 세계',
    '  (승자 유/무) MDD 모두 최저',
    '',
    '<b>새 규칙 TOP5</b> (전망상향 순)',
]
for i, (tk, rev90, fpe, gap) in enumerate(picks, 1):
    g = f'{gap:.1f}배' if gap is not None else '-'
    lines.append(f'{i}. <b>{tk}</b>')
    lines.append(f'   전망 +{rev90:.0f}% · PER {fpe:.0f} · 이익 {g}')
lines += [
    '',
    '보유 중 <b>DELL</b>은 새 기준',
    '(이익 1.5배)과 순위 모두 미달 —',
    '다음 교체일에 매도 신호 예정.',
    '',
    '과거 장부는 그대로(소급 재작성 0),',
    '새 규칙은 다음 교체일부터 적용.',
    '정식 매매 신호는 평소처럼',
    '아침 메시지 기준입니다.',
    '',
    '검증: GAP_BACKSOLVE_2026_07_09.md',
    '되돌리기: VM_GATE_LEGACY=1',
]
msg = '\n'.join(lines)
config = dr.load_config()
pid = config.get('telegram_private_id') or config.get('telegram_chat_id')
dr.send_telegram_long(msg, config, chat_id=pid)
print('sent to personal bot')
