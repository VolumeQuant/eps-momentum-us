# -*- coding: utf-8 -*-
"""C1 50% 스케일 배포 결론 → 개인봇(채널 X)."""
import os, sys, urllib.request, urllib.parse
sys.stdout.reconfigure(encoding='utf-8')

msg = """✅ <b>[국면전환 — C1 '50% 스케일' 적용 완료]</b>

<b>━━ 결론부터 ━━</b>
전문가 3인(퀀트·실전PM·레드팀) 자문 + 데이터 검증 결과,
<b>binary(전부/전무) 대신 'C1 50% 스케일'로 바꿔서 적용했습니다.</b>
지금은 시장 정상이라 <b>당장 바뀌는 건 0</b>, 다음 위험 때부터 작동.

<b>━━ 뭐가 바뀌었나: 국면 3단계 ━━</b>
🟢 <b>평소</b> (다 정상) → 주식 100%
🟡 <b>절반 방어 (NEW)</b> — 섹터 브레드스만 경보(애매한 조기신호) → 주식 50% + 안전자산 50%
🔴 <b>전량 방어</b> — 200일선·VIX 경보(확실한 위험) → 주식 0%

핵심 = <b>"확실한 위험엔 전량, 애매한 조기경보엔 절반."</b>
브레드스 조기신호는 <b>맞을 확률 25%</b>라, 애매한 신호에 전 재산을 안 건다.

<b>━━ 왜 binary 말고 50%냐 (검증) ━━</b>
• binary(전량): MDD는 더 낮지만 <b>2023 같은 협소장서 −23%p 헛방어</b>, 게다가 이득의
 대부분이 2000 닷컴 한 방(닷컴 빼면 우위 사라짐).
• <b>C1(50%): 2023 헛방어 −12%p로 절반</b>, 닷컴을 빼도 여전히 기존보다 우위(0.61 vs 0.60).
 → "2000형 보험은 유지, 보험료는 반으로." 전문가 셋 다 "저정밀 신호는 스케일하라"로 일치.

<b>━━ 효과 (26년 실데이터) ━━</b>
• 최대낙폭 −36.5% → −30.9%, 위험대비수익 0.36 → 0.41
• 거래비용 1%까지·방어자산·프록시(SPY) 검증 통과
• 발동 시점: 2000 −28%→−3.4%, 2018Q4 −18%→−11% (느리게 빠지던 천장을 일찍)

<b>━━ 안전 확인 ━━</b>
• 현재 브레드스 82%(정상) → <b>회귀검증: 시스템 누적 251.03%로 적용 전과 byte 단위까지 동일</b> = 즉시 영향 0.
• 확실위험(MA/VIX)·시뮬·메시지 다 정합 확인. 기본 ON, 끄려면 REGIME_BREADTH_DISABLE=1 한 줄.
• 전부 커밋·푸시 완료(commit fbc0a41).

<b>━━ KR 시스템 전달 ━━</b>
결과 요약을 <b>KR_HANDOFF_breadth.md</b>로 정리(자동적용 금지·검토후보). 핵심: KR은 삼성전자 집중도가
US보다 심해 <b>50% 스케일이 KR에선 더 필수</b>. KR 업종지수로 동일 검증을 다음 후보로 권고.

<b>한 줄:</b> "다 빠진 뒤에야 현금 전환"하던 약점을, <b>확실한 위험=전량 / 애매한 조기경보=절반</b>으로
고쳤습니다. 가장 큰 부작용(협소장 헛방어)도 절반으로 줄였고, 지금은 휴면이라 안전합니다."""

tok = os.environ.get('TELEGRAM_BOT_TOKEN'); pid = os.environ.get('TELEGRAM_PRIVATE_ID')
if not tok or not pid:
    print('시크릿 없음 — 미리보기:\n'); print(msg); sys.exit(0)
parts, buf = [], ''
for line in msg.split('\n'):
    if len(buf) + len(line) + 1 > 3900:
        parts.append(buf); buf = ''
    buf += line + '\n'
if buf:
    parts.append(buf)
for i, p in enumerate(parts):
    data = urllib.parse.urlencode({'chat_id': pid, 'text': p, 'parse_mode': 'HTML'}).encode()
    r = urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{tok}/sendMessage', data=data))
    print(f'발송 {i+1}/{len(parts)}:', r.status)
