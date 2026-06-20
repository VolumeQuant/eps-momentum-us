# -*- coding: utf-8 -*-
"""복귀 확인일 최종 결론 → 개인봇(채널 X)."""
import os, sys, urllib.request, urllib.parse
sys.stdout.reconfigure(encoding='utf-8')

msg = """🔁 <b>[복귀(정상 비중 재개) 확인일 — 최종 결론]</b>

<b>━━ 결론 ━━</b>
<b>US 복귀 = 15일 그대로 유지.</b> ("들어갈 땐 3일, 나올 땐 15일" 비대칭)
변경 없음(이미 15) — 데이터로 재확인만 했고, 메모리·문서·커밋 완료.

<b>━━ 용어 ━━</b>
• <b>진입(확인일)</b> = 절반 방어를 *켜는* 데 필요한 연속일. 현재 <b>3일</b>(브레드스 45%↓ 3일).
• <b>복귀(확인일)</b> = 정상 100%로 *돌아가는* 데 필요한 연속일. 현재 <b>15일</b>(45%↑ 15일).
• 둘이 다른 이유 = <b>비대칭(히스테리시스)</b>: 위험은 빨리 피하고, 복귀는 신중하게(헛바운스 방지).

<b>━━ "진입 3일이면 복귀도 3일?" → 아니오 (수치) ━━</b>
복귀일만 바꿔 26년 백테스트 (진입은 3일 고정):
• 복귀 <b>3일</b>: 최대낙폭 <b>−36.5%</b>(개선 0) / Calmar <b>0.34</b> ← <b>기존(0.36)보다도 나쁨</b>
• 복귀 5일: −36.5% / 0.34
• 복귀 10일: −32.0% / 0.40
• 복귀 <b>15일(현행)</b>: <b>−30.9% / 0.41</b>
• 복귀 20일: −26.8% / 0.47
→ <b>복귀 3~5일은 효과가 죽음.</b> 약세장 반등에 100%로 바로 들어갔다 다음 하락을 또 맞음(휩쏘) →
  결국 −36% 다 먹음. <b>안전 구간은 10~25일</b>, 15는 그 중앙이라 유지. (20이 미세 우위지만 '제일 좋은
  값 콕 집기'는 과적합 위험이라 검증된 중앙값 15 고수.)

<b>━━ KR은 "복귀 3일"이 최적이라던데? — 화해 ━━</b>
• KR은 <b>7.4년·약세장 사실상 1개(2022)</b>에서 3일이 최적으로 나옴.
• 근데 <b>복귀일은 '그 한 번의 반등 모양'에 가장 민감한 값</b> → 약세 1개로 정하면 <b>2022에 과적합</b>
  위험이 가장 큰 파라미터. US는 약세 4개에서 15~20이 일관(robust).
• 내가 KR 섹터ETF(약세 5개)로 재검증 시도 → <b>KOSDAQ 자체가 부진(26년 수익 ~0%·낙폭 −65%)</b>이라
  Calmar가 전부 0.01~0.03 = <b>판별 불가</b>. 방향만 보면 긴 복귀가 미세 우위(US와 동일), 3이 최저.
• <b>판정: KR "복귀 3일"은 낮은 신뢰</b>(2022 과적합 가능성). 진짜 KR이 더 V자·빠른반등이라 다를 수도
  있으나, 약세 1개로는 '과적합 vs 시장구조 차이'를 구분 못 함. <b>KR 23업종 다(多)약세 데이터로 재스윕
  권고</b>. 안전하게 가려면 KR도 복귀를 늘리는 쪽(휩쏘 방지)이 보수적.

<b>━━ 정리 ━━</b>
• US: 진입 3일 / 복귀 <b>15일</b> 유지(확정). 코드 변경 없음.
• KR: 복귀 3일은 표본 1개 과적합 의심 → 재검증 필요(전달함).
• 메모리·FINDINGS·KR핸드오프·복귀스윕 스크립트 전부 업데이트·커밋·푸시 완료(622ee64)."""

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
