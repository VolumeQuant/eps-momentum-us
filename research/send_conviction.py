# -*- coding: utf-8 -*-
"""US top종목 비중 2x/3x 검증 결론 → 개인봇(채널 X). 중학생도 이해 쉽게."""
import os, sys, urllib.request, urllib.parse
sys.stdout.reconfigure(encoding='utf-8')

msg = """🔬 <b>[US도 1등 종목에 2배·3배 비중 실어볼까? — 검증 결과]</b>

<b>━━ 질문 ━━</b>
"KR처럼, 우리 US도 보유 2종목 중 <b>1등(가장 확신 높은 종목)에 비중을 2배·3배로 더 실으면</b>
더 벌지 않을까?"

<b>━━ 결론 (먼저) ━━</b>
<b>US에선 하면 안 됩니다.</b> 두 가지 방법 다 '함정'이에요. 그대로 <b>50:50 유지</b>가 정답.
(사실 이건 6/25에도 한 번 검증돼서, 이번엔 형 아이디어 그대로 다시 숫자로 확인한 거예요.)

<b>━━ 먼저 쉬운 용어 ━━</b>
• <b>최대낙폭</b> = 고점에서 제일 많이 깨진 폭(작을수록 좋음).
• <b>레버리지</b> = 빚내서 원래보다 더 큰 금액을 베팅(2배·3배).
• <b>LOWO 테스트</b> = "대박종목 하나를 빼고 다시 계산". 빼도 여전히 좋으면 진짜 실력, 빼니까
  무너지면 그냥 그 종목 운("지나고 보니")이었단 뜻.

<b>━━ 방법 1: 100% 안에서 1등에 더 몰기 ━━</b>
백테스트론 좋아 보임: 1등에 100% 몰면 +217% → <b>+294%</b>, 낙폭도 살짝 줄어듦.
<b>하지만 함정</b> 🚨 — 그 추가 수익이 <b>전부 SNDK(우리 대박종목) 하나</b> 덕분.
LOWO로 <b>SNDK만 빼면 오히려 손해</b>(−13%p)로 뒤집힘.
🔸 비유: "지나고 보니 1등이 SNDK였으니 SNDK에 몰빵했으면 좋았다" = 사후약방문.
   미래엔 <b>누가 SNDK가 될지 모름</b> → 몰빵은 도박.

<b>━━ 방법 2: 빚내서 2배·3배 (레버리지) ━━</b>
백테스트론 화려함: 3배면 +217% → <b>+869%</b>!
<b>하지만 더 큰 함정</b> 🚨🚨 —
① 이 91일 구간엔 <b>폭락장이 한 번도 없었음</b>(강세장만 봄). 하방이 전혀 검증 안 됨.
② 강세장에서도 낙폭이 −22% → <b>−37%(3배)</b>로 커짐.
③ ★우리가 막으려던 "방어 켜지기 전 초기 급락"(−20~−36%, 2000·2018·2022)이
   <b>3배면 −60~−100% = 계좌 깡통</b>. 빚이라 마진콜(강제청산)도 옴.
= 폭락장만 한 번 와도 끝. 강세장만 본 <b>착시</b>.

<b>━━ 왜 KR은 됐는데 US는 안 되나 ━━</b>
• KR: 종목이 <b>1900개로 넓고 다양</b> → "확신 종목"이 진짜 여러 개로 분산 → 비중 더 줘도 안전(robust).
  (KR은 Calmar 3.9→5.5로 실제 개선)
• US: 종목이 <b>137개로 좁고 다 비슷한 테크·성장주</b>(서로 0.83만큼 닮음) → "확신 종목 = 똑같은
  소수 대박주 하나" → 비중 더 주기 = 그 한 종목 몰빵 = 도박.
🔸 <b>같은 레시피, 다른 재료, 다른 결과.</b> KR 주방엔 재료가 많고, US 주방엔 한 종류뿐.

<b>━━ 그래서 ━━</b>
• <b>US 2슬롯 50:50 그대로 유지.</b> 집중·레버리지 일체 안 함(배포 변경 0).
• 집중 = 대박주 사후몰빵(LOWO 기각) / 레버리지 = 폭락장 깡통(하방 미검증).
• 기록·검증코드 커밋 완료(`conviction_bt_2026_06_25.py`, `CONVICTION_FINDINGS_2026_06_25.md`).

한 줄: <b>"1등에 더 싣자"는 백테스트론 예뻐 보이지만, US에선 (1) 대박주 사후몰빵이거나 (2) 폭락장
깡통이라 함정. KR이 되는 건 KR 시장이 넓어서고, US는 좁아서 안 됩니다. 50:50 유지.</b>"""

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
