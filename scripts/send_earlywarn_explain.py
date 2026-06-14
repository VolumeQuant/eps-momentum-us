# -*- coding: utf-8 -*-
import os, json, time, urllib.request, urllib.parse
MSGS=[
"""🤔 <b>[조기경보 정리 1/2] "경보기"가 두 개였어요 — 하나는 버리고 하나는 남김</b>

제가 둘 다 "조기경보"라고 불러서 헷갈리셨을 거예요. 정리하면 비슷한 이름의 <b>두 가지</b>가 있었어요:

❌ <b>버린 것 = "VIX 텀구조 자동매매 가속"</b>
• 아이디어: 공포지수 2개(1개월 VIX, 3개월 VIX3M)를 비교해 위기를 더 빨리 감지하고 → <b>자동으로 더 일찍 팔기</b>
• 즉 <b>실제 매매를 바꾸는</b> 거였어요. → 이건 버렸어요(이유는 다음 메시지).

✅ <b>남긴 것 = "Day-1 표시 경보"</b>
• 매매는 안 바꾸고 <b>메시지에 카운트다운만 보여주기.</b> → 이건 남겼어요.""",

"""🤔 <b>[조기경보 정리 2/2] 왜 자동매매 가속은 버렸나 (과적합)</b>

❌ <b>버린 이유 — 검증하니 "운 좋게만 좋아 보이는 것(과적합)"이었어요</b>:
• 민감도를 <b>딱 한 값(1.05)</b>으로 맞췄을 때만 좋았는데, 그 값은 동시에 <b>가짜 경보(괜히 팔고 반등)</b>도 냈어요(2014·2026).
• 안전하게 둔감하게(1.08↑) 바꾸니 <b>효과가 싹 사라져서</b> 기존과 똑같았어요.
• 계산 방법만 살짝 바꿔도 결과가 흔들렸고요.
• 26년에 빠른 폭락이 <b>3번뿐</b>이라, 그 3번에 맞춘 신호는 <b>미래엔 안 통할 위험</b>이 커요.
→ 그래서 <b>실전엔 안 쓰기로(껐어요).</b> 사장님이 "과적합 아니냐" 하신 게 정확했어요. 👍

✅ <b>남긴 "Day-1 표시 경보"는 완전 다른 거예요</b>:
• <b>매매를 안 바꿔요.</b> "S&P 200일선 아래 3일째(15일 되면 방어)" 같은 <b>카운트다운을 메시지에 보여주기만</b> 해요.
• 미리 마음의 준비만 하라는 <b>정보 표시</b> → 위험·과적합 없음 → 그래서 남겼어요.

<b>한 줄 요약: "빨리 파는 자동 기능"은 못 믿어서 버리고, "미리 알려주는 표시"만 남겼어요.</b> (매매 규칙 자체는 검증된 그대로: 200일선 15일 OR VIX36 2일 → 전량 매도.)""",
]
def send(t,c,x):
    d=urllib.parse.urlencode({'chat_id':c,'text':x,'parse_mode':'HTML','disable_web_page_preview':'true'}).encode()
    with urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{t}/sendMessage',data=d),timeout=30) as r:
        return json.loads(r.read().decode()).get('ok')
if __name__=='__main__':
    t=os.environ['TELEGRAM_BOT_TOKEN'];c=os.environ['TELEGRAM_PRIVATE_ID']
    for i,m in enumerate(MSGS): print(f'msg {i+1}/{len(MSGS)} ok={send(t,c,m)}');time.sleep(1.5)
    print('done')
