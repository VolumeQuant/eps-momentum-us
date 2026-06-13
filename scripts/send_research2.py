# -*- coding: utf-8 -*-
"""fwd_pe_chg·파라미터·융합 실험 결과를 개인봇으로 쉽게 발송 (2차). 자체완결. 채널 X."""
import os, json, time
import urllib.request, urllib.parse

MSGS = [
"""🔬 <b>[추가연구 1/4] "PE 변화 가중치(fwd_pe_chg)" 재검증</b>

우리 시스템은 "주가가 이익 대비 싸졌나"를 7일·30일·60일·90일 변화를 섞어서 봐요. 지금은 <b>90일(장기)에 가장 무게(50%)</b>를 둡니다.

→ 이 무게를 여러 방식으로 바꿔가며 '예측력'을 다시 쟀어요. 결과: <b>어떤 조합이든 예측력이 거의 0</b>이었어요.
즉 이 가중치는 성과에 큰 영향이 없는 부분이라, 바꿔도 별 이득이 없어요 → <b>현행 유지</b>.""",

"""🔬 <b>[2/4] 시스템 숫자들 전부 점검 (진입·이탈·비중)</b>

하나씩 바꿔봤어요:
• 진입 순위(상위 3개/5개/8개): <b>차이 거의 없음</b>
• 이탈 기준(10위 vs 12위): 12위가 +20% 좋아 보였는데 → 까보니 <b>LITE 한 종목을 4일 더 들고 있던 것</b>뿐(착시)
• 비중(50:50 vs 1등 몰빵): 몰빵이 수익은 ↑지만 손실폭(MDD)도 ↑ = 이미 아는 맞바꿈

→ <b>진짜로 좋아지는 건 없었어요.</b>""",

"""🔬 <b>[3/4] "좋은 신호들을 합치면 더 좋아질까?" 융합 실험</b>

"줍줍(C2) 종목이 더 잘 오른다"는 걸 알았으니, 그런 종목에 <b>돈을 더 많이 넣어</b>봤어요.

→ 오히려 <b>−10% 나빠졌어요!</b>
왜냐면 들고 있는 종목은 이미 올라서 더는 '줍줍'이 아닌데, 줍줍에 더 넣으면 = <b>잘 나가는 winner 비중을 줄이는</b> 꼴이 되거든요.

교훈: "줍줍"은 <b>살 때 고르는</b> 기준이지, <b>들고 있을 때 비중</b> 기준은 아니에요.""",

"""🔬 <b>[4/4] 최종 결론 — 시스템은 이미 잘 만들어져 있어요</b>

실험을 다 해본 결과: <b>바꿔서 robust하게 좋아지는 건 없었어요.</b>

이 시스템 성과의 비밀은 결국 <b>"MU·SNDK 같은 몇 개 대박 종목을 잡아서 끝까지 들고 가는 것"</b>이고, 그걸 위한 장치(winner 보유 / 집중 / 데이터 글리치 방어 / 약세장 방어)는 <b>이미 다 들어있어요.</b>

괜히 복잡하게 손대면 4개월·1개 사례에 과적합되니까, <b>지금은 안 건드리는 게 정답</b>이에요. 사장님이 만든 '단순·집중'이 옳았습니다. 👍
(더 길게 검증할 거리가 쌓이면 또 실험할게요!)""",
]


def send(token, chat_id, text):
    data = urllib.parse.urlencode({'chat_id': chat_id, 'text': text,
        'parse_mode': 'HTML', 'disable_web_page_preview': 'true'}).encode()
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=30) as r:
        return json.loads(r.read().decode()).get('ok')


if __name__ == '__main__':
    token = os.environ['TELEGRAM_BOT_TOKEN']; chat_id = os.environ['TELEGRAM_PRIVATE_ID']
    for i, m in enumerate(MSGS):
        print(f'msg {i+1}/{len(MSGS)} ok={send(token, chat_id, m)}'); time.sleep(1.5)
    print('done')
