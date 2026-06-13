# -*- coding: utf-8 -*-
import os, json, time, urllib.request, urllib.parse
MSGS=[
"""🏗️ <b>[4팩터 시스템 만들기 #1] 새 프로젝트 안전하게 시작</b>

사장님 지시대로 했어요:
• 지금 잘 도는 EPS 시스템은 <b>손 안 대고 백업</b>(복원점 저장) ✅
• <b>완전히 별도 폴더</b>에서 새 시스템 제작 시작 (서로 영향 0)

<b>뭘 만드냐면</b>: 한국(KR) 시스템처럼 <b>4가지 잣대로 종목을 채점</b>하는 시스템이에요.
• 가치(싼가) · 품질(돈 잘 버나) · 성장(크고 있나) · 모멘텀(오르고 있나)
지금 US는 1가지(이익전망)만 보는데, KR은 4개를 섞어서 더 균형 잡혀 있거든요(한국 7.8년 검증).""",

"""🏗️ <b>[#1 계속] 오늘 한 일 + 솔직한 한계</b>

<b>오늘 한 것</b>: 8년치 주가 데이터를 <b>1483종목</b> 모았어요(2018~2026). 이게 모멘텀 계산이랑 백테스트의 기초예요.

<b>솔직한 한계 (꼭 알아두세요)</b>:
• 야후 데이터라 재무(매출·이익)는 <b>5년치</b>뿐, '이익전망 과거기록'은 <b>아예 없음</b>.
• 그래서 이 새 시스템은 <b>"순수 4팩터"</b>로, 지금 시스템(이익전망 특화)과는 <b>별개</b>예요.
• 88일 짧은 데이터로 억지 최적화하면 과적합이라, <b>장기 데이터부터 제대로 모으는 중</b>.

<b>다음 단계</b>: 재무제표(매출·이익·자산 등) 모아서 → 가치·품질·성장 점수 만들기. 진행되면 또 보고할게요!""",
]
def send(t,c,x):
    d=urllib.parse.urlencode({'chat_id':c,'text':x,'parse_mode':'HTML','disable_web_page_preview':'true'}).encode()
    with urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{t}/sendMessage',data=d),timeout=30) as r:
        return json.loads(r.read().decode()).get('ok')
if __name__=='__main__':
    t=os.environ['TELEGRAM_BOT_TOKEN'];c=os.environ['TELEGRAM_PRIVATE_ID']
    for i,m in enumerate(MSGS): print(f'msg {i+1}/{len(MSGS)} ok={send(t,c,m)}');time.sleep(1.5)
    print('done')
