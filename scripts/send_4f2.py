# -*- coding: utf-8 -*-
import os, json, time, urllib.request, urllib.parse
MSGS=[
"""🧪 <b>[4팩터 시스템 만들기 #2] 4가지 잣대 다 만들고 "진짜 맞는지" 채점</b>

오늘 한 일:
• 4가지 점수(가치·품질·성장·모멘텀)를 <b>8년치로 전부 계산</b> ✅
• 각 잣대가 "정말 다음 달 오를 종목을 골라내나"를 <b>96개월 내내 채점</b> (IC = 적중도 지표)

쉽게 비유하면: 점쟁이 4명한테 8년간 매달 "다음 달 뭐 오를까?" 물어보고, 실제로 맞췄는지 성적표를 매긴 거예요.""",

"""🧪 <b>[#2 핵심결과] 혼자선 다 별로, 근데 "성장+모멘텀" 합치면 통해요</b>

솔직한 성적표:
• 가치(싼가)·품질(돈 잘 버나) → US에선 <b>거의 안 맞음</b> (찍기 수준)
• 성장·모멘텀 → <b>살짝 맞음</b>

근데 진짜 중요한 건 이거예요:
• "성장+모멘텀" 점수로 <b>상위 10종목만</b> 사면 → 시장평균보다 <b>매달 +2.5~2.8% 더 벌었음</b> (8년 평균)
• 반대로 KR이 약세장에 쓰는 "가치+품질 위주" 조합은 US에선 <b>+0%</b> (효과 없음)

즉 US에선 "싸고 안정적인 회사"가 아니라 <b>"크고 오르는 회사"가 이겨요.</b> 지금 EPS 시스템이 저마진 성장주(SNDK·MU)로 번 거랑 똑같은 얘기!

⚠️ 단, +2.5% 절대수치는 "오늘 살아남은 종목만 본" 생존편향으로 좀 부풀려졌을 수 있어요. 하지만 "성장+모멘텀이 가치+품질을 이긴다"는 <b>순위 자체는 믿을 만함.</b>""",

"""🧪 <b>[#2 마무리] 모멘텀의 약점 발견 → "방어=현금"이 왜 필요한지</b>

연도별로 뜯어보니:
• 모멘텀은 2023~2026엔 잘 맞는데, <b>2021년엔 크게 빗나감</b> (그때 시장이 갑자기 뒤집힘 — 오르던 게 폭락)
• 이게 <b>"모멘텀 크래시"</b>예요. 잘 나가다 한방에 무너지는 약점.

그래서 사장님이 말한 <b>"방어 국면엔 현금 보유"가 핵심 방패</b>예요. 무너질 조짐이면 다 팔고 현금 → 크래시 회피.

다음 단계:
• 4팩터 합치는 <b>최적 비율</b> 찾기 (성장+모멘텀 위주로)
• <b>"언제 현금으로 도망갈지"</b> 국면전환 규칙 만들기
• <b>8년 백테스트</b>로 진짜 돈 됐는지 검증 (과적합 안 속게 walk-forward·DSR)

차근차근 보고할게요!""",
]
def send(t,c,x):
    d=urllib.parse.urlencode({'chat_id':c,'text':x,'parse_mode':'HTML','disable_web_page_preview':'true'}).encode()
    with urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{t}/sendMessage',data=d),timeout=30) as r:
        return json.loads(r.read().decode()).get('ok')
if __name__=='__main__':
    t=os.environ['TELEGRAM_BOT_TOKEN'];c=os.environ['TELEGRAM_PRIVATE_ID']
    for i,m in enumerate(MSGS): print(f'msg {i+1}/{len(MSGS)} ok={send(t,c,m)}');time.sleep(1.5)
    print('done')
