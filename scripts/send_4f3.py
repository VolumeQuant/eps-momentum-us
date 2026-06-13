# -*- coding: utf-8 -*-
import os, json, time, urllib.request, urllib.parse
MSGS=[
"""🏁 <b>[4팩터 시스템 만들기 #3] 시스템 완성 + 8년 백테스트 (솔직판)</b>

지난번에 "성장+모멘텀이 엔진"이라고 했죠. 이번엔 <b>실제로 사고팔며 돈 됐는지</b> 끝까지 돌렸어요.

<b>최적 배합 찾기</b>: 4팩터를 어떤 비율로 섞을지 5가지 실험 →
• <b>승자: KR 공격배합(성장55+모멘텀30+가치15, 품질0)</b> 압도적
• 의외로 한국 시스템 비율이 미국에도 잘 맞았어요!
• 품질(Q)은 빼는 게 나음(미국 성장주 판에선 무용지물 = 지난 보고와 일치)""",

"""🏁 <b>[#3] "언제 현금으로 도망갈지" 국면 규칙도 완성</b>

S&P500이 200일 평균선 밑으로 <b>15일 연속</b> 내려가면 → <b>다 팔고 현금</b> (회복하면 복귀).

• 이걸 넣으니 폭락 방어가 돼서 위험대비수익(Calmar)이 확 올랐어요.
• 재밌는 건: 한국이 쓰는 정확한 숫자(170일/8일)는 미국엔 안 맞고, <b>미국은 200일/15일</b>이 맞았어요. 근데 이게 지금 EPS 시스템이 쓰는 방어규칙이랑 <b>똑같이 나옴</b> → 서로 다른 길로 같은 답 = 신뢰도 ↑""",

"""🏁 <b>[#3] 성적표 (생존편향 빼고 솔직하게)</b>

4.4년 백테스트 (2022~2026):
• <b>시스템: 연 +90% · Calmar 2.4 · Sharpe 1.7</b>
• SPY(미국전체): 연 +12% · Calmar 0.5
→ 위험대비 약 <b>5배</b> 우수

<b>"한 종목빨" 아닌지 검사(제일 중요)</b>:
• 제일 많이 번 WDC·MU 다 빼도 성적 거의 그대로 유지 ✅
• 이게 지금 EPS 시스템(SNDK·MU 빼면 무너짐)과 <b>결정적 차이</b> — 여러 종목에 골고루 분산된 진짜 실력!""",

"""🏁 <b>[#3 마무리] 솔직한 한계 + 다음</b>

<b>아직 실전 투입 안 해요. 이유:</b>
• <b>생존편향</b>: 지금 살아남은 종목만 봐서, 망해서 사라진 종목이 빠짐 → 수익이 부풀려졌을 수 있음(연 90%는 과대평가).
• 재무데이터가 야후 재작성본(진짜 그 시점 데이터 아님) + 실측구간 4.4년(강세장 위주).

<b>믿어도 되는 것</b>: 절대수익이 아니라 ① 배합 순위(성장+모멘텀) ② "한 종목빨 아님" ③ 현금방어 효과 — 이 <b>상대적·구조적 결론</b>들이에요.

<b>실전 가려면</b>: 사라진 종목 포함한 깨끗한 데이터 + 종이거래(모의) 검증이 필요. 지금 EPS 시스템은 그대로 잘 돌고 있고, 이건 <b>별도 연구 시스템</b>으로 키워갈게요. 👍""",
]
def send(t,c,x):
    d=urllib.parse.urlencode({'chat_id':c,'text':x,'parse_mode':'HTML','disable_web_page_preview':'true'}).encode()
    with urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{t}/sendMessage',data=d),timeout=30) as r:
        return json.loads(r.read().decode()).get('ok')
if __name__=='__main__':
    t=os.environ['TELEGRAM_BOT_TOKEN'];c=os.environ['TELEGRAM_PRIVATE_ID']
    for i,m in enumerate(MSGS): print(f'msg {i+1}/{len(MSGS)} ok={send(t,c,m)}');time.sleep(1.5)
    print('done')
