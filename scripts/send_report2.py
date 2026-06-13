# -*- coding: utf-8 -*-
"""고PER 최적화 + 수익귀속 방법론 보고 개인봇 발송. 자체완결. 채널 X."""
import os, json, time, urllib.request, urllib.parse
MSGS=[
"""🔬 <b>[최적화 보고 1/4] 고PER 기준 재검증 (SNDK/MU 제외 기준)</b>

사장님 지시대로 "SNDK·MU 빼고도 좋은가"를 기준으로 고PER(매도) 기준을 다시 검증했어요.
• SNDK/MU 없이도 시스템 <b>+63%</b> (잔여 알파는 있음)
• 현행 <b>PER 15 매도</b>는 "메가-최적"이에요 — SNDK·MU가 PER 9~10이라 계속 보유됨.
• 메가 없는 robust 기준으론 <b>PER을 느슨하게(30~40) 풀수록 +10~14%p 유리</b> — 마진 높은 작은 winner를 일찍 안 잘라야 더 먹음. (모든 winner 빼도 +14p 유지 = 착시 아님)""",

"""🔬 <b>[2/4] 근데 공짜 점심은 아니에요 (트레이드오프)</b>

PER 매도기준을 느슨하게 풀면:
• 메가 없는 경우(robust): <b>+10~14%p 좋아짐</b> ✅
• 메가 포함 전체에선: <b>−12%p 나빠짐</b> ❌ + 거품방어(비싼 거 빨리 팔기) 약화

→ "미래에 SNDK 같은 싼 메가가 또 나온다" 믿으면 PER 15 유지, "메가 의존 줄이자"면 느슨화.
<b>신념의 문제지 확실한 개선이 아니에요.</b> (중도: PER 20은 양쪽 거의 동일 = 무난)""",

"""🔬 <b>[3/4] ★가장 중요한 발견 — 우리 성과의 정체</b>

방법론(수익 귀속 분석)으로 +229%가 어디서 나왔나 분해했어요:
• 3구간 다 플러스(+18% / +60% / +75%) — 겉보기엔 꾸준한 엣지
• 근데 SNDK/MU를 <b>84일 중 82일(98%) 보유</b> 중이었고,
• <b>SNDK·MU 보유한 날 평균 +1.63%/일, 안 보유한 날 +0.00%/일</b> ← 성과의 거의 <b>100%가 이 두 메가</b>에서.

→ 우리 시스템은 본질적으로 <b>"메가 winner 하나 잡아서 끝까지 들고 가는 기계"</b>예요.""",

"""🔬 <b>[4/4] 결론 — 왜 아무리 튜닝해도 안 좋아졌나</b>

이제 명확해요: 시스템 성과 = <b>메가 포착·보유</b>. 그건 이미 규칙(싼 메가는 PER로 안 팔고 유지 + 순위 10위 보유)이 잘 하고 있어요.
그래서 신호·계산식·파라미터를 아무리 바꿔도 개선이 안 됐던 거예요 — 알파의 정체가 "메가 포착"이고, 그건 이미 최대화돼 있으니까.

→ 진짜 과제는 (a) <b>다음 메가를 놓치지 않는 그물(EPS 모멘텀 스크린)을 넓고 깨끗하게 유지</b> (b) <b>메가 없을 때(수익 0인 구간) 손실 안 키우기</b>. '더 정교한 계산식'이 아니에요.

세계적 대가들(과적합 경고)과 우리 데이터(수익귀속·LOWO·기간분석)가 한목소리예요: <b>단순·집중 유지, 그만 만지고, 실제 성과로 검증.</b> 👍""",
]
def send(t,c,x):
    d=urllib.parse.urlencode({'chat_id':c,'text':x,'parse_mode':'HTML','disable_web_page_preview':'true'}).encode()
    with urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{t}/sendMessage',data=d),timeout=30) as r:
        return json.loads(r.read().decode()).get('ok')
if __name__=='__main__':
    t=os.environ['TELEGRAM_BOT_TOKEN'];c=os.environ['TELEGRAM_PRIVATE_ID']
    for i,m in enumerate(MSGS): print(f'msg {i+1}/{len(MSGS)} ok={send(t,c,m)}');time.sleep(1.5)
    print('done')
