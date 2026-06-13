# -*- coding: utf-8 -*-
"""신규데이터+보완전략 병렬 자율주행 결과 개인봇 발송(4차). 자체완결. 채널 X."""
import os, json, time, urllib.request, urllib.parse
MSGS=[
"""🔭 <b>[신규연구 1/3] "야후에서 안 쓰던 새 데이터 찾기"</b>

야후가 주는데 우리가 안 쓰던 데이터를 전부 뒤졌어요 — 애널 목표가 변경 이력, 공매도, 기관보유, 내부자거래, 옵션 등.

가장 유망해 보인 건 <b>"애널리스트 목표가 인상폭"</b>. 처음엔 예측력(IC) +0.31에, 대박종목(MU·SNDK) 빼도 살아남아서 "오 이번엔 진짜?!" 했어요.""",

"""🔭 <b>[2/3] 근데 까보니... "그냥 가격 따라가기"였어요</b>

제가 한 번 더 파보니: 그 "목표가 인상폭"이 <b>과거 주가 상승과 76% 똑같이</b> 움직였어요.
즉 애널들이 <b>이미 오른 주식의 목표가를 뒤늦게 올리는 것</b>이라, '미래 예측'이 아니라 '과거 따라가기' 신호였어요.

게다가 우리 전략에 넣어봤자 <b>효과 0%</b> (이미 우리가 보는 모멘텀과 겹침).
→ 또 "좋아 보이지만 쓸모없는" 케이스. (목표가 수준·공매도 같은 단면 데이터는 과거 기록이 없어 검증조차 불가 → 보류.)""",

"""🔭 <b>[3/3] "다른 전략을 같이 굴리면 안정적일까?" + 최종 결론</b>

저변동성·과대낙폭반등·저PER·매출성장·채권 등 <b>5가지 보완 전략</b>을 본 전략과 섞어봤어요.
→ 전부 본 전략과 <b>같이 움직여서</b>(상관 +) 손실 방어가 안 됐어요. 우리가 빠지는 날엔 다 같이 빠짐. MDD를 줄이려니 수익이 더 깎이고요.
(진짜 약세장 방어는 이미 있는 "200일선·VIX → 채권 대피" 장치가 담당해요.)

<b>최종</b>: 새 데이터도, 보완 전략도 robust한 개선은 없었어요. 세계적 대가들 말처럼 — 지금 시스템은 단순·집중으로 잘 만들어져 있고, 더 손대기보다 <b>시간으로(실제 성과로) 검증</b>하는 게 정답입니다. 👍""",
]
def send(t,c,x):
    d=urllib.parse.urlencode({'chat_id':c,'text':x,'parse_mode':'HTML','disable_web_page_preview':'true'}).encode()
    with urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{t}/sendMessage',data=d),timeout=30) as r:
        return json.loads(r.read().decode()).get('ok')
if __name__=='__main__':
    t=os.environ['TELEGRAM_BOT_TOKEN'];c=os.environ['TELEGRAM_PRIVATE_ID']
    for i,m in enumerate(MSGS): print(f'msg {i+1}/{len(MSGS)} ok={send(t,c,m)}');time.sleep(1.5)
    print('done')
