# -*- coding: utf-8 -*-
import os, json, time, urllib.request, urllib.parse
MSGS=[
"""🌏 <b>[KR vs US 시스템 비중 1/2] 둘을 어떻게 나눠 담을까?</b>

한국(KR)과 미국(US) 두 시스템에 돈을 어떻게 나눌지 데이터로 봤어요.

<b>확인한 것</b>:
• 두 시장(코스피 vs 나스닥) 수익률 <b>상관관계 0.64</b> → 같이 움직이는 편이나 완전히 똑같진 않음. 섞으면 변동성 약 <b>9% 감소</b>(분산효과 "보통").
• 코스피가 더 출렁임(연 22% vs 미국 15~18%).
• 환율: 지금 <b>1달러=1518원</b>(20년 평균 1170원)으로 원화가 약한 편 → 미국(달러) 보유는 <b>원화 약세 헤지</b>가 됨.""",

"""🌏 <b>[2/2] 권고: 지금은 한국에 더, 미국은 키워가며</b>

<b>핵심 판단</b>:
• KR 시스템 = <b>7.8년 검증된 탄탄한 시스템</b>(분산형, Calmar 3.86)
• US 시스템 = 아직 <b>4개월 실전 + 2종목 집중</b>(변동 큼, 대박주 의존). 유망하나 미검증

→ 지금은 <b>검증된 KR에 더 싣기</b>:
• 시작: <b>KR 65% / US 35%</b> (시스템 투자금 기준)
• US가 실전 기록을 쌓고 잘 버티면 → 점차 <b>50:50</b>로
• US 35%는 <b>달러 분산</b> 효과도 덤(원화 약세 대비)

⚠️ <b>솔직히</b>: 미국은 검증기간 짧고 집중형이라 "대박 기대"로 몰빵하면 위험. 검증된 KR을 중심에 두고 미국을 분산·달러노출용으로 더하는 게 데이터가 지지하는 그림. (단, US 장기 데이터가 없어 이 비중은 <b>백테스트 최적화가 아니라 구조적 판단</b>입니다.)
※ 이건 "시스템끼리" 비중이고, <b>각 시스템 안에서 주식 85~90%+안전자산, 약세장 자동방어는 그대로</b> 적용돼요.""",
]
def send(t,c,x):
    d=urllib.parse.urlencode({'chat_id':c,'text':x,'parse_mode':'HTML','disable_web_page_preview':'true'}).encode()
    with urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{t}/sendMessage',data=d),timeout=30) as r:
        return json.loads(r.read().decode()).get('ok')
if __name__=='__main__':
    t=os.environ['TELEGRAM_BOT_TOKEN'];c=os.environ['TELEGRAM_PRIVATE_ID']
    for i,m in enumerate(MSGS): print(f'msg {i+1}/{len(MSGS)} ok={send(t,c,m)}');time.sleep(1.5)
    print('done')
