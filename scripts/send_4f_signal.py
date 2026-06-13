# -*- coding: utf-8 -*-
import os, json, time, urllib.request, urllib.parse
MSGS=[
"🧪 <b>[US 4팩터 연구시스템] 신호 미리보기</b> · 2026-06-12(금)\n<i>※ 실전 미적용 — 어떤 종목이 나오는지 보기용</i>\n\n📈 <b>시스템 연환산 +90%/년 · Calmar 2.4</b> (4.4년 BT)\n    같은 기간 S&P500 연 +10.4%/년 · Calmar 0.5\n    <i>(누적 +1627%지만 생존편향 과대 — 상대비교·견고성만 신뢰)</i>\n\n📊 <b>시장 국면</b>: 📈 <b>공격 국면</b> (S&P500이 200일선 위) — 주식 보유\n\n━━━━━━━━━━━━━━━\n🛒 <b>매수 후보</b> (성장+모멘텀+가치 결합점수 Top)\n━━━━━━━━━━━━━━━\n1. <b>AG</b> · 점수 2.84 · $18\n    성장 +4.2σ · 모멘텀 +1.8σ · 가치 -0.2σ · 품질 +0.2σ\n2. <b>WDC</b> · 점수 2.83 · $563\n    성장 +2.9σ · 모멘텀 +4.3σ · 가치 -0.4σ · 품질 -0.0σ\n3. <b>MU</b> · 점수 2.80 · $982\n    성장 +2.8σ · 모멘텀 +4.3σ · 가치 -0.4σ · 품질 +0.4σ\n4. <b>STX</b> · 점수 2.73 · $931\n    성장 +2.6σ · 모멘텀 +4.3σ · 가치 +0.0σ · 품질 -1.3σ\n5. <b>ONDS</b> · 점수 2.65 · $9\n    성장 +2.6σ · 모멘텀 +4.3σ · 가치 -0.4σ · 품질 -1.1σ\n6. <b>VICR</b> · 점수 2.52 · $304\n    성장 +2.3σ · 모멘텀 +4.3σ · 가치 -0.3σ · 품질 +0.4σ\n7. <b>COHR</b> · 점수 2.49 · $385\n    성장 +2.2σ · 모멘텀 +4.3σ · 가치 -0.3σ · 품질 -0.2σ\n8. <b>CIFR</b> · 점수 2.45 · $24\n    성장 +2.2σ · 모멘텀 +4.3σ · 가치 -0.6σ · 품질 -1.9σ\n\n<i>↑ 상위 3종목이 월말 리밸런스 시 매수 대상</i>",
"🧪 <b>[#연구시스템] 현재 보유 + 규칙</b>\n\n💼 <b>현재 보유</b>: 없음 — 월말 리밸런스 대기 (직전 보유분 트레일링스탑 청산됨)\n\n📋 <b>규칙</b>\n• 배합: <b>성장55 + 모멘텀30 + 가치15</b> (KR 공격가중, 품질은 US서 무효라 제외)\n• 매수: 매월말 결합점수 상위 3종목 (균등)\n• 매도: 6위 밖 이탈 / 손절 −10% / 트레일링 −15%\n• 방어국면(S&P500 200일선 15일 이탈): <b>전량 현금</b>\n\n⚠️ <b>아직 실전 미적용 이유</b>: 생존편향(사라진 종목 누락)+재무데이터 한계로 수익 과대평가 가능. 검증된 건 ①배합 순위(성장+모멘텀) ②한 종목빨 아님(LOWO 통과) ③현금방어 효과.",
]
def send(t,c,x):
    d=urllib.parse.urlencode({'chat_id':c,'text':x,'parse_mode':'HTML','disable_web_page_preview':'true'}).encode()
    with urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{t}/sendMessage',data=d),timeout=30) as r:
        return json.loads(r.read().decode()).get('ok')
if __name__=='__main__':
    t=os.environ['TELEGRAM_BOT_TOKEN'];c=os.environ['TELEGRAM_PRIVATE_ID']
    for i,m in enumerate(MSGS): print(f'msg {i+1}/{len(MSGS)} ok={send(t,c,m)}');time.sleep(1.5)
    print('done')
