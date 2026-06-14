# -*- coding: utf-8 -*-
import os, json, urllib.request, urllib.parse
MSG = """📘 <b>우리 미국 주식 시스템 — 한 장 완전 정리</b>

<b>① 한마디로?</b>
"앞으로 돈 더 잘 벌 거라고 전문가(애널리스트)들이 <b>이익 전망을 올리는 중</b>인데, 아직 안 비싼" 미국 주식을 매일 자동으로 골라줘요. 감이 아니라 규칙으로만 움직여요.

<b>② 종목 어떻게 고르나</b>
매일 미국 ~1,200개를 점수로 줄세움:
• 핵심: 향후 12개월 예상이익(EPS) <b>상향 속도</b>
• + 안 비싼가 + 거래 잘 되나(하루 10억달러+)
• 자동 제외: 적자+현금부족 / 한 분기 반짝이익 / 단기폭락(−25%) / 원자재·금융 섹터
• <b>3일 연속 상위(✅)</b> 검증된 것만 진짜 후보

<b>③ 매매 규칙</b>
• 최대 <b>2종목</b> 보유(집중)
• 비중: 1·2위 점수차 크면 1위 100%, 비슷하면 50:50
• 매도: ⓐ순위 10위 밖 + 비싸짐(forward PER 30↑) ⓑ이익전망 꺾이면 즉시
• 싸고(PER 30 미만) 전망 좋으면 순위 밀려도 보유(대박주 안 놓치려)
• 손절·트레일링 없음(가격 아닌 순위·실적으로 판단)

<b>④ 강세장↔약세장 (제일 중요)</b>
• 약세장 전량 매도: S&P500 200일선 아래 <b>15일 연속</b> OR 공포지수(VIX) 36 넘고 <b>2일</b>
• 강세장 복귀(전량 매수): 200일선 위 <b>15일</b> 회복
• 하루 깜짝 신호엔 안 움직임(가짜 거르기)
• ★새 기능: 신호 쌓이는 <b>1일째부터 "조기경보"</b>로 미리 알려줌(매매는 그대로 확인 후 — 대비용)
• 약세장엔 현금/달러 단기채 보유

<b>⑤ 현금 비중 (정확히)</b>
• 시스템 85% / 안전버퍼 15%(달러 발행어음 3~5%)
• 시스템 85% 안에서 <b>한국:미국 = 60:40</b>
• 예) 1억 = 버퍼 1,500만 + 한국 5,100만 + 미국 3,400만
• 분기(1·4·7·10월)마다 재조정

<b>⑥ 솔직한 한계</b>
2종목 집중이라 출렁임 큼 + 실전 검증기간 짧음. 그래서 <b>비중(85/15) 지키고 여유자금으로</b>. 약세장 방어가 큰 손실 막는 핵심이니 방어 신호는 꼭 따르기. 신호는 참고용, 최종 판단은 본인."""
def send(t, c, x):
    d = urllib.parse.urlencode({'chat_id': c, 'text': x, 'parse_mode': 'HTML', 'disable_web_page_preview': 'true'}).encode()
    with urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{t}/sendMessage', data=d), timeout=30) as r:
        return json.loads(r.read().decode()).get('ok')
if __name__ == '__main__':
    t = os.environ['TELEGRAM_BOT_TOKEN']; c = os.environ['TELEGRAM_PRIVATE_ID']
    print('len(chars):', len(MSG), 'ok=', send(t, c, MSG))
