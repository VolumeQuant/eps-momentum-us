"""LITE / BE 순위 누락 분석을 개인봇으로 송출."""
import json
import requests
from pathlib import Path

ROOT = Path(__file__).parent
with open(ROOT / 'config.json', encoding='utf-8') as f:
    cfg = json.load(f)

TOKEN = cfg['telegram_bot_token']
CHAT_ID = cfg['telegram_chat_id']

MSG = """🔍 <b>LITE / BE 순위 누락 분석</b>

━━━━━━━━━━━━━━━
📊 <b>LITE (Lumentum)</b>
━━━━━━━━━━━━━━━

<b>실제 상태</b>: Watchlist 5위 (메시지에 ✅ 5번으로 표시됨)
<b>매수 후보 누락 이유</b>: score_100 4위로 Top 3 못 들어감

cr / part2_rank 추적:
- 4/22: p2=20, cr=8 (정상)
- 4/23: p2=10, cr=10 (정상)
- 4/24: p2=15, <b>cr=26</b> (adj_gap +8.70 양수, 약화)
- 4/27: p2=17, <b>cr=26</b> (지속 약화)
- 4/28: p2=9, <b>cr=3 회복</b> (adj_gap -4.71 음수)

<b>회복 메커니즘</b>:
- 4/24~4/27: 가격 상승 vs NTM 둔화 → fwd_pe_chg 양수 → 약한 매수 신호
- 4/28: NTM 다시 상향 → fwd_pe_chg 음수 회복 → cr=3 진입

<b>매수 후보 못 들어간 이유</b>:
- score_100 정렬: LRCX(100) / LNG(98.8) / ASML(96.4) / MU(95.2) / <b>LITE(89.9)</b>
- LITE는 5위 → Top 3 진입 못 함
- 가까운 4위 MU (95.2)와 약 5점 차이

━━━━━━━━━━━━━━━
📊 <b>BE (Bloom Energy 추정)</b>
━━━━━━━━━━━━━━━

<b>실제 상태</b>: <b>part2_rank 자체에 진입 못 함</b> (Watchlist Top 30 외부)
<b>완전 차단 이유</b>: adj_gap 매우 양수 (고평가) + 4/28엔 eligible 풀에서도 제외

cr / adj_gap 추적:
- 4/22: cr=41, adj_gap=<b>+32.15</b>
- 4/23: cr=41, adj_gap=<b>+38.22</b>
- 4/24: cr=42, adj_gap=<b>+40.68</b>
- 4/27: cr=46, adj_gap=<b>+48.71</b>
- 4/28: <b>composite_rank NULL</b> (eligible 풀에서 빠짐), adj_gap=+21.14

<b>해석</b>:
- adj_gap 매우 양수 = 가격이 EPS보다 훨씬 빠르게 상승 = 매우 고평가
- 시스템 아젠다 ("파괴적 혁신 기업을 <b>싸게</b> 살래") 정반대
- BE는 매수 후보 자체로 부적합

<b>4/28 eligible 차단 이유 추정</b>:
- min_seg &lt; -2% (이탈 신호)
- 또는 다른 필터 (저마진 / 하향과반 / 매출 부족)
- composite_rank NULL = 1차 필터에서 탈락

━━━━━━━━━━━━━━━
🎯 <b>결론</b>
━━━━━━━━━━━━━━━

🟢 <b>LITE</b>: <b>정상 모니터링 중</b> (Watchlist 5위)
- 4/28 cr=3로 강한 회복
- 점수 4위라 매수 후보 Top 3 못 들어감
- 만약 점수 더 오르면 매수 후보 진입 가능

🔴 <b>BE</b>: <b>시스템이 차단</b>
- 매우 고평가 (adj_gap +21~+49)
- 시스템 아젠다 ("싸게 사라")와 정반대
- 4/28엔 eligible 풀에서도 빠짐 (1차 필터 차단)
- 매수 후보 진입 가능성 매우 낮음

━━━━━━━━━━━━━━━
💡 <b>참고</b>
━━━━━━━━━━━━━━━

- 시스템은 "EPS 대비 저평가" 종목만 매수 후보로 선정
- adj_gap 음수 (저평가) 종목 우선
- adj_gap 양수 (고평가) 종목 제외
- BE처럼 가격이 EPS보다 빨리 오른 종목은 시스템 디자인상 매수 X"""


def send(text):
    url = f'https://api.telegram.org/bot{TOKEN}/sendMessage'
    r = requests.post(url, data={
        'chat_id': CHAT_ID,
        'text': text,
        'parse_mode': 'HTML',
        'disable_web_page_preview': True,
    })
    return r.status_code, r.text[:200]


code, body = send(MSG)
print(f'전송: {code} ({len(MSG)}자)')
if code != 200:
    print(f'실패: {body}')
