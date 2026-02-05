import requests
import os
import time

os.chdir(r'C:\dev\claude code\eps-momentum-us')

BOT_TOKEN = '7948087946:AAGVHj7FdBxr0LRJzQTzfEp0HadzAtoXs-8'
CHAT_ID = '7580571403'

msg1 = """📊 11-25위 종목 분석
━━━━━━━━━━━━━━━━━━━━━━

11. FTI | TechnipFMC
    에너지장비 | $57
    종합: 43.6점 | 💎59 💵20
    💡 52주 고점 -2% 돌파 임박. RSI 77 과열. 조정 후 진입.

12. CACI | CACI International
    IT서비스 | $605
    종합: 42.1점 | 💎44 💵40
    💡 방산/정부IT 안정적. RSI 48 중립. RED 시장 방어적.

13. RGLD | Royal Gold
    금로열티 | $266
    종합: 42.0점 | 💎57 💵63
    💡 금 로열티 저위험 모델. NEM과 함께 금 테마 분산용.

14. CRS | Carpenter Technology
    특수금속 | $334
    종합: 41.5점 | 💎65 💵27
    💡 항공우주 티타늄 수혜. RSI 54 중립이나 고평가 부담.

15. FDX | FedEx
    물류 | $363
    종합: 41.0점 | 💎55 💵20
    💡 RSI 79 극과열 + 버블. 고점물림 위험. 패스.

16. HII | Huntington Ingalls
    방산조선 | $413
    종합: 40.2점 | 💎54 💵35
    💡 해군 조선 독점. 영업익 +113%. 지정학 수혜주.

17. F | Ford Motor
    자동차 | $14
    종합: 39.7점 | 💎51 💵63
    💡 영업익 +77% 회복. 경기민감주 리스크. 보수적 접근.

18. ATI | ATI Inc.
    특수금속 | $128
    종합: 39.3점 | 💎65 💵22
    💡 항공우주 특수금속. B+급 밸류이나 고평가.

19. CTSH | Cognizant
    IT서비스 | $77
    종합: 35.4점 | 💎54 💵47
    💡 ⚠️ RSI 29 과매도! 급락 반등 트레이딩 기회.

20. INCY | Incyte
    바이오 | $103
    종합: 34.8점 | 💎54 💵45
    💡 영업익 +155% 흑자전환. RSI 42 저점. 방어 섹터.

21. FIVE | Five Below
    소매 | $193
    종합: 34.4점 | 💎56 💵42
    💡 저가 소매 체인. RSI 44 중립. 소비재 불확실성.

22. JBL | Jabil
    전자부품 | $236
    종합: 33.3점 | 💎60 💵35
    💡 EMS 리더, ROE 48%. RSI 46 중립. 기술주 반등 시 후순위.

23. TPR | Tapestry
    명품패션 | $130
    종합: 31.4점 | 💎55 💵35
    💡 Coach/Kate Spade 모회사. 명품 소비 둔화 우려.

24. GMED | Globus Medical
    의료기기 | $87
    종합: 30.3점 | 💎45 💵42
    💡 ⚠️ RSI 30 과매도! 척추로봇 테마. 반등 기대.

25. CVNA | Carvana
    중고차 | $393
    종합: 29.5점 | 💎54 💵30
    💡 ROE 68% 턴어라운드. RSI 34 저점. 고변동성.

━━━━━━━━━━━━━━━━━━━━━━
🤖 EPS Momentum v7.0.6"""

msg2 = """📊 26-37위 종목 분석
━━━━━━━━━━━━━━━━━━━━━━

26. DRI | Darden Restaurants
    외식 | $212
    종합: 29.4점 | 💎54 💵30
    💡 Olive Garden 운영. ROE 54%. 외식업 경기민감.

27. LLY | Eli Lilly
    제약 | $1,107
    종합: 28.7점 | 💎65 💵17
    💡 비만/당뇨약 블록버스터. ROE 97%! 그러나 극심한 고평가.

28. CBOE | Cboe Global Markets
    거래소 | $271
    종합: 27.9점 | 💎50 💵30
    💡 옵션거래소. 변동성 증가 시 수혜. VIX 급등 시 관심.

29. CCL | Carnival
    크루즈 | $32
    종합: 27.8점 | 💎49 💵30
    💡 크루즈 회복세. 부채 부담 리스크. RED 시장 부적합.

30. ROK | Rockwell Automation
    산업자동화 | $430
    종합: 26.6점 | 💎59 💵17
    💡 공장자동화 수혜. 영업익 +61%. 가격17점 버블.

31. CAH | Cardinal Health
    의약품유통 | $207
    종합: 26.0점 | 💎34 💵40
    💡 의약품 유통. C급 밸류 한계. 우선순위 낮음.

32. DGX | Quest Diagnostics
    진단검사 | $189
    종합: 25.9점 | 💎44 💵30
    💡 진단검사 서비스. 방어적 헬스케어. 성장성 제한.

33. PH | Parker-Hannifin
    산업장비 | $968
    종합: 23.3점 | 💎50 💵17
    💡 52주 고점 근접. 가격17점 고평가. 상승 여력 의문.

34. ROL | Rollins
    해충방제 | $64
    종합: 22.4점 | 💎54 💵10
    💡 ⛔ 가격10점 극심한 버블. 진입 금지.

35. CASY | Casey's General Stores
    편의점 | $648
    종합: 21.7점 | 💎45 💵17
    💡 중서부 편의점. 고점 근접 + 가격17점. 너무 비쌈.

36. WDC | Western Digital
    저장장치 | $269
    종합: 20.4점 | 💎76 💵60
    💡 ⚠️ 밸류76점 A급! 단기 급등으로 점수 낮음. 조정 시 1순위.

37. WTS | Watts Water Technologies
    산업장비 | $307
    종합: 19.7점 | 💎44 💵12
    💡 물 인프라 장비. 가격12점 버블. 투자 매력 낮음.

━━━━━━━━━━━━━━━━━━━━━━
🤖 11-37위 중 주목

✅ 과매도 반등
• CTSH (RSI29) - IT서비스 급락
• GMED (RSI30) - 의료기기 급락

🛡️ RED 시장 방어주
• CACI - 방산IT
• HII - 해군 조선
• RGLD - 금 로열티

⛔ 진입 금지 (버블)
• ROL/CASY/WTS

━━━━━━━━━━━━━━━━━━━━━━
🤖 EPS Momentum v7.0.6"""

url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'

resp1 = requests.post(url, json={'chat_id': CHAT_ID, 'text': msg1})
print(f'11-25: {resp1.status_code}')
time.sleep(1)

resp2 = requests.post(url, json={'chat_id': CHAT_ID, 'text': msg2})
print(f'26-37: {resp2.status_code}')
