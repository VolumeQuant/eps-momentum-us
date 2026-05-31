"""ETF Pulse AI Chatbot — Claude API 자연어 어드바이저

사용자 질문 예시:
  - "AI 관련 ETF 추천"
  - "오늘 자금 가장 많이 들어온 ETF 뭐야?"
  - "내 포트폴리오에 채권 더 넣어야 할까?"
  - "VOO와 SPY 중 뭐가 좋아?"

데이터는 etf_pulse.db에서 실시간 조회 → context로 Claude에게.
"""
import sys
import os
import json
import sqlite3
import re
from pathlib import Path
from urllib import request as urlreq, parse as urlparse

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def get_db_context(query):
    """질문에서 관련 데이터를 DB에서 추출 (간단 키워드 매칭)"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    latest = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]

    context = {'date': latest, 'snippets': []}

    # 1. ETF ticker 직접 언급 — 그 ETF 정보
    tickers_in_query = re.findall(r'\b[A-Z]{2,5}\b', query.upper())
    for tk in tickers_in_query[:5]:
        r = cur.execute('''
            SELECT category, price, day_return, volume_spike, aum, expense_ratio, dividend_yield
            FROM etf_daily WHERE ticker=? AND date=?
        ''', (tk, latest)).fetchone()
        if r:
            context['snippets'].append(f'{tk} ({r[0]}): ${r[1]:.2f}, '
                                       f'어제 {r[2]:+.2f}%, vol spike {r[3]:.2f}x, '
                                       f'AUM ${r[4]/1e9:.1f}B, 수수료 {r[5]*100:.3f}%, '
                                       f'배당 {r[6]*100:.2f}%')

    # 2. 키워드 매칭
    keywords = {
        '거래량': 'volume', '폭증': 'spike', '핫': 'spike',
        '수익률': 'return', '강세': 'top_return',
        '약세': 'bottom', '하락': 'bottom',
        'AI': 'AI', '반도체': 'semi', '바이오': 'bio',
        '배당': 'dividend', '채권': 'bonds',
        '신고가': 'high', '신저가': 'low',
    }
    matched = [k for k, _ in keywords.items() if k in query]

    if '거래량' in matched or '폭증' in matched or '핫' in matched:
        rows = cur.execute('''
            SELECT ticker, category, volume_spike, day_return FROM etf_daily
            WHERE date=? AND volume_spike > 1.5 ORDER BY volume_spike DESC LIMIT 5
        ''', (latest,)).fetchall()
        if rows:
            context['snippets'].append('거래량 spike Top 5: ' + ', '.join(
                f'{r[0]}({r[2]:.1f}x, {r[3]:+.2f}%)' for r in rows))

    if '수익률' in matched or '강세' in matched:
        rows = cur.execute('''
            SELECT ticker, category, day_return FROM etf_daily
            WHERE date=? AND aum > 1e8 ORDER BY day_return DESC LIMIT 5
        ''', (latest,)).fetchall()
        if rows:
            context['snippets'].append('수익률 Top 5: ' + ', '.join(
                f'{r[0]}({r[2]:+.2f}%)' for r in rows))

    if 'AI' in matched:
        rows = cur.execute('''
            SELECT ticker, day_return, aum FROM etf_daily
            WHERE date=? AND ticker IN ('BOTZ','ROBO','IRBO','AIQ')
        ''', (latest,)).fetchall()
        if rows:
            context['snippets'].append('AI ETF: ' + ', '.join(
                f'{r[0]}({r[1]:+.2f}%, AUM ${r[2]/1e9:.1f}B)' for r in rows))

    if '반도체' in matched or 'semi' in matched.__str__().lower():
        rows = cur.execute('''
            SELECT ticker, day_return, aum FROM etf_daily
            WHERE date=? AND ticker IN ('SOXX','SMH','XSD','SOXL')
        ''', (latest,)).fetchall()
        if rows:
            context['snippets'].append('반도체 ETF: ' + ', '.join(
                f'{r[0]}({r[1]:+.2f}%)' for r in rows))

    if '채권' in matched or 'bonds' in matched.__str__().lower():
        rows = cur.execute('''
            SELECT ticker, day_return, aum FROM etf_daily
            WHERE date=? AND category='bonds' AND aum > 5e9 ORDER BY aum DESC LIMIT 5
        ''', (latest,)).fetchall()
        if rows:
            context['snippets'].append('주요 채권 ETF: ' + ', '.join(
                f'{r[0]}({r[1]:+.2f}%, AUM ${r[2]/1e9:.1f}B)' for r in rows))

    # 3. 카테고리 강도 (분위기 질문)
    if any(k in query for k in ['오늘', '시장', '분위기', '어제', '뭐가']):
        cat_rows = cur.execute('''
            SELECT category, AVG(day_return) FROM etf_daily WHERE date=? GROUP BY category
            ORDER BY AVG(day_return) DESC
        ''', (latest,)).fetchall()
        context['snippets'].append('카테고리 강도: ' + ', '.join(
            f'{r[0]}({r[1]:+.2f}%)' for r in cat_rows))

    conn.close()
    return context


def call_claude_api(prompt, api_key=None):
    """Anthropic Claude API 호출
    필요시 환경변수 ANTHROPIC_API_KEY 또는 인자로 전달
    """
    api_key = api_key or os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        return None, 'ANTHROPIC_API_KEY 없음'

    body = json.dumps({
        'model': 'claude-haiku-4-5-20251001',
        'max_tokens': 1024,
        'messages': [{'role': 'user', 'content': prompt}],
    }).encode('utf-8')
    req = urlreq.Request(
        'https://api.anthropic.com/v1/messages',
        data=body,
        headers={
            'x-api-key': api_key,
            'anthropic-version': '2023-06-01',
            'content-type': 'application/json',
        },
        method='POST'
    )
    try:
        with urlreq.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
            content = data.get('content', [{}])[0].get('text', '')
            return content, None
    except Exception as e:
        return None, str(e)


def call_gemini_api(prompt, api_key=None):
    """Gemini API 호출 (fallback)"""
    api_key = api_key or os.environ.get('GEMINI_API_KEY')
    if not api_key:
        # config.json 시도
        cfg_path = Path(__file__).parent.parent / 'config.json'
        if cfg_path.exists():
            cfg = json.loads(cfg_path.read_text(encoding='utf-8'))
            api_key = cfg.get('gemini_api_key')
    if not api_key:
        return None, 'GEMINI_API_KEY 없음'

    body = json.dumps({
        'contents': [{'parts': [{'text': prompt}]}],
        'generationConfig': {'maxOutputTokens': 1024},
    }).encode('utf-8')
    url = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}'
    req = urlreq.Request(url, data=body, headers={'content-type': 'application/json'}, method='POST')
    try:
        with urlreq.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
            content = data['candidates'][0]['content']['parts'][0]['text']
            return content, None
    except Exception as e:
        return None, str(e)


def ask(query, api='gemini'):
    """사용자 질문에 답"""
    context = get_db_context(query)
    snippets = '\n'.join(f'- {s}' for s in context['snippets'])

    prompt = f"""당신은 ETF 데이터 어드바이저입니다. 사용자 질문에 다음 데이터를 활용해 답하세요.

데이터 (기준일 {context['date']}):
{snippets if snippets else '(질문 관련 데이터 자동 추출 결과 없음 — 일반 지식으로 답)'}

규칙:
- 한국어로 친절하고 간결하게 답
- "투자 추천 아님" 명시
- 데이터 기반으로 답하되 일반 지식 활용 가능
- 짧고 명확하게 (3~5문장)

사용자 질문: {query}
"""

    if api == 'claude':
        content, err = call_claude_api(prompt)
    else:
        content, err = call_gemini_api(prompt)

    if err:
        return f'[API 오류: {err}]\n\n[수집된 데이터 직접 표시]\n{snippets}'
    return content


if __name__ == '__main__':
    queries = [
        '오늘 시장 분위기 어때?',
        'AI 관련 ETF 추천해줘',
        '반도체 ETF 중에 어떤 게 좋아?',
        '채권 ETF에 자금 들어왔어?',
        'VOO와 SPY 차이가 뭐야?',
    ]
    for q in queries:
        print('=' * 70)
        print(f'❓ {q}')
        print('-' * 70)
        ans = ask(q)
        print(ans)
        print()
