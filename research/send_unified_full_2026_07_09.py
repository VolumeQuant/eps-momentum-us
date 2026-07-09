# -*- coding: utf-8 -*-
"""오늘자 통합 신호 풀버전 3종 메시지 — 사용자 지시 전체 반영판을 개인봇 발송.
US=실시간 DB, KR=최신 커밋 로그(회사PC 데이터). 정규 발송은 회사PC 18:10이 담당."""
import sys, os, csv
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import unified_vm_track as u
import daily_runner as dr

config = dr.load_config()
pid = config.get('telegram_private_id') or config.get('telegram_chat_id')

# ── 데이터 조립: US 실시간 + KR 로그 ──
_, us = u.us_candidates()
rows = list(csv.DictReader(open(os.path.join(BASE, 'data_cache', 'unified_vm_log.csv'), encoding='utf-8')))
last_day = rows[-1]['run_date']
trows = [r for r in rows if r['run_date'] == last_day]
starts = [i for i, r in enumerate(trows) if r['rank'] == '1']
if starts:
    trows = trows[starts[-1]:]
kr = [dict(market='KR', ticker=r['ticker'], rev90=float(r['rev90']), fwd_per=float(r['fwd_per']),
           gap=float(r['gap']) if r['gap'] else None, dv_musd=None, price=None)
      for r in trows if r['market'] == 'KR']
merged = sorted(us + kr, key=lambda d: -d['rev90'])[:20]
KRN = {'000660.KS': 'SK하이닉스', '005930.KS': '삼성전자', '011070.KS': 'LG이노텍'}

briefs = u._ai_stock_briefs(merged)
cards = u._us_cards([d['ticker'] for d in merged if d['market'] == 'US'])
import re
def brief_lines(tk, indent='   '):
    b = briefs.get(tk)
    if not b:
        return []
    out = []
    for sent in re.split(r'(?<=[.다])\s+', b):
        for wl in u._wrap(sent.strip(), 90):
            if wl:
                out.append(indent + wl)
    return out

# 교체 카운트다운·전략 누적 (로그 리플레이)
all_days = sorted({r['run_date'] for r in rows})
idx = len(all_days) - 1
next_in = u.REBAL - (idx % u.REBAL)
nav = 1.0; hold = []; ppx = {}
for i, d in enumerate(all_days):
    day = [r for r in rows if r['run_date'] == d]
    st2 = [k for k, r in enumerate(day) if r['rank'] == '1']
    if st2:
        day = day[st2[-1]:]
    px = {r['ticker']: float(r['price']) for r in day if r.get('price')}
    if hold:
        rr = [px[t] / ppx[t] - 1 for t in hold if t in px and t in ppx and ppx[t] > 0]
        if rr:
            nav *= 1 + sum(rr) / len(rr)
    if i % u.REBAL == 0:
        hold = [r['ticker'] for r in day if r.get('in_top4') == '1']
    ppx.update(px)

# ── 메시지 1: TOP5 본론 ──
m1 = ['🌏 <b>미국+한국 이익전망 TOP5</b>',
      '증권가의 이익 눈높이(1년 예상이익)가',
      '가장 빠르게 오르는 5종목을 각 20%씩.',
      f'다음 교체까지 {next_in}거래일 (그때까지 유지)', '']
for i, d in enumerate(merged[:5], 1):
    nm = u._display_name(d['ticker'])
    tkd = d['ticker'].replace('.KS', '')
    sect = u._industry_tag(d)
    m1.append(f"{i}. <b>{nm}</b> ({tkd}" + (f" · {sect})" if sect else ")"))
    m1.append(f"   증권가 이익 눈높이 3개월새 +{d['rev90']:.0f}%")
    if d.get('gap'):
        m1.append(f"   1년 예상이익 = 지난 1년의 {d['gap']:.1f}배")
    m1.append(f"   주가는 예상이익의 {d['fwd_per']:.0f}배 (낮을수록 저렴)")
    for cl in cards.get(d['ticker'], []):
        m1.append('   ' + cl)
    m1 += brief_lines(d['ticker'])
    m1.append('')
m1 += [f'전략 누적 {(nav - 1) * 100:+.1f}% ({all_days[0][5:].replace("-", "/")}~)', '',
       '📋 매매는 교체일에만 합니다.',
       '미국 종목 = 당일 밤 개장,',
       '한국 종목 = 다음날 아침 개장에.']
from memory_cycle_alert import build_message
amsg, fired = build_message()
m1 += ['', amsg]

# ── 메시지 2: 다음 후보 6~20위 ──
m2 = ['📊 <b>다음 후보 6~20위</b> (참고용 · 매수 아님)',
      'TOP5와 같은 검사를 통과한',
      '다음 순위 종목들이에요.', '']
for j, d in enumerate(merged[5:20], 6):
    nm2 = u._display_name(d['ticker'])
    tk2 = d['ticker'].replace('.KS', '')
    sect2 = u._industry_tag(d)
    gtxt = f" · 예상이익 작년의 {d['gap']:.1f}배" if d.get('gap') else ''
    m2.append(f"<b>{j}. {nm2}</b> ({tk2}" + (f" · {sect2})" if sect2 else ")"))
    m2.append(f"   눈높이 +{d['rev90']:.0f}% · 주가/예상이익 {d['fwd_per']:.0f}배{gtxt}")
    m2 += brief_lines(d['ticker'])
    m2.append('')

# ── 메시지 3: AI 시장 분석 ──
m3 = u._market_page()

dr.send_telegram_long('\n'.join(m1), config, chat_id=pid)
dr.send_telegram_long('\n'.join(m2), config, chat_id=pid)
if m3:
    dr.send_telegram_long(m3, config, chat_id=pid)
print(f'sent 3 messages / merged {len(merged)} / briefs {len(briefs)} / fired={fired}')
