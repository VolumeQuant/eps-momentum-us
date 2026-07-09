# -*- coding: utf-8 -*-
"""통합 신호 새 포맷(6~20위+AI시황) 미리보기 개인봇 발송 — KR은 최신 로그, US는 실시간."""
import sys, os, csv
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import unified_vm_track as u
import daily_runner as dr

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
merged = sorted(us + kr, key=lambda d: -d['rev90'])
KRN = {'000660.KS': 'SK하이닉스', '005930.KS': '삼성전자', '011070.KS': 'LG이노텍'}
IND = {'SNDK': '미국 · 낸드 반도체', 'MU': '미국 · 메모리 반도체', 'HPE': '미국 · AI서버',
       'DELL': '미국 · 서버·PC', 'FLEX': '미국 · 전자 제조', 'MCHP': '미국 · 반도체',
       '000660.KS': '한국 · 메모리 반도체', '005930.KS': '한국 · 전자'}
lines = ['🌏 <b>미국+한국 이익전망 TOP5</b> (미리보기)',
         '애널리스트 이익전망이 가장 빠르게',
         '좋아지는 5종목을 각 20%씩 담습니다.', '']
for i, d in enumerate(merged[:u.N_TOP], 1):
    nm = KRN.get(d['ticker'], d['ticker'])
    sect = IND.get(d['ticker'], '')
    lines.append(f"{i}. <b>{nm}</b>" + (f' ({sect})' if sect else ''))
    lines.append(f"   90일간 이익전망 +{d['rev90']:.0f}% 상향")
    sub = f"   예상이익 대비 주가 {d['fwd_per']:.0f}배"
    if d.get('gap'):
        sub += f" · 이익 {d['gap']:.1f}배 성장 예상"
    lines.append(sub)
lines += ['', '📊 <b>다음 후보 6~20위</b> (참고용 · 매수 아님)',
          '같은 검사를 통과한 종목의 이익전망 순위예요.']
for j, d in enumerate(merged[u.N_TOP:20], u.N_TOP + 1):
    nm2 = KRN.get(d['ticker'], d['ticker'])
    cc = '한' if d['market'] == 'KR' else '미'
    lines.append(f"{j}. {nm2}({cc}) +{d['rev90']:.0f}%")
lines += ['', '📋 매매는 교체일에만 합니다.']
brief = u._ai_market_brief()
if brief:
    lines += ['', '📰 <b>오늘 시장 한눈에</b>']
    import re
    for sent in re.split(r'(?<=\.)\s+', brief):
        for wl in u._wrap(sent.strip(), 32):
            if wl:
                lines.append(wl)
else:
    lines += ['', '(📰 AI 시황: 키 없음 — 회사PC 설정 필요)']
msg = '\n'.join(lines)
config = dr.load_config()
pid = config.get('telegram_private_id') or config.get('telegram_chat_id')
dr.send_telegram_long(msg, config, chat_id=pid)
print('sent, KR rows:', len(kr), 'merged:', len(merged))
