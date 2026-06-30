# -*- coding: utf-8 -*-
"""안전장치(gap·거래량)에 막히는 고순위 종목 페이퍼 추적기 (forward-only).

목적: TSEM처럼 "리비전 순위는 진입권(cr<=2 or p2<=2)인데 gap<2.5 또는 거래량<$1B로
막히는" 종목이 실제로 좋은 매수였는지 n>0 데이터를 매일 누적. 매매 무관·관찰 전용.

매일: DB 최신일 상태를 paper_track_log.csv에 append(중복일 스킵).
      과거 로그 중 N거래일 경과분의 forward 수익을 DB 가격으로 계산.
환경: WATCH_TICKERS(쉼표구분, 기본 'TSEM'), --send(개인봇), 인자없으면 콘솔.
"""
import os, sys, csv, json, sqlite3, urllib.request, urllib.parse
sys.stdout.reconfigure(encoding='utf-8')

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(os.path.dirname(HERE), 'eps_momentum_data.db')
TE = os.path.join(os.path.dirname(HERE), 'data_cache', 'trailing_eps_ttm.json')
LOG = os.path.join(HERE, 'paper_track_log.csv')
GAP_THR, VOL_THR = 2.5, 1000.0   # 현행 안전장치 (비교 기준)

WATCH = [t.strip().upper() for t in os.environ.get('WATCH_TICKERS', 'TSEM').split(',') if t.strip()]


def _pit_ttm(te, t, d):
    v = None
    for rd, e in te.get(t, []):
        if rd <= d:
            v = e
        else:
            break
    return v


def _state(cur, te, t, d):
    r = cur.execute("SELECT composite_rank,part2_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d FROM ntm_screening WHERE ticker=? AND date=?", (t, d)).fetchone()
    if not r or not r[2] or not r[3]:
        return None
    cr, p2, px, nc, n7, n30, n60, n90, dv = r
    ttm = _pit_ttm(te, t, d)
    gap = (nc / ttm) if (ttm and ttm > 0) else None
    fpe = px / nc
    rev30 = (nc / n30 - 1) * 100 if n30 else 0
    rev7 = (nc / n7 - 1) * 100 if n7 else 0
    segs = [(a - b) / abs(b) * 100 for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)] if b]
    mseg = min(segs) if segs else 0
    eligible = (cr is not None and cr <= 2) or (p2 is not None and p2 <= 2)   # 진입권 순위
    gap_pass = (gap is None) or (gap >= GAP_THR)
    vol_pass = (dv or 0) >= VOL_THR
    blocked = []
    if not gap_pass: blocked.append('gap')
    if not vol_pass: blocked.append('vol')
    return dict(date=d, ticker=t, cr=cr, p2=p2, price=round(px, 2), fwd_per=round(fpe, 1),
                gap=(round(gap, 2) if gap is not None else ''), rev30=round(rev30, 1), rev7=round(rev7, 1),
                minseg=round(mseg, 2), dvol_M=round(dv or 0), eligible=int(eligible),
                gap_pass=int(gap_pass), vol_pass=int(vol_pass), blocked='|'.join(blocked),
                would_buy=int(eligible and gap_pass and vol_pass))


FIELDS = ['date', 'ticker', 'cr', 'p2', 'price', 'fwd_per', 'gap', 'rev30', 'rev7', 'minseg',
          'dvol_M', 'eligible', 'gap_pass', 'vol_pass', 'blocked', 'would_buy']


def main():
    conn = sqlite3.connect(DB); cur = conn.cursor()
    te = json.load(open(TE, encoding='utf-8'))
    today = os.environ.get('MARKET_DATE', '').strip() or cur.execute("SELECT MAX(date) FROM ntm_screening WHERE ntm_current IS NOT NULL").fetchone()[0]

    # 기존 로그 로드
    rows = []
    if os.path.exists(LOG):
        with open(LOG, encoding='utf-8') as f:
            rows = list(csv.DictReader(f))
    logged_keys = {(r['date'], r['ticker']) for r in rows}

    # 오늘 상태 append (중복 스킵)
    added = []
    for t in WATCH:
        if (today, t) in logged_keys:
            continue
        st = _state(cur, te, t, today)
        if st:
            rows.append({k: st[k] for k in FIELDS}); added.append(st)
    rows.sort(key=lambda r: (r['date'], r['ticker']))
    with open(LOG, 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=FIELDS); w.writeheader(); w.writerows(rows)

    # forward 수익 계산: 진입권(eligible)인데 막힌(blocked) 날들 중 가격 데이터로 경과분
    def fwd_ret(t, d, horizon):
        seq = [r[0] for r in cur.execute("SELECT price FROM ntm_screening WHERE ticker=? AND date>=? AND price>0 ORDER BY date", (t, d))]
        return ((seq[horizon] / seq[0] - 1) * 100) if len(seq) > horizon else None

    lines = []
    lines.append("📡 <b>[페이퍼 추적: 안전장치에 막히는 고순위 종목]</b>")
    lines.append(f"기준일 {today} · 감시 {','.join(WATCH)} · 매매 무관 관찰")
    lines.append("")
    for t in WATCH:
        tr = [r for r in rows if r['ticker'] == t]
        st = next((r for r in tr if r['date'] == today), None)
        if not st:
            continue
        bl = st['blocked'] or '없음(통과)'
        _f = lambda v: float(v) if v not in ('', None) else 0.0
        lines.append(f"<b>{t}</b> — 당일순위 {st['cr']} / 가중 {st['p2']} / 선행PER {st['fwd_per']} / gap {st['gap']} / 거래대금 ${st['dvol_M']}M")
        lines.append(f"  리비전 30일 {_f(st['rev30']):+.1f}% · 7일 {_f(st['rev7']):+.1f}% · minSeg {st['minseg']}")
        verdict = '✅ 시스템 매수대상' if str(st['would_buy']) == '1' else f"❌ 막힘({bl})"
        lines.append(f"  → {verdict}")
        # 누적: 진입권+막힘 날들의 forward
        blocked_days = [r for r in tr if str(r['eligible']) == '1' and r['blocked']]
        lines.append(f"  누적: 진입권인데 막힌 날 {len(blocked_days)}일")
        for hz, lab in [(20, '20일'), (40, '40일')]:
            vals = [fwd_ret(t, r['date'], hz) for r in blocked_days]
            vals = [v for v in vals if v is not None]
            if vals:
                avg = sum(vals) / len(vals); win = sum(1 for v in vals if v > 0) / len(vals) * 100
                lines.append(f"    {lab} 경과 {len(vals)}건: 평균 {avg:+.1f}% 승률 {win:.0f}%  ← 막은 게 맞았나 검증")
        lines.append("")
    lines.append("ℹ️ 막힌 날들의 forward가 충분히(20일+) 쌓이면 '안전장치가 과하게 막나'를 룰로 판정 가능.")
    msg = '\n'.join(lines)

    if '--send' in sys.argv:
        tok = os.environ.get('TELEGRAM_BOT_TOKEN'); pid = os.environ.get('TELEGRAM_PRIVATE_ID')
        if tok and pid:
            data = urllib.parse.urlencode({'chat_id': pid, 'text': msg, 'parse_mode': 'HTML'}).encode()
            r = urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{tok}/sendMessage', data=data))
            print('발송:', r.status)
        else:
            print('시크릿 없음 — 미리보기:\n'); print(msg)
    else:
        print(msg)
    print(f"\n[로그] {LOG} 총 {len(rows)}행 (오늘 {len(added)}건 추가)")


if __name__ == '__main__':
    main()
