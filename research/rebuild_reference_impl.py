# -*- coding: utf-8 -*-
"""재설계 v2 참조 구현 (deploy-ready 초안, production 미통합).
컨셉: 가치우선 단일 포트폴리오 — 싼 것(fwd_PER<=상한) + 전망건강(min_seg>=0) + 유동($1B) 중 top-N 동일보유.
모두가 같은 포트폴리오(coherent). 보유=매수(비대칭 없음). 진입일 무관. 주1회 리밸.
근본원인 해결: carryover·에폭 없음 / 순위=가치우선 / dv 전종목 추적(아래 _dollar_vol는 마지막값 carry, production은 파이프라인 fix 필요).
"""
import sqlite3, os, sys, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr

# ── 튜닝 파라미터 (검증됨: 인접성 평지·walk-forward robust) ──
PE_MAX = 18       # 가치 게이트: fwd_PER 상한 (15~20 robust plateau)
N = 5             # 포트폴리오 종목수 (동일가중 1/N)
MIN_DV_M = 1000   # 유동성 $1B


def _min_seg(nc, n7, n30, n60, n90):
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        segs.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(segs) if segs else 0


def _last_known_dv(cur, tk, d):
    """순위 밖이어도 거래대금 확보 — 마지막 알려진 값(대형주는 유동 유지).
    ★production: update_dollar_volumes를 Top30→가치후보 전체로 확장하면 이 carry 불필요."""
    r = cur.execute('SELECT dollar_volume_30d FROM ntm_screening WHERE ticker=? AND dollar_volume_30d IS NOT NULL AND date<=? ORDER BY date DESC LIMIT 1', (tk, d)).fetchone()
    return r[0] if r else None


def select_portfolio(date_str, pe_max=PE_MAX, n=N):
    """그날의 단일 목표 포트폴리오 = 가치우선 top-N. 순수 함수(오늘 데이터만) → coherent."""
    conn = sqlite3.connect(dr.DB_PATH); cur = conn.cursor()
    rows = cur.execute(
        'SELECT ticker,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening WHERE date=? AND price IS NOT NULL AND ntm_current>0',
        (date_str,)).fetchall()
    cand = []
    for tk, px, nc, n7, n30, n60, n90 in rows:
        if _min_seg(nc, n7, n30, n60, n90) < 0:          # 전망 꺾임 제외
            continue
        fpe = px / nc
        if fpe > pe_max:                                  # 가치 게이트: 싸야 함
            continue
        dv = _last_known_dv(cur, tk, date_str)
        if (dv or 0) < MIN_DV_M:                           # 유동성
            continue
        cand.append((tk, fpe))
    conn.close()
    cand.sort(key=lambda x: x[1])                          # 제일 싼 순
    return [t for t, _ in cand[:n]]


def format_message(today, prev):
    """coherent 메시지 — 보유/매수/매도가 명확. '이탈' 노트 없음."""
    add = [t for t in today if t not in prev]
    drop = [t for t in prev if t not in today]
    keep = [t for t in today if t in prev]
    w = round(100 / len(today)) if today else 0
    lines = [f"📊 오늘의 포트폴리오 ({len(today)}종목 · 각 {w}%)"]
    lines.append("  " + " · ".join(today) if today else "  (자격 종목 없음 → 현금)")
    lines.append("")
    if add:  lines.append(f"🟢 신규 매수: {', '.join(add)}")
    if drop: lines.append(f"🔴 매도(포트에서 빠짐): {', '.join(drop)}")
    if keep: lines.append(f"💎 계속 보유: {', '.join(keep)}")
    lines.append("")
    lines.append(f"규칙: 싼 것(fwd_PER≤{PE_MAX}) + 전망 오름 상위 {N}종목 동일보유. 매주 리스트만 갱신.")
    return "\n".join(lines)


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    conn = sqlite3.connect(dr.DB_PATH)
    ld = conn.execute('SELECT MAX(date) FROM ntm_screening WHERE part2_rank IS NOT NULL').fetchone()[0]
    dts = [r[0] for r in conn.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date DESC LIMIT 6')]
    conn.close()
    prev_d = dts[1] if len(dts) > 1 else ld
    today = select_portfolio(ld)
    prev = select_portfolio(prev_d)
    print(f"=== 재설계 참조구현 데모 (PE_MAX={PE_MAX}, N={N}) — 최신 {ld} ===\n")
    print(format_message(today, prev))
    print(f"\n(어제 {prev_d} 포트폴리오: {prev})")
    print("\n각 fwd_PER:")
    conn = sqlite3.connect(dr.DB_PATH); cur = conn.cursor()
    for tk in today:
        r = cur.execute('SELECT price,ntm_current FROM ntm_screening WHERE ticker=? AND date=?', (tk, ld)).fetchone()
        if r and r[1]: print(f"  {tk}: fwd_PER {r[0]/r[1]:.1f}")
    conn.close()
