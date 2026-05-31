"""ETF Pulse Ranking Changes — 이번 주 ranking 변동 추적

매주 카테고리별 best ETF 변동 자동 감지:
- 어제 best → 오늘 best 변동
- 신규 진입 / 탈락
- 점수 큰 변동
"""
import sys
import sqlite3
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))
from compare import COMPARE_GROUPS, compare_group

DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def track_ranking_changes(date_str=None, lookback_days=7):
    """N일 전 vs 오늘 ranking 변동"""
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    if not date_str:
        date_str = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]

    # N일 전
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM etf_daily WHERE date <= ? ORDER BY date DESC LIMIT ?',
        (date_str, lookback_days + 1)).fetchall()]
    if len(dates) < 2:
        conn.close(); return None
    past_date = dates[-1]
    conn.close()

    changes = []
    for group, tks in COMPARE_GROUPS.items():
        today = compare_group(group, tks, date_str)
        past = compare_group(group, tks, past_date)
        if not today or not past:
            continue
        today_top = today['best']['ticker']
        past_top = past['best']['ticker']
        if today_top != past_top:
            changes.append({
                'group': group,
                'past_best': past_top,
                'today_best': today_top,
                'past_score': past['best']['total_score'],
                'today_score': today['best']['total_score'],
            })

    return {'date': date_str, 'past_date': past_date, 'changes': changes}


def gen_changes_md(date_str=None):
    """변동 콘텐츠"""
    r = track_ranking_changes(date_str)
    if r is None:
        return '# 데이터 부족\n'

    lines = [f'# 📊 이번 주 카테고리 best 변동', '']
    lines.append(f'기간: {r["past_date"]} → {r["date"]}')
    lines.append('')

    if not r['changes']:
        lines.append('변동 없음. 모든 카테고리 best ETF 동일.')
    else:
        lines.append(f'**{len(r["changes"])}개 카테고리에서 best 변동**:')
        lines.append('')
        for c in r['changes']:
            lines.append(f'## {c["group"]}')
            lines.append(f'  - 이전: {c["past_best"]} (점수 {c["past_score"]:.1f})')
            lines.append(f'  - 현재: **{c["today_best"]}** (점수 {c["today_score"]:.1f})')
            delta = c['today_score'] - c['past_score']
            lines.append(f'  - 점수 변화: {delta:+.1f}')
            lines.append('')

    lines.append('---')
    lines.append('_매주 자동 갱신. 변동 발생 시 Pro 구독자에게 알림._')
    return '\n'.join(lines)


if __name__ == '__main__':
    md = gen_changes_md()
    out = Path(__file__).parent / 'content' / 'weekly_changes.md'
    out.parent.mkdir(exist_ok=True)
    out.write_text(md, encoding='utf-8')
    print(f'저장: {out}')
    print(f'\n{md}')
