"""ETF Pulse Category Best — 카테고리별 매주 best 추천 콘텐츠

매주 발행 콘텐츠 (재방문 + 신뢰 자산):
- 30개 카테고리 best 3개씩
- 변동 알림 (이번 주 ranking 변동)
- 종합 점수 + 한 줄 추천
"""
import sys
import sqlite3
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))
from compare import COMPARE_GROUPS, compare_group

DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def gen_weekly_best_md(date_str=None):
    """카테고리별 best 3 종합 콘텐츠"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if not date_str:
        date_str = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]
    conn.close()

    lines = []
    lines.append(f'# 🏆 ETF 카테고리 Best — {date_str}')
    lines.append('')
    lines.append('30개 카테고리별 종합 점수 1위 ETF + 대안 2개.')
    lines.append('객관 점수 (AUM 50% + 운용보수 30% + 거래량 20%) 기반. 광고/편향 없음.')
    lines.append('')
    lines.append('---')
    lines.append('')

    # 그룹별 best
    for group, tks in COMPARE_GROUPS.items():
        result = compare_group(group, tks, date_str)
        if not result or len(result['etfs']) == 0:
            continue
        b = result['best']
        lines.append(f'## {group}')
        lines.append('')
        # 1위
        reason = result['recommendation'] if result['recommendation'] else f'종합 점수 {b["total_score"]:.1f}'
        lines.append(f'**🥇 {b["ticker"]}** — {reason}')
        lines.append(f'  - AUM ${b["aum"]/1e9:.1f}B · 수수료 {b["expense_ratio"]*100:.3f}%' if b["expense_ratio"] else f'  - AUM ${b["aum"]/1e9:.1f}B')
        # 2-3위
        if len(result['etfs']) >= 2:
            for i, e in enumerate(result['etfs'][1:3], 2):
                medal = '🥈' if i == 2 else '🥉'
                lines.append(f'{medal} **{e["ticker"]}** (점수 {e["total_score"]:.1f}) — AUM ${e["aum"]/1e9:.1f}B')
        lines.append('')

    lines.append('---')
    lines.append('')
    lines.append('_매주 갱신. 변동 시 자동 알림 (Pro)._')
    return '\n'.join(lines)


if __name__ == '__main__':
    md = gen_weekly_best_md()
    out = Path(__file__).parent / 'content' / 'category_best_weekly.md'
    out.parent.mkdir(exist_ok=True)
    out.write_text(md, encoding='utf-8')
    print(f'저장: {out}')
    print(f'\n{md[:2000]}')
    print('...')
