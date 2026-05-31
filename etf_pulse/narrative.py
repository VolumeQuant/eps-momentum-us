"""ETF Pulse Narrative Engine — 신호 → 자연어

1. Template-based narrative (rule-based, 안정적)
2. LLM enrichment (선택, Gemini 활용)
3. Markdown 콘텐츠 자동 생성
"""
import sys
import json
import re
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))
from signals import get_signals


# ━━━ 카테고리 한글 매핑 ━━━
CATEGORY_KR = {
    'core_us': '미국 코어 지수',
    'international': '국제/신흥국',
    'sectors': '섹터',
    'themes': '테마',
    'bonds': '채권',
    'commodity_hedge': '원자재/헷지',
    'income_lev': '인컴/레버리지',
}

# ━━━ ETF 한글 설명 (자주 등장 ETF만) ━━━
ETF_DESC = {
    'SPY': 'S&P 500', 'VOO': 'S&P 500', 'IVV': 'S&P 500',
    'QQQ': '나스닥 100', 'QQQM': '나스닥 100 미니',
    'SOXX': '반도체', 'SMH': '반도체', 'XSD': '반도체 (균등)',
    'SOXL': '반도체 3X 레버리지', 'TQQQ': '나스닥 3X 레버리지',
    'ARKK': 'ARK 혁신', 'ARKG': 'ARK 바이오', 'ARKW': 'ARK 차세대 인터넷',
    'ARKF': 'ARK 핀테크', 'ARKQ': 'ARK 자율주행/로봇', 'ARKX': 'ARK 우주',
    'IBIT': '비트코인 (BlackRock)', 'FBTC': '비트코인 (Fidelity)',
    'MSTU': 'MSTR 2X 레버리지', 'MSTZ': 'MSTR 2X 인버스',
    'NVDL': 'NVDA 2X 레버리지', 'NVDS': 'NVDA 인버스',
    'TSLL': 'TSLA 2X 레버리지', 'TSLZ': 'TSLA 인버스',
    'GLD': '금', 'IAU': '금', 'SLV': '은',
    'TLT': '미국 장기국채', 'IEF': '미국 중기국채', 'SHY': '미국 단기국채',
    'HYG': '하이일드 회사채', 'LQD': '투자등급 회사채',
    'JEPI': '커버드콜 인컴 (대형주)', 'JEPQ': '커버드콜 인컴 (나스닥)',
    'SCHD': '배당 성장', 'VYM': '고배당',
    'CIBR': '사이버보안', 'HACK': '사이버보안',
    'TAN': '태양광', 'ICLN': '클린 에너지',
    'GDX': '금광주', 'GDXJ': '소형 금광주',
    'URA': '우라늄', 'NLR': '원자력',
    'KWEB': '중국 인터넷', 'CWEB': '중국 인터넷 2X',
    'EWY': '한국', 'EWJ': '일본',
    'XLE': '에너지', 'XLF': '금융', 'XLV': '헬스케어',
    'XLK': '기술', 'XLI': '산업재', 'XLY': '소비재(임의)',
    'XLP': '소비재(필수)', 'XLU': '유틸리티', 'XLRE': '리츠',
    'PEJ': '여행/레저', 'JETS': '항공', 'BOTZ': '로봇/AI',
    'AIQ': '인공지능', 'CIBR': '사이버보안',
    'JPST': '단기 회사채 (액티브)', 'VCSH': '단기 회사채',
    'QUAL': 'Quality factor', 'MTUM': '모멘텀 factor',
    'USMV': '저변동성', 'SPHQ': 'S&P 500 quality',
    'IXUS': '국제 (ex-US)', 'IEFA': '선진국 (ex-US, ex-CAD)',
    'AMLP': 'MLP 인프라', 'DBC': '원자재 바스켓',
    'BITO': '비트코인 선물', 'BLOK': '블록체인',
}


def describe_etf(tk):
    """ETF에 한글 설명 추가"""
    desc = ETF_DESC.get(tk)
    return f'{tk} ({desc})' if desc else tk


def gen_narrative_kr(signals):
    """Template-based 한국어 narrative"""
    date = signals['date']
    lines = []

    # ━━━ 헤더 ━━━
    lines.append(f'# 🌅 ETF Pulse — {date}')
    lines.append('')
    lines.append(f'미국 시장 마감 후 ETF 신호 정리. AUM 상위 228개 ETF 대상.')
    lines.append('')

    # ━━━ 카테고리 강도 (시장 분위기) ━━━
    cs = signals['category_strength']
    if cs:
        strong = cs[0]
        weak = cs[-1]
        lines.append('## 🎯 어제 시장 분위기')
        lines.append('')
        lines.append(f'**강세 테마**: {CATEGORY_KR.get(strong["category"], strong["category"])} (평균 {strong["avg_return"]:+.2f}%)')
        lines.append(f'**약세 테마**: {CATEGORY_KR.get(weak["category"], weak["category"])} (평균 {weak["avg_return"]:+.2f}%)')
        lines.append('')
        lines.append('전체 카테고리별:')
        for c in cs:
            sign = '🟢' if c['avg_return'] >= 0.1 else '🔴' if c['avg_return'] <= -0.1 else '⚪'
            lines.append(f'- {sign} {CATEGORY_KR.get(c["category"], c["category"])}: {c["avg_return"]:+.2f}% '
                         f'(거래량 spike {c["avg_spike"]:.2f}x, {c["n_etfs"]}개)')
        lines.append('')

    # ━━━ 수익률 Top ━━━
    if signals['top_returns']:
        lines.append('## 📈 어제 수익률 Top 5')
        lines.append('')
        for s in signals['top_returns']:
            spike_note = f' 거래량 {s["spike"]:.1f}x 폭증' if s['spike'] > 1.5 else ''
            lines.append(f'- **{describe_etf(s["ticker"])}**: {s["day_return"]:+.2f}%{spike_note} '
                         f'(AUM ${s["aum_b"]:.1f}B)')
        lines.append('')

    # ━━━ 수익률 Bottom ━━━
    if signals['bottom_returns']:
        lines.append('## 📉 어제 수익률 Bottom 5')
        lines.append('')
        for s in signals['bottom_returns']:
            lines.append(f'- **{describe_etf(s["ticker"])}**: {s["day_return"]:+.2f}% '
                         f'(AUM ${s["aum_b"]:.1f}B)')
        lines.append('')

    # ━━━ 거래량 spike ━━━
    if signals['volume_spikes']:
        lines.append('## 🔥 거래량 폭증 Top 5')
        lines.append('')
        lines.append('30일 평균 거래량 대비 1.5배 이상 폭증한 ETF. 자금 이동 또는 이벤트 신호.')
        lines.append('')
        for s in signals['volume_spikes']:
            ret_note = f', 수익률 {s["day_return"]:+.2f}%' if abs(s['day_return']) > 0.5 else ''
            lines.append(f'- **{describe_etf(s["ticker"])}**: {s["spike"]:.2f}x 폭증{ret_note} '
                         f'(AUM ${s["aum_b"]:.1f}B)')
        lines.append('')

    # ━━━ 5일 모멘텀 ━━━
    if signals['momentum_5d']:
        lines.append('## 🚀 5일 강한 모멘텀')
        lines.append('')
        for s in signals['momentum_5d']:
            lines.append(f'- **{describe_etf(s["ticker"])}**: 최근 5거래일 {s["return_5d"]:+.2f}% '
                         f'(AUM ${s["aum_b"]:.1f}B)')
        lines.append('')

    # ━━━ 신고가/신저가 ━━━
    if signals['new_highs']:
        lines.append('## 🏔️ 30일 신고가 갱신')
        lines.append('')
        for h in signals['new_highs'][:5]:
            lines.append(f'- {describe_etf(h["ticker"])} (${h["price"]:.2f}, AUM ${h["aum_b"]:.1f}B)')
        lines.append('')

    if signals['new_lows']:
        lines.append('## 🏞️ 30일 신저가')
        lines.append('')
        for h in signals['new_lows'][:5]:
            lines.append(f'- {describe_etf(h["ticker"])} (${h["price"]:.2f}, AUM ${h["aum_b"]:.1f}B)')
        lines.append('')

    # ━━━ 자금 흐름 (가능한 경우) ━━━
    if signals['fund_flows']:
        lines.append('## 💰 어제 자금 흐름 (AUM diff 기반)')
        lines.append('')
        inflows = [f for f in signals['fund_flows'] if f['flow_m'] > 0][:5]
        outflows = [f for f in signals['fund_flows'] if f['flow_m'] < 0][:5]
        if inflows:
            lines.append('**유입**:')
            for f in inflows:
                lines.append(f'- {describe_etf(f["ticker"])}: +${f["flow_m"]:.0f}M (수익률 {f["day_return"]:+.2f}%)')
            lines.append('')
        if outflows:
            lines.append('**유출**:')
            for f in outflows:
                lines.append(f'- {describe_etf(f["ticker"])}: ${f["flow_m"]:.0f}M (수익률 {f["day_return"]:+.2f}%)')
            lines.append('')

    # ━━━ 자동 인사이트 (패턴 감지) ━━━
    insights = detect_patterns(signals)
    if insights:
        lines.append('## 🧠 자동 인사이트')
        lines.append('')
        for ins in insights:
            lines.append(f'- {ins}')
        lines.append('')

    # ━━━ 풋터 ━━━
    lines.append('---')
    lines.append('')
    lines.append('_ETF Pulse는 자동 데이터 분석 서비스입니다. 투자 추천이 아닙니다._')
    lines.append('')
    lines.append('데이터: yfinance | 대상: 미국 ETF AUM 상위 228개 | 갱신: 매일')

    return '\n'.join(lines)


def detect_patterns(signals):
    """신호에서 패턴 자동 감지"""
    insights = []

    # 1. 같은 카테고리 ETF 동시 강세 (테마 회전)
    top = signals['top_returns']
    cat_count = {}
    for s in top:
        cat_count[s['category']] = cat_count.get(s['category'], 0) + 1
    for cat, n in cat_count.items():
        if n >= 2:
            insights.append(f'**{CATEGORY_KR.get(cat, cat)}** 영역 ETF {n}개가 수익률 Top 5에 동시 진입 → 테마 회전 신호')

    # 2. 같은 테마 ETF 거래량 동시 폭증
    spike_tks = [s['ticker'] for s in signals['volume_spikes']]
    same_theme_spikes = {}
    # 알려진 테마 클러스터
    theme_clusters = {
        '사이버보안': ['CIBR', 'HACK'],
        '반도체': ['SOXX', 'SMH', 'SOXL', 'XSD'],
        'ARK': ['ARKK', 'ARKG', 'ARKW', 'ARKF', 'ARKQ', 'ARKX'],
        '클린에너지': ['TAN', 'ICLN', 'QCLN', 'PBW'],
        '금/금광': ['GLD', 'IAU', 'GDX', 'GDXJ'],
        '중국': ['FXI', 'KWEB', 'MCHI', 'CWEB', 'YINN', 'ASHR'],
        '비트코인': ['IBIT', 'FBTC', 'BITO', 'BITQ'],
    }
    for theme, tks in theme_clusters.items():
        # 거래량 spike 또는 수익률 Top에 동시 등장
        spike_match = sum(1 for tk in tks if tk in spike_tks)
        return_match = sum(1 for tk in tks if tk in [s['ticker'] for s in signals['top_returns']])
        if spike_match >= 2:
            insights.append(f'**{theme}** ETF {spike_match}개 거래량 동시 폭증')
        if return_match >= 2:
            insights.append(f'**{theme}** ETF {return_match}개 수익률 동시 상승')

    # 3. ARK 동시 신고가 (특별 케이스)
    new_high_tks = [h['ticker'] for h in signals['new_highs']]
    ark_highs = [tk for tk in new_high_tks if tk.startswith('ARK')]
    if len(ark_highs) >= 3:
        insights.append(f'**ARK 시리즈 {len(ark_highs)}개 동시 신고가** ({", ".join(ark_highs)}) → 성장주 강세')

    # 4. 레버리지/인버스 큰 movement
    big_movers = [s for s in signals['top_returns'] + signals['bottom_returns'] if s['category'] == 'income_lev' and abs(s['day_return']) > 5]
    if big_movers:
        for m in big_movers[:2]:
            insights.append(f'**{describe_etf(m["ticker"])}** {m["day_return"]:+.2f}% — 기초자산 큰 움직임')

    # 5. 채권 거래량 폭증 (안전자산 신호)
    bond_spikes = [s for s in signals['volume_spikes'] if s['category'] == 'bonds']
    if bond_spikes:
        insights.append(f'**채권 ETF {len(bond_spikes)}개 거래량 폭증** ({", ".join([s["ticker"] for s in bond_spikes])}) → 안전자산 자금 이동 가능성')

    # 6. commodity 약세 클러스터
    cs = signals['category_strength']
    if cs and cs[-1]['category'] == 'commodity_hedge' and cs[-1]['avg_return'] < -0.3:
        insights.append(f'**원자재/헷지 카테고리 약세** ({cs[-1]["avg_return"]:+.2f}%) → 리스크 온 분위기')

    # 7. 신저가 클러스터 (commodity)
    new_low_tks = [h['ticker'] for h in signals['new_lows']]
    commodity_lows = [tk for tk in new_low_tks if tk in ['DBC', 'USO', 'BNO', 'COMT', 'GSG', 'AMLP']]
    if len(commodity_lows) >= 2:
        insights.append(f'**원자재 ETF {len(commodity_lows)}개 30일 신저가** → 원자재 사이클 약세 지속')

    return insights


def save_content(content, date_str=None):
    """콘텐츠 파일 저장"""
    if date_str is None:
        date_str = datetime.now().strftime('%Y-%m-%d')
    out_dir = Path(__file__).parent / 'content'
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / f'pulse_{date_str}.md'
    out_file.write_text(content, encoding='utf-8')
    return out_file


if __name__ == '__main__':
    signals = get_signals()
    content = gen_narrative_kr(signals)
    print(content)
    out_file = save_content(content, signals['date'])
    print(f'\n저장: {out_file}')
