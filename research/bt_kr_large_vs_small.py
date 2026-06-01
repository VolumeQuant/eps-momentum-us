# -*- coding: utf-8 -*-
"""KR 대형주 vs 소형주 다년 비교 + 본인 KR 시스템 가설 검증
- TIGER 200 (대형주) vs KODEX 코스닥150 (소형주) 8년
- 반도체 sub-sector (삼전+하닉 vs 코스닥 반도체)
- 본인 KR 시스템 ranking history로 대형 vs 소형 alpha 분석
"""
import sys, json, glob
from pathlib import Path
import pandas as pd
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')

DATA = Path(__file__).parent / 'kr_etf_8y.parquet'
KR_OHLCV = Path(r'C:\dev\data_cache\all_ohlcv_20170601_20260529.parquet')
KR_STATE = Path(r'C:\dev\state')


def bh(s, start=None, end=None):
    if start: s = s.loc[start:]
    if end: s = s.loc[:end]
    s = s.dropna()
    if len(s) < 2: return None
    INIT = 100.0
    final = INIT * (s.iloc[-1]/s.iloc[0])
    total = (final/INIT - 1) * 100
    n = len(s)
    cagr = ((final/INIT)**(252/n) - 1) * 100
    rets = s.pct_change().fillna(0)
    sigma = rets.std() * np.sqrt(252)
    sharpe = (rets.mean()*252)/sigma if sigma>0 else 0
    peak = s.cummax(); dd = (s-peak)/peak; mdd = dd.min()*100
    cal = cagr/abs(mdd) if mdd<0 else 0
    return {'total': total, 'cagr': cagr, 'mdd': mdd, 'sharpe': sharpe, 'cal': cal}


def main():
    print('=' * 100)
    print('PART 1 — KR 대형주 vs 소형주 ETF 8년 비교')
    print('=' * 100)
    df = pd.read_parquet(DATA)
    df.index = pd.to_datetime(df.index)
    print(f'data: {df.shape}, {df.index[0].date()}~{df.index[-1].date()}\n')

    print(f'{"종목":<22}{"누적":>9}{"CAGR":>8}{"MDD":>9}{"Sharpe":>9}{"Calmar":>9}')
    print('-' * 75)
    names = {
        '^KS11': 'KOSPI',
        '^KQ11': 'KOSDAQ',
        '102110.KS': 'TIGER 200 (대형)',
        '229200.KS': 'KODEX 코스닥150',
        '278530.KS': 'KODEX 200 IT',
        '091160.KS': 'KODEX 반도체',
        '005930.KS': '삼성전자',
        '000660.KS': 'SK하이닉스',
        '035720.KS': '카카오',
        '035420.KS': 'NAVER',
        '273130.KS': 'KODEX 종합채권',
    }
    for t in names:
        if t not in df.columns: continue
        r = bh(df[t])
        if r:
            print(f'  {names[t]:<22}{r["total"]:>+8.1f}%{r["cagr"]:>+7.2f}%{r["mdd"]:>+8.2f}%{r["sharpe"]:>8.2f}{r["cal"]:>8.2f}')

    # 기간 분리
    print('\n--- 기간 분리 (대형 vs 소형) ---')
    periods = [
        ('2019', '2019-01-01', '2019-12-31'),
        ('2020 COVID', '2020-01-01', '2020-12-31'),
        ('2021 강세', '2021-01-01', '2021-12-31'),
        ('2022 약세', '2022-01-01', '2022-12-31'),
        ('2023 회복', '2023-01-01', '2023-12-31'),
        ('2024 AI', '2024-01-01', '2024-12-31'),
        ('2025-26 (YTD)', '2025-01-01', '2026-05-29'),
    ]
    print(f'{"기간":<15}{"KOSPI":>15}{"KOSDAQ":>15}{"TIGER200":>15}{"코스닥150":>15}{"삼전":>12}{"하이닉스":>12}')
    for label, s, e in periods:
        row = f'  {label:<13}'
        for t in ['^KS11', '^KQ11', '102110.KS', '229200.KS', '005930.KS', '000660.KS']:
            if t not in df.columns:
                row += f'{"N/A":>12}'; continue
            r = bh(df[t], s, e)
            if r is None:
                row += f'{"N/A":>12}'
            else:
                row += f'{r["total"]:>+10.1f}%'
        print(row)

    print('\n' + '=' * 100)
    print('PART 2 — 코스피 vs 코스닥 사이클 (12개월 rolling 상대 성과)')
    print('=' * 100)
    kospi = df['^KS11'].dropna()
    kosdaq = df['^KQ11'].dropna()
    common_idx = kospi.index.intersection(kosdaq.index)
    kospi = kospi.loc[common_idx]; kosdaq = kosdaq.loc[common_idx]
    rel = (kospi.pct_change(252).rolling(20).mean() - kosdaq.pct_change(252).rolling(20).mean()) * 100
    rel = rel.dropna()
    # 코스피 > 코스닥 vs 반대
    print(f'  코스피 outperform 일수: {(rel > 0).sum()} ({(rel>0).sum()/len(rel)*100:.0f}%)')
    print(f'  코스닥 outperform 일수: {(rel <= 0).sum()} ({(rel<=0).sum()/len(rel)*100:.0f}%)')
    # 최근 1년 트렌드
    recent = rel.iloc[-252:]
    print(f'  최근 1년 코스피 outperform: {(recent>0).sum()}/{len(recent)}일 ({(recent>0).sum()/len(recent)*100:.0f}%)')
    print(f'  최근 1년 평균 spread: {recent.mean():+.2f}%p (양수 = 코스피 우위)')

    # 본인 KR 시스템 ranking history 분석
    print('\n' + '=' * 100)
    print('PART 3 — 본인 KR 시스템 picks의 대형/소형 비중 분석')
    print('=' * 100)
    rank_files = sorted(glob.glob(str(KR_STATE / 'ranking_2026*.json')))
    print(f'2026년 ranking 파일 수: {len(rank_files)}')

    # 시총 정보 받기 어려우니, ticker code로 대형/소형 추정
    # 한국: 종목코드 6자리. KOSPI = 5-6자리 시작, 일부 분류 가능
    # 단순화: 상위 50% 시총 대형, 하위 50% 소형 — 시총 없으니 KOSPI 200 list로 추정

    # 대형주 = 일반적인 KOSPI 200 종목 코드 (단순화: 자주 등장하는 분류)
    # 임시: 종목 code 끝 5자리 기준 매핑 안 됨 — 그냥 picks 종목 추출

    all_picks = {}  # ticker -> count
    for fp in rank_files:
        try:
            with open(fp, encoding='utf-8') as f:
                d = json.load(f)
            for r in d.get('rankings', [])[:10]:  # top 10만
                t = r['ticker']
                all_picks[t] = all_picks.get(t, 0) + 1
        except: pass
    top_picks = sorted(all_picks.items(), key=lambda x: -x[1])[:30]
    print(f'  Top 30 가장 자주 등장한 picks (2026년):')
    for t, c in top_picks:
        print(f'    {t}: {c}회')

    print('\n' + '=' * 100)
    print('PART 4 — 본인 OHLCV 데이터로 2024-26 대형/소형 비교')
    print('=' * 100)
    kr_px = pd.read_parquet(KR_OHLCV)
    kr_px.index = pd.to_datetime(kr_px.index)
    # 대형주: 삼전(005930), 하이닉스(000660), 현대차(005380), 셀트리온(068270), POSCO(005490)
    # 소형주 sample: top picks
    large = ['005930', '000660', '005380', '068270', '005490', '035420', '051910']  # 삼전, 하이닉스, 현대차, 셀트리온, POSCO, NAVER, LG화학
    small_sample = [t for t, _ in top_picks[:7] if t in kr_px.columns]
    # 2024-26 비교
    start, end = '2024-01-01', '2026-05-29'
    print(f'  기간: {start} ~ {end}')
    print(f'  대형주 (sample 7):')
    for t in large:
        if t not in kr_px.columns: continue
        r = bh(kr_px[t], start, end)
        if r: print(f'    {t}: {r["total"]:+7.1f}% MDD{r["mdd"]:+6.1f}%')
    print(f'  소형주 (시스템 자주 등장 picks):')
    for t in small_sample:
        if t not in kr_px.columns: continue
        r = bh(kr_px[t], start, end)
        if r: print(f'    {t}: {r["total"]:+7.1f}% MDD{r["mdd"]:+6.1f}%')

    print('\n' + '=' * 100)
    print('★ 결론')
    print('=' * 100)


if __name__ == '__main__':
    main()
