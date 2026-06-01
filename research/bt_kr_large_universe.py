# -*- coding: utf-8 -*-
"""KR 시스템 ranking history에 시총 필터 적용 — 신규 KR universe 정의 + BT
1. 2024-26 ranking 모두 추출 + 시총 join
2. picks 중 시총 5천억+ 비율 분석
3. 가상 BT: 기존 시스템 vs 시총 5천억+ filter 시스템
"""
import sys, json, glob, os
from pathlib import Path
import pandas as pd
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')

STATE = Path(r'C:\dev\state')
MC_DIR = Path(r'C:\dev\data_cache')
OHLCV = pd.read_parquet(r'C:\dev\data_cache\all_ohlcv_20170601_20260529.parquet')
OHLCV.index = pd.to_datetime(OHLCV.index)


def load_rankings(year_filter='2024|2025|2026'):
    """Year_filter 정규식에 맞는 ranking files 로드"""
    import re
    pattern = re.compile(f'ranking_({year_filter})')
    files = sorted(glob.glob(str(STATE / 'ranking_*.json')))
    files = [f for f in files if pattern.search(os.path.basename(f))]
    print(f'  ranking 파일 {len(files)}개')
    return files


def load_mc(date_str):
    """date_str = '20260529' 같은 형식"""
    fp = MC_DIR / f'market_cap_ALL_{date_str}.parquet'
    if not fp.exists(): return None
    df = pd.read_parquet(fp)
    mc_col = '시가총액' if '시가총액' in df.columns else df.columns[1]
    df['mc'] = pd.to_numeric(df[mc_col], errors='coerce')
    return df['mc']


def main():
    print('='*100)
    print('KR 시스템 picks 시총 분석 + 신규 universe BT')
    print('='*100)

    files = load_rankings()
    if len(files) < 10:
        print('파일 부족')
        return

    # 각 ranking의 top 10 + 시총 join
    print('\n--- 1. 시스템 picks 시총 분포 분석 ---')

    # 샘플 — 2024, 2025, 2026 분기별
    sample_dates = ['20240102', '20240401', '20240701', '20241002',
                    '20250102', '20250401', '20250702', '20251002',
                    '20260102', '20260401']
    summary = []
    for d_str in sample_dates:
        fp = STATE / f'ranking_{d_str}.json'
        if not fp.exists():
            # find closest
            day = pd.to_datetime(d_str, format='%Y%m%d')
            for offset in range(10):
                alt = day + pd.Timedelta(days=offset)
                alt_str = alt.strftime('%Y%m%d')
                alt_fp = STATE / f'ranking_{alt_str}.json'
                if alt_fp.exists():
                    fp = alt_fp; d_str = alt_str
                    break
            if not fp.exists(): continue

        with open(fp, encoding='utf-8') as f:
            d = json.load(f)
        picks = d.get('rankings', [])[:10]
        if not picks: continue

        mc = load_mc(d_str)
        if mc is None:
            # try nearby
            for offset in range(5):
                day = pd.to_datetime(d_str, format='%Y%m%d') - pd.Timedelta(days=offset)
                mc = load_mc(day.strftime('%Y%m%d'))
                if mc is not None: break
            if mc is None: continue

        pick_mc = []
        for r in picks:
            t = r['ticker']
            m = mc.get(t)
            if m and not pd.isna(m):
                pick_mc.append((t, r.get('rank', 0), m/1e8))  # 억 단위
        if not pick_mc: continue

        # 분류
        large = sum(1 for _, _, m in pick_mc if m >= 5000)  # 5천억+
        mid = sum(1 for _, _, m in pick_mc if 1000 <= m < 5000)
        small = sum(1 for _, _, m in pick_mc if m < 1000)
        summary.append({
            'date': d_str, 'total': len(pick_mc),
            'large_5000': large, 'mid_1000': mid, 'small': small,
            'large_pct': large/len(pick_mc)*100,
        })
        print(f'  {d_str}: top10 — 5천억+ {large}, 1천억-5천억 {mid}, <1천억 {small}  대형비율 {large/len(pick_mc)*100:.0f}%')

    if summary:
        avg_large_pct = np.mean([s['large_pct'] for s in summary])
        print(f'\n  평균 대형주(5천억+) 비율: {avg_large_pct:.0f}%')

    # 2. 시뮬: 시총 5천억+ filter 적용 시 시스템 picks 변화
    print('\n--- 2. Filter 적용 시뮬: 매일 top 10 중 5천억+ 만 선별 → top 3 매수 ---')
    print(f'{"date":<10}{"original":<25}{"filtered":<25}{"picks불충분?":<10}')

    # 9개월 sample
    sample_files = [f for f in files if pd.to_datetime(os.path.basename(f).replace('ranking_','').replace('.json',''), format='%Y%m%d') >= pd.Timestamp('2025-09-01')]
    sample_files = sample_files[::20]  # 격주
    for fp in sample_files[:15]:
        d_str = os.path.basename(fp).replace('ranking_','').replace('.json','')
        with open(fp, encoding='utf-8') as f:
            d = json.load(f)
        picks = d.get('rankings', [])[:20]
        mc = load_mc(d_str)
        if mc is None: continue
        orig_top3 = [(r['ticker'], r.get('rank',0)) for r in picks[:3]]
        # 시총 5천억+ filter
        filtered = []
        for r in picks:
            m = mc.get(r['ticker'])
            if m and not pd.isna(m) and m >= 5e11:
                filtered.append((r['ticker'], r.get('rank',0)))
            if len(filtered) >= 3: break
        orig_str = ', '.join(f'{t}({r})' for t,r in orig_top3)
        filt_str = ', '.join(f'{t}({r})' for t,r in filtered) if filtered else '(없음)'
        insuff = '⚠️ <3' if len(filtered) < 3 else ''
        print(f'  {d_str[:10]:<10}{orig_str[:23]:<25}{filt_str[:23]:<25}{insuff}')

    # 3. 가상 BT: 매월 첫 영업일 = 시총 5천억+ + 시스템 rank 우선 매수
    print('\n--- 3. 가상 BT: 시총 5천억+ 필터, 월 1회 리밸런싱, 3종목 균등 ---')
    bt_files = sorted(files)
    rebal_dates = []
    last_month = -1
    for fp in bt_files:
        d_str = os.path.basename(fp).replace('ranking_','').replace('.json','')
        d = pd.to_datetime(d_str, format='%Y%m%d')
        if d.month != last_month:
            rebal_dates.append((d, fp))
            last_month = d.month

    print(f'  리밸런싱 횟수: {len(rebal_dates)}회')
    if len(rebal_dates) < 5:
        print('  표본 부족, BT 생략')
        return

    INIT = 100.0
    val_filtered = INIT  # 5천억+ filter
    val_original = INIT  # 원본 시스템 top3
    val_filtered_top3 = INIT  # 5천억+ top3

    prev_picks_filt = []
    prev_picks_orig = []
    prev_prices_filt = {}
    prev_prices_orig = {}

    history_filt = []
    history_orig = []

    for i, (d, fp) in enumerate(rebal_dates):
        d_str = d.strftime('%Y%m%d')
        with open(fp, encoding='utf-8') as f:
            data = json.load(f)
        picks_all = data.get('rankings', [])[:30]
        mc = load_mc(d_str)
        if mc is None:
            continue

        # 매월 새 picks
        # filtered: 시총 5천억+
        filt = []
        for r in picks_all:
            m = mc.get(r['ticker'])
            if m and not pd.isna(m) and m >= 5e11:
                filt.append(r['ticker'])
            if len(filt) >= 3: break
        # original: rank 그대로 top 3
        orig = [r['ticker'] for r in picks_all[:3]]

        # 매도 가격 (오늘 종가) — 어제 보유 종목 가치 평가
        if prev_picks_filt:
            ret_f = 0
            for tk in prev_picks_filt:
                if tk in OHLCV.columns:
                    try:
                        p_now = OHLCV[tk].asof(d)
                        p_prev = prev_prices_filt.get(tk)
                        if p_now and p_prev and p_prev > 0:
                            ret_f += (p_now/p_prev - 1) / len(prev_picks_filt)
                    except: pass
            val_filtered *= (1 + ret_f)
        if prev_picks_orig:
            ret_o = 0
            for tk in prev_picks_orig:
                if tk in OHLCV.columns:
                    try:
                        p_now = OHLCV[tk].asof(d)
                        p_prev = prev_prices_orig.get(tk)
                        if p_now and p_prev and p_prev > 0:
                            ret_o += (p_now/p_prev - 1) / len(prev_picks_orig)
                    except: pass
            val_original *= (1 + ret_o)

        # 새 picks의 진입 가격
        new_prices_f = {}
        for tk in filt:
            if tk in OHLCV.columns:
                try:
                    p = OHLCV[tk].asof(d)
                    if p and not pd.isna(p): new_prices_f[tk] = p
                except: pass
        new_prices_o = {}
        for tk in orig:
            if tk in OHLCV.columns:
                try:
                    p = OHLCV[tk].asof(d)
                    if p and not pd.isna(p): new_prices_o[tk] = p
                except: pass

        prev_picks_filt = list(new_prices_f.keys())
        prev_picks_orig = list(new_prices_o.keys())
        prev_prices_filt = new_prices_f
        prev_prices_orig = new_prices_o
        history_filt.append((d, val_filtered))
        history_orig.append((d, val_original))

    print(f'\n  📊 BT 결과 ({rebal_dates[0][0].date()} ~ {rebal_dates[-1][0].date()})')
    print(f'  {"시스템":<28}{"최종":>10}{"누적":>10}')
    print(f'  {"기존 (top3 시총무관)":<28}{val_original:>10.2f}{(val_original-INIT):>+10.1f}%')
    print(f'  {"신규 (top3 시총5천억+)":<28}{val_filtered:>10.2f}{(val_filtered-INIT):>+10.1f}%')

    # ETF 비교
    etf = pd.read_parquet(r'C:\dev\claude code\eps-momentum-us\research\kr_etf_8y.parquet')
    etf.index = pd.to_datetime(etf.index)
    start_bt = rebal_dates[0][0]
    end_bt = rebal_dates[-1][0]
    if '102110.KS' in etf.columns:
        tiger = etf['102110.KS'].loc[start_bt:end_bt].dropna()
        if len(tiger) > 1:
            tiger_ret = (tiger.iloc[-1]/tiger.iloc[0] - 1) * 100
            print(f'  {"TIGER 200 (대형주 buy-hold)":<28}{tiger.iloc[-1]/tiger.iloc[0]*100:>10.2f}{tiger_ret:>+10.1f}%')

    # 연환산
    n_years = (rebal_dates[-1][0] - rebal_dates[0][0]).days / 365.25
    print(f'\n  기간: {n_years:.1f}년')
    print(f'  기존 CAGR: {((val_original/INIT)**(1/n_years) - 1)*100:+.2f}%')
    print(f'  신규 CAGR: {((val_filtered/INIT)**(1/n_years) - 1)*100:+.2f}%')


if __name__ == '__main__':
    main()
