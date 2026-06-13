import yfinance as yf, json, time, warnings, pandas as pd
warnings.filterwarnings('ignore')
from concurrent.futures import ThreadPoolExecutor
u = json.load(open('research/_tmp_universe.json'))
px = pd.read_parquet('research/_price_hist_cache.parquet')
# only tickers present in price cache; sample 50
cand = [t for t in u if t in px.columns][:50]
print('fetching', len(cand), 'tickers, threads=2')

def fetch(tk):
    try:
        t = yf.Ticker(tk)
        out = {'ticker': tk}
        try:
            ud = t.upgrades_downgrades
            if ud is not None and len(ud):
                ud = ud.reset_index()
                ud['GradeDate'] = pd.to_datetime(ud['GradeDate'])
                out['ud'] = ud[['GradeDate','priceTargetAction','currentPriceTarget','priorPriceTarget','Action','ToGrade','FromGrade']].to_dict('records')
        except Exception as e:
            out['ud_err'] = type(e).__name__
        try:
            info = t.info or {}
            out['info'] = {k: info.get(k) for k in ['targetMeanPrice','recommendationMean','numberOfAnalystOpinions','currentPrice','shortPercentOfFloat','shortRatio','heldPercentInstitutions','earningsGrowth','revenueGrowth']}
        except Exception as e:
            out['info_err'] = type(e).__name__
        try:
            ip = t.insider_purchases
            if ip is not None and len(ip):
                # row '% Net Shares Purchased (Sold)'
                d = dict(zip(ip.iloc[:,0], ip.iloc[:,1]))
                out['insider_net_pct'] = d.get('% Net Shares Purchased (Sold)')
        except Exception:
            pass
        return out
    except Exception as e:
        return {'ticker': tk, 'fatal': type(e).__name__}

res = []
with ThreadPoolExecutor(max_workers=2) as ex:
    for r in ex.map(fetch, cand):
        res.append(r)
        marks = []
        if 'ud' in r: marks.append(f"ud{len(r['ud'])}")
        if r.get('info',{}).get('targetMeanPrice'): marks.append('tgt')
        if r.get('insider_net_pct') is not None: marks.append('ins')
        print(r['ticker'], ' '.join(marks) or r.get('fatal') or r.get('ud_err',''))

# convert datetimes to str
for r in res:
    for rec in r.get('ud',[]):
        rec['GradeDate'] = str(rec['GradeDate'])
json.dump(res, open('research/_tmp_ud_data.json','w'), default=str)
print('SAVED', len(res))
