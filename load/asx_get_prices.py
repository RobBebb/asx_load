from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter

from dotenv import load_dotenv
import logging

import pandas as pd

from securities_load.load.postgresql_database_functions import connect

import yfinance as yf

from asx_load.load.asx_functions import (get_tickers, get_ticker_id_using_yahoo_ticker, 
    get_ticker_using_id, add_or_update_daily_prices)

logger = logging.getLogger(__name__)
logging.basicConfig(filename='asx_load.log',
                    filemode='w',
                    encoding='utf-8',
                    level=logging.DEBUG,
                    format='{asctime} - {name}.{funcName} - {levelname} - {message}',
                    style='{')

load_dotenv()

logger.info('Start')

# disable chained assignments
pd.options.mode.chained_assignment = None 
logger.info('Chained assignments disabled')

# Open a connection
conn = connect()


class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

session = CachedLimiterSession(
    limiter=Limiter(RequestRate(2, Duration.SECOND*6)),  # max 2 requests per 6 seconds
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)

# tickers = get_tickers(conn, 'ASX')
# print(len(tickers))
tickers = [
'CCE.AX',
'DEL.AX',
'KPO.AX',
'RNE.AX',
'TML.AX',
'SGP.AX',
'GPT.AX',
'MGR.AX',
'CHC.AX',
'CLW.AX',
'GOZ.AX',
'CNI.AX',
'APZ.AX',
'REP.AX',
'WOT.AX',
'TOP.AX',
'TOT.AX',
'APW.AX',
'MXU.AX',
'GMG.AX',
'CIP.AX',
'DXI.AX',
'GDF.AX',
'HPI.AX',
'DXS.AX',
'CMW.AX',
'ABG.AX',
'COF.AX',
'GDI.AX',
'AOF.AX',
'ECF.AX',
'HCW.AX',
'INA.AX',
'URF.AX',
'SCG.AX',
'VCX.AX',
'HMC.AX',
'RGN.AX',
'BWP.AX',
'HDN.AX',
'CQR.AX',
'WPR.AX',
'URW.AX',
'DXC.AX',
'CDP.AX',
'ARF.AX',
'CQE.AX',
'RFF.AX',
'NSR.AX',
'ASK.AX',
'LLC.AX',
'UOS.AX',
'HGL.AX',
'RPG.AX',
'SRV.AX',
'EGH.AX',
'SSL.AX',
'DGH.AX',
'LIC.AX',
'WTN.AX',
'PPC.AX',
'CWP.AX',
'FRI.AX',
'CAQ.AX',
'MPX.AX',
'AXI.AX',
'TIA.AX',
'LHM.AX',
'IEQ.AX',
'PXA.AX',
'MEA.AX',
'ACU.AX',
'AU1.AX',
'^AORD',
'^ATOI',
'^AXNV',
'^AXLD',
'^AXPJ',
'^AXBK',
'^AXBN',
'^AXTJ',
'^AXDJ',
'^AXSJ',
'^AXNI',
'^AXRI',
'^AXET',
'^AXEJ',
'^AXEW',
'^AXJM',
'^AXJS',
'^AXFJ',
'^AXXJ',
'^AXFR',
'^AXFE',
'^AXFN',
'^AXIN',
'^AXJR',
'^AXUJ',
'^AXVI',
'^AXKO',
'^AXMM',
'^AXSY',
'^AXAG',
'^AXAT',
'^AXAF',
'^AXGD',
'^AXTX',
'^AXDI',
'^AXEC',
'^AXMD',
'^AXSO',
'^AXAE']

for ticker in tickers:
    if ticker in ['CCE.AX','NVQ.AX']:
        continue
    print(f"Ticker is {ticker}")
    yf_ticker = yf.Ticker(ticker, session=session)
    hist = yf_ticker.history(period="1y", repair=True)
    if not hist.empty:
        # print(hist[hist['Dividends']!=0])
        # print(hist[hist['Stock Splits']!=0])
        # print(hist[hist['Repaired?']])
        db_ticker_id = get_ticker_id_using_yahoo_ticker(conn, ticker)
        # print(f"db_ticker_id is {db_ticker_id}")
        hist['ticker_id'] = db_ticker_id
        db_ticker = get_ticker_using_id(conn, db_ticker_id)
        hist['data_vendor_id'] = 1
        hist['ticker'] = db_ticker[0]
        hist['exchange_code'] = db_ticker[1]
        hist = hist.reset_index()
        hist = hist.dropna()
        hist.drop(columns=['Dividends', 'Stock Splits', 'Repaired?'],inplace=True)
        hist.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'ticker_id', 'data_vendor_id', 'ticker', 'exchange_code']
        hist = hist[['data_vendor_id', 'ticker_id', 'exchange_code', 'ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]
        # print(hist.info())
        # print(hist.head(100))
        # print(len(hist))
        add_or_update_daily_prices(conn, hist)

# Close the connection
conn.close()