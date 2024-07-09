from dotenv import load_dotenv
import logging

import pandas as pd

from securities_load.load.postgresql_database_functions import connect
from asx_load.load.asx_functions import get_gics_sector_code
from asx_load.load.asx_functions import get_gics_industry_group_code
from asx_load.load.asx_functions import get_gics_industry_code
from asx_load.load.asx_functions import get_gics_sub_industry_code
from asx_load.load.asx_functions import add_tickers
from asx_load.load.asx_functions import add_or_update_tickers

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

def read_company_gics_codes() -> pd.DataFrame:
    logger.debug('Started')
    # Read in the Asx Sectors Excel file as a dictionary where each key is a sheet and each value is a dataframe of the sheet data
    companies = pd.read_excel('data/ASX Sectors with Companies.xlsx', sheet_name = 'Sectors')
    logger.debug('File read')
    return companies

def clean_companies(companies: pd.DataFrame) -> pd.DataFrame:
    logger.debug('Started')
    #  Check how many rows are missing a ticker code and the delete then  
    missing_rows_count = companies['code'].isna().sum()
    logger.debug(f'There are {missing_rows_count} rows without a ticker code. They will be deleted')
    clean_companies = companies.dropna(subset=['code'])
    return clean_companies

def transform_companies(conn, companies: pd.DataFrame) -> pd.DataFrame:
    logger.debug('Started')
    gics_sectors = []
    gics_industry_groups = []
    gics_industries = []
    gics_sub_industries = []

    companies['ticker'] = companies['code'].str.replace('ASX:','')
    companies['yahoo_ticker'] = companies['ticker'] + '.AX'
    companies['listcorp_url'] = 'https://www.listcorp.com/' + companies['market'] + '/' + companies['ticker']

    for row in companies.itertuples(name='none'):
        gics_sector_name = row.gics_sector_name
        gics_industry_group_name = row.gics_industry_group_name
        gics_industry_name = row.gics_industry_name
        gics_sub_industry_name = row.gics_sub_industry_name
        gics_sector_code = get_gics_sector_code(conn, gics_sector_name)
        gics_sectors.append(gics_sector_code)
        gics_industry_group_code = get_gics_industry_group_code(conn, gics_industry_group_name)
        gics_industry_groups.append(gics_industry_group_code)
        gics_industry_code = get_gics_industry_code(conn, gics_industry_name)
        gics_industries.append(gics_industry_code)
        gics_sub_industry_code = get_gics_sub_industry_code(conn, gics_sub_industry_name)
        gics_sub_industries.append(gics_sub_industry_code)

    companies['gics_sector_code'] = gics_sectors
    companies['gics_industry_group_code'] = gics_industry_groups
    companies['gics_industry_code'] = gics_industries
    companies['gics_sub_industry_code'] = gics_sub_industries

    return companies

def get_sectors(conn, companies: pd.DataFrame) -> pd.DataFrame:
    logger.debug('Started')
    
    sectors = companies[['gics_sector_name', 'sector_ticker', 'sector_ticker_yahoo']].drop_duplicates()
    
    gics_sector_codes = []
        
    for row in sectors.itertuples():
        gics_sector_name = row.gics_sector_name
        gics_sector_code = get_gics_sector_code(conn, gics_sector_name)
        gics_sector_codes.append(gics_sector_code)
    
    sectors['sector_code'] = gics_sector_codes
    
    return sectors

def load_companies(conn, companies: pd.DataFrame):
    logger.debug('Started')
    tickers = companies[['ticker', 'yahoo_ticker', 'name', 'gics_sector_code', 'gics_industry_group_code', 'gics_industry_code', 'gics_sub_industry_code', 'listcorp_url']]
    tickers['exchange_code'] = 'ASX'
    tickers['market_type'] = 'stocks'
    tickers['asset_class_type'] = 'stocks'
    
    add_or_update_tickers(conn, tickers)
    
    return
    

companies = read_company_gics_codes()

cleaned_companies = clean_companies(companies)

logger.debug(f'Clean companies length is {len(cleaned_companies)}')

# Open a connection
conn = connect()

sectors = get_sectors(conn, cleaned_companies)
# print(f'sector_indices are {sectors}')
# print(f'sector_tickers are {sectors.sector_ticker.to_list()}')

transformed_companies = transform_companies(conn, cleaned_companies)
print(transformed_companies.head())
print(transformed_companies.info())
print(transformed_companies.iloc[0])

load_companies(conn, transformed_companies)

# Close the connection
conn.close()