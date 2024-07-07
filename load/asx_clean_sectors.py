from dotenv import load_dotenv
import logging

import pandas as pd

from securities_load.load.postgresql_database_functions import connect
from asx_load.load.asx_functions import get_gics_sector_code
from asx_load.load.asx_functions import get_gics_industry_group_code
from asx_load.load.asx_functions import get_gics_industry_code
from asx_load.load.asx_functions import get_gics_sub_industry_code

logger = logging.getLogger(__name__)
logging.basicConfig(filename='asx_load.log',
                    filemode='w',
                    encoding='utf-8',
                    level=logging.DEBUG,
                    format='{asctime} - {name}.{funcName} - {levelname} - {message}',
                    style='{')

load_dotenv()

logger.info('Start')
def read_company_gics_codes() -> pd.DataFrame:
    # Read in the Asx Sectors Excel file as a dictionary where each key is a sheet and each value is a dataframe of the sheet data
    companies = pd.read_excel('data/ASX Sectors with Companies.xlsx', sheet_name = 'Sectors')
    logger.debug('File read')
    return companies

def clean_companies(companies: pd.DataFrame) -> pd.DataFrame:
    #  Check how many rows are missing a ticker code and the delete then  
    missing_rows_count = companies['code'].isna().sum()
    logger.debug(f'There are {missing_rows_count} rows without a ticker code. They will be deleted')
    clean_companies = companies.dropna(subset=['code'])
    return clean_companies

def transform_companies(conn, companies: pd.DataFrame) -> pd.DataFrame:
    gics_sectors = []
    gics_industry_groups = []
    gics_industries = []
    gics_sub_industries = []

    logger.debug(f'Clean companies length is {len(clean_companies)}')

    companies['ticker'] = companies['code'].str.replace('ASX:','')
    companies['yahoo_ticker'] = companies['ticker'] + '.AX'
    companies['listcorp_url'] = 'https://www.listcorp.com/' + companies['market'] + '/' + companies['ticker']

    for row in companies.itertuples(name='none'):
        gics_sector_name = row.sector
        gics_industry_group_name = row.industry_group
        gics_industry_name = row.industry
        gics_sub_industry_name = row.sub_industry
        gics_sector_code = get_gics_sector_code(conn, gics_sector_name)
        gics_sectors.append(gics_sector_code)
        gics_industry_group_code = get_gics_industry_group_code(conn, gics_industry_group_name)
        gics_industry_groups.append(gics_industry_group_code)
        gics_industry_code = get_gics_industry_code(conn, gics_industry_name)
        gics_industries.append(gics_industry_code)
        gics_sub_industry_code = get_gics_sub_industry_code(conn, gics_sub_industry_name)
        gics_sub_industries.append(gics_sub_industry_code)

    companies['sector_code'] = gics_sectors
    companies['industry_group_code'] = gics_industry_groups
    companies['industry_code'] = gics_industries
    companies['sub_industry_code'] = gics_sub_industries

    return companies

companies = read_company_gics_codes()

cleaned_companies = clean_companies(companies)

# Open a connection
conn = connect()

transformed_companies = transform_companies(conn, cleaned_companies)

# Close the connection
conn.close()