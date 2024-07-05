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
                    format='{asctime} - {name} - {levelname} - {message}',
                    style='{')

load_dotenv()

logger.info('Start')
def read_company_gics_codes():
    # Read in the Asx Sectors Excel file as a dictionary where each key is a sheet and each value is a dataframe of the sheet data
    companies = pd.read_excel('data/ASX Sectors with Companies.xlsx', sheet_name = 'Sectors')
    return companies

# Open a connection
conn = connect()

gics_sectors = []
gics_industry_groups = []
gics_industries = []
gics_sub_industries = []
companies = read_company_gics_codes()
print(f'companies length is {len(companies)}')

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

print(f'sectors length is {len(gics_sectors)}')
print(f'industry_groups length is {len(gics_industry_groups)}')
print(f'industries length is {len(gics_industries)}')
print(f'sub_industries length is {len(gics_sub_industries)}')

companies['sector_code'] = gics_sectors
companies['industry_group_code'] = gics_industry_groups
companies['industry_code'] = gics_industries
companies['sub_industry_code'] = gics_sub_industries

print(companies.info())
print(companies.head())
print(companies.iloc[0])
# Close the connection
conn.close()