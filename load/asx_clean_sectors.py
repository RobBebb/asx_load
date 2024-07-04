from dotenv import load_dotenv

import pandas as pd

from securities_load.load.postgresql_database_functions import connect
from asx_load.load.asx_functions import get_gics_sector_code
from asx_load.load.asx_functions import get_gics_industry_group_code
from asx_load.load.asx_functions import get_gics_industry_code
from asx_load.load.asx_functions import get_gics_sub_industry_code

def read_company_gics_codes():
    # Read in the Asx Sectors Excel file as a dictionary where each key is a sheet and each value is a dataframe of the sheet data
    companies = pd.read_excel('data/ASX Sectors with Companies.xlsx', sheet_name = 'Sectors')
    return companies

load_dotenv()

# Open a connection
conn = connect()

sectors = []
industry_groups = []
industries = []
sub_industries = []
companies = read_company_gics_codes()
print(f'companies length is {len(companies)}')

companies['ticker'] = companies['code'].str.replace('ASX:','')
companies['yahoo_ticker'] = companies['ticker'] + '.AX'
companies['url'] = 'https://www.listcorp.com/' + companies['market'] + '/' + companies['ticker']


for row in companies.itertuples(name='none'):
    sector_name = row.sector
    industry_group_name = row.industry_group
    industry_name = row.industry
    sub_industry_name = row.sub_industry
    sector_code = get_gics_sector_code(conn, sector_name)
    sectors.append(sector_code)
    industry_group_code = get_gics_industry_group_code(conn, industry_group_name)
    industry_groups.append(industry_group_code)
    industry_code = get_gics_industry_code(conn, industry_name)
    industries.append(industry_code)
    sub_industry_code = get_gics_sub_industry_code(conn, sub_industry_name)
    sub_industries.append(sub_industry_code)

print(f'sectors length is {len(sectors)}')
print(f'industry_groups length is {len(industry_groups)}')
print(f'industries length is {len(industries)}')
print(f'sub_industries length is {len(sub_industries)}')

companies['sector_code'] = sectors
companies['industry_group_code'] = industry_groups
companies['industry_code'] = industries
companies['sub_industry_code'] = sub_industries

print(companies.info())
print(companies.head())
print(companies.iloc[0])
# Close the connection
conn.close()