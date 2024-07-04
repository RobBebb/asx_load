import pandas as pd

asx_sectors = pd.read_excel('data/ASX Sectors with Companies.xlsx', sheet_name = 'Sectors')
print(type(asx_sectors))
print(asx_sectors.keys())
print(asx_sectors.head())
print(asx_sectors.info())
# sectors = asx_sectors['Sectors']
# print(type(sectors))
# print(sectors.describe())
industry = asx_sectors['industry'].unique()
print(industry)