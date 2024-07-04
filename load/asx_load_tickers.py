from datetime import datetime, timezone
import pandas as pd

from securities_load.load.postgresql_database_functions import connect
from asx_load.load.asx_functions import read_and_parse_asx_companies, add_tickers


def read_and_parse_asx_companies():
    """
    The ASX listed companies must be manually downloaded before this function runs and placed in 
    /home/ubuntuuser/karra/asx_load/data with a name of asx_listed_companies.csv
    
    Returns a list of tuples to add to the database.
    """
    
    # Stores the current time, for the created_at field
    now = datetime.now(timezone.utc)
    print(f"Now {now}")
    ticker_list = pd.read_csv("data/ASX_Listed_Companies.csv")
    print(ticker_list.head())
    columns = ['ticker', 'name', 'gics_industry_group', 'listed_utc',
        'market_cap']
    ticker_list.columns = columns
    ticker_list.drop(columns=["market_cap"])
    ticker_list["currency_code"] = "AUD"
    ticker_list["country_alpha_3"] = "aus"
    ticker_list["market_type"] = "stocks"
    ticker_list["asset_class_type"] = "stocks"
         
    return ticker_list


def load_tickers():
    """
    Get the tickers and load them into the equity ticker table"""
    print('xxx')
    # load_dotenv()
    print('xxx')
    # Open a connection
    conn = connect()
    ticker_list = read_and_parse_asx_companies()
    print(ticker_list.head())
    # Write the data to the local polygon database
    if ticker_list is not None:
        add_tickers(conn, ticker_list)

    # Close the connection
    conn.close()
    