"""
Date: 4/05/2024
Author: Rob Bebbington

Get ASX sysmbols from Market Index and insert them into our database.
"""
import logging
from datetime import datetime, timezone
import string
import pandas as pd
import psycopg2
import psycopg2.extras
from securities_load.load.postgresql_database_functions import connect

module_logger = logging.getLogger(__name__)

def add_tickers(conn, ticker_list: str) -> None:
    """
    Adds a tickers to the ticker table
    Parameters:
        conn - database connection
        ticker_list - dataframe of tickers
    """
    module_logger.debug('Started')
    
    table = "asx.ticker"

    # create a list of columns from the dataframe
    table_columns = list(ticker_list.columns)
    columns = ",".join(table_columns)
    # create VALUES('%s', '%s',...) one '%s' per column
    values = f"VALUES({','.join(['%s' for _ in ticker_list])})"
    # create INSERT INTO table (columns) VALUES('%s',...)
    insert_stmt = f"INSERT INTO {table} ({columns}) {values}"
    try:
        cur = conn.cursor()
        # add the rows from the dataframe to the table
        psycopg2.extras.execute_batch(cur, insert_stmt, ticker_list.values)
        conn.commit()
        module_logger.info(f"{ticker_list.shape[0]} rows added to {table} table.")
    except psycopg2.Error as error:
        module_logger.error("Error while inserting tickers to PostgreSQL", error)
    finally:
        if conn:
            cur.close()

def add_or_update_tickers(conn, ticker_list: pd.DataFrame) -> None:
    """
    Adds a tickers to the ticker table
    Parameters:
        conn - database connection
        ticker_list - dataframe of tickers
    """
    module_logger.debug('Started')
    
    table = "asx.ticker"

    # create a list of columns from the dataframe
    table_columns = list(ticker_list.columns)
    columns = ",".join(table_columns)
    # create VALUES('%s', '%s',...) one '%s' per column
    values = ', '.join(['%s' for _ in table_columns])
    # column names to use for update when there is a conflict
    conflict_columns = ", ".join(['EXCLUDED.' + column for column in table_columns])
    # create INSERT INTO table (columns) VALUES('%s',...)
    insert_stmt = f"INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT (exchange_code, ticker) DO UPDATE SET ({columns}, last_updated_date) = ({conflict_columns}, CURRENT_TIMESTAMP)"
    # print(insert_stmt)
    try:
        cur = conn.cursor()
        # add the rows from the dataframe to the table
        psycopg2.extras.execute_batch(cur, insert_stmt, ticker_list.values)
        conn.commit()
        module_logger.info(f"{ticker_list.shape[0]} rows added to {table} table.")
    except (Exception, psycopg2.Error) as error:
        module_logger.error("Error while inserting or updating tickers to PostgreSQL", error)
    finally:
        if conn:
            cur.close()

def add_or_update_watchlist_tickers(conn, watchlist_ticker_list: pd.DataFrame) -> None:
    """
    Adds watchlists to the watchlist_ticker table
    Parameters:
        conn - database connection
        watchlist_ticker_list - dataframe of watchlists
    """
    module_logger.debug('Started')
    
    table = "asx.watchlist_ticker"

    # create a list of columns from the dataframe
    table_columns = list(watchlist_ticker_list.columns)
    columns = ",".join(table_columns)
    # create VALUES('%s', '%s',...) one '%s' per column
    values = ', '.join(['%s' for _ in table_columns])
    # column names to use for update when there is a conflict
    conflict_columns = ", ".join(['EXCLUDED.' + column for column in table_columns])
    # create INSERT INTO table (columns) VALUES('%s',...)
    insert_stmt = f"INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT (watchlist_group_name, watchlist_name, ticker_id) DO UPDATE SET ({columns}, last_updated_date) = ({conflict_columns}, CURRENT_TIMESTAMP)"
    # print(insert_stmt)
    
    try:
        cur = conn.cursor()
        # add the rows from the dataframe to the table
        psycopg2.extras.execute_batch(cur, insert_stmt, watchlist_ticker_list.values)
        conn.commit()
        module_logger.info(f"{watchlist_ticker_list.shape[0]} rows added to {table} table.")
    except (Exception, psycopg2.Error) as error:
        module_logger.error("Error while inserting watchlist tickers to PostgreSQL", error)
    finally:
        if conn:
            cur.close()
            
def add_or_update_daily_prices(conn, daily_price_list: pd.DataFrame) -> None:
    """
    Adds a tickers to the ticker table
    Parameters:
        conn - database connection
        daily_price_list - dataframe of tickers
    """
    module_logger.debug('Started')
    
    table = "asx.daily_price"

    # create a list of columns from the dataframe
    table_columns = list(daily_price_list.columns)
    columns = ",".join(table_columns)
    # create VALUES('%s', '%s',...) one '%s' per column
    values = ', '.join(['%s' for _ in table_columns])
    # column names to use for update when there is a conflict
    conflict_columns = ", ".join(['EXCLUDED.' + column for column in table_columns])
    # create INSERT INTO table (columns) VALUES('%s',...)
    insert_stmt = f"INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT (data_vendor_id, exchange_code, ticker, date) DO UPDATE SET ({columns}, last_updated_date) = ({conflict_columns}, CURRENT_TIMESTAMP)"
    # print(insert_stmt)
    try:
        cur = conn.cursor()
        # add the rows from the dataframe to the table
        psycopg2.extras.execute_batch(cur, insert_stmt, daily_price_list.values)
        conn.commit()
        module_logger.info(f"{daily_price_list.shape[0]} rows added to {table} table.")
    except (Exception, psycopg2.Error) as error:
        module_logger.error("Error while inserting or updating daily_prices to PostgreSQL", error)
    finally:
        if conn:
            cur.close()

def get_ticker_using_id(conn, id: int) -> tuple:
    """
    Read the ticker table and return ticker
    Parameters:
        conn - database connection
        ticker_id - id of the instrument
    """
    module_logger.debug('Started')

    table = "asx.ticker"

    # create a list of columns from the dataframe
    table_columns = "ticker, exchange_code"

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE id = {id}"
    
    try:
        cur = conn.cursor()
        cur.execute(select_stmt)
        tickers = cur.fetchall()
    except (Exception, psycopg2.Error) as error:
        module_logger.error("Error while getting id from PostgreSQL", error)
    finally:
        if conn:
            cur.close()

    if tickers:        
        for row in tickers:
            ticker = row[0]
            exchange_code = row[1]
            module_logger.debug(f"ticker for id {id} is {ticker}.")
            return (ticker, exchange_code.strip())
    
    module_logger.debug(f'Ticker_id {id} cannot be found in {table}')
    return None


def get_ticker_id(conn, exchange_code: str, ticker: str) -> int:
    """
    Read the ticker table and return ticker_id
    Parameters:
        conn - database connection
        ticker - name of the instrument
    """
    module_logger.debug('Started')

    table = "asx.ticker"

    # create a list of columns from the dataframe
    table_columns = "id"

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE ticker = '{ticker}' AND exchange_code = '{exchange_code}'"
    
    try:
        cur = conn.cursor()
        cur.execute(select_stmt)
        tickers = cur.fetchall()
    except (Exception, psycopg2.Error) as error:
        module_logger.error("Error while getting ticker_id from PostgreSQL", error)
    finally:
        if conn:
            cur.close()

    if tickers:        
        for row in tickers:
            ticker_id = row[0]
            module_logger.debug(f"ticker_id for ticker {ticker} is {ticker_id}.")
            return ticker_id
    
    module_logger.debug(f'Ticker {ticker} cannot be found in {table}')
    return None

def get_ticker_id_using_yahoo_ticker(conn, yahoo_ticker: str) -> int:
    """
    Read the ticker table and return ticker_id
    Parameters:
        conn - database connection
        yahoo_ticker - name of the instrument
    """
    module_logger.debug('Started')

    table = "asx.ticker"

    # create a list of columns from the dataframe
    table_columns = "id"

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE yahoo_ticker = '{yahoo_ticker}'"
    
    try:
        cur = conn.cursor()
        cur.execute(select_stmt)
        tickers = cur.fetchall()
    except (Exception, psycopg2.Error) as error:
        module_logger.error("Error while getting ticker_id from PostgreSQL", error)
    finally:
        if conn:
            cur.close()

    if tickers:        
        for row in tickers:
            ticker_id = row[0]
            module_logger.debug(f"ticker_id for ticker {yahoo_ticker} is {ticker_id}.")
            return ticker_id
    
    module_logger.debug(f'Ticker {yahoo_ticker} cannot be found in {table}')
    return None


def get_tickers(conn, exchange_code: str) -> list:
    """
    Read the ticker table and return ticker_id
    Parameters:
        conn - database connection
        exchange - name of the instrument
    """
    module_logger.debug('Started')

    table = "asx.ticker"

    # create a list of columns from the dataframe
    table_columns = "yahoo_ticker"

    select_stmt = f"SELECT {table_columns} FROM {table}"
    
    try:
        cur = conn.cursor()
        cur.execute(select_stmt)
        tickers = cur.fetchall()
    except (Exception, psycopg2.Error) as error:
        module_logger.error("Error while getting ticker_id from PostgreSQL", error)
    finally:
        if conn:
            cur.close()

    yahoo_tickers = []
    
    if tickers:   
        for row in tickers:
            # print(row)
            yahoo_ticker = row[0]
            yahoo_tickers.append(yahoo_ticker)
    
    return yahoo_tickers

def get_gics_sector_code(conn, sector_name: str) -> str:
    module_logger.debug('Started')
    table = 'asx.gics_sector'
    
    # create a list of columns to get from the table
    table_columns = "code"

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE name = '{sector_name}'"
    
    try:
        cur = conn.cursor()
        cur.execute(select_stmt)
        codes = cur.fetchall()
    except (Exception, psycopg2.Error) as error:
        module_logger.error("Error while getting gics_sector_code from PostgreSQL", error)
    finally:
        if conn:
            cur.close()
    
    if codes:        
        for row in codes:
            code = row[0]
            return code    
    
    module_logger.debug(f'Sector_name {sector_name} cannot be found in {table}')
    return None

def get_gics_industry_group_code(conn, industry_group_name):
    module_logger.debug('Started')
    table = 'asx.gics_industry_group'
    
    # create a list of columns to get from the table
    table_columns = "code"

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE name = '{industry_group_name}'"
    
    try:
        cur = conn.cursor()
        cur.execute(select_stmt)
        codes = cur.fetchall()
    except (Exception, psycopg2.Error) as error:
        module_logger.error("Error while getting gics_industry_group_code from PostgreSQL", error)
    finally:
        if conn:
            cur.close()
    
    if codes:
        for row in codes:
            code = row[0]
            return code
        
    module_logger.debug(f'Industry_group_name {industry_group_name} cannot be found in {table}')
    return None

def get_gics_industry_code(conn, industry_name):
    module_logger.debug('Started')
    table = 'asx.gics_industry'
    
    # create a list of columns to get from the table
    table_columns = "code"

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE name = '{industry_name}'"
    
    try:
        cur = conn.cursor()
        cur.execute(select_stmt)
        codes = cur.fetchall()
    except (Exception, psycopg2.Error) as error:
        module_logger.error("Error while getting gics_industry_code from PostgreSQL", error)
    finally:
        if conn:
            cur.close()

    if codes:        
        for row in codes:
            code = row[0]
            return code

    module_logger.debug(f'Industry_name {industry_name} cannot be found in {table}')
    return None

def get_gics_sub_industry_code(conn, sub_industry_name):
    module_logger.debug('Started')
    table = 'asx.gics_sub_industry'
    
    # create a list of columns to get from the table
    table_columns = "code"

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE name = '{sub_industry_name}'"
    
    try:
        cur = conn.cursor()
        cur.execute(select_stmt)
        codes = cur.fetchall()
    except (Exception, psycopg2.Error) as error:
        module_logger.error("Error while getting gics_sub_industry_code from PostgreSQL", error)
    finally:
        if conn:
            cur.close()

    if codes:
        for row in codes:
            code = row[0]
            return code
    
    module_logger.debug(f'Sub_industry_name {sub_industry_name} cannot be found in {table}')
    return None