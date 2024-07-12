"""
Date: 4/05/2024
Author: Rob Bebbington

Get ASX sysmbols from Markey Index and insert them into our database.
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
        ticker_df - dataframe of tickers
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
            # print("PostgreSQL cursor is closed")

def add_or_update_tickers(conn, ticker_list: pd.DataFrame) -> None:
    """
    Adds a tickers to the ticker table
    Parameters:
        conn - database connection
        ticker_df - dataframe of tickers
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
    insert_stmt = f"INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT (ticker) DO UPDATE SET ({columns}, last_updated_date) = ({conflict_columns}, CURRENT_TIMESTAMP)"
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
            # print("PostgreSQL cursor is closed")

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

    cur = conn.cursor()

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE ticker = '{ticker}' AND exchange_code = '{exchange_code}'"
    cur.execute(select_stmt)
    tickers = cur.fetchall()
    for row in tickers:
        ticker_id = row[0]
        module_logger.debug(f"ticker_id for ticker {ticker} is {ticker_id}.")
        return ticker_id
    
    module_logger.debug(f'Ticker {ticker} cannot be found in {table}')
    return None

def get_gics_sector_code(conn, sector_name: str) -> str:
    module_logger.debug('Started')
    table = 'asx.gics_sector'
    
    # create a list of columns to get from the table
    table_columns = "code"

    cur = conn.cursor()

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE name = '{sector_name}'"
    
    cur.execute(select_stmt)
    codes = cur.fetchall()
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

    cur = conn.cursor()

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE name = '{industry_group_name}'"
    cur.execute(select_stmt)
    codes = cur.fetchall()
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

    cur = conn.cursor()

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE name = '{industry_name}'"
    cur.execute(select_stmt)
    codes = cur.fetchall()
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

    cur = conn.cursor()

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE name = '{sub_industry_name}'"
    cur.execute(select_stmt)
    codes = cur.fetchall()
    for row in codes:
        code = row[0]
        return code
    
    module_logger.debug(f'Sub_industry_name {sub_industry_name} cannot be found in {table}')
    return None