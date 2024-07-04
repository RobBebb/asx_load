"""
Date: 4/05/2024
Author: Rob Bebbington

Get ASX sysmbols from Markey Index and insert them into our database.
"""

from datetime import datetime, timezone
import pandas as pd
import psycopg2
import psycopg2.extras
from securities_load.load.postgresql_database_functions import connect

def add_tickers(conn, ticker_list):
    """
    Adds a tickers to the ticker table
    Parameters:
        conn - database connection
        ticker_df - dataframe of tickers
    """

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
        print(f"{ticker_list.shape[0]} rows added to {table} table.")
    except psycopg2.Error as error:
        print("Error while inserting tickers to PostgreSQL", error)
    finally:
        if conn:
            cur.close()
            # print("PostgreSQL cursor is closed")

def get_gics_sector_code(conn, sector_name):
    table = 'asx.gics_sector'
    
    # create a list of columns to get from the table
    table_columns = "code"

    cur = conn.cursor()

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE name = '{sector_name}'"
    cur.execute(select_stmt)
    codes = cur.fetchall()
    for row in codes:
        code = row[0]
        # print(f"get_gics_sector_code - Sector code for {sector_name} is {code}.")
        break
    try:
        code
    except NameError:
        code = None
        print(f'Sector_name {sector_name} cannot be found in {table}')
    return code

def get_gics_industry_group_code(conn, industry_group_name):
    table = 'asx.gics_industry_group'
    
    # create a list of columns to get from the table
    table_columns = "code"

    cur = conn.cursor()

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE name = '{industry_group_name}'"
    cur.execute(select_stmt)
    codes = cur.fetchall()
    for row in codes:
        code = row[0]
        # print(f"get_gics_sector_code - Sector code for {sector_name} is {code}.")
        break
    try:
        code
    except NameError:
        code = None
        print(f'Industry_group_name {industry_group_name} cannot be found in {table}')
    return code

def get_gics_industry_code(conn, industry_name):
    table = 'asx.gics_industry'
    
    # create a list of columns to get from the table
    table_columns = "code"

    cur = conn.cursor()

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE name = '{industry_name}'"
    cur.execute(select_stmt)
    codes = cur.fetchall()
    for row in codes:
        code = row[0]
        # print(f"get_gics_sector_code - Sector code for {sector_name} is {code}.")
        break
    try:
        code
    except NameError:
        code = None
        print(f'Industry_name {industry_name} cannot be found in {table}')
    return code

def get_gics_sub_industry_code(conn, sub_industry_name):
    table = 'asx.gics_sub_industry'
    
    # create a list of columns to get from the table
    table_columns = "code"

    cur = conn.cursor()

    select_stmt = f"SELECT {table_columns} FROM {table} WHERE name = '{sub_industry_name}'"
    cur.execute(select_stmt)
    codes = cur.fetchall()
    for row in codes:
        code = row[0]
        # print(f"get_gics_sector_code - Sector code for {sector_name} is {code}.")
        break
    try:
        code
    except NameError:
        code = None
        print(f'Sub_industry_name {sub_industry_name} cannot be found in {table}')
    return code