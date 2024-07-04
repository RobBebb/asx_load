"""
Date: 7/05/2023
Author: Rob Bebbington

Get static files from polygon and insert them into our database.
"""

from dotenv import load_dotenv

from securities_load.load.postgresql_database_functions import connect

import asx_load_tickers as alt

load_dotenv()

# Open a connection
conn = connect()

alt.load_tickers()

# Close the connection
conn.close()
