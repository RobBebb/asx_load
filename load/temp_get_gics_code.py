from dotenv import load_dotenv

from securities_load.load.postgresql_database_functions import connect

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
        print(f"get_gics_sector_code - Sector code for {sector_name} is {code}.")
        break
    return code    

load_dotenv()

# Open a connection
conn = connect()

sector_name = 'Materials'
code = get_gics_sector_code(conn, sector_name)
print(f"Sector code for {sector_name} is {code}.")

# Close the connection
conn.close()