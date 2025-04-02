import mysql.connector
import os
import numpy as np
from dotenv import load_dotenv

# Load .env files
load_dotenv()


def store_data_in_mysql(cleaned_sheets):
    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
    )
    cursor = connection.cursor()

    insert_query = """
    INSERT INTO price_data (geolocation, commodity, price, commodity_type, year, period)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        price = VALUES(price),
        commodity_type = VALUES(commodity_type);
    """

    for sheet_name, data_long in cleaned_sheets.items():
        data_long.replace({np.nan: None}, inplace=True)
        values = data_long.values.tolist()
        cursor.executemany(insert_query, values)

    connection.commit()
    print(f"✅ Inserted/Updated {cursor.rowcount} rows into MySQL.")
    cursor.close()
    connection.close()
