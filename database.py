import mysql.connector
import os
import numpy as np
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database credentials
DB_HOST = os.getenv("MYSQL_HOST")
DB_USER = os.getenv("MYSQL_USER")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD")
DB_NAME = os.getenv("MYSQL_DATABASE")


def store_data_in_mysql(cleaned_sheets):
    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
    )
    cursor = connection.cursor()

    # 1️⃣ Collect all new data keys for reference (geolocation, commodity, year, period)
    new_data_keys = set()

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

        # Track unique keys in new data
        for row in values:
            new_data_keys.add((row[0], row[1], row[4], row[5]))  # (geolocation, commodity, year, period)

        cursor.executemany(insert_query, values)

    # 2️⃣ Delete records that are no longer in cleaned_sheets
    placeholders = ", ".join(["(%s, %s, %s, %s)"] * len(new_data_keys))
    delete_query = f"""
    DELETE FROM price_data 
    WHERE (geolocation, commodity, year, period) NOT IN ({placeholders})
    """

    if new_data_keys:  # Ensure there is data before deleting
        cursor.execute(delete_query, tuple(item for key in new_data_keys for item in key))

    connection.commit()
    print(f"✅ Inserted/Updated {cursor.rowcount} rows. Deleted outdated records.")

    cursor.close()
    connection.close()