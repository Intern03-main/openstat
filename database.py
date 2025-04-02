import mysql.connector
from mysql.connector import Error
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


# Establish database connection
def create_connection():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            autocommit=True
        )
        if connection.is_connected():
            print("[INFO] Connected to MySQL database.")
            return connection
    except Error as e:
        print(f"[ERROR] MySQL connection error: {e}")
        return None


def batch_insert(cursor, query, data, batch_size=1000):
    """Inserts data in smaller batches to avoid connection loss."""
    for i in range(0, len(data), batch_size):
        cursor.executemany(query, data[i:i+batch_size])


# Store or update data in MySQL
def store_data_in_mysql(cleaned_sheets):
    connection = create_connection()
    if not connection:
        return

    cursor = connection.cursor()

    # Define the table name
    table_name = "price_data"

    # Define insert/update query
    insert_update_query = f'''
    INSERT INTO {table_name} (geolocation, commodity, price, commodity_type, year, period)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        price = VALUES(price),
        commodity_type = VALUES(commodity_type),
        year = VALUES(year),
        period = VALUES(period);
    '''

    all_values = []

    for sheet_name, data_long in cleaned_sheets.items():
        data_long.replace({np.nan: None}, inplace=True)
        values = data_long.values.tolist()
        all_values.extend(values)

    try:
        # Insert or update records
        batch_insert(cursor, insert_update_query, all_values)
        connection.commit()
        print(f"✅ Inserted/Updated {cursor.rowcount} rows into MySQL.")

        # Delete records that no longer exist
        if all_values:
            geolocations = [row[0] for row in all_values]  # Extract geolocation column
            commodities = [row[1] for row in all_values]   # Extract commodity column

            delete_query = f"""
            DELETE FROM {table_name}
            WHERE (geolocation, commodity) NOT IN ({','.join(['(%s, %s)'] * len(geolocations))})
            """
            cursor.execute(delete_query, [item for pair in zip(geolocations, commodities) for item in pair])
            connection.commit()
            print("✅ Deleted obsolete records.")

    except Error as e:
        print(f"[ERROR] MySQL operation error: {e}")
    finally:
        cursor.close()
        connection.close()
        print("[INFO] MySQL connection closed.")