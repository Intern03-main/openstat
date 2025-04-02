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

    # Drop table if it exists and create a new one
    cursor.execute("DROP TABLE IF EXISTS price_data;")

    create_table_query = """
    CREATE TABLE price_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        geolocation VARCHAR(50),
        commodity VARCHAR(70),
        price VARCHAR(50),  -- Keep as VARCHAR since "null" is stored as text
        year INT,
        period VARCHAR(10),
        commodity_type VARCHAR(30)
    );
    """
    cursor.execute(create_table_query)
    print("✅ Table `price_data` has been recreated.")

    insert_query = """
    INSERT INTO price_data (geolocation, commodity, price, commodity_type, year, period)
    VALUES (%s, %s, %s, %s, %s, %s);
    """

    for sheet_name, data_long in cleaned_sheets.items():
        data_long.replace({np.nan: None}, inplace=True)
        values = data_long.values.tolist()
        cursor.executemany(insert_query, values)

    connection.commit()
    print(f"✅ Inserted {cursor.rowcount} rows into MySQL.")
    cursor.close()
    connection.close()
