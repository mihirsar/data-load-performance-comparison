from memory_profiler import profile
import pandas as pd
import mysql.connector
import time 

@profile
def process_csv_to_mysql():
    try:
        # Read the CSV file
        df = pd.read_csv("test(1M).csv")

        # Establishing a connection to the MySQL server
        cnx = mysql.connector.connect(
            user='root',
            password='Shadu#2011ram',
            host='127.0.0.1',
            database='pandasparktest'
        )
        print("Connection done")
        print("Processing........")
        start_time = time.time()
        cursor = cnx.cursor()

        table_name = 'test_table_panda'
        create_table_query = f"CREATE TABLE {table_name} ("

        for col in df.columns:
            if df[col].dtype == 'int64':
                create_table_query += f"{col} INT, "
            elif df[col].dtype == 'float64':
                create_table_query += f"{col} FLOAT, "
            else:
                create_table_query += f"{col} VARCHAR(255), "

        create_table_query = create_table_query[:-2] + ")"  # Remove the last comma and space

        cursor.execute(create_table_query)

        for i, row in df.iterrows():
            insert_query = f"INSERT INTO {table_name} VALUES ("

            for val in row:
                if pd.isnull(val):
                    insert_query += "NULL, "
                elif isinstance(val, (int, float)):
                    insert_query += f"{val}, "
                else:
                    insert_query += f"'{val}', "

            insert_query = insert_query[:-2] + ")"  # Remove the last comma and space

            cursor.execute(insert_query)

        # Commit changes and close connection
        cnx.commit()
        cursor.close()
        end_time = time.time()

        cnx.close()
        print("Connection closed.")
        print(f"Time required PANDAS: {end_time-start_time}")
    except mysql.connector.Error as err:
        print(f"Error: {err}")

if __name__ == "__main__":
    process_csv_to_mysql()
