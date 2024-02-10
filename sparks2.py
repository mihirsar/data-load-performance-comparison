from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql import functions as F
import mysql.connector
import time

def process_csv_to_mysql():
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("CSV_to_MySQL").getOrCreate()

        # Read the CSV file into a PySpark DataFrame
        df = spark.read.csv("test(1M).csv", header=True, inferSchema=True)

        # Establishing a connection to the MySQL server
        cnx = mysql.connector.connect(
            user='root',
            password='Shadu#2011ram',
            host='127.0.0.1',
            database='pyspark_db'
        )
        print("Connection done")
        print("Processing........")
        start_time = time.time()

        # Create a MySQL table based on DataFrame schema
        table_name = 'test_table_spark'
        create_table_query = f"CREATE TABLE {table_name} ("

        for field in df.schema:
            col_name = field.name
            data_type = field.dataType

            if isinstance(data_type, IntegerType):
                create_table_query += f"{col_name} INT, "
            elif isinstance(data_type, FloatType):
                create_table_query += f"{col_name} FLOAT, "
            else:
                create_table_query += f"{col_name} VARCHAR(255), "

        create_table_query = create_table_query[:-2] + ")"  # Remove the last comma and space

        with cnx.cursor() as cursor:
            cursor.execute(create_table_query)

        # Define a UDF to convert PySpark DataFrame rows to MySQL insert statements
        @F.udf(StringType())
        def row_to_mysql_insert(row):
            values = [f"'{val}'" if isinstance(val, str) else str(val) for val in row]
            return f"({', '.join(values)})"

        # Add a new column with the MySQL insert statement
        df_with_insert = df.withColumn("mysql_insert", row_to_mysql_insert(F.struct(*df.columns)))

        # Insert data into MySQL using PySpark DataFrame
        df_with_insert.select("mysql_insert").write.format("jdbc").option("url", "jdbc:mysql://127.0.0.1/pyspark_db").option("dbtable", table_name).option("user", "root").option("password", "Shadu#2011ram").mode("append").save()

        end_time = time.time()

        # Close connection
        cnx.close()
        print("Connection closed.")
        print(f"Time required PySpark: {end_time - start_time}")
    except mysql.connector.Error as err:
        print(f"Error: {err}")

if __name__ == "__main__":
    process_csv_to_mysql()
