from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
import mysql.connector
import time
import pandas as pd
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

# Define your schema
schema = StructType([
    StructField("Id", IntegerType()),
    StructField("Name", StringType()),
    StructField("Path", StringType()),
    StructField("Size", IntegerType()),
    StructField("DateCreated", TimestampType()),
    StructField("DateAccessed", TimestampType()),
    StructField("DateModified", TimestampType()),
    StructField("Extension", StringType()),
    StructField("TypeHigh", StringType()),
    StructField("TypeLow", StringType()),
    StructField("Deleted", StringType()),
    StructField("DataAge", IntegerType()),
    StructField("AccessRecency", IntegerType()),
    StructField("ModifiedRecency", IntegerType()),
    StructField("Depth", IntegerType()),
    StructField("Root", StringType()),
    StructField("DataAsset", StringType()),
    StructField("RiskScore", IntegerType()),
    StructField("SensitiveData", StringType()),
    StructField("SensitivityScore", IntegerType()),
    StructField("Department", StringType()),
    StructField("Country", StringType()),
    StructField("AutoTags", StringType()),
    StructField("CountryCode", StringType()),
    StructField("Level", IntegerType()),
    StructField("Comment", StringType()),
    StructField("Source", StringType()),
    StructField("CorrectedScore", StringType()),
    StructField("md5", StringType()),
    StructField("ScanId", IntegerType()),
    StructField("ScanLevel", IntegerType()),
    StructField("ScanStatus", StringType()),
    StructField("MetaScanId", IntegerType())
])

def process_csv_to_mysql():
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("CSV_to_MySQL").getOrCreate()
        print("############################## Session/app created ##############################")

        # Read the CSV file into a PySpark DataFrame with inferred schema
        df = spark.read.csv("test(1M).csv", header=True, inferSchema=True)
        print("############### CSV read ##################")

        # Establish connection to MySQL server using Python MySQL connector
        cnx = mysql.connector.connect(
            user='root',
            password='Shadu#2011ram',
            host='localhost',
            database='pyspark_db'
        )
        print("Connection established")

        start_time = time.time()
        table_name = "pyspark_test"

        # Define MySQL table name
        create_table_query = f"CREATE TABLE {table_name} ("

        # Iterate through DataFrame columns and add corresponding SQL data type
        for field in schema.fields:
            column_name = field.name
            data_type = field.dataType

            if isinstance(data_type, IntegerType):
                create_table_query += f"{column_name} INT, "
            elif isinstance(data_type, TimestampType):
                create_table_query += f"{column_name} DATETIME, "
            else:
                create_table_query += f"{column_name} VARCHAR(255), "

        # Complete the query and remove the last comma and space
        create_table_query = create_table_query[:-2] + ")"
        print(create_table_query)

        with cnx.cursor() as cursor:
            cursor.execute(create_table_query)

        print("\nCreation executed....")

        # Function to perform parallel inserts
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def insert_to_mysql(iterator):
            cnx = mysql.connector.connect(
                user='root',
                password='Shadu#2011ram',
                host='localhost',
                database='pyspark_db'
            )

            with cnx.cursor() as cursor:
                for batch in iterator:
                    # Your logic to process each partition and return a DataFrame
                    processed_data = process_batch(batch)

                    # Convert the processed_data to a Pandas DataFrame with the defined schema
                    processed_df = pd.DataFrame(processed_data, columns=schema.names)

                    # Perform the insertion into MySQL using cursor.execute()

                    # Assuming 'pyspark_test' is your table name, adjust this query accordingly
                    insert_query = f"INSERT INTO {table_name} VALUES "

                    for index, row in processed_df.iterrows():
                        values = ",".join([f"'{val}'" if pd.notna(val) else "NULL" for val in row])
                        insert_query += f"({values}),"

                    insert_query = insert_query.rstrip(",")  # Remove the trailing comma
                    cursor.execute(insert_query)

            cnx.commit()
            cnx.close()

        # Add your logic to process batch and return a DataFrame
        def process_batch(batch):
            # Example: Double the values in each column
            return batch.apply(lambda col: col * 2)

        # Update the grouping column(s) in the groupBy clause
        # Replace "Country" with the actual column name you want to use for grouping
        df.groupBy("Country").apply(insert_to_mysql)

        print("Connection closed")

        end_time = time.time()
        print(f"Total time pyspark creation: {end_time - start_time} seconds")

        # Stop Spark session
        spark.stop()
    except mysql.connector.Error as err:
        print(f"MySQL Connection Error: {err}")

if __name__ == "__main__":
    process_csv_to_mysql()
