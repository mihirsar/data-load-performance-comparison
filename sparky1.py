from pyspark.sql import SparkSession
import mysql.connector
import time

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
            password='YOURPASSWORD',
            host='localhost',
            database='DBNAME'
        )
        print("Connection established")
        
        start_time = time.time()

        # Define MySQL table name
        table_name = "test_table_pyspark"

        # Create the MySQL table if it doesn't exist
        create_table_query = f"CREATE TABLE {table_name} ("
        for field in df.schema.fields:
            create_table_query += f"{field.name} {field.dataType.typeName()}, "
        create_table_query = create_table_query.rstrip(', ') + ")"
        
        with cnx.cursor() as cursor:
            cursor.execute(create_table_query)

        # Insert data into MySQL using PySpark DataFrame
        df.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/pyspark_pandas_test") \
            .option("dbtable", table_name).option("user", "root") \
            .option("password", "YOURPASSWORD").mode("overwrite").save()
        
        print("Data inserted into MySQL")

        # Close connection
        cnx.close()
        print("Connection closed")
        
        end_time = time.time()
        print(f"Total time pyspark: {end_time - start_time} seconds")

        # Stop Spark session
        spark.stop()
    except Exception as e:
        print(f"ERROR !!!: {e}")

if __name__ == "__main__":
    process_csv_to_mysql()
