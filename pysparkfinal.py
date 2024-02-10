from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
import time

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

mysql_connector_jar_path = "/home/mihir/Documents/sparking/mysql-connector-java-8.2.0.jar"
# Create a Spark session
spark = SparkSession.builder \
    .appName("MySQLInsert") \
    .config("spark.jars", mysql_connector_jar_path) \
    .getOrCreate()

# Load your data into a DataFrame (replace 'your_data.csv' with your actual data source)
startime = time.time()
df = spark.read.csv("test(1M).csv", schema=schema)

# Configure the MySQL connection properties
mysql_properties = {
    "url": "jdbc:mysql://localhost:3306/pyspark_db",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "root",
    "password": "Shadu#2011ram",
    "batchsize": "10000",  # Adjust the batch size as needed
}

# Define the MySQL table where you want to insert the data
mysql_table = "test_table_spark"

# Write the DataFrame to the MySQL table
df.write.jdbc(url=mysql_properties["url"], table=mysql_table, mode="append", properties=mysql_properties)
print("#############Data Inserted")
endtime = time.time()
print(f"TOTAL TIME INSERTION PYSPARK= {endtime-startime}")
# Stop the Spark session
spark.stop()
