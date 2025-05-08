# In Python 
from pyspark.sql import SparkSession
from os.path import abspath

# Define warehouse location
warehouse_location = abspath('spark-warehouse_python')
 # Main program
spark = SparkSession.builder \
    .appName("LocalSparkApp") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# // In Scala/Python
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

#  // In Scala/Python
spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")


 # In Python
 # Path to our US flight delays CSV file 
csv_file = "/data/departuredelays.csv"
 # Schema as defined in the preceding example
schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema=schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

print("Table created successfully", flights_df)

# // In Scala/Python
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("us_delay_flights_tbl")

print("Databases: ", spark.catalog.listDatabases())
print("Tables: ", spark.catalog.listTables())