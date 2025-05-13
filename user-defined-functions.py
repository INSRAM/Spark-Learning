# In Python
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSqlUDF").getOrCreate()

 # In Python
from pyspark.sql.types import LongType

# Create cubed function
def cubed(s):
 return s * s * s

 # Register UDF
spark.udf.register("cubed", cubed, LongType())

# Generate temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")

# Query to Cubed 
result = spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()